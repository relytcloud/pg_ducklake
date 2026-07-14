#include "pgddb/worker/scan_producer.hpp"

#include <algorithm>
#include <cstring>
#include <exception>
#include <mutex>

#include "pgddb/pgddb_process_lock.hpp"
#include "pgddb/pgddb_types.hpp"
#include "pgddb/scan/postgres_table_reader.hpp"
#include "pgddb/worker/transport/page_pool.hpp"
#include "pgddb/worker/transport/scan_ring.hpp"
#include "pgddb/worker/transport/session_channel.hpp"
#include "pgddb/worker/transport/session_protocol.hpp"

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/value.hpp>

extern "C" {
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
}

namespace pgddb {

namespace {
constexpr int SCAN_SLOT_BATCH = 32;
} // namespace

BackendScanState::BackendScanState()
    : reader(), slots(), chunk(), err_msg(), arrow_codes(), arrow_prec(), arrow_scale() {
}

BackendScanState::~BackendScanState() {
	if (reader) {
		// abort-path cleanup must not throw out of a destructor
		try {
			reader->Cleanup();
		} catch (...) {
		}
	}
}

void
BackendScanState::ReleaseReader() {
	slots.clear();
	carry = nullptr;
	has_carry = false;
	if (!reader)
		return;
	try {
		reader->Cleanup(); // exits parallel mode, drops the scan's executor state
	} catch (...) {
	}
	reader.reset();
}

void
InitBackendScan(BackendScanState &st, const std::string &sql, bool count_only) {
	st.is_count = count_only;
	st.reader = duckdb::make_uniq<PostgresTableReader>();
	// count_only makes the reader handle the COUNT(*) aggregate plan (GetNextCount).
	st.reader->Init(sql.c_str(), count_only); // guarded: a PG error becomes a C++ exception

	if (count_only) {
		// The worker reads the count from cell (0,0) of a single BIGINT chunk.
		duckdb::vector<duckdb::LogicalType> types {duckdb::LogicalType::BIGINT};
		st.ncols = 1;
		st.chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types);
		return;
	}

	// Data scans use the Arrow transport exclusively. Every column must be Arrow-encodable
	// (and fit the descriptor buffer); an unsupported column fails the scan loudly rather
	// than silently falling back, so the gap is visible and gets Arrow support next.
	if (!PagePool().Available())
		throw std::runtime_error("duckdb worker requires an Arrow page pool (arrow_pool_pages > 0)");

	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	for (int i = 0; i < SCAN_SLOT_BATCH; i++) {
		st.slots.push_back(st.reader->InitTupleSlot());
	}
	TupleDesc tupdesc = st.slots[0]->tts_tupleDescriptor;
	st.ncols = tupdesc->natts;
	if (st.ncols > 64)
		throw std::runtime_error("duckdb worker: too many columns for the Arrow scan transport (max 64)");
	for (int i = 0; i < st.ncols; i++) {
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		duckdb::LogicalType type = ConvertPostgresToDuckColumnType(attr);
		ArrowColCode code;
		uint8_t p, s;
		if (!ArrowColumnSpec(attr->atttypid, type, code, p, s))
			throw std::runtime_error("duckdb worker: column \"" + std::string(NameStr(attr->attname)) +
			                         "\" (type oid " + std::to_string((unsigned)attr->atttypid) +
			                         ") is not supported by the Arrow scan transport");
		st.arrow_codes.push_back(code);
		st.arrow_prec.push_back(p);
		st.arrow_scale.push_back(s);
	}
	st.carry = st.reader->InitTupleSlot();
}

ssize_t
ProduceArrowChunk(BackendScanState &st, char *page, std::size_t capacity, int page_index, char *desc_out,
                  std::size_t desc_cap) {
	ArrowBatchBuilder b(page, capacity, st.arrow_codes, st.arrow_prec, st.arrow_scale, STANDARD_VECTOR_SIZE);
	if (!b.LayoutOk())
		throw std::runtime_error("duckdb worker: scan column layout too large for an Arrow page; "
		                         "raise the arrow page size");

	std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
	if (st.has_carry) {
		slot_getallattrs(st.carry);
		if (!b.AppendSlot(st.carry))
			throw std::runtime_error("row too large for arrow page; raise the arrow page size");
		st.has_carry = false;
	}
	while (b.Rows() < STANDARD_VECTOR_SIZE && !st.done) {
		int valid = st.reader->GetNextInProcessTuples(st.slots.data(), 1);
		if (valid == 0) {
			st.done = true;
			break;
		}
		slot_getallattrs(st.slots[0]);
		if (!b.AppendSlot(st.slots[0])) {
			if (b.Rows() == 0)
				throw std::runtime_error("row too large for arrow page; raise the arrow page size");
			ExecCopySlot(st.carry, st.slots[0]); // defer to the next page
			st.has_carry = true;
			break;
		}
	}
	if (b.Rows() == 0)
		return 0;
	return (ssize_t)b.Finalize(page_index, desc_out, desc_cap);
}

bool
ProduceCountChunk(BackendScanState &st) {
	if (st.done) {
		return false;
	}
	uint64_t total = 0, partial = 0;
	{
		std::lock_guard<std::recursive_mutex> lock(GlobalProcessLock::GetLock());
		while (st.reader->GetNextCount(&partial)) {
			total += partial;
		}
	}
	st.chunk.Reset();
	st.chunk.SetValue(0, 0, duckdb::Value::BIGINT((int64_t)total));
	st.chunk.SetCardinality(1);
	st.done = true;
	return true;
}

/* Code that runs only in the dedicated scan-producer bgworker processes. */
namespace producer {

namespace {

/* Scan-worker side: produce all chunks for one task and push them into the scan's
 * ready-ring. Data scans build an Arrow batch into a pool page (descriptor at the page
 * front, data after the reserved area); COUNT(*) serializes its single BIGINT chunk into
 * a page. Stops early (returns) if the scan was torn down by the consumer. */
void
PoolProduceScan(BackendScanState &st, const ScanRange &range, int *held_page) {
	const std::size_t reserve = ScanRing::DescReserve();
	while (!st.done) {
		PagePool pool;
		PageSlot *slot = pool.Acquire();
		if (slot == nullptr) { // pool momentarily empty -> wait for the consumer to free pages
			if (!ScanRing().Alive(range.scan_id, range.generation))
				return;
			CHECK_FOR_INTERRUPTS();
			pg_usleep(500);
			continue;
		}
		int page_idx = slot->index;
		*held_page = page_idx; // we own this page until it is pushed or released
		char *page = slot->data;
		std::size_t cap = (std::size_t)pool.PageSize();

		uint32_t desc_len = 0, byte_len = 0;
		if (st.is_count) {
			if (!ProduceCountChunk(st)) { // already produced
				pool.Release(slot);
				*held_page = -1;
				break;
			}
			duckdb::MemoryStream stream(reinterpret_cast<duckdb::data_ptr_t>(page), cap);
			SerializeDataChunk(st.chunk, stream);
			byte_len = (uint32_t)stream.GetPosition();
		} else {
			char desc[sizeof(ArrowBatchHeader) + 64 * sizeof(ArrowColDesc)];
			ssize_t n = ProduceArrowChunk(st, page + reserve, cap - reserve, page_idx, desc, sizeof(desc));
			if (n == 0) { // end of scan
				pool.Release(slot);
				*held_page = -1;
				break;
			}
			std::memcpy(page, desc, (std::size_t)n); // descriptor in the reserved page front
			desc_len = (uint32_t)n;
		}

		if (!ScanRing().Push(range.scan_id, range.generation, (uint32_t)page_idx, desc_len, byte_len)) {
			pool.Release(slot); // scan torn down
			*held_page = -1;
			return;
		}
		*held_page = -1; // ring owns it now
	}
}

/* Splice a CTID block-range predicate into the base scan SQL at `where_off` (right
 * after FROM <relation>), AND-ing with any existing WHERE. Range is blocks [lo, hi). */
std::string
BuildRangedScanSql(const std::string &base, uint32_t where_off, uint32_t lo, uint32_t hi) {
	std::string pred = "ctid >= '(" + std::to_string(lo) + ",1)' AND ctid < '(" + std::to_string(hi) + ",1)'";
	std::string head = base.substr(0, where_off);
	std::string tail = base.substr(where_off);
	const std::string kWhere = " WHERE ";
	if (tail.compare(0, kWhere.size(), kWhere) == 0)
		return head + " WHERE " + pred + " AND " + tail.substr(kWhere.size());
	return head + " WHERE " + pred + tail;
}

} // namespace

/* The scan worker's currently-claimed range (main-thread-only plain flag), so the
 * shmem-exit callback can error it out on FATAL (which skips PG_CATCH). */
ScanRange g_active_range;
bool g_have_active_range = false;
int g_held_page = -1;

/* The body runs in a PG_TRY so a raw PG error (longjmp) is turned into a scan error
 * for the consumer instead of killing the producer and hanging the consumer; an inner
 * C++ try/catch handles the guarded PostgresTableReader exceptions. Either way the
 * scan gets TaskDone or SetError. */
void
ProcessScanRange(const ScanRange &range) {
	std::vector<char> sqlbuf(16384), snapbuf(8192);
	std::size_t sqllen = 0, snaplen = 0;
	bool count_only = false;
	uint32_t where_off = 0;
	if (!ScanRing().GetInputs(range.scan_id, range.generation, sqlbuf.data(), sqlbuf.size(), &sqllen, snapbuf.data(),
	                          snapbuf.size(), &snaplen, &count_only, &where_off))
		return; // scan torn down before we started
	std::string sql(sqlbuf.data(), sqllen);
	if (range.hi_blk > 0) // ranged task: scan only blocks [lo, hi)
		sql = BuildRangedScanSql(sql, where_off, range.lo_blk, range.hi_blk);

	g_active_range = range;
	g_have_active_range = true;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	PushActiveSnapshot(RestoreSnapshot(snapbuf.data()));

	volatile bool ok = true;
	std::string errmsg;
	g_held_page = -1; // page the producer owns; freed here (or by the shmem-exit hook on FATAL)
	PG_TRY();
	{
		// BackendScanState is a C++ local: its destructor runs on a normal return or a
		// C++ exception, and is skipped on a PG longjmp (where AbortCurrentTransaction
		// reclaims its PG resources instead).
		try {
			BackendScanState st;
			InitBackendScan(st, sql, count_only);
			PoolProduceScan(st, range, &g_held_page);
		} catch (const std::exception &e) {
			if (g_held_page >= 0) {
				PagePool().Release(PagePool().Slot(g_held_page));
				g_held_page = -1;
			}
			ok = false;
			errmsg = e.what();
		}
	}
	PG_CATCH();
	{
		if (g_held_page >= 0) {
			PagePool().Release(PagePool().Slot(g_held_page));
			g_held_page = -1;
		}
		MemoryContext ctx = MemoryContextSwitchTo(TopMemoryContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();
		MemoryContextSwitchTo(ctx);
		ok = false;
		errmsg = edata->message ? edata->message : "scan worker error";
		FreeErrorData(edata);
	}
	PG_END_TRY();

	if (ok)
		ScanRing().TaskDone(range.scan_id, range.generation);
	else
		ScanRing().SetError(range.scan_id, range.generation, errmsg.c_str());
	g_have_active_range = false;

	PopActiveSnapshot();
	PopActiveSnapshot();
	if (ok)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();
}

} // namespace producer

/* Code that runs only inside the duckdb worker process. */
namespace worker {

namespace {

/* Worker side: a heap scan that pulls DataChunks from the requesting backend over
 * the session channel, so the worker's execution threads never call PostgreSQL.
 * Each fetch is a round-trip: send ScanFetch (scan_id, + inner SQL on the first
 * call) on the result queue, read one ScanChunk/ScanDone/ScanError on the control
 * queue. */
class InversionScanStream : public RemoteScanStream {
public:
	InversionScanStream(const InversionScanStream &) = delete;
	InversionScanStream &operator=(const InversionScanStream &) = delete;
	InversionScanStream(SessionChannel *ch, std::string sql, uint32_t scan_id, bool count_only)
	    : ch_(ch), sql_(std::move(sql)), scan_id_(scan_id), count_only_(count_only) {
	}

	~InversionScanStream() override {
		// Early teardown (LIMIT satisfied, error, cancel) can leave fetches outstanding;
		// drain them so their Arrow pages are freed, then drop the demux lane (freeing
		// pages of any frame that was routed there but never consumed).
		if (!done_) {
			try {
				DrainOutstanding();
			} catch (...) {
			}
		}
		ch_->CloseScanLane(scan_id_, [](FrameTag tag, const std::string &payload) {
			if (tag == FrameTag::ScanChunkArrow && payload.size() >= sizeof(ArrowBatchHeader)) {
				auto *hdr = reinterpret_cast<const ArrowBatchHeader *>(payload.data());
				PagePool().Release(PagePool().Slot((int)hdr->page_index));
			}
		});
	}

	duckdb::unique_ptr<duckdb::DataChunk>
	Next(duckdb::ClientContext &context, const duckdb::vector<duckdb::LogicalType> &output_types) override {
		// Read-ahead window: keep up to WINDOW ScanFetch requests outstanding so the
		// backend produces chunks ahead while the worker computes -- the scan leaves
		// the per-chunk critical path. Replies arrive on this scan's demux lane (they
		// carry a scan_id prefix), so several scans and metadata RPCs share the channel.
		std::lock_guard<std::mutex> lock(mutex_);
		if (done_)
			return nullptr;

		while (outstanding_ < WINDOW) {
			std::string req(reinterpret_cast<const char *>(&scan_id_), sizeof(scan_id_));
			if (!started_) {
				req += static_cast<char>(count_only_ ? 1 : 0); // first fetch carries the flag + SQL
				req += sql_;
				started_ = true;
			}
			ch_->SerializedSendResult(FrameTag::ScanFetch, req.data(), req.size());
			outstanding_++;
		}

		FrameTag tag = FrameTag::ScanError;
		std::string payload;
		if (ch_->RecvScanReply(scan_id_, &tag, &payload) != FrameResult::Ok) {
			done_ = true;
			throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, "duckdb worker: scan channel detached");
		}
		outstanding_--;
		if (tag == FrameTag::ScanChunkArrow) {
			// Zero-copy: import the Arrow batch in the global pool page; the page is
			// returned to the pool when DuckDB drops the chunk (release callback). On an
			// import failure the page must be freed here since no callback was registered.
			auto *hdr = reinterpret_cast<const ArrowBatchHeader *>(payload.data());
			int page_index = (int)hdr->page_index;
			char *page = PagePool().Slot(page_index)->data;
			auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
			try {
				ImportArrowBatch(context, page, payload.data(), payload.size(), output_types, *chunk,
				                 [page_index]() { PagePool().Release(PagePool().Slot(page_index)); });
			} catch (...) {
				PagePool().Release(PagePool().Slot(page_index));
				throw;
			}
			return chunk;
		}
		if (tag == FrameTag::ScanChunk) {
			auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
			DeserializeDataChunk(payload.data(), payload.size(), *chunk);
			return chunk;
		}
		if (tag == FrameTag::ScanDone) {
			// Data is FIFO-before the first ScanDone, so the remaining outstanding
			// replies are all terminal; drain them to leave the lane clean.
			done_ = true;
			DrainOutstanding();
			return nullptr;
		}
		done_ = true; /* ScanError */
		DrainOutstanding();
		throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, payload);
	}

private:
	void
	DrainOutstanding() {
		while (outstanding_ > 0) {
			FrameTag tag;
			std::string payload;
			if (ch_->RecvScanReply(scan_id_, &tag, &payload) != FrameResult::Ok)
				break;
			// A drained Arrow response owns a pool page that nobody will import; free it.
			if (tag == FrameTag::ScanChunkArrow && payload.size() >= sizeof(ArrowBatchHeader)) {
				auto *hdr = reinterpret_cast<const ArrowBatchHeader *>(payload.data());
				PagePool().Release(PagePool().Slot((int)hdr->page_index));
			}
			outstanding_--;
		}
	}

	static constexpr int WINDOW = 8;
	SessionChannel *ch_;
	std::string sql_;
	uint32_t scan_id_;
	bool count_only_;
	bool started_ = false;
	bool done_ = false;
	int outstanding_ = 0;
	std::mutex mutex_ = {};
};

/* Worker side: a heap scan fed by the shared scan worker pool. Scan workers scan
 * block ranges under the backend's shipped snapshot and push Arrow/serialized pages
 * into this scan's ready-ring; Next drains it. No per-chunk round-trip to the backend,
 * and multiple DuckDB threads drain concurrently (the ring is MPSC-safe). */
class PoolScanStream : public RemoteScanStream {
public:
	PoolScanStream(SessionChannel *ch, uint32_t scan_id) : ch_(ch), scan_id_(scan_id) {
	}
	PoolScanStream(const PoolScanStream &) = delete;
	PoolScanStream &operator=(const PoolScanStream &) = delete;

	~PoolScanStream() override {
		CloseRing();
	}

	duckdb::unique_ptr<duckdb::DataChunk>
	Next(duckdb::ClientContext &context, const duckdb::vector<duckdb::LogicalType> &output_types) override {
		if (done_)
			return nullptr;
		for (;;) {
			uint32_t page_index = 0, desc_len = 0, byte_len = 0;
			char errbuf[1024];
			int r = ScanRing().TryNext(scan_id_, &page_index, &desc_len, &byte_len, errbuf, sizeof(errbuf));
			if (r == 1) {
				char *page = PagePool().Slot((int)page_index)->data;
				auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
				try {
					if (desc_len > 0) { // Arrow: descriptor at page front, data after the reserve
						ImportArrowBatch(context, page + ScanRing::DescReserve(), page, desc_len, output_types, *chunk,
						                 [page_index]() { PagePool().Release(PagePool().Slot((int)page_index)); });
					} else { // serialized: copied out, free the page immediately
						DeserializeDataChunk(page, byte_len, *chunk);
						PagePool().Release(PagePool().Slot((int)page_index));
					}
				} catch (...) {
					// import/deserialize failed before taking ownership
					PagePool().Release(PagePool().Slot((int)page_index));
					throw;
				}
				return chunk;
			}
			if (r == 0) {
				done_ = true;
				return nullptr;
			}
			if (r == -1) {
				done_ = true;
				throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, std::string(errbuf));
			}
			/* Nothing ready: bail on backend cancel / worker shutdown instead of
			 * polling a ring nobody will fill. */
			if (ch_->IsCancelRequested() || IsWorkerDraining()) {
				CloseRing();
				done_ = true;
				throw duckdb::Exception(duckdb::ExceptionType::EXECUTOR, "query cancelled");
			}
			pg_usleep(300); /* nothing ready yet; producers are filling the ring */
		}
	}

private:
	void
	CloseRing() {
		ScanRing().Close(scan_id_, [](int idx) { PagePool().Release(PagePool().Slot(idx)); });
	}

	SessionChannel *ch_;
	uint32_t scan_id_;
	bool done_ = false;
};

} // namespace

duckdb::unique_ptr<RemoteScanStream>
OpenInversionScanStream(SessionChannel *ch, const std::string &sql, bool count_only) {
	static std::atomic<uint32_t> next_scan_id {1};
	return duckdb::make_uniq<InversionScanStream>(ch, sql, next_scan_id.fetch_add(1), count_only);
}

duckdb::unique_ptr<RemoteScanStream>
OpenPoolScanStream(SessionChannel *ch, uint32_t db_oid, const std::string &scan_sql, bool count_only,
                   const RemoteScanInfo &info, const std::string &snapshot_bytes, int producers) {
	/* Degree: split a rangeable scan into block ranges, capped by the producers GUC
	 * and the relation size (at least MIN_BLOCKS_PER_TASK blocks per task). */
	constexpr uint32_t MIN_BLOCKS_PER_TASK = 64;
	uint32_t degree = 1;
	if (info.rangeable && info.nblocks >= 2 * MIN_BLOCKS_PER_TASK) {
		uint32_t want = (uint32_t)std::max(1, producers);
		degree = std::min(want, info.nblocks / MIN_BLOCKS_PER_TASK);
		if (degree < 1)
			degree = 1;
	}

	ScanHandle h = ScanRing().Open(db_oid, scan_sql.c_str(), scan_sql.size(), snapshot_bytes.data(),
	                               snapshot_bytes.size(), count_only, info.where_off, /*ntasks=*/degree);
	if (h.scan_id == 0)
		return nullptr;

	bool ok = true;
	if (degree == 1) {
		ok = ScanQueue().Enqueue(db_oid, h.scan_id, h.generation, 0, 0); /* (0,0): no CTID injection */
	} else {
		for (uint32_t i = 0; i < degree && ok; i++) {
			uint32_t lo = (uint32_t)((uint64_t)info.nblocks * i / degree);
			uint32_t hi = (uint32_t)((uint64_t)info.nblocks * (i + 1) / degree);
			if (i == degree - 1)
				hi = info.nblocks; /* last range covers the tail exactly */
			ok = ScanQueue().Enqueue(db_oid, h.scan_id, h.generation, lo, hi);
		}
	}
	if (ok)
		return duckdb::make_uniq<PoolScanStream>(ch, h.scan_id);
	ScanRing().Close(h.scan_id, [](int idx) { PagePool().Release(PagePool().Slot(idx)); });
	return nullptr;
}

} // namespace worker

} // namespace pgddb
