# RFC 001: PostgreSQL-Native DuckLake Writer

Status: Implemented

## Summary

Implement a DuckLake 1.0 writer inside pg_ducklake for batches produced by PostgreSQL. PostgreSQL stores the DuckLake metadata catalog and inlined row tables, and one PostgreSQL transaction atomically publishes both row data and metadata.

The steady-state deployment assumes pg_ducklake is the authoritative writer. Multiple PostgreSQL backends may write concurrently. A standard external DuckLake writer may also write occasionally, including concurrently, and must remain interoperable and correct.

The writer is a protocol implementation, separate from the SQL mechanism that produces a batch. VALUES, prepared UNNEST, COPY, and future producers commit through the same writer.

The central performance decision is that a publication retry must not evaluate, convert, or insert the user payload again. Rows are inserted into the PostgreSQL inline table once in the statement transaction. Retryable work is isolated in an internal subtransaction and changes only snapshot-dependent system columns and metadata.

## Use Case

A deployment has:

- PostgreSQL as the DuckLake metadata database.
- pg_ducklake as the normal and authoritative writer.
- Many PostgreSQL clients inserting through independent backend sessions.
- DuckDB and other DuckLake-compatible clients reading the catalog.
- Occasional external writers that follow the standard DuckLake protocol.

The common path is optimized for PostgreSQL-only writing. Correctness does not depend on an external writer using pg_ducklake locks, queues, or APIs.

## Goals

### Correctness

- Implement the DuckLake 1.0 writer protocol and reject unsupported metadata versions.
- Commit inlined rows and their metadata atomically in one PostgreSQL transaction.
- Preserve PostgreSQL statement semantics for every accepted input batch.
- Remain correct when standard external DuckLake writers commit concurrently.
- Produce metadata that any conforming DuckLake reader can consume without pg_ducklake-specific safeguards.

### Write Throughput

- Insert each accepted payload into the inline table at most once, including across publication retries.
- Never reevaluate expressions, reconvert values, reread COPY input, or recompute batch statistics on a publication retry.
- Keep row insertion concurrent across PostgreSQL backends.
- Minimize fixed cost per statement.
- Process rows and column statistics in batches.
- Avoid per-row metadata queries and updates.
- Avoid unnecessary conversion between PostgreSQL and DuckDB representations.
- Keep the metadata publication phase short.
- Support COPY-sized ingestion without memory use growing with the full input size.

### Concurrent Scalability

- Allow input evaluation, conversion, validation, statistics collection, and physical row insertion to proceed concurrently across PostgreSQL backends.
- Serialize only protocol state that requires global ordering.
- Avoid lost updates in row IDs, snapshots, table statistics, and column statistics.
- Rebase non-conflicting appends rather than fail when DuckLake permits it.
- Use bounded retry with randomized backoff for retryable races.
- Keep the PostgreSQL-only common path free of payload retagging when practical.

## Non-Goals

- Extending or changing the committed DuckLake 1.0 metadata or inline-table format.
- Requiring external DuckLake writers to use pg_ducklake-specific APIs or advisory locks.
- Optimizing concurrent external writers beyond correctness and interoperability.
- Supporting every PostgreSQL INSERT shape in the first version.
- Supporting a metadata version whose protocol semantics are not implemented and tested.
- Publishing from explicit multi-statement PostgreSQL transactions in the first version. Unsupported cases use the existing fallback path.

## Invariants

### Protocol Version

Before prewriting any rows, the writer reads `ducklake_metadata.version`. It fails closed unless the value is exactly a version implemented and tested by this writer. The first implementation supports DuckLake 1.0 only.

### Atomic Publication

A committed batch exposes all or none of:

- Its inlined rows.
- Its DuckLake snapshot.
- Its snapshot change record.
- Updated table-level counters.
- Updated column statistics.
- Updated protocol allocation state.

Prewritten rows remain uncommitted in the outer PostgreSQL transaction. Rolling back a publication subtransaction may retain those rows for another attempt, but no other transaction can observe them. Failure of the statement rolls back both the rows and all metadata.

### Insert-Once Payload

The user-column payload is physically inserted into the inline table once. A publication retry may retag the batch's `row_id` and `begin_snapshot`, but it must not delete and reinsert the payload.

The writer must identify exactly the rows owned by the current batch without adding a committed private column to the DuckLake inline table. The implementation may use PostgreSQL transaction identity, captured tuple identities, or a spillable private side structure. It must not select rows by candidate snapshot and row-ID range alone because a winning concurrent batch may have committed the same candidate values.

Insert-once does not mean that rebase is free. A PostgreSQL UPDATE of `row_id` and `begin_snapshot` creates a new MVCC tuple version and can rewrite and WAL-log the user-column bytes even if it is HOT. The design therefore permits at most one retag, only after a successful claim, and uses the pg-native reservation queue to avoid retagging in the normal PostgreSQL-only case. Retag cost is an explicit performance gate, not assumed to be negligible.

### Snapshot Ordering and Claim

Every commit publishes a unique snapshot immediately after the latest committed snapshot.

After reading and validating the current state, the first mutation in every publication attempt is the insert into `ducklake_snapshot`. Its primary-key insertion is the publication claim. The claim uses `ON CONFLICT (snapshot_id) DO NOTHING RETURNING snapshot_id`; no returned row is a retryable collision. The attempt exits without making later protocol mutations, and the outer transaction retains the payload for another attempt.

This matches the DuckLake 1.0 reference writer, whose commit batch starts with `InsertSnapshotSql()`. Once the claim succeeds, another conforming writer cannot publish the same next snapshot and must wait or retry.

### Schema Binding

A batch is prepared against a specific table identity and inline-table schema version. Before publication, the writer verifies that the table still exists and that its binding is unchanged.

The first implementation does not rebase across a target-table schema change. Any intervening ALTER, DROP, replacement, or inline-table-version change fails the statement. This is deliberately stricter than eventual compatible-schema rebasing and avoids publishing prewritten tuples against a different physical representation.

### Row IDs

Every live row in a table has a unique row ID. Allocation remains monotonic and collision-free across:

- Concurrent pg_ducklake writers.
- Standard external writers.
- Flushes and compaction.
- Publication retries and transaction aborts.

Candidate row IDs are not authoritative. After obtaining the snapshot claim, the writer reloads `next_row_id`; if the candidate range is stale, it retags only its own rows with the final range.

Gaps caused by aborted private reservations are permitted if DuckLake 1.0 permits them and `next_row_id` advances beyond every retained reservation. Otherwise successors collapse the gap by retagging before publication.

### Table Statistics

Record counts, next row IDs, file IDs, and related table state reflect every committed batch exactly once. The publication attempt derives them from the latest state after conflict classification. Batch row count and statistics contributions are immutable across retries.

### Column Statistics

Persisted statistics never exclude a committed value. In particular:

- Minimum and maximum bounds may widen but never become too narrow.
- Unknown information is not promoted to known from an incomplete batch.
- Null and NaN knowledge follows DuckLake 1.0 merge semantics.
- Missing, partial, nested, and extra statistics remain safe.
- Deletes may leave conservative bounds but must not make them unsafe.

The producer computes a complete contribution for its supported types while consuming the payload. The protocol writer merges that contribution with the latest persisted state only after obtaining the publication claim.

If a type cannot be represented and merged according to the protocol, the producer declines the native path before prewriting rows. The statement then uses the supported fallback writer or fails before publication.

### PostgreSQL Semantics

Every accepted batch preserves applicable PostgreSQL behavior, including:

- Target-column mapping and coercion.
- Defaults and generated values.
- NOT NULL and CHECK constraints.
- Permissions and row-level security.
- Trigger behavior where supported.
- Statement atomicity and error behavior.

A producer declines an operation whose semantics it cannot preserve. In particular, the VALUES producer gate excludes VOLATILE expressions rather than claiming to preserve their evaluation semantics. Accepted VALUES expressions are evaluated once while producing the batch and are not reevaluated during rebase.

### External Writer Compatibility

Correctness relies on DuckLake metadata, the snapshot primary key, and PostgreSQL transaction isolation, not on cooperation through private pg_ducklake state. A private queue may optimize pg_ducklake writers, but every publisher still validates shared metadata and obtains the standard snapshot claim.

### Reader Visibility

After commit, a fresh conforming DuckLake reader observes the new rows and truthful metadata. No candidate or private reservation is committed. Existing readers do not reuse stale protocol state after a data change.

## Implementation Structure

### Batch Producer

A producer turns a supported PostgreSQL statement or input stream into validated typed rows. It owns PostgreSQL expression and statement semantics but does not author DuckLake metadata.

Initial producers are:

- Restricted VALUES handled by the existing direct-insert matcher.
- Prepared `UNNEST($n)` with execution-bound arrays, using PostgreSQL target-list SRF lockstep and NULL-padding semantics.
- COPY FROM STDIN for a fully compatible target column list.

Unsupported triggers, row-level-security behavior, generated/default expressions, conflict clauses, partition routing, or coercions cause the producer to decline unless it can prove that the native path preserves their PostgreSQL semantics.

### Protocol Batch

The producer creates a batch descriptor containing:

- Target PostgreSQL relation, DuckLake table identity, and inline-table schema version.
- Starting DuckLake snapshot.
- Row count.
- Complete per-column statistics contributions.
- Candidate snapshot and row-ID range.
- A handle that identifies the prewritten rows owned by this batch.
- Information required to validate supported PostgreSQL semantics.

The descriptor does not need to retain the full payload after it has been prewritten. Statistics use one representation that can express known and unknown fields independently, including min, max, null count, NaN knowledge, validity, and extra statistics.

### Protocol Writer

The protocol writer is the only component that mutates DuckLake metadata. It owns:

- Version checks.
- Conflict detection.
- Snapshot claims.
- Row-ID allocation and retagging.
- Statistics merging.
- Snapshot change records.
- Retry classification.
- Interaction with the optional pg-native reservation queue.

All batch producers use this component.

## Commit Model

### 1. Prepare and Prewrite Phase

In the outer PostgreSQL statement transaction, with no publication lock held, the writer:

1. Reads and validates DuckLake 1.0 metadata and the target binding.
2. Obtains candidate snapshot and row-ID values, optionally from the pg-native reservation queue.
3. Evaluates and validates input exactly once.
4. Converts values directly to their inline representation.
5. Accumulates typed column statistics.
6. Inserts bounded chunks into the PostgreSQL inline table using the candidate system values.
7. Retains only the batch descriptor and a bounded or spillable ownership handle.

These rows are MVCC-invisible to other transactions. COPY can stream directly into the inline table and discard each input chunk after insertion; it does not need to retain or replay the input stream.

### 2. Publish Attempt

Each attempt runs in a PostgreSQL internal subtransaction:

1. Read the latest committed snapshot, table state, and changes since the batch's starting snapshot.
2. Apply DuckLake conflict classification.
3. Verify the exact target table and inline schema binding.
4. Compute the required next snapshot and final row-ID range.
5. Insert the new `ducklake_snapshot` row as the first mutation with `ON CONFLICT DO NOTHING RETURNING`; a returned row obtains the publication claim, while no row ends this attempt as a collision.
6. If candidate system values are stale, retag only this batch's prewritten rows.
7. Merge and write table and column statistics.
8. Write the snapshot change record and any other required allocation state.
9. Release the internal subtransaction.

The outer PostgreSQL transaction then commits, atomically making the claimed snapshot, metadata, and rows visible.

On the no-conflict common path, candidate IDs are final and step 6 is skipped.

### 3. Retry Phase

A snapshot claim that returns no row, or another specifically classified publication race, ends only the internal publication attempt. The writer then:

1. Waits using bounded randomized backoff.
2. Reloads the latest snapshot and metadata.
3. Rechecks intervening changes.
4. Attempts a new snapshot claim.
5. After a successful claim, retags the prewritten rows at most once to the final snapshot and row-ID range.
6. Recomputes only snapshot-dependent metadata merges.

The writer does not reread COPY input, reevaluate expressions, reconvert values, recompute batch statistics, or reinsert user rows.

A unique violation from any protocol mutation is not automatically retryable; the expected snapshot collision is handled by the claim's `ON CONFLICT`, not by catching a unique error. Non-retryable conflicts abort the outer statement transaction, which removes the prewritten rows.

### Pseudo-SQL

PostgreSQL does not support an independent nested transaction, so the "inner transaction" below is an internal subtransaction, represented as a savepoint. `batch_owned_rows` is pseudocode for the transaction-private ownership handle; it is not a committed DuckLake table or column.

```sql
/* outer transaction */
BEGIN;

/* Validate before changing the inline table. */
SELECT value
FROM ducklake.ducklake_metadata
WHERE key = 'version';                       -- must be exactly '1.0'

SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM ducklake.ducklake_snapshot
ORDER BY snapshot_id DESC
LIMIT 1;                                     -- candidate allocation base

SELECT next_row_id
FROM ducklake.ducklake_table_stats
WHERE table_id = :table_id;                  -- candidate row-ID base

/*
 * Prewrite once. These candidate values may come from the pg-native
 * reservation queue. The rows are invisible outside this transaction.
 * Conversion and batch-statistics collection happen while producing them.
 */
INSERT INTO ducklake.ducklake_inlined_data_<table_id>_<schema_version>
       (row_id, begin_snapshot, end_snapshot, <data_columns>)
VALUES (:candidate_row_id, :candidate_snapshot_id, NULL, <data>),
       ...;

/* Keep a transaction-private way to identify exactly the rows above. */
/* batch_owned_rows := <xid/TID/spillable-side-structure handle>; */

publication_retry:

/* inner transaction / publication attempt */
SAVEPOINT ducklake_publish_attempt;

/* Reads and conflict checks do not mutate protocol state. */
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM ducklake.ducklake_snapshot
ORDER BY snapshot_id DESC
LIMIT 1;                                     -- :latest_snapshot

SELECT changes_made
FROM ducklake.ducklake_snapshot_changes
WHERE snapshot_id > :batch_start_snapshot
ORDER BY snapshot_id;                        -- classify intervening changes

SELECT next_row_id, record_count, file_size_bytes
FROM ducklake.ducklake_table_stats
WHERE table_id = :table_id;                  -- final allocation base

SELECT column_id, contains_null, contains_nan,
       min_value, max_value, extra_stats
FROM ducklake.ducklake_table_column_stats
WHERE table_id = :table_id;                  -- latest merge base

/* Revalidate table identity and the exact inline schema version here. */

/*
 * First mutation of the publication attempt: claim the next snapshot.
 * The primary key serializes us with native and standard DuckLake writers.
 */
INSERT INTO ducklake.ducklake_snapshot
       (snapshot_id, snapshot_time, schema_version,
        next_catalog_id, next_file_id)
VALUES (:latest_snapshot + 1, now(), :final_schema_version,
        :final_next_catalog_id, :final_next_file_id)
ON CONFLICT (snapshot_id) DO NOTHING
RETURNING snapshot_id;

/*
 * No returned row means another writer claimed this snapshot. None of the
 * statements below have run. End the attempt, preserve the outer transaction's
 * prewritten rows, apply bounded backoff, and retry metadata only.
 */

/* No-op on the no-conflict/reservation-queue common path. */
UPDATE ducklake.ducklake_inlined_data_<table_id>_<schema_version> AS dst
SET row_id = owned.final_row_id,
    begin_snapshot = :latest_snapshot + 1
FROM batch_owned_rows AS owned
WHERE dst.ctid = owned.tid
  AND (:candidate_snapshot_id <> :latest_snapshot + 1
       OR owned.candidate_row_id <> owned.final_row_id);

UPDATE ducklake.ducklake_table_stats
SET record_count = record_count + :batch_row_count,
    next_row_id = :final_next_row_id
WHERE table_id = :table_id;

/* Pseudocode: merge each immutable batch contribution into latest stats. */
UPDATE ducklake.ducklake_table_column_stats AS persisted
SET contains_null = ducklake_merge_nulls(persisted, batch_stats),
    contains_nan  = ducklake_merge_nan(persisted, batch_stats),
    min_value     = ducklake_merge_min(persisted, batch_stats),
    max_value     = ducklake_merge_max(persisted, batch_stats),
    extra_stats   = ducklake_merge_extra(persisted, batch_stats)
FROM batch_column_stats AS batch_stats
WHERE persisted.table_id = :table_id
  AND persisted.column_id = batch_stats.column_id;

INSERT INTO ducklake.ducklake_snapshot_changes
       (snapshot_id, changes_made, author, commit_message, commit_extra_info)
VALUES (:latest_snapshot + 1,
        'inlined_insert:' || :table_id,
        :author, :commit_message, :commit_extra_info);

/* inner transaction */
RELEASE SAVEPOINT ducklake_publish_attempt;

/* outer transaction: rows and metadata become visible together */
COMMIT;
```

If conflict classification, schema validation, or any post-claim operation fails, the writer rolls back the outer transaction rather than committing the prewritten rows without valid metadata.

## PostgreSQL-Native Reservation Queue

The snapshot primary key is the correctness mechanism. A process-shared pg-native reservation queue is an optional performance accelerator for the dominant PostgreSQL-only workload.

The queue may reserve candidate publication order and per-table row-ID ranges for pg_ducklake backends before they prewrite. This allows backends to insert rows concurrently with distinct candidate IDs and then publish in reservation order. If every participant is a pg_ducklake writer and predecessors commit normally, the common path requires neither payload replay nor retagging.

The queue must satisfy these rules:

- A reservation is never treated as committed protocol state.
- Every publisher reloads shared DuckLake metadata and obtains the standard snapshot claim.
- Backend exit, cancellation, transaction abort, queue reset, or a missing predecessor invalidates affected candidate snapshot values.
- An external commit may invalidate any private reservation. The affected native writer rebases through the normal publish loop.
- Queue failure can reduce performance but cannot affect correctness.
- Waiting for publication order must be interruptible and bounded by statement cancellation and retry settings.

The implemented queue is enabled by default with a short adaptive wait cap. A stalled predecessor causes the writer to abandon its reservation and return to authoritative optimistic publication, limiting catalog-wide head-of-line blocking. Correctness never depends on queue availability.

## Concurrency Model

### Concurrent Appends

Independent append batches prewrite concurrently. Their publish attempts are ordered by successful snapshot claims. A later publisher uses the table state produced by the earlier publisher.

Without a valid private reservation, multiple batches may prewrite the same candidate IDs. Only one can claim that snapshot. Losers retain their own uncommitted rows, claim a later snapshot, and retag only those rows.

### Intervening Schema Changes

Any target-table or target-inline-schema change after preparation is non-retryable in the first implementation. Changes to unrelated tables may be rebased if DuckLake's conflict rules permit it.

### Deletes, Updates, and Maintenance

The writer classifies concurrent deletes, updates, flushes, and compaction using DuckLake 1.0 conflict rules. It does not infer compatibility solely because the table still exists. Append rebase is allowed only when the intervening changes leave the prewritten payload and target binding valid.

### External Commits

An external writer may win publication at any time before the native writer obtains its snapshot claim. pg_ducklake detects this through the shared catalog, then rebases or fails according to DuckLake rules. Private reservations are discarded or adjusted as needed.

The implementation must test the claim ordering and retry behavior against the pinned DuckLake 1.0 reference writer. If a nominally compatible external writer mutates metadata before attempting its snapshot claim, PostgreSQL may resolve the opposing lock order as a deadlock; that attempt must roll back and be classified conservatively. No committed state may be corrupted.

## Transaction Scope

The first native writer supports one statement in an implicit PostgreSQL transaction. This matches the current direct-insert restriction and ensures that a successful publication claim is followed immediately by top-level commit.

Explicit multi-statement transactions use the existing fallback path. Supporting them later requires transaction-level batching: one DuckLake snapshot at PostgreSQL commit, or a proven way to prevent uncommitted per-statement snapshots from blocking the publication queue. The native writer must not silently publish a DuckLake snapshot before its containing PostgreSQL transaction commits.

## Resource Limits

- Row conversion and table insertion operate in bounded chunks.
- COPY input is not retained after its row has been prewritten.
- Batch ownership tracking is bounded or spills to PostgreSQL-managed temporary storage.
- Statistics state is O(number of columns), excluding protocol-defined extra statistics with an explicit cap.
- The VALUES producer enforces the configured inline-row limit while planning. Prepared UNNEST cardinality is not bound until execution, and COPY cardinality is streamed, so those producers remain bounded-memory but do not dynamically switch a cached native plan to a file-writer fallback.
- Inputs that should become data files rather than inline rows use a file-writer fallback when one is available before native-plan selection.

## Testing Strategy

### Conformance

For each supported operation:

1. Apply an equivalent batch through the pinned DuckLake 1.0 writer.
2. Apply it through the PostgreSQL-native writer.
3. Compare normalized metadata and visible rows.
4. Read the result from a fresh standard DuckLake client.

Tests explicitly verify that the reference commit batch claims `ducklake_snapshot` before mutating inline data and metadata. A change in that assumption blocks a DuckLake dependency upgrade until this RFC is reevaluated.

### Differential Queries

Run filtered and unfiltered queries over every maintained statistics type, including:

- Values outside previous bounds.
- NULL-only batches.
- NaN and infinity.
- Partial or missing statistics.
- Values written before and after flush.

### Concurrency

Test at least:

- Many PostgreSQL appenders to one table, with the private queue enabled and disabled.
- Many PostgreSQL appenders to different tables.
- PostgreSQL append racing an external append.
- Append racing ALTER, DROP, DELETE, flush, and compaction.
- Reservation owner cancellation or backend termination.
- Retry exhaustion and transaction cancellation.

Append, claim-collision, rebase, and reservation-recovery tests verify row uniqueness, exact row count, metadata consistency, and fresh-client visibility. Schema-change and maintenance race tests instead verify their operation-specific commit-or-fail invariants and catalog consistency.

Retry tests instrument physical payload insertion and producer consumption. They prove producer-once behavior with evidence that cannot be regenerated inside publication retry, such as COPY stream consumption counters and execution-bound UNNEST input, even when multiple snapshot claims fail. They also prove that a successful rebase retags only rows owned by the losing batch. VOLATILE VALUES are not used as retry evidence because the VALUES producer rejects them at its gate.

### Performance Gates

Before enabling the writer by default, benchmark:

- No-contention VALUES, UNNEST, and COPY throughput.
- Same-table throughput and tail latency at increasing backend counts.
- Different-table throughput at increasing backend counts.
- Forced collision cost for small and large inline batches.
- Retag WAL volume, dead tuples, and vacuum pressure.
- Reservation-queue head-of-line blocking and backend-abort recovery.

The benchmark comparison includes the historical insert-then-client-retry behavior as an instrumented baseline. The native writer demonstrates that retries do not cause a second payload insertion and that concurrent PostgreSQL throughput does not regress materially.

### Fault Injection

Inject failures after prewrite and after every publish step. Verify that:

- Publication-subtransaction rollback retains only the current transaction's invisible payload for retry.
- Top-level rollback leaves neither visible rows nor metadata.
- A committed snapshot never contains candidate IDs from a failed allocation.
- Queue state cannot make invalid data visible.

## Alternatives Considered

### Abort and Replay the Entire Statement

This was the behavior before the native publication retry path. It is rejected because it rereads or resends input and reinserts the same payload, making collision cost proportional to ingestion cost.

### Claim the Snapshot Before Reading or Inserting COPY

This avoids replay but holds the global publication claim for the whole input stream, serializing large writes and allowing a slow client to block every writer. It is rejected for streaming producers.

### Stage the Entire Batch in a Temporary Relation

A spillable staging relation permits claim-first insertion into the real inline table, but every successful write pays an extra physical write and read. It remains a possible fallback for ownership tracking or semantic validation, not the default data path.

### Allocate IDs Only Through PostgreSQL-Private Sequences

External DuckLake writers do not consume those sequences, so private IDs alone cannot establish DuckLake snapshot order or collision-free row IDs. Private reservations are useful only as hints that are validated against shared metadata.

### Add a Batch Token Column to Inline Tables

A token would simplify retry retagging but changes the physical table shape seen by external writers and risks positional INSERT incompatibility. The committed DuckLake format remains unchanged.

## Resolved Decisions

- Publication retry is an internal subtransaction retry, not a top-level statement replay.
- Payload rows are prewritten once in the outer statement transaction.
- Snapshot insertion is the first mutation in every publication attempt.
- Batch values and statistics are immutable across retries.
- A successful rebase may update only snapshot-dependent system columns on rows owned by that batch.
- Target-table schema changes are not rebaseable in the first implementation.
- The first version supports implicit one-statement transactions only.
- COPY uses bounded direct prewrite rather than retaining the full input for replay.
- DuckLake 1.0 is the only initially supported metadata version.
- A pg-native reservation queue is a performance optimization, never a correctness dependency.

## Deferred Work

- Compatible-schema rebasing.
- Explicit multi-statement PostgreSQL transactions.
- Additional INSERT shapes and PostgreSQL semantic features.
- A PostgreSQL-native data-file writer for batches that should not remain inlined.
- Broader type and nested-statistics support.
