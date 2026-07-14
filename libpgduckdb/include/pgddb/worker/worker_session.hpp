#pragma once

#include <string>

namespace duckdb {
class Connection;
}

namespace pgddb {

class SessionChannel;

// Runs only inside the duckdb worker process.
namespace worker {

// Execute `sql` on `con` and stream the result to `channel`: one Chunk frame per
// result DataChunk, then a Complete frame; on failure, a single Error frame whose
// payload is the message text. Polls the channel's cancel flag between chunks and
// interrupts the running query if cancellation was requested.
void RunWorkerSession(duckdb::Connection &con, SessionChannel &channel, const std::string &sql);

} // namespace worker

} // namespace pgddb
