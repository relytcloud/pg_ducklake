/*
 * S3 / Azure / GCS secret management for pg_ducklake.
 *
 * Credentials live in the PostgreSQL catalog as a FOREIGN SERVER (public
 * options, e.g. endpoint/region) plus a USER MAPPING (secret options, hidden
 * from other roles by PG ACLs) on the dedicated "ducklake_secret" FDW. On each
 * DuckDB connection, DuckDBManager drops and re-emits matching DuckDB
 * CREATE SECRET statements. This mirrors pg_duckdb's secret model, kept separate
 * from the unrelated "ducklake_fdw" (which attaches DuckLake databases).
 */

#include "pgducklake/ducklake_secret.hpp"
#include "pgducklake/duckdb_manager.hpp"

#include <sstream>
#include <string>
#include <vector>

#include "pgddb/pg/functions.hpp"
#include "pgddb/pgddb_duckdb.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/keyword_helper.hpp"

extern "C" {
#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/syscache.h"

#include "pgddb/vendor/pg_list.hpp"
}

#include "pgddb/utility/cpp_wrapper.hpp"

namespace pgducklake {

static const char *const SECRET_FDW_NAME = "ducklake_secret";
static const char *const SECRET_PREFIX = "pgducklake_secret_";

namespace {

const char *
FindOption(List *options_list, const char *name) {
	foreach_node(DefElem, def, options_list) {
		if (strcmp(def->defname, name) == 0) {
			return defGetString(def);
		}
	}
	return nullptr;
}

/*
 * Modified version of Postgres' GetUserMapping: returns nullptr instead of
 * throwing when no mapping exists, and optionally fetches the options.
 */
UserMapping *
FindUserMapping(Oid userid, Oid serverid, bool with_options) {
	HeapTuple tp = SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(userid), ObjectIdGetDatum(serverid));
	if (!HeapTupleIsValid(tp)) {
		/* Not found for the specific user -- try PUBLIC */
		tp = SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(InvalidOid), ObjectIdGetDatum(serverid));
	}
	if (!HeapTupleIsValid(tp)) {
		return nullptr;
	}

	UserMapping *um = (UserMapping *)palloc(sizeof(UserMapping));
	um->umid = ((Form_pg_user_mapping)GETSTRUCT(tp))->oid;
	um->userid = userid;
	um->serverid = serverid;
	um->options = NIL;

	if (!with_options) {
		ReleaseSysCache(tp);
		return um;
	}

	bool isnull;
	Datum datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp, Anum_pg_user_mapping_umoptions, &isnull);
	if (!isnull) {
		um->options = untransformRelOptions(datum);
	}
	ReleaseSysCache(tp);
	return um;
}

void
appendOptions(StringInfoData &buf, List *options) {
	foreach_node(DefElem, def, options) {
		// Option names went through PG's validation, no need to sanitize.
		appendStringInfo(&buf, ", %s %s", def->defname, quote_literal_cstr(defGetString(def)));
	}
}

char *
MakeCreateSecretQuery(const char *server_name, const char *type, List *server_options,
                      List *mapping_options = nullptr) {
	StringInfoData buf;
	initStringInfo(&buf);
	// Quote the secret name as an identifier: server_name comes from the
	// catalog and may contain arbitrary characters from a quoted CREATE SERVER.
	auto secret_name = duckdb::KeywordHelper::WriteOptionallyQuoted(std::string(SECRET_PREFIX) + server_name);
	appendStringInfo(&buf, "CREATE SECRET %s (", secret_name.c_str());
	appendStringInfo(&buf, "TYPE %s", type);
	appendOptions(buf, server_options);
	if (list_length(mapping_options) > 0) {
		appendOptions(buf, mapping_options);
	}
	appendStringInfoString(&buf, ")");
	return buf.data;
}

/*
 * Find a unique server name for a given prefix by appending an incrementing
 * suffix until an unused name is found.
 */
std::string
FindServerName(const char *server_prefix) {
	if (get_foreign_server_oid(server_prefix, true) == InvalidOid) {
		return server_prefix;
	}
	uint32_t i = 0;
	std::ostringstream oss;
	oss << server_prefix << "_";
	const auto len = oss.str().length();
	while (true) {
		oss.seekp(len);
		oss << ++i;
		auto server_name = oss.str();
		if (get_foreign_server_oid(server_name.c_str(), true) == InvalidOid) {
			return server_name;
		}
	}
}

std::string
ReadOptions(FunctionCallInfo fcinfo, int start, const std::vector<std::string> &names) {
	std::ostringstream oss;
	int opt_idx = start;
	for (const auto &name : names) {
		if (opt_idx >= PG_NARGS()) {
			break;
		}
		auto value = pgddb::pg::GetArgString(fcinfo, opt_idx++);
		if (value.empty()) {
			continue;
		}
		if (!oss.str().empty()) {
			oss << ", ";
		}
		oss << name << " " << duckdb::KeywordHelper::WriteQuoted(value);
	}
	return oss.str();
}

/*
 * Create the secret on a throwaway connection and roll back, so the main
 * connection's transaction state is untouched. Returns an error string (palloc'd)
 * or nullptr if the secret is valid. Also rejects redact_keys placed on the
 * SERVER instead of the USER MAPPING (they would be world-readable).
 */
const char *
GetQueryError(const char *query, List *server_options) {
	auto con = pgducklake::DuckDBManager::Get().CreateConnection();

	auto tx_query = duckdb::StringUtil::Format("BEGIN; %s", query);
	auto res = con->Query(tx_query);
	if (res->HasError()) {
		con->Query("ROLLBACK;");
		return pstrdup(res->GetErrorObject().RawMessage().c_str());
	}

	auto &secret_manager = duckdb::SecretManager::Get(*con->context);
	auto transaction = duckdb::CatalogTransaction::GetSystemCatalogTransaction(*con->context);
	auto secret_entry = secret_manager.GetSecretByName(transaction, "pgducklake_secret_validation");
	if (!secret_entry) {
		return "FATAL: Failed to get secret";
	} else if (!secret_entry->secret) {
		return "FATAL: No secret attached to the entry";
	}

	auto kv_secret = dynamic_cast<const duckdb::KeyValueSecret *>(secret_entry->secret.get());
	if (!kv_secret) {
		return "FATAL: Secret is not a duckdb::KeyValueSecret";
	}

	std::vector<std::string> restricted_options_in_server;
	for (const auto &k : kv_secret->redact_keys) {
		if (FindOption(server_options, k.c_str()) != nullptr) {
			restricted_options_in_server.push_back(k);
		}
	}
	if (restricted_options_in_server.empty()) {
		return nullptr;
	}

	std::ostringstream oss;
	oss << (restricted_options_in_server.size() == 1 ? "Option " : "Options ");
	for (size_t i = 0; i < restricted_options_in_server.size(); ++i) {
		oss << "'" << restricted_options_in_server[i] << "'";
		if (i != restricted_options_in_server.size() - 1) {
			oss << ", ";
		}
	}
	oss << " cannot be used in the SERVER's OPTIONS, please move it to the USER MAPPING";

	con->Query("ROLLBACK;");
	return pstrdup(oss.str().c_str());
}

// Set by the ProcessUtility hook so the FDW validator (which only receives
// options) can reach the target server's TYPE / oid.
const char *CurrentSecretServerType = nullptr;
Oid CurrentSecretServerOid = InvalidOid;

bool
IsSecretFdwServer(const char *servername) {
	ForeignServer *server = GetForeignServerByName(servername, true);
	if (!server) {
		return false;
	}
	Oid fdw_oid = get_foreign_data_wrapper_oid(SECRET_FDW_NAME, true);
	return fdw_oid != InvalidOid && server->fdwid == fdw_oid;
}

bool secret_callback_configured = false;

void
InvalidateSecretsCallback(Datum, int, uint32) {
	InvokeCPPFunc(pgducklake::DuckDBManager::InvalidateSecretsIfInitialized);
}

} // namespace

List *
ListCreateSecretQueries() {
	MemoryContext entry_ctx = CurrentMemoryContext;
	SPI_connect();

	auto query = R"(
		SELECT fs.oid AS server_oid
		FROM pg_foreign_server fs
		INNER JOIN pg_foreign_data_wrapper fdw ON fdw.oid = fs.srvfdw
		WHERE fdw.fdwname = 'ducklake_secret';
	)";

	auto ret = SPI_exec(query, 0);
	if (ret != SPI_OK_SELECT) {
		elog(ERROR, "Can't list DuckLake secrets: %s", SPI_result_code_string(ret));
	}

	List *results = NIL;
	for (uint64_t i = 0; i < SPI_processed; ++i) {
		HeapTuple tuple = SPI_tuptable->vals[i];
		bool is_null = false;
		Datum server_oid_datum = SPI_getbinval(tuple, SPI_tuptable->tupdesc, 1, &is_null);
		if (is_null) {
			elog(ERROR, "Expected server oid to be returned, but found NULL");
		}

		const Oid server_oid = DatumGetObjectId(server_oid_datum);
		auto user_mapping = FindUserMapping(GetUserId(), server_oid, true);
		List *user_mapping_options = user_mapping ? user_mapping->options : NIL;

		auto server = GetForeignServer(server_oid);

		// Build under the pre-SPI context so the result survives SPI_finish.
		MemoryContext spi_mem_ctx = MemoryContextSwitchTo(entry_ctx);
		auto secret_query =
		    MakeCreateSecretQuery(server->servername, server->servertype, server->options, user_mapping_options);
		results = lappend(results, secret_query);
		MemoryContextSwitchTo(spi_mem_ctx);
	}

	SPI_finish();
	return results;
}

void
ValidateSecret(const char *type, List *server_options, List *mapping_options) {
	if (type == nullptr) {
		elog(ERROR, "Missing required option: 'type'");
	}
	auto query = MakeCreateSecretQuery("validation", type, server_options, mapping_options);
	auto err = InvokeCPPFunc(GetQueryError, query, server_options);
	if (err != nullptr) {
		elog(ERROR, "%s", err);
	}
}

void
CaptureSecretServer(Node *parsetree) {
	CurrentSecretServerType = nullptr;
	CurrentSecretServerOid = InvalidOid;
	if (!parsetree) {
		return;
	}

	if (IsA(parsetree, CreateForeignServerStmt)) {
		// The server does not exist yet; read TYPE straight from the statement.
		auto *stmt = castNode(CreateForeignServerStmt, parsetree);
		if (strcmp(stmt->fdwname, SECRET_FDW_NAME) != 0 || stmt->servertype == nullptr) {
			return;
		}
		CurrentSecretServerType = pstrdup(stmt->servertype);
	} else if (IsA(parsetree, AlterForeignServerStmt)) {
		auto *stmt = castNode(AlterForeignServerStmt, parsetree);
		if (!IsSecretFdwServer(stmt->servername)) {
			return;
		}
		ForeignServer *server = GetForeignServerByName(stmt->servername, false);
		if (server->servertype != nullptr) {
			CurrentSecretServerType = pstrdup(server->servertype);
		}
	} else if (IsA(parsetree, CreateUserMappingStmt)) {
		auto *stmt = castNode(CreateUserMappingStmt, parsetree);
		if (!IsSecretFdwServer(stmt->servername)) {
			return;
		}
		CurrentSecretServerOid = GetForeignServerByName(stmt->servername, false)->serverid;
	} else if (IsA(parsetree, AlterUserMappingStmt)) {
		auto *stmt = castNode(AlterUserMappingStmt, parsetree);
		if (!IsSecretFdwServer(stmt->servername)) {
			return;
		}
		CurrentSecretServerOid = GetForeignServerByName(stmt->servername, false)->serverid;
	}
}

void
InitSecrets() {
	if (secret_callback_configured) {
		return;
	}
	secret_callback_configured = true;
	CacheRegisterSyscacheCallback(USERMAPPINGOID, InvalidateSecretsCallback, (Datum)0);
	CacheRegisterSyscacheCallback(FOREIGNSERVEROID, InvalidateSecretsCallback, (Datum)0);
}

} // namespace pgducklake

extern "C" {

DECLARE_PG_FUNCTION(ducklake_secret_fdw_handler) {
	// Secrets only; no foreign tables are created on this FDW.
	PG_RETURN_POINTER(nullptr);
}

DECLARE_PG_FUNCTION(ducklake_secret_fdw_validator) {
	Oid catalog = PG_GETARG_OID(1);
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));

	if (catalog == ForeignDataWrapperRelationId) {
		foreach_node(DefElem, def, options_list) {
			elog(ERROR, "'ducklake_secret' FDW does not take any option, found '%s'", def->defname);
		}
		PG_RETURN_VOID();
	} else if (catalog == ForeignServerRelationId) {
		auto server_type = pgducklake::CurrentSecretServerType;
		pgducklake::CurrentSecretServerType = nullptr;
		pgducklake::ValidateSecret(server_type, options_list);
		PG_RETURN_VOID();
	} else if (catalog == UserMappingRelationId) {
		Oid server_oid = pgducklake::CurrentSecretServerOid;
		pgducklake::CurrentSecretServerOid = InvalidOid;
		if (server_oid == InvalidOid) {
			elog(ERROR, "USER MAPPING on the 'ducklake_secret' FDW could not resolve its server");
		}
		auto server = GetForeignServer(server_oid);
		pgducklake::ValidateSecret(server->servertype, server->options, options_list);
		PG_RETURN_VOID();
	}

	elog(ERROR, "'ducklake_secret' FDW only supports SERVER and USER MAPPING objects");
	PG_RETURN_VOID();
}

DECLARE_PG_FUNCTION(ducklake_create_s3_secret) {
	auto type = pgddb::pg::GetArgString(fcinfo, 0);
	auto lc_type = duckdb::StringUtil::Lower(type);
	if (lc_type != "r2" && lc_type != "s3" && lc_type != "gcs") {
		elog(ERROR,
		     "Invalid type '%s': this helper only supports 's3', 'gcs' or 'r2'. Use a raw CREATE SERVER / CREATE USER "
		     "MAPPING on the ducklake_secret FDW for advanced secrets.",
		     type.c_str());
	}

	auto key = pgddb::pg::GetArgString(fcinfo, 1);
	auto secret = pgddb::pg::GetArgString(fcinfo, 2);
	auto session_token = pgddb::pg::GetArgString(fcinfo, 3);
	auto secret_name = "simple_" + lc_type + "_secret";

	SPI_connect();
	auto server_name = pgducklake::FindServerName(secret_name.c_str());
	{
		std::ostringstream create_server_query;
		create_server_query << "CREATE SERVER " << server_name << " TYPE '" << type
		                    << "' FOREIGN DATA WRAPPER ducklake_secret";
		auto options = pgducklake::ReadOptions(
		    fcinfo, 4, {"region", "url_style", "provider", "endpoint", "scope", "validation", "use_ssl"});
		if (!options.empty()) {
			create_server_query << " OPTIONS (" << options << ")";
		}
		auto ret = SPI_exec(create_server_query.str().c_str(), 0);
		if (ret != SPI_OK_UTILITY) {
			elog(ERROR, "Could not create '%s' SERVER: %s", type.c_str(), SPI_result_code_string(ret));
		}
	}
	{
		std::ostringstream create_mapping_query;
		create_mapping_query << "CREATE USER MAPPING FOR CURRENT_USER SERVER " << server_name << " OPTIONS (KEY_ID "
		                     << duckdb::KeywordHelper::WriteQuoted(key) << ", SECRET "
		                     << duckdb::KeywordHelper::WriteQuoted(secret);
		if (!session_token.empty()) {
			create_mapping_query << ", session_token " << duckdb::KeywordHelper::WriteQuoted(session_token);
		}
		create_mapping_query << ");";
		auto ret = SPI_exec(create_mapping_query.str().c_str(), 0);
		if (ret != SPI_OK_UTILITY) {
			elog(ERROR, "Could not create '%s' USER MAPPING: %s", type.c_str(), SPI_result_code_string(ret));
		}
	}
	SPI_finish();

	PG_RETURN_TEXT_P(cstring_to_text(server_name.c_str()));
}

DECLARE_PG_FUNCTION(ducklake_create_azure_secret) {
	auto connection_string = pgddb::pg::GetArgString(fcinfo, 0);
	SPI_connect();
	auto server_name = pgducklake::FindServerName("azure_secret");
	{
		std::ostringstream create_server_query;
		create_server_query << "CREATE SERVER " << server_name << " TYPE 'azure' FOREIGN DATA WRAPPER ducklake_secret";
		auto options = pgducklake::ReadOptions(fcinfo, 1, {"scope"});
		if (!options.empty()) {
			create_server_query << " OPTIONS (" << options << ")";
		}
		auto ret = SPI_exec(create_server_query.str().c_str(), 0);
		if (ret != SPI_OK_UTILITY) {
			elog(ERROR, "Could not create 'azure' SERVER: %s", SPI_result_code_string(ret));
		}
	}
	auto query = "CREATE USER MAPPING FOR CURRENT_USER SERVER " + server_name + " OPTIONS (connection_string " +
	             duckdb::KeywordHelper::WriteQuoted(connection_string) + ");";
	auto ret = SPI_exec(query.c_str(), 0);
	if (ret != SPI_OK_UTILITY) {
		elog(ERROR, "Could not create 'azure' USER MAPPING: %s", SPI_result_code_string(ret));
	}
	SPI_finish();

	PG_RETURN_TEXT_P(cstring_to_text(server_name.c_str()));
}

} // extern "C"
