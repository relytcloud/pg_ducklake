#ifndef PGDDB_RULEUTILS_H
#define PGDDB_RULEUTILS_H

#include "postgres.h"
#include "lib/stringinfo.h"
#include "pgddb/vendor/pg_list.hpp"

#ifdef __cplusplus
extern "C" {
#endif

/* Forward decls only; consumers pull the full PG node definitions themselves. */
struct Var;
struct Expr;
struct Const;
struct Plan;
struct Query;
struct RangeTblFunction;
struct SubscriptingRef;

typedef struct StarReconstructionContext {
	List *target_list;
	int varno_star;
	int varattno_star;
	bool added_current_star;
} StarReconstructionContext;

/*
 * Deparser hook surface; stays C-compatible because the vendored ruleutils
 * includes this header as C. Each wrapper applies the generic kernel rule then
 * iterates registered hooks until one handles the node. Hooks are OBJECT-SCOPED:
 * each handles only the types/relations/functions it owns and otherwise declines
 * so the kernel falls through to the next hook (then PG's built-in default).
 */

// Function name override (e.g. "system.main.f"); palloc'd name, or NULL to decline.
typedef char *(*pgddb_function_name_hook_t)(Oid funcid, bool *use_variadic_p);
void Register_pgddb_function_name(pgddb_function_name_hook_t fn);
char *pgddb_function_name(Oid funcid, bool *use_variadic_p);

// Override the entire qualified relation name; palloc'd name, or NULL to decline.
typedef char *(*pgddb_relation_name_hook_t)(Oid relid);
void Register_pgddb_relation_name(pgddb_relation_name_hook_t fn);

// CREATE TABLE column-type-name override is a DdlUtils virtual; see pgddb/pgddb_ddl.hpp.

// Is this a parser-only "fake" PG type that must not be cast-emitted to DuckDB?
typedef bool (*pgddb_is_fake_type_hook_t)(Oid type_oid);
void Register_pgddb_is_fake_type(pgddb_is_fake_type_hook_t fn);
bool pgddb_is_fake_type(Oid type_oid);

// Is this Var one of your "row" passthrough types (expanded to <ref>.*)?
typedef bool (*pgddb_var_is_duckdb_row_hook_t)(Var *var);
void Register_pgddb_var_is_duckdb_row(pgddb_var_is_duckdb_row_hook_t fn);
bool pgddb_var_is_duckdb_row(Var *var);

// Extract the subscript base Var from one of your subscripting exprs, or NULL.
typedef Var *(*pgddb_duckdb_subscript_var_hook_t)(Expr *expr);
void Register_pgddb_duckdb_subscript_var(pgddb_duckdb_subscript_var_hook_t fn);
Var *pgddb_duckdb_subscript_var(Expr *expr);

// Does this set-returning function return one of your "row" types?
typedef bool (*pgddb_func_returns_duckdb_row_hook_t)(RangeTblFunction *rtfunc);
void Register_pgddb_func_returns_duckdb_row(pgddb_func_returns_duckdb_row_hook_t fn);
bool pgddb_func_returns_duckdb_row(RangeTblFunction *rtfunc);

// Replace a subquery with a view reference; write into buf and return true if handled.
typedef bool (*pgddb_replace_subquery_with_view_hook_t)(Query *query, StringInfo buf);
void Register_pgddb_replace_subquery_with_view(pgddb_replace_subquery_with_view_hook_t fn);
bool pgddb_replace_subquery_with_view(Query *query, StringInfo buf);

// Decide the showtype for a Const cast: -1 suppresses the cast, the passed-in
// showtype keeps it. Kernel applies generic rules (e.g. drop bare ::numeric) first.
typedef int (*pgddb_show_type_hook_t)(Const *constval, int original_showtype);
void Register_pgddb_show_type(pgddb_show_type_hook_t fn);
int pgddb_show_type(Const *constval, int original_showtype);

// Override for Const literals whose PG text form DuckDB misreads (e.g. bytea);
// writes to buf and returns true if handled, else deparser uses simple_quote_literal.
bool pgddb_deparse_const_literal(Const *constval, StringInfo buf);

// Star reconstruction step. Mutate ctx and return true if handled.
typedef bool (*pgddb_reconstruct_star_step_hook_t)(StarReconstructionContext *ctx, ListCell *tle_cell);
void Register_pgddb_reconstruct_star_step(pgddb_reconstruct_star_step_hook_t fn);
bool pgddb_reconstruct_star_step(StarReconstructionContext *ctx, ListCell *tle_cell);

// Strip the first subscript of a `r['c']` ref into buf (-> `r.c`). Return the
// shortened ref if handled, or the input sbsref unchanged to decline.
typedef SubscriptingRef *(*pgddb_strip_first_subscript_hook_t)(SubscriptingRef *sbsref, StringInfo buf);
void Register_pgddb_strip_first_subscript(pgddb_strip_first_subscript_hook_t fn);
SubscriptingRef *pgddb_strip_first_subscript(SubscriptingRef *sbsref, StringInfo buf);

// Does this subscript Var carry a custom column alias?
typedef bool (*pgddb_subscript_has_custom_alias_hook_t)(Plan *plan, List *rtable, Var *subscript_var, char *colname);
void Register_pgddb_subscript_has_custom_alias(pgddb_subscript_has_custom_alias_hook_t fn);
bool pgddb_subscript_has_custom_alias(Plan *plan, List *rtable, Var *subscript_var, char *colname);

// db.schema resolution: return (db_name, schema_name) 2-list for relations of
// YOUR table-AM, or NULL to decline. Unclaimed relations fall back to the kernel's
// "pgduckdb" storage catalog, which reads plain PG heap relations.
typedef List *(*pgddb_db_and_schema_hook_t)(const char *postgres_schema_name, const char *table_am_name);
void Register_pgddb_db_and_schema(pgddb_db_and_schema_hook_t fn);

// Row reference expansion (`<ref>.*` at top level, `<ref>` otherwise); generic,
// kernel-handled with no hook. Called once a row-type predicate above has matched.
char *pgddb_write_row_refname(StringInfo buf, char *refname, bool is_top_level);

bool pgddb_is_not_default_expr(Node *node, void *context);
bool is_system_sampling(const char *tsm_name, int num_args);
bool is_bernoulli_sampling(const char *tsm_name, int num_args);
void pgddb_add_tablesample_percent(const char *tsm_name, StringInfo buf, int num_args);

char *pgddb_relation_name(Oid relid);
char *pgddb_get_querydef(Query *);
const char *pgddb_db_and_schema_string(const char *postgres_schema_name, const char *table_am_name);

/*
 * CREATE/ALTER/RENAME TABLE deparsers and their customization points live on
 * pgddb::DdlUtils in pgddb/pgddb_ddl.hpp, invoked directly by consumers rather
 * than through this (C) vendored-ruleutils surface.
 */

extern bool outermost_query;

#ifdef __cplusplus
}
#endif

#endif /* PGDDB_RULEUTILS_H */
