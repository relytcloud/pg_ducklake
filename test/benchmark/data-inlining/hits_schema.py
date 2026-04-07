# ClickBench HITS schema constants for data-inlining benchmarks.
# Source: athena_partitioned/hits_0.parquet (105 columns).
#
# The parquet stores timestamps as epoch-seconds (int64), dates as
# days-since-epoch (uint16), and booleans as int16.  Both benchmark
# scripts convert these to proper Python/Arrow types before insertion.

from datetime import date, datetime, timezone

# -- pg_ducklake DDL & DML ------------------------------------------------

CREATE_TABLE_SQL = """\
CREATE TABLE hits (
    "WatchID" BIGINT,
    "JavaEnable" BOOLEAN,
    "Title" TEXT,
    "GoodEvent" BOOLEAN,
    "EventTime" TIMESTAMP,
    "EventDate" DATE,
    "CounterID" INTEGER,
    "ClientIP" INTEGER,
    "RegionID" INTEGER,
    "UserID" BIGINT,
    "CounterClass" SMALLINT,
    "OS" SMALLINT,
    "UserAgent" SMALLINT,
    "URL" TEXT,
    "Referer" TEXT,
    "IsRefresh" BOOLEAN,
    "RefererCategoryID" SMALLINT,
    "RefererRegionID" INTEGER,
    "URLCategoryID" SMALLINT,
    "URLRegionID" INTEGER,
    "ResolutionWidth" SMALLINT,
    "ResolutionHeight" SMALLINT,
    "ResolutionDepth" SMALLINT,
    "FlashMajor" SMALLINT,
    "FlashMinor" SMALLINT,
    "FlashMinor2" TEXT,
    "NetMajor" SMALLINT,
    "NetMinor" SMALLINT,
    "UserAgentMajor" SMALLINT,
    "UserAgentMinor" TEXT,
    "CookieEnable" BOOLEAN,
    "JavascriptEnable" BOOLEAN,
    "IsMobile" BOOLEAN,
    "MobilePhone" SMALLINT,
    "MobilePhoneModel" TEXT,
    "Params" TEXT,
    "IPNetworkID" INTEGER,
    "TraficSourceID" SMALLINT,
    "SearchEngineID" SMALLINT,
    "SearchPhrase" TEXT,
    "AdvEngineID" SMALLINT,
    "IsArtifical" BOOLEAN,
    "WindowClientWidth" SMALLINT,
    "WindowClientHeight" SMALLINT,
    "ClientTimeZone" SMALLINT,
    "ClientEventTime" TIMESTAMP,
    "SilverlightVersion1" SMALLINT,
    "SilverlightVersion2" SMALLINT,
    "SilverlightVersion3" INTEGER,
    "SilverlightVersion4" SMALLINT,
    "PageCharset" TEXT,
    "CodeVersion" INTEGER,
    "IsLink" BOOLEAN,
    "IsDownload" BOOLEAN,
    "IsNotBounce" BOOLEAN,
    "FUniqID" BIGINT,
    "OriginalURL" TEXT,
    "HID" INTEGER,
    "IsOldCounter" BOOLEAN,
    "IsEvent" BOOLEAN,
    "IsParameter" BOOLEAN,
    "DontCountHits" BOOLEAN,
    "WithHash" BOOLEAN,
    "HitColor" TEXT,
    "LocalEventTime" TIMESTAMP,
    "Age" SMALLINT,
    "Sex" SMALLINT,
    "Income" SMALLINT,
    "Interests" SMALLINT,
    "Robotness" SMALLINT,
    "RemoteIP" INTEGER,
    "WindowName" INTEGER,
    "OpenerName" INTEGER,
    "HistoryLength" SMALLINT,
    "BrowserLanguage" TEXT,
    "BrowserCountry" TEXT,
    "SocialNetwork" TEXT,
    "SocialAction" TEXT,
    "HTTPError" SMALLINT,
    "SendTiming" INTEGER,
    "DNSTiming" INTEGER,
    "ConnectTiming" INTEGER,
    "ResponseStartTiming" INTEGER,
    "ResponseEndTiming" INTEGER,
    "FetchTiming" INTEGER,
    "SocialSourceNetworkID" SMALLINT,
    "SocialSourcePage" TEXT,
    "ParamPrice" BIGINT,
    "ParamOrderID" TEXT,
    "ParamCurrency" TEXT,
    "ParamCurrencyID" SMALLINT,
    "OpenstatServiceName" TEXT,
    "OpenstatCampaignID" TEXT,
    "OpenstatAdID" TEXT,
    "OpenstatSourceID" TEXT,
    "UTMSource" TEXT,
    "UTMMedium" TEXT,
    "UTMCampaign" TEXT,
    "UTMContent" TEXT,
    "UTMTerm" TEXT,
    "FromTag" TEXT,
    "HasGCLID" BOOLEAN,
    "RefererHash" BIGINT,
    "URLHash" BIGINT,
    "CLID" INTEGER
) USING ducklake"""

# Direct insert requires PREPARE/EXECUTE to produce real $N Param nodes.
# PREPARE types must match the DDL column types.
PREPARE_SQL = """\
PREPARE di (
    int8[], bool[], text[], bool[], timestamp[], date[], int4[], int4[], int4[], int8[],
    int2[], int2[], int2[], text[], text[],
    bool[], int2[], int4[], int2[], int4[],
    int2[], int2[], int2[], int2[], int2[],
    text[], int2[], int2[], int2[], text[],
    bool[], bool[], bool[], int2[], text[],
    text[], int4[], int2[], int2[], text[],
    int2[], bool[], int2[], int2[], int2[],
    timestamp[], int2[], int2[], int4[], int2[],
    text[], int4[], bool[], bool[], bool[],
    int8[], text[], int4[], bool[], bool[],
    bool[], bool[], bool[], text[], timestamp[],
    int2[], int2[], int2[], int2[], int2[],
    int4[], int4[], int4[], int2[], text[],
    text[], text[], text[], int2[], int4[],
    int4[], int4[], int4[], int4[], int4[],
    int2[], text[], int8[], text[], text[],
    int2[], text[], text[], text[], text[],
    text[], text[], text[], text[], text[],
    text[], bool[], int8[], int8[], int4[]
) AS INSERT INTO hits (
    "WatchID", "JavaEnable", "Title", "GoodEvent", "EventTime",
    "EventDate", "CounterID", "ClientIP", "RegionID", "UserID",
    "CounterClass", "OS", "UserAgent", "URL", "Referer",
    "IsRefresh", "RefererCategoryID", "RefererRegionID", "URLCategoryID", "URLRegionID",
    "ResolutionWidth", "ResolutionHeight", "ResolutionDepth", "FlashMajor", "FlashMinor",
    "FlashMinor2", "NetMajor", "NetMinor", "UserAgentMajor", "UserAgentMinor",
    "CookieEnable", "JavascriptEnable", "IsMobile", "MobilePhone", "MobilePhoneModel",
    "Params", "IPNetworkID", "TraficSourceID", "SearchEngineID", "SearchPhrase",
    "AdvEngineID", "IsArtifical", "WindowClientWidth", "WindowClientHeight", "ClientTimeZone",
    "ClientEventTime", "SilverlightVersion1", "SilverlightVersion2", "SilverlightVersion3", "SilverlightVersion4",
    "PageCharset", "CodeVersion", "IsLink", "IsDownload", "IsNotBounce",
    "FUniqID", "OriginalURL", "HID", "IsOldCounter", "IsEvent",
    "IsParameter", "DontCountHits", "WithHash", "HitColor", "LocalEventTime",
    "Age", "Sex", "Income", "Interests", "Robotness",
    "RemoteIP", "WindowName", "OpenerName", "HistoryLength", "BrowserLanguage",
    "BrowserCountry", "SocialNetwork", "SocialAction", "HTTPError", "SendTiming",
    "DNSTiming", "ConnectTiming", "ResponseStartTiming", "ResponseEndTiming", "FetchTiming",
    "SocialSourceNetworkID", "SocialSourcePage", "ParamPrice", "ParamOrderID", "ParamCurrency",
    "ParamCurrencyID", "OpenstatServiceName", "OpenstatCampaignID", "OpenstatAdID", "OpenstatSourceID",
    "UTMSource", "UTMMedium", "UTMCampaign", "UTMContent", "UTMTerm",
    "FromTag", "HasGCLID", "RefererHash", "URLHash", "CLID"
) SELECT
    UNNEST($1),  UNNEST($2),  UNNEST($3),  UNNEST($4),  UNNEST($5),
    UNNEST($6),  UNNEST($7),  UNNEST($8),  UNNEST($9),  UNNEST($10),
    UNNEST($11), UNNEST($12), UNNEST($13), UNNEST($14), UNNEST($15),
    UNNEST($16), UNNEST($17), UNNEST($18), UNNEST($19), UNNEST($20),
    UNNEST($21), UNNEST($22), UNNEST($23), UNNEST($24), UNNEST($25),
    UNNEST($26), UNNEST($27), UNNEST($28), UNNEST($29), UNNEST($30),
    UNNEST($31), UNNEST($32), UNNEST($33), UNNEST($34), UNNEST($35),
    UNNEST($36), UNNEST($37), UNNEST($38), UNNEST($39), UNNEST($40),
    UNNEST($41), UNNEST($42), UNNEST($43), UNNEST($44), UNNEST($45),
    UNNEST($46), UNNEST($47), UNNEST($48), UNNEST($49), UNNEST($50),
    UNNEST($51), UNNEST($52), UNNEST($53), UNNEST($54), UNNEST($55),
    UNNEST($56), UNNEST($57), UNNEST($58), UNNEST($59), UNNEST($60),
    UNNEST($61), UNNEST($62), UNNEST($63), UNNEST($64), UNNEST($65),
    UNNEST($66), UNNEST($67), UNNEST($68), UNNEST($69), UNNEST($70),
    UNNEST($71), UNNEST($72), UNNEST($73), UNNEST($74), UNNEST($75),
    UNNEST($76), UNNEST($77), UNNEST($78), UNNEST($79), UNNEST($80),
    UNNEST($81), UNNEST($82), UNNEST($83), UNNEST($84), UNNEST($85),
    UNNEST($86), UNNEST($87), UNNEST($88), UNNEST($89), UNNEST($90),
    UNNEST($91), UNNEST($92), UNNEST($93), UNNEST($94), UNNEST($95),
    UNNEST($96), UNNEST($97), UNNEST($98), UNNEST($99), UNNEST($100),
    UNNEST($101), UNNEST($102), UNNEST($103), UNNEST($104), UNNEST($105)"""

# EXECUTE template for psycopg ClientCursor (client-side param binding).
# Text columns need ::text[] cast because psycopg emits untyped string literals.
EXECUTE_SQL = """\
EXECUTE di (
    %s, %s, %s::text[], %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s::text[], %s::text[],
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s::text[], %s, %s, %s, %s::text[],
    %s, %s, %s, %s, %s::text[],
    %s::text[], %s, %s, %s, %s::text[],
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s::text[], %s, %s, %s, %s,
    %s, %s::text[], %s, %s, %s,
    %s, %s, %s, %s::text[], %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s::text[],
    %s::text[], %s::text[], %s::text[], %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s::text[], %s, %s::text[], %s::text[],
    %s, %s::text[], %s::text[], %s::text[], %s::text[],
    %s::text[], %s::text[], %s::text[], %s::text[], %s::text[],
    %s::text[], %s, %s, %s, %s
)"""

# -- DuckDB DDL (same schema, no USING ducklake, lake.main prefix) --------

CREATE_TABLE_DUCKDB_SQL = CREATE_TABLE_SQL.replace(
    "CREATE TABLE hits", "CREATE TABLE lake.main.hits"
).replace(") USING ducklake", ")")

# -- Heap table DDL (same schema, no USING ducklake -- for baseline)

CREATE_HEAP_TABLE_SQL = CREATE_TABLE_SQL.replace(
    ") USING ducklake", ")"
)

# -- Queries (full ClickBench Q0-Q42) ------------------------------------
#
# All 43 official ClickBench queries adapted for the HITS schema.
# PG queries use double-quoted identifiers and PG-native boolean syntax
# (our schema declares IsRefresh/DontCountHits/etc. as BOOLEAN, whereas
# ClickBench originals use integer columns with = 0 / <> 0 comparisons).
# DuckDB queries are derived via _pg_to_duckdb() transformation.

# Q29: 90 SUM expressions on ResolutionWidth (generated to avoid a wall of text)
_Q29_SUMS = ', '.join(f'SUM("ResolutionWidth" + {i})' for i in range(90))

QUERIES_PG = [
    # Q0: count
    """SELECT COUNT(*) FROM hits""",
    # Q1: count with filter
    """SELECT COUNT(*) FROM hits WHERE "AdvEngineID" <> 0""",
    # Q2: sum, count, avg
    """SELECT SUM("AdvEngineID"), COUNT(*), AVG("ResolutionWidth") FROM hits""",
    # Q3: avg bigint
    """SELECT AVG("UserID") FROM hits""",
    # Q4: count distinct
    """SELECT COUNT(DISTINCT "UserID") FROM hits""",
    # Q5: count distinct text
    """SELECT COUNT(DISTINCT "SearchPhrase") FROM hits""",
    # Q6: min/max date
    """SELECT MIN("EventDate"), MAX("EventDate") FROM hits""",
    # Q7: group by with filter
    """SELECT "AdvEngineID", COUNT(*) FROM hits WHERE "AdvEngineID" <> 0 GROUP BY "AdvEngineID" ORDER BY COUNT(*) DESC""",
    # Q8: count distinct + group by
    """SELECT "RegionID", COUNT(DISTINCT "UserID") AS u FROM hits GROUP BY "RegionID" ORDER BY u DESC LIMIT 10""",
    # Q9: multi-agg + count distinct + group by
    """SELECT "RegionID", SUM("AdvEngineID"), COUNT(*) AS c, AVG("ResolutionWidth"), COUNT(DISTINCT "UserID") FROM hits GROUP BY "RegionID" ORDER BY c DESC LIMIT 10""",
    # Q10: mobile phone model group by
    """SELECT "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "MobilePhoneModel" <> '' GROUP BY "MobilePhoneModel" ORDER BY u DESC LIMIT 10""",
    # Q11: two-column group by
    """SELECT "MobilePhone", "MobilePhoneModel", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "MobilePhoneModel" <> '' GROUP BY "MobilePhone", "MobilePhoneModel" ORDER BY u DESC LIMIT 10""",
    # Q12: search phrase group by
    """SELECT "SearchPhrase", COUNT(*) AS c FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10""",
    # Q13: search phrase count distinct
    """SELECT "SearchPhrase", COUNT(DISTINCT "UserID") AS u FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY u DESC LIMIT 10""",
    # Q14: search engine + phrase group by
    """SELECT "SearchEngineID", "SearchPhrase", COUNT(*) AS c FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchEngineID", "SearchPhrase" ORDER BY c DESC LIMIT 10""",
    # Q15: user id group by
    """SELECT "UserID", COUNT(*) FROM hits GROUP BY "UserID" ORDER BY COUNT(*) DESC LIMIT 10""",
    # Q16: user + search phrase ordered
    """SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10""",
    # Q17: user + search phrase unordered
    """SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase" LIMIT 10""",
    # Q18: extract minute + three-way group by
    """SELECT "UserID", extract(minute FROM "EventTime") AS m, "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", m, "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10""",
    # Q19: point lookup
    """SELECT "UserID" FROM hits WHERE "UserID" = 435090932899640449""",
    # Q20: LIKE string scan
    """SELECT COUNT(*) FROM hits WHERE "URL" LIKE '%google%'""",
    # Q21: LIKE + group by
    """SELECT "SearchPhrase", MIN("URL"), COUNT(*) AS c FROM hits WHERE "URL" LIKE '%google%' AND "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10""",
    # Q22: LIKE + NOT LIKE + count distinct
    """SELECT "SearchPhrase", MIN("URL"), MIN("Title"), COUNT(*) AS c, COUNT(DISTINCT "UserID") FROM hits WHERE "Title" LIKE '%Google%' AND "URL" NOT LIKE '%.google.%' AND "SearchPhrase" <> '' GROUP BY "SearchPhrase" ORDER BY c DESC LIMIT 10""",
    # Q23: SELECT * with LIKE + ORDER BY
    """SELECT * FROM hits WHERE "URL" LIKE '%google%' ORDER BY "EventTime" LIMIT 10""",
    # Q24: ORDER BY timestamp
    """SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY "EventTime" LIMIT 10""",
    # Q25: ORDER BY text
    """SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY "SearchPhrase" LIMIT 10""",
    # Q26: ORDER BY two columns
    """SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY "EventTime", "SearchPhrase" LIMIT 10""",
    # Q27: AVG string length + HAVING
    """SELECT "CounterID", AVG(length("URL")) AS l, COUNT(*) AS c FROM hits WHERE "URL" <> '' GROUP BY "CounterID" HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25""",
    # Q28: REGEXP_REPLACE + HAVING
    """SELECT REGEXP_REPLACE("Referer", '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(length("Referer")) AS l, COUNT(*) AS c, MIN("Referer") FROM hits WHERE "Referer" <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25""",
    # Q29: wide SUM (90 expressions)
    f"""SELECT {_Q29_SUMS} FROM hits""",
    # Q30: multi-col group by + boolean SUM
    """SELECT "SearchEngineID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"::int), AVG("ResolutionWidth") FROM hits WHERE "SearchPhrase" <> '' GROUP BY "SearchEngineID", "ClientIP" ORDER BY c DESC LIMIT 10""",
    # Q31: high-cardinality group by + boolean SUM
    """SELECT "WatchID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"::int), AVG("ResolutionWidth") FROM hits WHERE "SearchPhrase" <> '' GROUP BY "WatchID", "ClientIP" ORDER BY c DESC LIMIT 10""",
    # Q32: high-cardinality group by no filter
    """SELECT "WatchID", "ClientIP", COUNT(*) AS c, SUM("IsRefresh"::int), AVG("ResolutionWidth") FROM hits GROUP BY "WatchID", "ClientIP" ORDER BY c DESC LIMIT 10""",
    # Q33: URL group by
    """SELECT "URL", COUNT(*) AS c FROM hits GROUP BY "URL" ORDER BY c DESC LIMIT 10""",
    # Q34: constant column + URL group by
    """SELECT 1, "URL", COUNT(*) AS c FROM hits GROUP BY 1, "URL" ORDER BY c DESC LIMIT 10""",
    # Q35: expression group by
    """SELECT "ClientIP", "ClientIP" - 1, "ClientIP" - 2, "ClientIP" - 3, COUNT(*) AS c FROM hits GROUP BY "ClientIP", "ClientIP" - 1, "ClientIP" - 2, "ClientIP" - 3 ORDER BY c DESC LIMIT 10""",
    # Q36: date range + boolean filter
    """SELECT "URL", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND NOT "DontCountHits" AND NOT "IsRefresh" AND "URL" <> '' GROUP BY "URL" ORDER BY PageViews DESC LIMIT 10""",
    # Q37: title + date range
    """SELECT "Title", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND NOT "DontCountHits" AND NOT "IsRefresh" AND "Title" <> '' GROUP BY "Title" ORDER BY PageViews DESC LIMIT 10""",
    # Q38: boolean filters + OFFSET
    """SELECT "URL", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND NOT "IsRefresh" AND "IsLink" AND NOT "IsDownload" GROUP BY "URL" ORDER BY PageViews DESC LIMIT 10 OFFSET 1000""",
    # Q39: CASE + multi-col group by + OFFSET
    """SELECT "TraficSourceID", "SearchEngineID", "AdvEngineID", CASE WHEN ("SearchEngineID" = 0 AND "AdvEngineID" = 0) THEN "Referer" ELSE '' END AS Src, "URL" AS Dst, COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND NOT "IsRefresh" GROUP BY "TraficSourceID", "SearchEngineID", "AdvEngineID", Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000""",
    # Q40: hash filter + IN list
    """SELECT "URLHash", "EventDate", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND NOT "IsRefresh" AND "TraficSourceID" IN (-1, 6) AND "RefererHash" = 3594120000172545465 GROUP BY "URLHash", "EventDate" ORDER BY PageViews DESC LIMIT 10 OFFSET 100""",
    # Q41: hash point filter + large OFFSET
    """SELECT "WindowClientWidth", "WindowClientHeight", COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-01' AND "EventDate" <= '2013-07-31' AND NOT "IsRefresh" AND NOT "DontCountHits" AND "URLHash" = 2868770270353813622 GROUP BY "WindowClientWidth", "WindowClientHeight" ORDER BY PageViews DESC LIMIT 10 OFFSET 10000""",
    # Q42: DATE_TRUNC + narrow time range
    """SELECT DATE_TRUNC('minute', "EventTime") AS M, COUNT(*) AS PageViews FROM hits WHERE "CounterID" = 62 AND "EventDate" >= '2013-07-14' AND "EventDate" <= '2013-07-15' AND NOT "IsRefresh" AND NOT "DontCountHits" GROUP BY DATE_TRUNC('minute', "EventTime") ORDER BY DATE_TRUNC('minute', "EventTime") LIMIT 10 OFFSET 1000""",
]


def _pg_to_duckdb(q):
    """Convert PG-syntax query to DuckDB-syntax for lake.main.hits.

    Transformations: table name, remove PG identifier quoting, remove
    ::int casts (DuckDB SUMs booleans natively), length -> STRLEN.
    """
    q = q.replace(' FROM hits', ' FROM lake.main.hits')
    q = q.replace('::int', '')
    q = q.replace('"', '')
    q = q.replace('length(', 'STRLEN(')
    return q


QUERIES_DUCKDB = [_pg_to_duckdb(q) for q in QUERIES_PG]

# -- Parquet column indices that need type conversion ---------------------
# (parquet stores these as raw integers)

TIMESTAMP_COL_INDICES = frozenset([4, 45, 64])   # EventTime, ClientEventTime, LocalEventTime
DATE_COL_INDICES = frozenset([5])                  # EventDate
BOOLEAN_COL_INDICES = frozenset([                  # 20 boolean columns
    1, 3, 15, 30, 31, 32, 41, 52, 53, 54,
    58, 59, 60, 61, 62, 101,
])

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_ORD = _EPOCH_DATE.toordinal()


def convert_batch_params(columns):
    """Convert pyarrow column lists (to_pylist()) to proper Python types
    for psycopg ClientCursor: epoch-seconds -> datetime, days -> date,
    0/1 -> bool."""
    params = []
    for i, col_values in enumerate(columns):
        if i in TIMESTAMP_COL_INDICES:
            params.append([
                datetime.fromtimestamp(v, tz=timezone.utc) if v is not None else None
                for v in col_values
            ])
        elif i in DATE_COL_INDICES:
            params.append([
                date.fromordinal(_EPOCH_ORD + v) if v is not None else None
                for v in col_values
            ])
        elif i in BOOLEAN_COL_INDICES:
            params.append([
                bool(v) if v is not None else None for v in col_values
            ])
        else:
            params.append(col_values)
    return params
