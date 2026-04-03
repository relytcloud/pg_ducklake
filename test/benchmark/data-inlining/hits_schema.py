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

# -- Queries --------------------------------------------------------------

# ClickBench queries selected to stress PG's row executor:
#  Q0  simple count (baseline)
#  Q4  COUNT DISTINCT on high-cardinality BIGINT
#  Q9  multi-agg + COUNT DISTINCT + GROUP BY
#  Q12 string filter + high-cardinality GROUP BY
#  Q16 high-cardinality UserID GROUP BY
#  Q20 LIKE string scan
#  Q29 wide multi-expression SUM (90 SUMs, touches many columns)
#  Q33 high-cardinality text GROUP BY (URL)
QUERIES_PG = [
    # Q0: simple count
    'SELECT COUNT(*) FROM hits',
    # Q4: COUNT DISTINCT on high-cardinality column
    'SELECT COUNT(DISTINCT "UserID") FROM hits',
    # Q9: multi-agg with COUNT DISTINCT + GROUP BY
    ('SELECT "RegionID", SUM("AdvEngineID"), COUNT(*) AS c, '
     'AVG("ResolutionWidth"), COUNT(DISTINCT "UserID") FROM hits '
     'GROUP BY "RegionID" ORDER BY c DESC LIMIT 10'),
    # Q12: string filter + high-cardinality text GROUP BY
    ('SELECT "SearchPhrase", COUNT(*) AS c FROM hits '
     "WHERE \"SearchPhrase\" <> '' GROUP BY \"SearchPhrase\" ORDER BY c DESC LIMIT 10"),
    # Q16: high-cardinality GROUP BY on BIGINT
    ('SELECT "UserID", COUNT(*) FROM hits '
     'GROUP BY "UserID" ORDER BY COUNT(*) DESC LIMIT 10'),
    # Q20: LIKE string scan
    "SELECT COUNT(*) FROM hits WHERE \"URL\" LIKE '%google%'",
    # Q29: wide multi-expression SUM (touches many numeric columns)
    ('SELECT SUM("ResolutionWidth"), SUM("ResolutionWidth" + 1), '
     'SUM("ResolutionWidth" + 2), SUM("ResolutionWidth" + 3), '
     'SUM("ResolutionHeight"), SUM("ResolutionHeight" + 1), '
     'SUM("ResolutionHeight" + 2), SUM("ResolutionHeight" + 3), '
     'SUM("ResolutionDepth"), SUM("ResolutionDepth" + 1), '
     'SUM("FlashMajor"), SUM("FlashMajor" + 1), '
     'SUM("FlashMinor"), SUM("FlashMinor" + 1), '
     'SUM("NetMajor"), SUM("NetMajor" + 1), '
     'SUM("NetMinor"), SUM("NetMinor" + 1), '
     'SUM("UserAgentMajor"), SUM("UserAgentMajor" + 1), '
     'SUM("WindowClientWidth"), SUM("WindowClientWidth" + 1), '
     'SUM("WindowClientHeight"), SUM("WindowClientHeight" + 1) '
     'FROM hits'),
    # Q33: high-cardinality text GROUP BY
    ('SELECT "URL", COUNT(*) AS c FROM hits '
     'GROUP BY "URL" ORDER BY c DESC LIMIT 10'),
]

# DuckDB is case-insensitive; queries target lake.main.hits.
QUERIES_DUCKDB = [
    'SELECT COUNT(*) FROM lake.main.hits',
    'SELECT COUNT(DISTINCT UserID) FROM lake.main.hits',
    ('SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, '
     'AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM lake.main.hits '
     'GROUP BY RegionID ORDER BY c DESC LIMIT 10'),
    ('SELECT SearchPhrase, COUNT(*) AS c FROM lake.main.hits '
     "WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"),
    ('SELECT UserID, COUNT(*) FROM lake.main.hits '
     'GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10'),
    "SELECT COUNT(*) FROM lake.main.hits WHERE URL LIKE '%google%'",
    ('SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), '
     'SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), '
     'SUM(ResolutionHeight), SUM(ResolutionHeight + 1), '
     'SUM(ResolutionHeight + 2), SUM(ResolutionHeight + 3), '
     'SUM(ResolutionDepth), SUM(ResolutionDepth + 1), '
     'SUM(FlashMajor), SUM(FlashMajor + 1), '
     'SUM(FlashMinor), SUM(FlashMinor + 1), '
     'SUM(NetMajor), SUM(NetMajor + 1), '
     'SUM(NetMinor), SUM(NetMinor + 1), '
     'SUM(UserAgentMajor), SUM(UserAgentMajor + 1), '
     'SUM(WindowClientWidth), SUM(WindowClientWidth + 1), '
     'SUM(WindowClientHeight), SUM(WindowClientHeight + 1) '
     'FROM lake.main.hits'),
    ('SELECT URL, COUNT(*) AS c FROM lake.main.hits '
     'GROUP BY URL ORDER BY c DESC LIMIT 10'),
]

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
