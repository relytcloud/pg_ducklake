duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 9de3296f40ed03e8e063394887f0d6a46144e847
)

# Optional Apache Arrow support: bundle paleolimbot/duckdb-nanoarrow so
# read_arrow() works on .arrow / .arrows files (and streams via httpfs).
# Toggled via the WITH_NANOARROW make/cmake variable; default OFF.
if(WITH_NANOARROW)
    duckdb_extension_load(nanoarrow
        GIT_URL https://github.com/paleolimbot/duckdb-nanoarrow
        GIT_TAG 42e4199a67c4cd0789087562a025e87e7130fdc3
    )
endif()
