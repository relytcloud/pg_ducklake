duckdb_extension_load(json)
duckdb_extension_load(icu)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 9c7d34977b10346d0b4cbbde5df807d1dab0b2bf
)

duckdb_extension_load(ducklake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/ducklake
)

# Enable postgres_scanner extension when ENABLE_POSTGRES_SCANNER environment variable is set
if($ENV{ENABLE_POSTGRES_SCANNER})
    duckdb_extension_load(postgres_scanner
        DONT_LINK
        GIT_URL https://github.com/duckdb/duckdb-postgres
        GIT_TAG f012a4f99cea1d276d1787d0dc84b1f1a0e0f0b2
    )
endif()
