.PHONY: ducklake clean-ducklake pg_duckdb install_pg_duckdb

MODULE_big = pg_ducklake
EXTENSION = pg_ducklake
DATA = pg_ducklake.control $(wildcard sql/pg_ducklake--*.sql)

SRCS = $(wildcard src/*.cpp src/*/*.cpp)
OBJS = $(subst .cpp,.o, $(SRCS))

C_SRCS = $(wildcard src/*.c src/*/*.c)
OBJS += $(subst .c,.o, $(C_SRCS))

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------
PG_DUCKDB_DIR = $(CURDIR)/third_party/pg_duckdb
DUCKDB_SRC_DIR = $(PG_DUCKDB_DIR)/third_party/duckdb
DUCKLAKE_DIR = $(CURDIR)/third_party/ducklake
POSTGRES_SCANNER_DIR = $(CURDIR)/third_party/duckdb-postgres

DUCKDB_BUILD_TYPE ?= release
DUCKLAKE_BUILD_DIR = $(DUCKLAKE_DIR)/build/$(DUCKDB_BUILD_TYPE)
DUCKLAKE_STATIC_LIB = $(DUCKLAKE_BUILD_DIR)/extension/ducklake/libducklake_extension.a
POSTGRES_SCANNER_BUILD_DIR = $(POSTGRES_SCANNER_DIR)/build/$(DUCKDB_BUILD_TYPE)
POSTGRES_SCANNER_STATIC_LIB = $(POSTGRES_SCANNER_BUILD_DIR)/extension/postgres_scanner/libpostgres_scanner_extension.a

# ---------------------------------------------------------------------------
# Include paths
# ---------------------------------------------------------------------------

# DuckDB + ducklake + postgres_scanner headers (for the DuckDB-facing bridge TU only)
DUCKDB_INCLUDES = \
	-isystem $(DUCKDB_SRC_DIR)/src/include \
	-isystem $(DUCKDB_SRC_DIR)/third_party/re2 \
	-I$(DUCKLAKE_DIR)/src/include \
	-I$(POSTGRES_SCANNER_DIR)/src/include

# Project-local headers (bridge header)
LOCAL_INCLUDES = -I$(CURDIR)/include

# ---------------------------------------------------------------------------
# Compiler flags
# ---------------------------------------------------------------------------

# PG-facing TU: standard PGXS flags + bridge header path
override PG_CPPFLAGS += $(LOCAL_INCLUDES) $(DUCKDB_INCLUDES)
override PG_CXXFLAGS += -std=c++17

# DuckDB-facing TU: no PG headers allowed
DUCKDB_CXXFLAGS = -std=c++17 -fPIC

# ---------------------------------------------------------------------------
# Linker flags
# ---------------------------------------------------------------------------

# Force-load the ducklake and postgres_scanner static libraries so all symbols
# are available to DuckDB's LoadStaticExtension<T>() registration.
# Without this, the linker would drop .o files not directly referenced.
# postgres_scanner is built with -Bsymbolic to avoid libpq symbol conflicts.
ifeq ($(shell uname -s), Darwin)
	SHLIB_LINK += -Wl,-force_load,$(DUCKLAKE_STATIC_LIB)
	SHLIB_LINK += -Wl,-force_load,$(POSTGRES_SCANNER_STATIC_LIB)
else
	SHLIB_LINK += -Wl,--whole-archive $(DUCKLAKE_STATIC_LIB) $(POSTGRES_SCANNER_STATIC_LIB) -Wl,--no-whole-archive
endif

# Link against libduckdb from PG_LIB (installed by pg_duckdb)
SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -L$(PG_LIB) -lduckdb -lstdc++

# Allow pg_duckdb symbols to resolve at load time
ifeq ($(shell uname -s), Darwin)
	SHLIB_LINK += -Wl,-undefined,dynamic_lookup
else
	# -Bsymbolic prevents symbol conflicts between postgres_scanner and
	# the PostgreSQL backend (both define many of the same symbols)
	SHLIB_LINK += -Wl,-Bsymbolic
endif

# ---------------------------------------------------------------------------
# PGXS
# ---------------------------------------------------------------------------
include Makefile.global

installcheck: all install
	$(MAKE) -C test/regression check-regression

check-regression:
	$(MAKE) -C test/regression check-regression

clean-regression:
	$(MAKE) -C test/regression clean-regression

# ---------------------------------------------------------------------------
# Submodules
# ---------------------------------------------------------------------------
PG_DUCKDB_HEAD = .git/modules/third_party/pg_duckdb/HEAD
DUCKDB_HEAD = .git/modules/third_party/pg_duckdb/modules/third_party/duckdb/HEAD
DUCKLAKE_HEAD = .git/modules/third_party/ducklake/HEAD
POSTGRES_SCANNER_HEAD = .git/modules/third_party/duckdb-postgres/HEAD

$(PG_DUCKDB_HEAD):
	git submodule update --init --recursive third_party/pg_duckdb

$(DUCKDB_HEAD):
	git submodule update --init --recursive third_party/pg_duckdb

$(DUCKLAKE_HEAD):
	git submodule update --init --depth=1 third_party/ducklake

$(POSTGRES_SCANNER_HEAD):
	git submodule update --init --depth=1 third_party/duckdb-postgres

# ---------------------------------------------------------------------------
# pg_duckdb
# ---------------------------------------------------------------------------
PG_DUCKDB_TARGET = $(PG_DUCKDB_DIR)/pg_duckdb$(DLSUFFIX)

pg_duckdb: $(PG_DUCKDB_TARGET)

$(PG_DUCKDB_TARGET): $(PG_DUCKDB_HEAD)
	DUCKDB_BUILD_TYPE=$(DUCKDB_BUILD_TYPE) \
	$(MAKE) -C $(PG_DUCKDB_DIR)

install_pg_duckdb: pg_duckdb
	$(MAKE) -C $(PG_DUCKDB_DIR) install

# ---------------------------------------------------------------------------
# Build ducklake using its own cmake-based build system
# ---------------------------------------------------------------------------
ducklake: $(DUCKLAKE_STATIC_LIB)

$(DUCKLAKE_STATIC_LIB): $(DUCKLAKE_HEAD) $(DUCKDB_HEAD)
	DUCKDB_SRCDIR=$(DUCKDB_SRC_DIR) \
	CMAKE_VARS="-DBUILD_SHELL=0 -DBUILD_PYTHON=0 -DBUILD_UNITTESTS=0" \
	DISABLE_SANITIZER=1 \
	$(MAKE) -C $(DUCKLAKE_DIR) $(DUCKDB_BUILD_TYPE)

clean-ducklake:
	rm -rf $(DUCKLAKE_DIR)/build

# ---------------------------------------------------------------------------
# Build postgres_scanner with -Bsymbolic fix (PR #402)
# ---------------------------------------------------------------------------
.PHONY: postgres_scanner clean-postgres-scanner

postgres_scanner: $(POSTGRES_SCANNER_STATIC_LIB)

$(POSTGRES_SCANNER_STATIC_LIB): $(POSTGRES_SCANNER_HEAD) $(DUCKDB_HEAD)
	@echo "Building postgres_scanner with -Bsymbolic fix..."
	DUCKDB_SRCDIR=$(DUCKDB_SRC_DIR) \
	CMAKE_VARS="-DBUILD_SHELL=0 -DBUILD_PYTHON=0 -DBUILD_UNITTESTS=0" \
	BUILD_STATIC_EXTENSION=1 \
	DISABLE_SANITIZER=1 \
	$(MAKE) -C $(POSTGRES_SCANNER_DIR) $(DUCKDB_BUILD_TYPE)

clean-postgres-scanner:
	rm -rf $(POSTGRES_SCANNER_DIR)/build

# ---------------------------------------------------------------------------
# Compilation rules
# ---------------------------------------------------------------------------

# DuckDB bridge TU: compiled with DuckDB+ducklake headers, NOT PG headers.

# PG-facing TU uses the default PGXS pattern rule (includes PG server headers).
# Our PG_CPPFLAGS += $(LOCAL_INCLUDES) adds the bridge header path.

$(OBJS): $(PG_DUCKDB_HEAD)

# Shared library depends on ducklake and postgres_scanner static libs
$(shlib): $(DUCKLAKE_STATIC_LIB) $(POSTGRES_SCANNER_STATIC_LIB)
