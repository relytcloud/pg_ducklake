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

DUCKDB_BUILD_TYPE ?= release
DUCKLAKE_BUILD_DIR = $(DUCKLAKE_DIR)/build/$(DUCKDB_BUILD_TYPE)
DUCKLAKE_STATIC_LIB = $(DUCKLAKE_BUILD_DIR)/extension/ducklake/libducklake_extension.a

# ---------------------------------------------------------------------------
# Include paths
# ---------------------------------------------------------------------------

# DuckDB + ducklake headers (for the DuckDB-facing bridge TU only)
DUCKDB_INCLUDES = \
	-isystem $(DUCKDB_SRC_DIR)/src/include \
	-isystem $(DUCKDB_SRC_DIR)/third_party/re2 \
	-I$(DUCKLAKE_DIR)/src/include

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

# Force-load the ducklake static library so all symbols are available to
# DuckDB's LoadStaticExtension<DucklakeExtension>() registration.
# Without this, the linker would drop .o files not directly referenced.
ifeq ($(shell uname -s), Darwin)
	SHLIB_LINK += -Wl,-force_load,$(DUCKLAKE_STATIC_LIB)
else
	SHLIB_LINK += -Wl,--whole-archive $(DUCKLAKE_STATIC_LIB) -Wl,--no-whole-archive
endif

# Link against libduckdb from PG_LIB (installed by pg_duckdb)
SHLIB_LINK += -Wl,-rpath,$(PG_LIB)/ -L$(PG_LIB) -lduckdb -lstdc++

# Allow pg_duckdb symbols to resolve at load time
ifeq ($(shell uname -s), Darwin)
	SHLIB_LINK += -Wl,-undefined,dynamic_lookup
endif

# ---------------------------------------------------------------------------
# PGXS
# ---------------------------------------------------------------------------
include Makefile.global

# ---------------------------------------------------------------------------
# Build ducklake using its own cmake-based build system
# ---------------------------------------------------------------------------
.PHONY: ducklake clean-ducklake

ducklake: $(DUCKLAKE_STATIC_LIB)

$(DUCKLAKE_STATIC_LIB):
	DUCKDB_SRCDIR=$(DUCKDB_SRC_DIR) \
	CMAKE_VARS="-DBUILD_SHELL=0 -DBUILD_PYTHON=0 -DBUILD_UNITTESTS=0" \
	DISABLE_SANITIZER=1 \
	$(MAKE) -C $(DUCKLAKE_DIR) $(DUCKDB_BUILD_TYPE)

clean-ducklake:
	rm -rf $(DUCKLAKE_DIR)/build

# ---------------------------------------------------------------------------
# Compilation rules
# ---------------------------------------------------------------------------

# DuckDB bridge TU: compiled with DuckDB+ducklake headers, NOT PG headers.

# PG-facing TU uses the default PGXS pattern rule (includes PG server headers).
# Our PG_CPPFLAGS += $(LOCAL_INCLUDES) adds the bridge header path.

# Shared library depends on ducklake static lib
$(shlib): $(DUCKLAKE_STATIC_LIB)
