.PHONY: clean-all lintcheck check-regression-duckdb clean-regression

# Pull in the kernel sources + duckdb-build wiring from the repo root.
ROOT_DIR := $(abspath $(CURDIR)/..)
# Extensions must set EXTENSION_CONFIGS BEFORE including the root Makefile so
# the $(FULL_DUCKDB_LIB) rule sees the right prereq.
EXTENSION_CONFIGS := $(CURDIR)/third_party/pg_duckdb_extensions.cmake
include $(ROOT_DIR)/Makefile

PG_DUCKDB_VERSION ?= $(shell git describe --always --dirty 2>/dev/null || echo "unknown")

MODULE_big = pg_duckdb
EXTENSION = pg_duckdb
DATA = pg_duckdb.control $(wildcard sql/pg_duckdb--*.sql)

SRCS = $(wildcard src/*.cpp src/*/*.cpp)
OBJS = $(subst .cpp,.o, $(SRCS))

C_SRCS = $(wildcard src/*.c src/*/*.c)
OBJS += $(subst .c,.o, $(C_SRCS))

# Kernel objects, compiled in-place at $(ROOT_DIR)/libpgduckdb/.
OBJS += $(PGDDB_OBJS)

# -lssl -lcrypto: the bundle's vendored httplib (in duckdb-httpfs) refs
# OpenSSL APIs directly. On Linux those come in transitively via libcurl;
# on macOS libcurl uses Secure Transport, so link explicitly.
PG_DUCKDB_LINK_FLAGS = $(FULL_DUCKDB_LIB) -lcurl -lssl -lcrypto -lstdc++ -llz4

# --exclude-libs,ALL hides symbols from every static archive we pull in
# (notably libduckdb_bundle.a) so the extension .so only exports its own
# UDFs + _PG_init. Prevents UDF-name collisions with bundled-extension
# C symbols as more extensions are added. Linux/GNU-ld only; macOS ld
# has no direct equivalent (the pg_vortex_version rename remains as a
# safety net there).
ifeq ($(shell uname -s),Linux)
	PG_DUCKDB_LINK_FLAGS += -Wl,--exclude-libs,ALL
endif

# Ensure -lstdc++fs is included for GCC 8 builds
CXX ?= c++
IS_GCC := $(shell $(CXX) --version 2>/dev/null | grep -q "Free Software Foundation" && echo true || echo false)
ifeq ($(IS_GCC),true)
  GCC_MAJOR := $(shell $(CXX) -dumpversion 2>/dev/null | cut -d. -f1)
  ifeq ($(GCC_MAJOR),8)
    PG_DUCKDB_LINK_FLAGS += -lstdc++fs
  endif
endif

ERROR_ON_WARNING ?=
ifeq ($(ERROR_ON_WARNING), 1)
	ERROR_ON_WARNING = -Werror
else
	ERROR_ON_WARNING =
endif

COMPILER_FLAGS=-Wno-sign-compare -Wshadow -Wswitch -Wunused-parameter -Wunreachable-code -Wno-unknown-pragmas -Wall -Wextra ${ERROR_ON_WARNING}

override PG_CPPFLAGS += -Iinclude $(PGDDB_INCLUDE) $(PGDDB_DUCKDB_INCLUDE) -isystem $(INCLUDEDIR_SERVER) ${COMPILER_FLAGS}
override PG_CXXFLAGS += -std=c++17 ${DUCKDB_BUILD_CXX_FLAGS} ${COMPILER_FLAGS} -Wno-register -Weffc++

# Opt-in Apache Arrow support (see root Makefile WITH_NANOARROW).
ifeq ($(WITH_NANOARROW),1)
    override PG_CPPFLAGS += -DPG_DUCKDB_WITH_NANOARROW=1
    override PG_CXXFLAGS += -DPG_DUCKDB_WITH_NANOARROW=1
endif
# Ignore declaration-after-statement warnings in our code. Postgres enforces
# this because their ancient style guide requires it, but we don't care. It
# would only apply to C files anyway, and we don't have many of those. The only
# ones that we do have are vendored in from Postgres (ruleutils), and allowing
# declarations to be anywhere is even a good thing for those as we can keep our
# changes to the vendored code in one place.
override PG_CFLAGS += -Wno-declaration-after-statement

SHLIB_LINK += $(PG_DUCKDB_LINK_FLAGS)

include $(ROOT_DIR)/Makefile.pgxs

# Only pass the version define to the one file that needs it, so that ccache
# doesn't invalidate everything on every commit.
src/pgduckdb.o: PG_CPPFLAGS += -DPG_DUCKDB_VERSION="\"$(PG_DUCKDB_VERSION)\""

# We need the DuckDB header files to build our own .o files. Depend on the
# submodule HEAD marker (defined in the root Makefile) so first-time builds
# init the submodule before compiling extension .o files.
$(OBJS): $(PGDDB_DIR)/.git/modules/duckdb/HEAD

COMPILE.cc.bc += $(PG_CPPFLAGS)
COMPILE.cxx.bc += $(PG_CXXFLAGS)

$(shlib): $(FULL_DUCKDB_LIB) $(OBJS)

NO_INSTALLCHECK = 1

PYTEST_CONCURRENCY = auto

check-regression-duckdb:
	$(MAKE) -C test/regression check-regression-duckdb

clean-regression:
	$(MAKE) -C test/regression clean-regression

# Specify AWS_REGION to make sure test output the same thing regardless of where they are run
installcheck: all install
	AWS_REGION=us-east-1 $(MAKE) check-regression-duckdb

pycheck: all install
	LD_LIBRARY_PATH=$(PG_LIBDIR):${LD_LIBRARY_PATH} pytest -n $(PYTEST_CONCURRENCY)

check: installcheck pycheck schedulecheck

schedulecheck:
	./scripts/schedule-check.sh

clean-all: clean clean-regression clean-duckdb

lintcheck:
	clang-tidy $(SRCS) -- -I$(INCLUDEDIR) -I$(INCLUDEDIR_SERVER) -Iinclude $(CPPFLAGS) -std=c++17
	ruff check

format:
	find src include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs git clang-format origin/main
	ruff format

format-all:
	find src include -iname '*.hpp' -o -iname '*.h' -o -iname '*.cpp' -o -iname '*.c' | xargs clang-format -i
	ruff format
