# Pull in the kernel sources + duckdb-build wiring from the repo root.
ROOT_DIR := $(abspath $(CURDIR)/../..)
EXTENSION_CONFIGS := $(CURDIR)/pg_vortex_extensions.cmake
include $(ROOT_DIR)/Makefile

MODULE_big = pg_vortex
EXTENSION = pg_vortex
DATA = $(wildcard pg_vortex--*.sql)

SRCS = $(wildcard src/*.cpp src/*/*.cpp)
OBJS = $(SRCS:.cpp=.o)
C_SRCS = $(wildcard src/*.c src/*/*.c)
OBJS += $(C_SRCS:.c=.o)

# Kernel objects compiled in-place at $(ROOT_DIR)/libpgduckdb/.
OBJS += $(PGDDB_OBJS)

# -lssl -lcrypto and --exclude-libs,ALL: see pg_duckdb's Makefile.
PG_VORTEX_LINK_FLAGS = $(FULL_DUCKDB_LIB) -lcurl -lssl -lcrypto -lstdc++ -llz4
ifeq ($(shell uname -s),Linux)
	PG_VORTEX_LINK_FLAGS += -Wl,--exclude-libs,ALL
# Bind the bundled kernel to this .so, not a copy in another loaded extension.
	PG_VORTEX_LINK_FLAGS += -Wl,-Bsymbolic
endif
# duckdb-vortex's Rust deps need macOS Security/CoreFoundation; the
# object-extraction bundle loses cmake's link metadata, so add them here.
ifeq ($(shell uname -s),Darwin)
	PG_VORTEX_LINK_FLAGS += -framework Security -framework CoreFoundation
endif

ERROR_ON_WARNING ?=
ifeq ($(ERROR_ON_WARNING), 1)
	ERROR_ON_WARNING_FLAG = -Werror
else
	ERROR_ON_WARNING_FLAG =
endif

COMPILER_FLAGS = -Wno-sign-compare -Wshadow -Wswitch -Wunused-parameter -Wunreachable-code -Wno-unknown-pragmas -Wall -Wextra ${ERROR_ON_WARNING_FLAG}

override PG_CPPFLAGS += -Iinclude $(PGDDB_INCLUDE) $(PGDDB_DUCKDB_INCLUDE) -isystem $(INCLUDEDIR_SERVER) ${COMPILER_FLAGS}
override PG_CXXFLAGS += -std=c++17 ${COMPILER_FLAGS} -Wno-register
# Vendored pg_ruleutils_*.c are not declaration-after-statement clean.
override PG_CFLAGS += -Wno-declaration-after-statement

SHLIB_LINK += $(PG_VORTEX_LINK_FLAGS)

REGRESS = basic
REGRESS_OPTS = --temp-instance=./tmp_check --temp-config=$(CURDIR)/regression.conf

include $(ROOT_DIR)/Makefile.pgxs

# First-time builds must init the duckdb submodule before compiling.
$(OBJS): $(PGDDB_DIR)/.git/modules/duckdb/HEAD

$(shlib): $(FULL_DUCKDB_LIB) $(OBJS)

clean-all: clean clean-duckdb
