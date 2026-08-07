ROOT_DIR := $(abspath $(CURDIR)/..)
EXTENSION_CONFIGS := $(CURDIR)/pg_ducklake_extensions.cmake
EXTENSION_BUNDLE_EXCLUDE := ducklake postgres_scanner
include $(ROOT_DIR)/Makefile

MODULE_big = pg_ducklake
EXTENSION = pg_ducklake
DATA = pg_ducklake.control $(wildcard sql/pg_ducklake--*.sql)

SRCS = $(wildcard src/*.cpp src/*/*.cpp)
OBJS = $(SRCS:.cpp=.o)
C_SRCS = $(wildcard src/*.c src/*/*.c)
OBJS += $(C_SRCS:.c=.o)

# Kernel objects compiled in-place at $(ROOT_DIR)/libpgduckdb/.
OBJS += $(PGDDB_OBJS)

# Vendored ducklake: pristine community submodule + ordered patches
# (third_party/ducklake-NNN-*.patch) applied at build time, stamp-guarded.
DUCKLAKE_DIR := $(CURDIR)/third_party/ducklake
DUCKLAKE_PATCHES := $(sort $(wildcard $(CURDIR)/third_party/ducklake-*.patch))
DUCKLAKE_STAMP := $(DUCKLAKE_DIR)/.patched
DUCKLAKE_SUBMODULE_HEAD := $(ROOT_DIR)/.git/modules/pg_ducklake/third_party/ducklake/HEAD
DUCKLAKE_INCLUDE = -I$(DUCKLAKE_DIR)/src/include

override PG_CPPFLAGS += -Iinclude $(PGDDB_INCLUDE) $(PGDDB_DUCKDB_INCLUDE) $(DUCKLAKE_INCLUDE) -isystem $(INCLUDEDIR_SERVER)
override PG_CXXFLAGS += -std=c++17 -Wno-register

DUCKLAKE_EXT_LIB := $(DUCKDB_BUILD_DIR)/extension/ducklake/libducklake_extension.a

POSTGRES_SCANNER_EXT_LIB := $(DUCKDB_BUILD_DIR)/extension/postgres_scanner/libpostgres_scanner_extension.a

# DuckLake needs CRoaring (deletion vectors). CI provides a vcpkg toolchain via
# VCPKG_TOOLCHAIN_PATH, with the vendored ducklake dir as the manifest dir;
# without one (local macOS) find_package falls back to homebrew croaring.
ifneq ($(VCPKG_TOOLCHAIN_PATH),)
DUCKDB_EXTRA_CMAKE_VARS += -DCMAKE_TOOLCHAIN_FILE='$(VCPKG_TOOLCHAIN_PATH)' -DVCPKG_BUILD=1
DUCKDB_EXTRA_CMAKE_VARS += -DVCPKG_MANIFEST_DIR='$(DUCKLAKE_DIR)'
ifneq ($(VCPKG_TARGET_TRIPLET),)
DUCKDB_EXTRA_CMAKE_VARS += -DVCPKG_TARGET_TRIPLET='$(VCPKG_TARGET_TRIPLET)'
endif
# vcpkg manifest mode installs static deps under the duckdb build dir.
ROARING_LIB_DIR := $(DUCKDB_BUILD_DIR)/vcpkg_installed/$(VCPKG_TARGET_TRIPLET)/lib
endif

PG_DUCKLAKE_LINK_FLAGS = $(FULL_DUCKDB_LIB)
ifeq ($(shell uname -s),Darwin)
	PG_DUCKLAKE_LINK_FLAGS += -Wl,-force_load,$(DUCKLAKE_EXT_LIB)
	PG_DUCKLAKE_LINK_FLAGS += -Wl,-force_load,$(POSTGRES_SCANNER_EXT_LIB)
else
	PG_DUCKLAKE_LINK_FLAGS += -Wl,--whole-archive $(DUCKLAKE_EXT_LIB) $(POSTGRES_SCANNER_EXT_LIB) -Wl,--no-whole-archive
endif
# -lroaring must come AFTER the whole-archive'd extension archives: GNU ld
# resolves static archives in a single left-to-right pass.
PG_DUCKLAKE_LINK_FLAGS += -lssl -lcrypto -lstdc++ -llz4
ifneq ($(ROARING_LIB_DIR),)
	# Link the vcpkg static archive by exact path: the pgxs -L/usr/lib prefix
	# otherwise wins the search order and binds -lroaring to the ancient system
	# libroaring.so (0.2.66), which lacks symbols like roaring_free.
	PG_DUCKLAKE_LINK_FLAGS += $(ROARING_LIB_DIR)/libroaring.a
else
	PG_DUCKLAKE_LINK_FLAGS += -lroaring
endif
ifneq ($(shell uname -s),Darwin)
	PG_DUCKLAKE_LINK_FLAGS += -Wl,--exclude-libs,ALL
endif

SHLIB_LINK += $(PG_DUCKLAKE_LINK_FLAGS)

# `make clean` drops the patch stamp so the next build re-applies onto a fresh submodule reset.
EXTRA_CLEAN += $(DUCKLAKE_STAMP)

include $(ROOT_DIR)/Makefile.pgxs

# Non-recursive init: ducklake's inner submodules are only for its own CI.
$(DUCKLAKE_SUBMODULE_HEAD):
	git -C $(ROOT_DIR) submodule update --init pg_ducklake/third_party/ducklake

# Apply patches onto the pristine submodule; reset first so editing a patch
# re-applies from a clean base. The working tree is intentionally left dirty.
$(DUCKLAKE_STAMP): $(DUCKLAKE_PATCHES) $(DUCKLAKE_SUBMODULE_HEAD)
	git -C $(DUCKLAKE_DIR) checkout -q .
	git -C $(DUCKLAKE_DIR) clean -fdq src
	@for p in $(DUCKLAKE_PATCHES); do \
		echo "  ducklake patch: $$(basename $$p)"; \
		git -C $(DUCKLAKE_DIR) apply -p1 --whitespace=nowarn $$p || exit 1; \
	done
	touch $@

# Extension .o files and the duckdb build both consume patched ducklake source.
$(OBJS): $(PGDDB_DIR)/.git/modules/duckdb/HEAD $(DUCKLAKE_STAMP)
$(FULL_DUCKDB_LIB): $(DUCKLAKE_STAMP)

# Both archives are side effects of the duckdb cmake build; declaring that lets
# a clean build make duckdb first, and listing them as $(shlib) prereqs relinks
# the dylib when only the vendored ducklake source changed.
$(DUCKLAKE_EXT_LIB) $(POSTGRES_SCANNER_EXT_LIB): $(FULL_DUCKDB_LIB)

$(shlib): $(FULL_DUCKDB_LIB) $(DUCKLAKE_EXT_LIB) $(POSTGRES_SCANNER_EXT_LIB) $(OBJS)

FORMAT_FILES = $(shell find src/ include/ -name '*.cpp' -o -name '*.hpp' -o -name '*.h')

.PHONY: format check-format check-regression clean-regression check-isolation clean-isolation check-e2e clean-all

format:
	clang-format -i $(FORMAT_FILES)

check-format:
	clang-format --dry-run -Werror $(FORMAT_FILES)

installcheck: all install
	$(MAKE) check-regression check-isolation

check-regression:
	$(MAKE) -C test/regression check-regression

clean-regression:
	$(MAKE) -C test/regression clean-regression

check-isolation:
	$(MAKE) -C test/isolation check-isolation

# Needs `uv`; s3:// tests use dockerized MinIO and skip without docker
# (E2E_REQUIRE_S3=1 turns skips into failures).
# Usage: make check-e2e [TEST=<pytest -k expr>] [E2E_PYTEST_ARGS=...]
check-e2e: all install
	PG_CONFIG='$(PG_CONFIG)' uv run --project test/e2e -- pytest test/e2e $(if $(TEST),-k '$(TEST)') $(E2E_PYTEST_ARGS)

clean-isolation:
	$(MAKE) -C test/isolation clean-isolation

clean-all: clean clean-regression clean-isolation clean-duckdb
