# Root Makefile: libpgduckdb kernel source list, DuckDB submodule build, and
# top-level targets. Extension Makefiles set ROOT_DIR and `include` this file,
# then append PGDDB_OBJS to OBJS to bundle the kernel into their .so.

# `all` comes from PGXS in extension dirs and from the top-level section below.
.DEFAULT_GOAL := all

# --- libpgduckdb kernel ---

PGDDB_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PGDDB_INCLUDE := -I$(PGDDB_DIR)/libpgduckdb/include
PGDDB_DUCKDB_INCLUDE := -isystem $(PGDDB_DIR)/duckdb/src/include \
                        -isystem $(PGDDB_DIR)/duckdb/third_party/re2
PGDDB_CPP_SRCS := $(wildcard $(PGDDB_DIR)/libpgduckdb/*.cpp $(PGDDB_DIR)/libpgduckdb/*/*.cpp $(PGDDB_DIR)/libpgduckdb/*/*/*.cpp)
PGDDB_C_SRCS := $(wildcard $(PGDDB_DIR)/libpgduckdb/*.c $(PGDDB_DIR)/libpgduckdb/*/*.c $(PGDDB_DIR)/libpgduckdb/*/*/*.c)
PGDDB_SRCS := $(PGDDB_CPP_SRCS) $(PGDDB_C_SRCS)
PGDDB_OBJS := $(PGDDB_CPP_SRCS:.cpp=.o) $(PGDDB_C_SRCS:.c=.o)

# --- DuckDB submodule build ---
# Each extension's EXTENSION_CONFIGS gets its own libduckdb_bundle.a in a
# tagged build dir, statically linked into the extension's .so.

DUCKDB_GEN ?= ninja
DUCKDB_VERSION = v1.5.4

# Optional Apache Arrow support via the bundled nanoarrow DuckDB extension.
# Default OFF -- when enabled, the cmake EXTENSION_CONFIGS file selects the
# nanoarrow extension and PG_DUCKDB_WITH_NANOARROW is defined for the C++
# compile so read_arrow() routes to DuckDB rather than ereport'ing.
WITH_NANOARROW ?= 0

# Escape hatch for extra cmake vars to forward to the DuckDB build (e.g. to
# pin SDK libcurl, or to opt in to a third-party extension).
EXTRA_DUCKDB_CMAKE_VARS ?=

ifeq ($(WITH_NANOARROW),1)
    EXTRA_DUCKDB_CMAKE_VARS += -DWITH_NANOARROW=ON
endif

DUCKDB_CMAKE_VARS = -DCXX_EXTRA=-fvisibility=default -DBUILD_SHELL=0 -DBUILD_PYTHON=0 -DBUILD_UNITTESTS=0 -DOVERRIDE_GIT_DESCRIBE=$(DUCKDB_VERSION) $(EXTRA_DUCKDB_CMAKE_VARS)
DUCKDB_DISABLE_ASSERTIONS ?= 0

# Optional compiler cache (e.g. sccache) for the DuckDB build.
COMPILER_LAUNCHER ?=
ifneq ($(COMPILER_LAUNCHER),)
	DUCKDB_CMAKE_VARS += -DCMAKE_C_COMPILER_LAUNCHER=$(COMPILER_LAUNCHER) -DCMAKE_CXX_COMPILER_LAUNCHER=$(COMPILER_LAUNCHER)
endif

DUCKDB_BUILD_CXX_FLAGS=
ifeq ($(DUCKDB_BUILD), Debug)
	DUCKDB_BUILD_CXX_FLAGS = -g -O0 -D_GLIBCXX_ASSERTIONS
	DUCKDB_BUILD_TYPE = debug
	DUCKDB_CMAKE_BUILD_TYPE = Debug
	DUCKDB_EXTRA_CMAKE_VARS = -DDEBUG_MOVE=1
else
	DUCKDB_BUILD_TYPE = release
	DUCKDB_CMAKE_BUILD_TYPE = Release
endif
ifeq ($(DUCKDB_DISABLE_ASSERTIONS), 1)
	DUCKDB_EXTRA_CMAKE_VARS += -DDISABLE_ASSERTIONS=1
endif
ifeq ($(DUCKDB_GEN), ninja)
	DUCKDB_CMAKE_GENERATOR := -G Ninja
	DUCKDB_CMAKE_FORCE_COLOR := -DFORCE_COLORED_OUTPUT=1
endif

# Absolute path to the extension's *_extensions.cmake; empty = no third-party
# duckdb extensions baked in.
EXTENSION_CONFIGS ?=

DUCKDB_BUILD_TAG := $(if $(EXTENSION_CONFIGS),$(basename $(notdir $(EXTENSION_CONFIGS)))-$(shell shasum -a 256 '$(EXTENSION_CONFIGS)' 2>/dev/null | cut -c1-8),default)$(if $(filter 1,$(WITH_NANOARROW)),-arrow,)
DUCKDB_BUILD_DIR := $(PGDDB_DIR)/duckdb/build/$(DUCKDB_BUILD_TYPE)-$(DUCKDB_BUILD_TAG)
FULL_DUCKDB_LIB = $(DUCKDB_BUILD_DIR)/libduckdb_bundle.a

.PHONY: duckdb clean-duckdb

duckdb: $(FULL_DUCKDB_LIB)

$(PGDDB_DIR)/.git/modules/duckdb/HEAD:
	git -C $(PGDDB_DIR) submodule update --init --recursive

# The bundling steps inline duckdb's bundle-setup / bundle-library-o targets
# (duckdb's own hard-code build/release/, so per-extension dirs can't use
# them). EXTENSION_BUNDLE_EXCLUDE keeps force-load'd extensions (e.g.
# pg_ducklake's ducklake) out of the bundle to avoid duplicate symbols.
$(FULL_DUCKDB_LIB): $(PGDDB_DIR)/.git/modules/duckdb/HEAD $(EXTENSION_CONFIGS)
	mkdir -p $(DUCKDB_BUILD_DIR)/vcpkg_installed
	cmake -S $(PGDDB_DIR)/duckdb -B $(DUCKDB_BUILD_DIR) \
		$(DUCKDB_CMAKE_GENERATOR) $(DUCKDB_CMAKE_FORCE_COLOR) \
		-DENABLE_SANITIZER=FALSE -DENABLE_UBSAN=0 \
		$(DUCKDB_CMAKE_VARS) $(DUCKDB_EXTRA_CMAKE_VARS) \
		-DDUCKDB_EXTENSION_CONFIGS="$(EXTENSION_CONFIGS)" \
		-DLOCAL_EXTENSION_REPO="" \
		-DOVERRIDE_GIT_DESCRIBE="$(DUCKDB_VERSION)" \
		-DDUCKDB_EXPLICIT_VERSION="" \
		-DCMAKE_BUILD_TYPE=$(DUCKDB_CMAKE_BUILD_TYPE)
	cmake --build $(DUCKDB_BUILD_DIR) --config $(DUCKDB_CMAKE_BUILD_TYPE)
	rm -f $(DUCKDB_BUILD_DIR)/libduckdb_bundle.a
	rm -rf $(DUCKDB_BUILD_DIR)/bundle
	mkdir -p $(DUCKDB_BUILD_DIR)/bundle
	cp $(DUCKDB_BUILD_DIR)/src/libduckdb_static.a $(DUCKDB_BUILD_DIR)/bundle/.
	cp $(DUCKDB_BUILD_DIR)/third_party/*/libduckdb_*.a $(DUCKDB_BUILD_DIR)/bundle/.
	find $(DUCKDB_BUILD_DIR)/extension -maxdepth 2 \
		\( -name 'lib*_extension.a' -o -name 'lib*_duckdb.a' \
		   -o -name 'libduckdb_generated_extension_loader.a' \) \
		$(foreach n,$(EXTENSION_BUNDLE_EXCLUDE),-not -name 'lib$(n)_extension.a' -not -name 'lib$(n)_duckdb.a') \
		-exec cp {} $(DUCKDB_BUILD_DIR)/bundle/. \;
	find $(DUCKDB_BUILD_DIR)/vcpkg_installed -name '*.a' -exec cp {} $(DUCKDB_BUILD_DIR)/bundle/. \;
	# Extensions vendored via cmake FetchContent (e.g. nanoarrow's libnanoarrow,
	# libnanoarrow_ipc, libflatccrt) land under _deps/; bundle their archives too.
	if [ -d $(DUCKDB_BUILD_DIR)/_deps ]; then \
		find $(DUCKDB_BUILD_DIR)/_deps -name 'lib*.a' -exec cp {} $(DUCKDB_BUILD_DIR)/bundle/. \;; \
	fi
	cd $(DUCKDB_BUILD_DIR)/bundle && \
		find . -name '*.a' -exec mkdir -p {}.objects \; -exec mv {} {}.objects \; && \
		find . -name '*.a' -execdir $(AR) -x {} \;
	cd $(DUCKDB_BUILD_DIR)/bundle && echo ./*/*.o | xargs $(AR) cr ../libduckdb_bundle.a

clean-duckdb:
	rm -rf $(PGDDB_DIR)/duckdb/build

# --- Top-level targets (skipped when included from an extension) ---

ifndef ROOT_DIR

# `make` / `make install` etc. from the repo root operate on pg_ducklake.
PG_DUCKLAKE_TARGETS := all install installcheck check-regression check-isolation check-e2e format check-format clean clean-all

.PHONY: $(PG_DUCKLAKE_TARGETS)
$(PG_DUCKLAKE_TARGETS):
	$(MAKE) -C pg_ducklake $@

# make <extension>/<target>
pg_duckdb/% pg_ducklake/% examples/%:
	$(MAKE) -C $(@D) $(@F)

endif
