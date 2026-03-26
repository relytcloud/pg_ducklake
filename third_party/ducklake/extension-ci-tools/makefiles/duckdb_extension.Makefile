# Reusable makefile for building out-of-tree extension with the DuckDB C++ based extension template
#
# Inputs
#   EXT_NAME          : Upper case string describing the name of the out-of-tree extension
#   EXT_CONFIG        : Path to the extension config file specifying how to build the extension
#   EXT_FLAGS         : Extra CMake flags to pass to the build
#   EXT_RELEASE_FLAGS : Extra CMake flags to pass to the release build
#   EXT_DEBUG_FLAGS   : Extra CMake flags to pass to the debug build
#   SKIP_TESTS        : Replaces all test targets with a NOP step
#
# 	BUILD_EXTENSION_TEST_DEPS   : Can be set to either `default`, `full`, or `none`. Toggles which extension dependencies are built
#	DEFAULT_TEST_EXTENSION_DEPS : `;`-separated list of extensions that are built in `default` and `full` mode
#	FULL_TEST_EXTENSION_DEPS    : `;`-separated list of extensions that are built in `full` mode

.PHONY: all clean clean-python format debug release pull update wasm_mvp wasm_eh wasm_threads test test_release test_debug test_reldebug test_release_internal test_debug_internal test_reldebug_internal set_duckdb_version set_duckdb_tag  output_distribution_matrix

all: release

TEST_PATH="/test/unittest"
DUCKDB_PATH="/duckdb"

DUCKDB_SRCDIR ?= "./duckdb/"

TESTS_BASE_DIRECTORY = "test/"

ifeq (${SUBSET_EXTENSIONS_TESTS},complete)
	TESTS_BASE_DIRECTORY=""
endif

#### Extension test dependency code
ifeq (${BUILD_EXTENSION_TEST_DEPS},)
	BUILD_EXTENSION_TEST_DEPS:=default
endif

ifeq (${BUILD_EXTENSION_TEST_DEPS},default)
	ifneq (${DEFAULT_TEST_EXTENSION_DEPS},)
		CORE_EXTENSIONS:=${CORE_EXTENSIONS};${DEFAULT_TEST_EXTENSION_DEPS}
	endif
else ifeq (${BUILD_EXTENSION_TEST_DEPS},full)
	ifneq (${DEFAULT_TEST_EXTENSION_DEPS},)
		CORE_EXTENSIONS:=${CORE_EXTENSIONS};${DEFAULT_TEST_EXTENSION_DEPS}
	endif
	ifneq (${FULL_TEST_EXTENSION_DEPS},)
		CORE_EXTENSIONS:=${CORE_EXTENSIONS};${FULL_TEST_EXTENSION_DEPS}
	endif
else ifneq (${BUILD_EXTENSION_TEST_DEPS}, none)
$(error Unknown option passed to BUILD_EXTENSION_TEST_DEPS variable: ${BUILD_EXTENSION_TEST_DEPS})
endif

#### Core extensions, allows easily building one of the core extensions
ifneq ($(CORE_EXTENSIONS),)
	CORE_EXTENSION_VAR:=-DCORE_EXTENSIONS="$(CORE_EXTENSIONS)"
endif

#### OSX config
OSX_BUILD_FLAG=
ifneq (${OSX_BUILD_ARCH}, "")
	OSX_BUILD_FLAG=-DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif

ifeq ("${OSX_BUILD_ARCH}", "arm64")
	RUST_FLAGS=-DRust_CARGO_TARGET=aarch64-apple-darwin
else ifeq ("${OSX_BUILD_ARCH}", "x86_64")
	RUST_FLAGS=-DRust_CARGO_TARGET=x86_64-apple-darwin
endif

#### Windows config
ifeq ($(DUCKDB_PLATFORM),windows_amd64_mingw)
	RUST_FLAGS=-DRust_CARGO_TARGET=x86_64-pc-windows-gnu
else ifeq ($(DUCKDB_PLATFORM),windows_amd64_rtools)
	RUST_FLAGS=-DRust_CARGO_TARGET=x86_64-pc-windows-gnu
endif

#### VCPKG config
EXTENSION_CONFIG_STEP ?=
EXTENSION_CONFIG_STEP_WASM ?=

# Set the toolchain
VCPKG_TOOLCHAIN_PATH?=
ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_BUILD=1 -DCMAKE_TOOLCHAIN_FILE='${VCPKG_TOOLCHAIN_PATH}'
endif

# Add the extension config step which ensures the vcpkg dependencies of all extensions get merged properly
ifeq (${USE_MERGED_VCPKG_MANIFEST}, 1)
	EXTENSION_CONFIG_STEP= build/extension_configuration/vcpkg.json
	EXTENSION_CONFIG_STEP_WASM=extension_configuration_wasm
	VCPKG_MANIFEST_FLAGS:=-DVCPKG_MANIFEST_DIR='${PROJ_DIR}build/extension_configuration'
else ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	VCPKG_MANIFEST_FLAGS:=-DVCPKG_MANIFEST_DIR='${PROJ_DIR}'
endif

ifneq ("${VCPKG_TARGET_TRIPLET}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_TARGET_TRIPLET='${VCPKG_TARGET_TRIPLET}'
endif
ifneq ("${VCPKG_HOST_TRIPLET}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_HOST_TRIPLET='${VCPKG_HOST_TRIPLET}'
endif

#### Enable Ninja as generator
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja" -DFORCE_COLORED_OUTPUT=1
endif

### Extension configs
ifneq ("${EXTRA_EXTENSION_CONFIGS}", "")
	EXTENSION_CONFIGS:=${EXTRA_EXTENSION_CONFIGS};${EXT_CONFIG}
else
	EXTENSION_CONFIGS:=${EXT_CONFIG}
endif
EXTENSION_CONFIG_FLAG=-DDUCKDB_EXTENSION_CONFIGS='${EXTENSION_CONFIGS}'

#### Configuration for this extension

# This setting controls how DuckDB is linked into the loadable extensions.
# Setting this to 0 will speed up linking and reduce binary size, but may render your extension binaries unloadable on some platforms.
EXTENSION_STATIC_BUILD ?= 1

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=$(EXTENSION_STATIC_BUILD) $(EXTENSION_FLAGS) $(EXTENSION_CONFIG_FLAG) ${EXT_FLAGS} $(CORE_EXTENSION_VAR) $(OSX_BUILD_FLAG) $(RUST_FLAGS) $(TOOLCHAIN_FLAGS) -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}' -DCUSTOM_LINKER=${CUSTOM_LINKER} -DOVERRIDE_GIT_DESCRIBE="${OVERRIDE_GIT_DESCRIBE}" -DUNITTEST_ROOT_DIRECTORY="$(PROJ_DIR)" -DBENCHMARK_ROOT_DIRECTORY="$(PROJ_DIR)" -DENABLE_UNITTEST_CPP_TESTS=FALSE

#### Extra Flags
ifeq (${CRASH_ON_ASSERT}, 1)
	BUILD_FLAGS += -DCRASH_ON_ASSERT=1
endif
ifeq ($(BUILD_BENCHMARK), 1)
	BUILD_FLAGS += -DBUILD_BENCHMARKS=1
endif
ifeq (${TREAT_WARNINGS_AS_ERRORS}, 1)
	BUILD_FLAGS += -DTREAT_WARNINGS_AS_ERRORS=1
endif
ifeq (${DISABLE_SANITIZER}, 1)
	BUILD_FLAGS += -DENABLE_SANITIZER=FALSE -DENABLE_UBSAN=0
endif
ifeq (${DISABLE_UBSAN}, 1)
	BUILD_FLAGS += -DENABLE_UBSAN=0
endif
ifeq (${THREADSAN}, 1)
	BUILD_FLAGS += -DENABLE_THREAD_SANITIZER=1
endif
ifneq (${BUILD_EXTENSION_TEST_DEPS}, )
	BUILD_FLAGS += -DBUILD_EXTENSION_TEST_DEPS=${BUILD_EXTENSION_TEST_DEPS}
endif

#### Clang Tidy
ifneq ($(TIDY_THREADS),)
	TIDY_THREAD_PARAMETER := -j ${TIDY_THREADS}
endif
ifneq ($(TIDY_BINARY),)
	TIDY_BINARY_PARAMETER := -clang-tidy-binary ${TIDY_BINARY}
endif
ifneq ($(TIDY_CHECKS),)
        TIDY_PERFORM_CHECKS := '-checks=${TIDY_CHECKS}'
endif

debug: ${EXTENSION_CONFIG_STEP}
	mkdir -p build/debug
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Debug -S $(DUCKDB_SRCDIR) -B build/debug
	cmake --build build/debug --config Debug

release: ${EXTENSION_CONFIG_STEP}
	mkdir -p build/release
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(DUCKDB_SRCDIR) -B build/release
	cmake --build build/release --config Release

relassert: ${EXTENSION_CONFIG_STEP}
	mkdir -p build/relassert
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=RelWithDebInfo -S $(DUCKDB_SRCDIR) -DFORCE_ASSERT=1 -B build/relassert
	cmake --build build/relassert --config RelWithDebInfo

reldebug: ${EXTENSION_CONFIG_STEP}
	mkdir -p build/reldebug
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=RelWithDebInfo -S $(DUCKDB_SRCDIR) -B build/reldebug
	cmake --build build/reldebug

# Main tests
test: test_release

TEST_RELEASE_TARGET=test_release_internal
TEST_DEBUG_TARGET=test_debug_internal
TEST_RELDEBUG_TARGET=test_reldebug_internal

# Disable testing outside docker: the unittester is currently dynamically linked by default
ifeq ($(LINUX_CI_IN_DOCKER),0)
	SKIP_TESTS=1
endif

ifeq ($(SKIP_TESTS),1)
	TEST_RELEASE_TARGET=tests_skipped
	TEST_DEBUG_TARGET=tests_skipped
	TEST_RELDEBUG_TARGET=tests_skipped
endif

test_release: $(TEST_RELEASE_TARGET)
test_debug: $(TEST_DEBUG_TARGET)
test_reldebug: $(TEST_RELDEBUG_TARGET)

test_release_internal:
	./build/release/$(TEST_PATH) "$(TESTS_BASE_DIRECTORY)*"
test_debug_internal:
	./build/debug/$(TEST_PATH) "$(TESTS_BASE_DIRECTORY)*"
test_reldebug_internal:
	./build/reldebug/$(TEST_PATH) "$(TESTS_BASE_DIRECTORY)*"

tests_skipped:
	@echo "Tests are skipped in this run..."

# WASM config
VCPKG_EMSDK_FLAGS=-DVCPKG_CHAINLOAD_TOOLCHAIN_FILE=$(EMSDK)/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake
WASM_COMPILE_TIME_COMMON_FLAGS=-DWASM_LOADABLE_EXTENSIONS=1 -DBUILD_EXTENSIONS_ONLY=1 $(TOOLCHAIN_FLAGS) $(VCPKG_EMSDK_FLAGS)
WASM_CXX_MVP_FLAGS=
WASM_CXX_EH_FLAGS=$(WASM_CXX_MVP_FLAGS) -fwasm-exceptions -DWEBDB_FAST_EXCEPTIONS=1
WASM_CXX_THREADS_FLAGS=$(WASM_COMPILE_TIME_EH_FLAGS) -DWITH_WASM_THREADS=1 -DWITH_WASM_SIMD=1 -DWITH_WASM_BULK_MEMORY=1 -pthread

wasm_pre_build_step:

# WASM targets
wasm_mvp: wasm_pre_build_step ${EXTENSION_CONFIG_STEP_WASM}
	mkdir -p build/wasm_mvp
	emcmake cmake $(GENERATOR) $(EXTENSION_CONFIG_FLAG) $(VCPKG_MANIFEST_FLAGS) $(WASM_COMPILE_TIME_COMMON_FLAGS) $(BUILD_FLAGS) -Bbuild/wasm_mvp -DCMAKE_CXX_FLAGS="$(WASM_CXX_MVP_FLAGS)" -S $(DUCKDB_SRCDIR) -DDUCKDB_EXPLICIT_PLATFORM=wasm_mvp -DDUCKDB_CUSTOM_PLATFORM=wasm_mvp
	emmake make -j8 -Cbuild/wasm_mvp

wasm_eh: wasm_pre_build_step ${EXTENSION_CONFIG_STEP_WASM}
	mkdir -p build/wasm_eh
	emcmake cmake $(GENERATOR) $(EXTENSION_CONFIG_FLAG) $(VCPKG_MANIFEST_FLAGS) $(WASM_COMPILE_TIME_COMMON_FLAGS) $(BUILD_FLAGS) -Bbuild/wasm_eh -DCMAKE_CXX_FLAGS="$(WASM_CXX_EH_FLAGS)" -S $(DUCKDB_SRCDIR) -DDUCKDB_EXPLICIT_PLATFORM=wasm_eh -DDUCKDB_CUSTOM_PLATFORM=wasm_eh
	emmake make -j8 -Cbuild/wasm_eh

wasm_threads: wasm_pre_build_step ${EXTENSION_CONFIG_STEP_WASM}
	mkdir -p ./build/wasm_threads
	emcmake cmake $(GENERATOR) $(EXTENSION_CONFIG_FLAG) $(VCPKG_MANIFEST_FLAGS) $(WASM_COMPILE_TIME_COMMON_FLAGS) $(BUILD_FLAGS) -Bbuild/wasm_threads -DCMAKE_CXX_FLAGS="$(WASM_CXX_THREADS_FLAGS)" -S $(DUCKDB_SRCDIR) -DDUCKDB_EXPLICIT_PLATFORM=wasm_threads -DDUCKDB_CUSTOM_PLATFORM=wasm_threads
	emmake make -j8 -Cbuild/wasm_threads


###### Extension config step
extension_configuration_default:
	mkdir -p build/extension_configuration
	mkdir -p duckdb/build/extension_configuration
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) -DEXTENSION_CONFIG_BUILD=TRUE -DVCPKG_BUILD=1 -DCMAKE_BUILD_TYPE=Debug -S $(DUCKDB_SRCDIR) -B build/extension_configuration
	cmake --build build/extension_configuration
	cp duckdb/build/extension_configuration/vcpkg.json build/extension_configuration/vcpkg.json

extension_configuration_wasm:
	mkdir -p build/extension_configuration
	mkdir -p duckdb/build/extension_configuration
	emcmake cmake $(GENERATOR) $(EXTENSION_CONFIG_FLAG) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) -DEXTENSION_CONFIG_BUILD=TRUE -Bbuild/extension_configuration -S $(DUCKDB_SRCDIR)
	emmake make -j8 -Cbuild/extension_configuration
	cp duckdb/build/extension_configuration/vcpkg.json build/extension_configuration/vcpkg.json

EXTENSION_CONFIG_TARGET?=extension_configuration_default

extension_configuration: build/extension_configuration/vcpkg.json

build/extension_configuration/vcpkg.json: ${EXTENSION_CONFIG_TARGET}

#### Misc
format-check:
	python3 duckdb/scripts/format.py --all --check --directories src test

format:
	python3 duckdb/scripts/format.py --all --fix --noconfirm --directories src test

format-fix:
	python3 duckdb/scripts/format.py --all --fix --noconfirm --directories src test

format-main:
	python3 duckdb/scripts/format.py main --fix --noconfirm --directories src test

tidy-check:
	mkdir -p ./build/tidy
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) -DDISABLE_UNITY=1 -DCLANG_TIDY=1 -S $(DUCKDB_SRCDIR) -B build/tidy
	cp duckdb/.clang-tidy build/tidy/.clang-tidy
	cd build/tidy && python3 ../../duckdb/scripts/run-clang-tidy.py '$(PROJ_DIR)src/.*/' -header-filter '$(PROJ_DIR)src/.*/' -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS}

update:
	git submodule update --remote --merge

pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf testext
	make $@ -C $(DUCKDB_SRCDIR)

clean-python:
	make $@ -C $(DUCKDB_SRCDIR)

set_duckdb_repository:
	rm -rf duckdb
	mkdir duckdb
	git clone $(DUCKDB_GIT_REPOSITORY) duckdb

set_duckdb_version:
	cd duckdb && git checkout $(DUCKDB_GIT_VERSION)

set_duckdb_tag:
	cd duckdb && git tag $(DUCKDB_TAG)

output_distribution_matrix:
	cat duckdb/.github/config/distribution_matrix.json

configure_ci:
	@echo "configure_ci step is skipped for this extension build..."

