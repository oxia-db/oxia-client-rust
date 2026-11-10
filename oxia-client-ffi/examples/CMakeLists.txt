# Copyright 2025-2026 The Oxia Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.13)
project(oxia_c_example C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED True)

add_executable(example example.c)
target_include_directories(example PRIVATE ..)
target_compile_options(example PRIVATE -Wall -Wextra)

# Where `cargo build` put the library. The workspace may redirect its target
# directory (CARGO_TARGET_DIR, or `build.target-dir` in .cargo/config.toml),
# so let it be overridden:
#   cmake -S . -B build -DOXIA_FFI_LIB_DIR=/path/to/target/release
set(OXIA_FFI_LIB_DIR "" CACHE PATH "Directory holding liboxia_client_ffi")
if(OXIA_FFI_LIB_DIR)
  set(OXIA_FFI_SEARCH_PATHS "${OXIA_FFI_LIB_DIR}")
else()
  # Ask cargo where the target directory is, rather than assuming ../../target.
  execute_process(
    COMMAND cargo metadata --format-version 1 --no-deps
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    OUTPUT_VARIABLE OXIA_CARGO_METADATA
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  string(JSON OXIA_TARGET_DIR ERROR_VARIABLE OXIA_JSON_ERROR
         GET "${OXIA_CARGO_METADATA}" "target_directory")
  if(OXIA_JSON_ERROR)
    set(OXIA_TARGET_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../target")
  endif()
  set(OXIA_FFI_SEARCH_PATHS "${OXIA_TARGET_DIR}/debug" "${OXIA_TARGET_DIR}/release")
endif()

find_library(
  OXIA_FFI_LIBRARY
  NAMES oxia_client_ffi
  PATHS ${OXIA_FFI_SEARCH_PATHS}
  NO_DEFAULT_PATH
)

if(NOT OXIA_FFI_LIBRARY)
  message(FATAL_ERROR
    "Could not find liboxia_client_ffi in ${OXIA_FFI_SEARCH_PATHS}. "
    "Run 'cargo build -p oxia-client-ffi' first, or pass -DOXIA_FFI_LIB_DIR=<dir>.")
endif()

target_link_libraries(example PRIVATE ${OXIA_FFI_LIBRARY})
