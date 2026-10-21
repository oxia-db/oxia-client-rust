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

cmake_minimum_required(VERSION 3.10)
project(oxia_c_example C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED True)

include_directories(../)

add_executable(example example.c)

set(RUST_LIB_DIR "${CMAKE_SOURCE_DIR}/../../target/debug")

find_library(
  OXIA_FFI_LIBRARY
  NAMES oxia_client_ffi
  PATHS ${RUST_LIB_DIR}
)

if(NOT OXIA_FFI_LIBRARY)
  message(FATAL_ERROR "Could not find oxia_client_ffi. Please run 'cargo build' first.")
endif()

target_link_libraries(example PRIVATE ${OXIA_FFI_LIBRARY})
