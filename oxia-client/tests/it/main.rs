// Copyright 2025-2026 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The Docker-backed integration tests, as one test binary: each file under
//! `tests/` would be a crate of its own, linking the whole dependency graph
//! again. Add new suites as modules here.

mod common;

mod auth;
mod cluster;
mod integration;
mod interop;
mod java_parity;
#[cfg(feature = "tls")]
mod tls;
