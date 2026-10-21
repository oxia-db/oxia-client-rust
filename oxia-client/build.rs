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

fn main() {
    // Compile the proto with the pure-Rust `protox` compiler so that building
    // this crate does not require a system `protoc` install. protox produces a
    // `FileDescriptorSet` that tonic-build turns into the client stubs.
    let file_descriptors = protox::compile(["proto/client.proto"], ["proto"])
        .expect("failed to compile proto/client.proto with protox");
    tonic_build::configure()
        .build_client(true)
        // Server stubs are used only by the in-process mock server that the
        // hermetic unit tests run against (src/mock_server.rs, cfg(test)).
        .build_server(true)
        // Generate `bytes::Bytes` for all proto `bytes` fields (record values):
        // decoding borrows the receive buffer and cloning is refcounted, so
        // values move through the client without byte copies.
        .bytes(["."])
        .compile_fds(file_descriptors)
        .expect("failed to generate gRPC code from the proto descriptor set");
}
