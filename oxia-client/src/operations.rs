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

//! Internal plumbing between request builders and the per-shard batchers: a
//! pending operation is the already-built wire request plus the oneshot used to
//! deliver its response. Requests are *moved* into the outgoing batch at flush
//! time — values are `Bytes`, so nothing is deep-copied on the write path.

use crate::errors::OxiaError;
use crate::proto;
use tokio::sync::oneshot;

/// A wire request paired with the channel that completes the caller's future.
pub(crate) struct Pending<Req, Resp> {
    pub(crate) request: Req,
    pub(crate) callback: oneshot::Sender<Result<Resp, OxiaError>>,
}

impl<Req, Resp> Pending<Req, Resp> {
    pub(crate) fn new(request: Req) -> (Self, oneshot::Receiver<Result<Resp, OxiaError>>) {
        let (tx, rx) = oneshot::channel();
        (
            Pending {
                request,
                callback: tx,
            },
            rx,
        )
    }
}

pub(crate) type PendingPut = Pending<proto::PutRequest, proto::PutResponse>;
pub(crate) type PendingDelete = Pending<proto::DeleteRequest, proto::DeleteResponse>;
pub(crate) type PendingDeleteRange = Pending<proto::DeleteRangeRequest, proto::DeleteRangeResponse>;
pub(crate) type PendingGet = Pending<proto::GetRequest, proto::GetResponse>;
