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

//! Streaming result types: ordered range scans / lists merged across shards,
//! and the notification / sequence-update subscription handles.

use crate::client::SubscriptionGuard;
use crate::errors::OxiaError;
use crate::key::KeyOrder;
use crate::types::{GetResult, Notification};
use futures::Stream;
use std::cmp::Ordering;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// A boxed stream of results coming from a single shard.
pub(crate) type ShardStream<T> = Pin<Box<dyn Stream<Item = Result<T, OxiaError>> + Send>>;

/// Items that can be merged across shards in the order of the shards.
pub(crate) trait MergeItem {
    /// Compares two items in the order in which a shard returns them, given
    /// the key order of the namespace.
    fn merge_cmp(&self, other: &Self, order: KeyOrder) -> Ordering;
}

impl MergeItem for String {
    // Through an index, a shard lists its keys in the order of their
    // secondary keys, which a list response leaves out: the merge can only
    // compare the keys.
    fn merge_cmp(&self, other: &Self, order: KeyOrder) -> Ordering {
        order.compare(self, other)
    }
}

impl MergeItem for GetResult {
    fn merge_cmp(&self, other: &Self, order: KeyOrder) -> Ordering {
        self.compare_keys(other, order)
    }
}

/// An ordered k-way merge over per-shard streams.
///
/// Each shard returns its results in the key order of the namespace, and
/// through an index, in the order of their secondary keys; holding at most
/// one pending item per shard and always yielding the smallest in that order
/// ([`MergeItem::merge_cmp`]) keeps the global order identical to the
/// server's while buffering O(shards) items.
pub(crate) struct Merged<T> {
    streams: Vec<ShardStream<T>>,
    peeked: Vec<Option<T>>,
    done: Vec<bool>,
    finished: bool,
    order: KeyOrder,
}

pub(crate) fn merged<T>(streams: Vec<ShardStream<T>>, order: KeyOrder) -> Merged<T> {
    let count = streams.len();
    Merged {
        streams,
        peeked: (0..count).map(|_| None).collect(),
        done: vec![false; count],
        finished: false,
        order,
    }
}

impl<T: MergeItem + Unpin> Stream for Merged<T> {
    type Item = Result<T, OxiaError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        // Fill every empty slot; a stream that is Pending keeps the whole merge
        // Pending (its waker is registered), because yielding without knowing
        // its next key could break the global order.
        let mut pending = false;
        for i in 0..this.streams.len() {
            while this.peeked[i].is_none() && !this.done[i] {
                match this.streams[i].as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(item))) => this.peeked[i] = Some(item),
                    Poll::Ready(Some(Err(err))) => {
                        this.finished = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                    Poll::Ready(None) => this.done[i] = true,
                    Poll::Pending => {
                        pending = true;
                        break;
                    }
                }
            }
        }
        if pending {
            return Poll::Pending;
        }
        let mut min_idx: Option<usize> = None;
        for i in 0..this.peeked.len() {
            if let (Some(candidate), Some(m)) = (&this.peeked[i], min_idx) {
                let current = this.peeked[m].as_ref().expect("min slot is filled");
                if candidate.merge_cmp(current, this.order).is_lt() {
                    min_idx = Some(i);
                }
            } else if this.peeked[i].is_some() && min_idx.is_none() {
                min_idx = Some(i);
            }
        }
        match min_idx {
            Some(i) => Poll::Ready(Some(Ok(this.peeked[i].take().expect("slot is filled")))),
            None => {
                this.finished = true;
                Poll::Ready(None)
            }
        }
    }
}

/// An ordered stream of keys produced by
/// [`ListBuilder::stream`](crate::ListBuilder::stream).
///
/// Yields keys in the server's global key order (the key sorting of the
/// namespace), merged across shards while buffering at most one pending key
/// per shard. The stream has
/// no overall deadline; it is naturally backpressured — keys are pulled from
/// the server only as fast as they are consumed. The first error is yielded
/// as an `Err` item and terminates the stream. Dropping the stream cancels
/// the underlying RPCs.
pub struct ListStream {
    inner: Merged<String>,
}

impl ListStream {
    pub(crate) fn new(inner: Merged<String>) -> Self {
        ListStream { inner }
    }
}

impl Stream for ListStream {
    type Item = Result<String, OxiaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl fmt::Debug for ListStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListStream").finish_non_exhaustive()
    }
}

/// An ordered stream of records produced by
/// [`RangeScanBuilder::stream`](crate::RangeScanBuilder::stream).
///
/// Yields records in the server's global key order (the key sorting of the
/// namespace), merged across shards while buffering at most one pending
/// record per shard — a
/// scan of any size runs in O(shards) memory. The stream has no overall
/// deadline; it is naturally backpressured — records are pulled from the
/// server only as fast as they are consumed. The first error is yielded as an
/// `Err` item and terminates the stream. Dropping the stream cancels the
/// underlying RPCs.
pub struct RangeScanStream {
    inner: Merged<GetResult>,
}

impl RangeScanStream {
    pub(crate) fn new(inner: Merged<GetResult>) -> Self {
        RangeScanStream { inner }
    }
}

impl Stream for RangeScanStream {
    type Item = Result<GetResult, OxiaError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl fmt::Debug for RangeScanStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RangeScanStream").finish_non_exhaustive()
    }
}

/// A subscription to database change notifications, created with
/// [`OxiaClient::notifications`](crate::OxiaClient::notifications).
///
/// Consume it with [`recv`](Notifications::recv) or as a [`Stream`].
/// Notifications from the same shard arrive in the order the changes were
/// applied; interleaving across shards is unspecified. The subscription
/// survives connection loss: each per-shard listener reconnects and resumes
/// from its last delivered offset. Dropping the handle releases the
/// subscription.
pub struct Notifications {
    rx: mpsc::Receiver<Notification>,
    // Stops the per-shard listeners when this handle is dropped.
    _guard: SubscriptionGuard,
}

impl Notifications {
    pub(crate) fn new(rx: mpsc::Receiver<Notification>, guard: SubscriptionGuard) -> Self {
        Notifications { rx, _guard: guard }
    }

    /// Receives the next notification.
    ///
    /// Returns `None` once the client is closed. Cancel-safe: dropping the
    /// returned future loses no notification.
    pub async fn recv(&mut self) -> Option<Notification> {
        self.rx.recv().await
    }
}

impl fmt::Debug for Notifications {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Notifications").finish_non_exhaustive()
    }
}

impl Stream for Notifications {
    type Item = Notification;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// A subscription to sequence advances, created with
/// [`OxiaClient::sequence_updates`](crate::OxiaClient::sequence_updates).
///
/// Yields the highest assigned sequence key each time the sequence advances;
/// rapid advances may be coalesced into one delivery carrying the highest
/// key. The subscription survives connection loss and re-subscribes
/// automatically. Consume it with [`recv`](SequenceUpdates::recv) or as a
/// [`Stream`]; dropping the handle releases the subscription.
pub struct SequenceUpdates {
    rx: mpsc::Receiver<String>,
    // Stops the listener when this handle is dropped.
    _guard: SubscriptionGuard,
}

impl SequenceUpdates {
    pub(crate) fn new(rx: mpsc::Receiver<String>, guard: SubscriptionGuard) -> Self {
        SequenceUpdates { rx, _guard: guard }
    }

    /// Receives the next sequence key.
    ///
    /// Returns `None` once the client is closed. Cancel-safe: dropping the
    /// returned future loses no update.
    pub async fn recv(&mut self) -> Option<String> {
        self.rx.recv().await
    }
}

impl fmt::Debug for SequenceUpdates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequenceUpdates").finish_non_exhaustive()
    }
}

impl Stream for SequenceUpdates {
    type Item = String;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn shard(items: Vec<Result<String, OxiaError>>) -> ShardStream<String> {
        Box::pin(futures::stream::iter(items))
    }

    fn keys(items: &[&str]) -> ShardStream<String> {
        shard(items.iter().map(|s| Ok(s.to_string())).collect())
    }

    #[tokio::test]
    async fn merges_in_key_order() {
        let merged = merged(
            vec![keys(&["a", "d", "e"]), keys(&["b", "c", "f"]), keys(&[])],
            KeyOrder::default(),
        );
        let out: Vec<String> = merged.map(|r| r.unwrap()).collect().await;
        assert_eq!(out, vec!["a", "b", "c", "d", "e", "f"]);
    }

    #[tokio::test]
    async fn merges_with_slash_order() {
        // "a" sorts before "/" in Oxia's slash-aware order.
        let merged = merged(vec![keys(&["/x"]), keys(&["a"])], KeyOrder::Legacy);
        let out: Vec<String> = merged.map(|r| r.unwrap()).collect().await;
        assert_eq!(out, vec!["a", "/x"]);
    }

    /// Each shard returns its keys in the key sorting of the namespace, and
    /// the merge keeps it (vectors from the Go reference,
    /// `TestAggregateAndSortRangeScanAcrossShardsKeySorting`).
    #[tokio::test]
    async fn merges_in_the_key_sorting_of_the_namespace() {
        for (order, shards, expected) in [
            (
                KeyOrder::Natural,
                [["a/x", "a0"], ["a/y/z", "ab/y"]],
                ["a/x", "a/y/z", "a0", "ab/y"],
            ),
            (
                KeyOrder::Hierarchical,
                [["a0", "a/x"], ["ab/y", "a/y/z"]],
                ["a0", "ab/y", "a/x", "a/y/z"],
            ),
            (
                KeyOrder::Legacy,
                [["a0", "a/x"], ["a/y/z", "ab/y"]],
                ["a0", "a/x", "a/y/z", "ab/y"],
            ),
        ] {
            let merged = merged(shards.iter().map(|shard| keys(shard)).collect(), order);
            let out: Vec<String> = merged.map(|r| r.unwrap()).collect().await;
            assert_eq!(out, expected, "{order:?}");
        }
    }

    fn record(key: &str, secondary_key: Option<&str>) -> GetResult {
        GetResult {
            key: key.to_string(),
            value: None,
            version: crate::proto::Version::default().into(),
            secondary_index_key: secondary_key.map(String::from),
        }
    }

    /// A shard that returns the records `(key, secondary key)` of a range
    /// scan through an index.
    fn index_records(records: &[(&str, &str)]) -> ShardStream<GetResult> {
        let records: Vec<Result<GetResult, OxiaError>> = records
            .iter()
            .map(|(key, secondary_key)| Ok(record(key, Some(secondary_key))))
            .collect();
        Box::pin(futures::stream::iter(records))
    }

    /// Through an index, each shard returns its records in the order of
    /// their secondary keys, and the merge compares the secondary keys, then
    /// the keys (vectors from the Go reference,
    /// `TestAggregateAndSortRangeScanAcrossShardsSecondaryKeys`).
    #[tokio::test]
    async fn merges_by_the_secondary_keys() {
        for (case, order, shards, expected) in [
            (
                "natural",
                KeyOrder::Natural,
                vec![
                    vec![("p4", "a/x"), ("p2", "a0")],
                    vec![("p3", "a/y/z"), ("p1", "ab/y")],
                ],
                vec!["p4", "p3", "p2", "p1"],
            ),
            (
                "hierarchical",
                KeyOrder::Hierarchical,
                vec![
                    vec![("p4", "a0"), ("p2", "a/x")],
                    vec![("p3", "ab/y"), ("p1", "a/y/z")],
                ],
                vec!["p4", "p3", "p2", "p1"],
            ),
            (
                "unknown",
                KeyOrder::Legacy,
                vec![
                    vec![("p4", "a0"), ("p2", "a/x")],
                    vec![("p3", "a/y/z"), ("p1", "ab/y")],
                ],
                vec!["p4", "p2", "p3", "p1"],
            ),
            // The records with the same secondary key are ordered by their
            // keys.
            (
                "same secondary key",
                KeyOrder::Natural,
                vec![
                    vec![("p2", "a"), ("p3", "b")],
                    vec![("p1", "b"), ("p4", "c")],
                ],
                vec!["p2", "p1", "p3", "p4"],
            ),
            (
                "empty secondary key",
                KeyOrder::Natural,
                vec![vec![("p2", "")], vec![("p1", "a")]],
                vec!["p2", "p1"],
            ),
        ] {
            let merged = merged(
                shards.iter().map(|shard| index_records(shard)).collect(),
                order,
            );
            let out: Vec<String> = merged.map(|r| r.unwrap().key).collect().await;
            assert_eq!(out, expected, "{case}");
        }
    }

    /// The records without a secondary key, which the servers older than
    /// oxia-db/oxia#1508 return from a range scan through an index, are
    /// ordered by their keys only, even against a record that has one.
    #[tokio::test]
    async fn merges_a_record_without_a_secondary_key_by_its_key() {
        let shard = |record: GetResult| -> ShardStream<GetResult> {
            Box::pin(futures::stream::iter([Ok(record)]))
        };
        let merged = merged(
            vec![shard(record("p2", None)), shard(record("p1", Some("z")))],
            KeyOrder::Natural,
        );
        let out: Vec<String> = merged.map(|r| r.unwrap().key).collect().await;
        assert_eq!(out, ["p1", "p2"]);
    }

    #[tokio::test]
    async fn error_terminates_stream() {
        let merged = merged(
            vec![
                shard(vec![Ok("a".to_string()), Err(OxiaError::Timeout)]),
                keys(&["b", "c"]),
            ],
            KeyOrder::default(),
        );
        let out: Vec<Result<String, OxiaError>> = merged.collect().await;
        assert!(matches!(out[0], Ok(ref k) if k == "a"));
        assert!(out.iter().any(|r| r.is_err()));
        // Nothing after the error.
        let err_pos = out.iter().position(|r| r.is_err()).unwrap();
        assert_eq!(err_pos, out.len() - 1);
    }

    #[tokio::test]
    async fn empty_merge_ends() {
        let merged = merged::<String>(vec![], KeyOrder::default());
        let out: Vec<_> = merged.collect().await;
        assert!(out.is_empty());
    }
}
