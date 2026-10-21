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

use crate::address::ensure_protocol;
use crate::errors::OxiaError;
use crate::hash::shard_key_hash;
use crate::key::KeyOrder;
use crate::proto::shard_assignment::ShardBoundaries;
use crate::proto::{NamespaceShardsAssignment, ShardAssignmentsRequest, ShardKeyRouter};
use crate::provider_manager::ProviderManager;
use crate::retry::{RetryError, retry_until_cancelled};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::StreamExt;
use tracing::info;

pub struct ShardManagerOptions {
    pub address: String,
    pub namespace: String,
    pub provider_manager: Arc<ProviderManager>,
    pub request_timeout: Duration,
}

pub(crate) struct Node {
    pub service_address: String,
}

/// One shard's hash range and leader, used to route keys to shards.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ShardRange {
    min_hash: u32,
    max_hash: u32,
    pub(crate) shard: i64,
}

impl ShardRange {
    fn contains(&self, code: u32) -> bool {
        self.min_hash <= code && code <= self.max_hash
    }

    fn overlaps(&self, other: &ShardRange) -> bool {
        self.min_hash <= other.max_hash && other.min_hash <= self.max_hash
    }
}

/// A shard removed from the assignments, after a split, and the shards that
/// replaced it: those of the assignments it was removed from whose hash
/// ranges overlap its own.
#[derive(Debug)]
pub(crate) struct ShardReplacement {
    pub(crate) removed: i64,
    pub(crate) successors: Vec<ShardRange>,
    shard_key_router: i32,
}

impl ShardReplacement {
    /// The position in `successors` of the shard that took over `key`.
    pub(crate) fn successor_for(&self, key: &str) -> Option<usize> {
        let code = shard_key_hash(self.shard_key_router, key);
        self.successors
            .iter()
            .position(|range| range.contains(code))
    }
}

/// An immutable snapshot of a namespace's shard assignments.
///
/// The [`ShardManager`] publishes a fresh snapshot atomically via [`ArcSwap`] on
/// every update, so lookups never observe a half-applied table (the previous
/// `DashMap` was cleared then repopulated, exposing a window where a key routed
/// to nothing). Hash ranges are kept sorted so routing is a binary search rather
/// than a linear scan.
struct Assignments {
    shard_key_router: i32,
    /// The order of the keys in the shards, from the namespace's key sorting.
    key_order: KeyOrder,
    /// Shard hash ranges, sorted ascending by `min_hash`.
    ranges: Vec<ShardRange>,
    /// Shard id -> leader address.
    leaders: HashMap<i64, String>,
    /// The shards removed from the assignments since the first ones, in the
    /// order they were removed.
    replacements: Vec<Arc<ShardReplacement>>,
}

impl Assignments {
    fn empty() -> Self {
        Assignments {
            shard_key_router: ShardKeyRouter::Xxhash3 as i32,
            key_order: KeyOrder::default(),
            ranges: Vec::new(),
            leaders: HashMap::new(),
            replacements: Vec::new(),
        }
    }

    fn from_namespace(namespace_assignment: &NamespaceShardsAssignment) -> Self {
        let mut ranges = Vec::with_capacity(namespace_assignment.assignments.len());
        let mut leaders = HashMap::with_capacity(namespace_assignment.assignments.len());
        for sa in &namespace_assignment.assignments {
            leaders.insert(sa.shard, sa.leader.clone());
            if let Some(ShardBoundaries::Int32HashRange(range)) = &sa.shard_boundaries {
                ranges.push(ShardRange {
                    min_hash: range.min_hash_inclusive,
                    max_hash: range.max_hash_inclusive,
                    shard: sa.shard,
                });
            }
        }
        ranges.sort_by_key(|r| r.min_hash);
        Assignments {
            shard_key_router: namespace_assignment.shard_key_router,
            key_order: KeyOrder::from_key_sorting(namespace_assignment.key_sorting),
            ranges,
            leaders,
            replacements: Vec::new(),
        }
    }

    /// The assignments that `namespace_assignment` replaces these with,
    /// recording the shards it no longer has that other shards replaced: the
    /// shards of `namespace_assignment` overlapping their hash ranges.
    fn next(&self, namespace_assignment: &NamespaceShardsAssignment) -> Self {
        let mut next = Assignments::from_namespace(namespace_assignment);
        next.replacements = self.replacements.clone();
        for removed in self
            .ranges
            .iter()
            .filter(|range| !next.leaders.contains_key(&range.shard))
        {
            let successors: Vec<ShardRange> = next
                .ranges
                .iter()
                .filter(|range| range.overlaps(removed))
                .copied()
                .collect();
            if successors.is_empty() {
                continue;
            }
            next.replacements.push(Arc::new(ShardReplacement {
                removed: removed.shard,
                successors,
                shard_key_router: next.shard_key_router,
            }));
        }
        next
    }

    /// Finds the shard whose hash range contains `code`, in O(log n).
    fn shard_for_hash(&self, code: u32) -> Option<&ShardRange> {
        // The last range whose `min_hash <= code` is the only candidate; check it
        // actually contains `code` (guards gaps in an incomplete assignment).
        let idx = self.ranges.partition_point(|r| r.min_hash <= code);
        let range = self.ranges.get(idx.checked_sub(1)?)?;
        (code <= range.max_hash).then_some(range)
    }
}

struct Inner {
    provider_manager: Arc<ProviderManager>,
    assignments: ArcSwap<Assignments>,
    /// Bumped after every update of `assignments`.
    version: watch::Sender<u64>,
}

impl Inner {
    fn set_assignments(&self, assignments: Assignments) {
        self.assignments.store(Arc::new(assignments));
        self.version.send_modify(|version| *version += 1);
    }
}

pub struct ShardManager {
    inner: Arc<Inner>,
    context: CancellationToken,
    assignment_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for ShardManager {
    fn drop(&mut self) {
        self.context.cancel();
    }
}

async fn start_assignments_listener(
    context: CancellationToken,
    address: String,
    namespace: String,
    inner: Arc<Inner>,
    init_tx: mpsc::Sender<Result<(), OxiaError>>,
) {
    let op = || {
        let local_address = address.clone();
        let local_inner = inner.clone();
        let local_context = context.clone();
        let ns = namespace.clone();
        let init_sender = init_tx.clone();
        async move {
            let mut provider = local_inner
                .provider_manager
                .get_provider(local_address)
                .await
                .map_err(RetryError::transient)?;
            let mut streaming = match provider
                .get_shard_assignments(ShardAssignmentsRequest {
                    namespace: ns.clone(),
                })
                .await
            {
                Ok(response) => response.into_inner(),
                Err(status) => return classify_status(status, &init_sender).await,
            };
            loop {
                tokio::select! {
                    _ = local_context.cancelled() => {
                        info!("Shard assignment listener stopped: cancelled.");
                        return Ok(());
                    }
                    message = streaming.next() => match message {
                        // A graceful close (e.g. the coordinator restarting) is
                        // transient: reconnect rather than stop with stale routing.
                        None => {
                            return Err(RetryError::transient(OxiaError::Disconnected(
                                "shard assignment stream closed by server".to_string(),
                            )));
                        }
                        Some(Err(status)) => return classify_status(status, &init_sender).await,
                        Some(Ok(assignments)) => {
                            if let Some(na) = assignments.namespaces.get(&ns) {
                                // This task is the only one to store them.
                                let next = local_inner.assignments.load().next(na);
                                local_inner.set_assignments(next);
                                if !init_sender.is_closed() {
                                    let _ = init_sender.send(Ok(())).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    };
    retry_until_cancelled(&context, "shard-assignments", op).await;
}

/// Classifies a gRPC error from the assignment stream. `Unauthenticated` (bad
/// credentials) and `NotFound` (on this stream, only the namespace can be not
/// found) are permanent — retrying cannot help — so the error is also forwarded
/// to [`ShardManager::new`] to unblock it immediately. Everything else is
/// transient and retried.
async fn classify_status(
    status: tonic::Status,
    init_sender: &mpsc::Sender<Result<(), OxiaError>>,
) -> Result<(), RetryError> {
    let fatal = matches!(
        status.code(),
        tonic::Code::Unauthenticated | tonic::Code::NotFound
    );
    let err = OxiaError::from(status);
    if fatal {
        if !init_sender.is_closed() {
            let _ = init_sender.send(Err(err.clone())).await;
        }
        Err(RetryError::fatal(err))
    } else {
        Err(RetryError::transient(err))
    }
}

impl ShardManager {
    pub async fn new(options: ShardManagerOptions) -> Result<Self, OxiaError> {
        let context = CancellationToken::new();
        let inner = Arc::new(Inner {
            provider_manager: options.provider_manager,
            assignments: ArcSwap::from_pointee(Assignments::empty()),
            version: watch::Sender::new(0),
        });
        let (init_tx, mut init_rx) = mpsc::channel::<Result<(), OxiaError>>(1);
        let assignment_handle = tokio::spawn(start_assignments_listener(
            context.clone(),
            options.address,
            options.namespace,
            inner.clone(),
            init_tx,
        ));

        // Fail fast: never hand back a client with an empty routing table. Wait
        // for the first assignment, bounded by the request timeout, and surface a
        // permanent error (bad namespace or credentials) as soon as it is known.
        match tokio::time::timeout(options.request_timeout, init_rx.recv()).await {
            Ok(Some(Ok(()))) => {}
            Ok(Some(Err(err))) => {
                context.cancel();
                return Err(err);
            }
            Ok(None) => {
                context.cancel();
                return Err(OxiaError::Disconnected(
                    "shard assignment stream ended before delivering any assignment".to_string(),
                ));
            }
            Err(_) => {
                context.cancel();
                return Err(OxiaError::Timeout);
            }
        }

        Ok(ShardManager {
            inner,
            context,
            assignment_handle: Mutex::new(Some(assignment_handle)),
        })
    }

    pub async fn close(&self) -> Result<(), OxiaError> {
        self.context.cancel();
        let mut handle_guard = self.assignment_handle.lock().await;
        if let Some(handle) = handle_guard.take() {
            handle
                .await
                .map_err(|err| OxiaError::Disconnected(err.to_string()))?;
        }
        Ok(())
    }

    /// A manager with no assignments and no watcher, for unit tests of code
    /// paths that never resolve a leader.
    #[cfg(test)]
    pub(crate) fn detached_for_tests() -> Arc<Self> {
        Arc::new(ShardManager {
            inner: Arc::new(Inner {
                provider_manager: Arc::new(crate::provider_manager::ProviderManager::new(
                    std::time::Duration::from_secs(1),
                )),
                assignments: ArcSwap::from_pointee(Assignments::empty()),
                version: watch::Sender::new(0),
            }),
            context: CancellationToken::new(),
            assignment_handle: Mutex::new(None),
        })
    }

    /// The shard's leader, or `None` if the shard is unknown or has no leader:
    /// the coordinator publishes an empty leader while it elects one.
    pub fn get_leader(&self, shard_id: i64) -> Option<Node> {
        self.inner
            .assignments
            .load()
            .leaders
            .get(&shard_id)
            .filter(|leader| !leader.is_empty())
            .map(|leader| Node {
                service_address: ensure_protocol(leader.clone()),
            })
    }

    /// The version of the shard assignments, bumped on every update.
    pub(crate) fn assignments_version(&self) -> u64 {
        *self.inner.version.borrow()
    }

    /// Waits until the shard assignments are updated past `version`, for at
    /// most `timeout`.
    pub(crate) async fn wait_for_update(&self, version: u64, timeout: Duration) {
        let mut updates = self.inner.version.subscribe();
        // `self` owns the sender, so the wait cannot fail with `RecvError`.
        let updated = updates.wait_for(|current| *current != version);
        let _ = tokio::time::timeout(timeout, updated).await;
    }

    pub fn get_shard(&self, key: &str) -> Option<i64> {
        let snapshot = self.inner.assignments.load();
        let code = shard_key_hash(snapshot.shard_key_router, key);
        snapshot.shard_for_hash(code).map(|r| r.shard)
    }

    /// The ids of all shards in the namespace, per the current assignments.
    /// Whether the shard is present in the current assignments (false once it
    /// has been split or merged away).
    pub fn shard_exists(&self, shard_id: i64) -> bool {
        self.inner
            .assignments
            .load()
            .leaders
            .contains_key(&shard_id)
    }

    pub fn get_shard_ids(&self) -> Vec<i64> {
        self.inner
            .assignments
            .load()
            .leaders
            .keys()
            .copied()
            .collect()
    }

    /// The order of the keys in the shards of the namespace, per the current
    /// assignments.
    pub(crate) fn key_order(&self) -> KeyOrder {
        self.inner.assignments.load().key_order
    }

    /// The shards removed from the assignments after the first `from` of
    /// them, in the order they were removed, each with the shards that
    /// replaced it.
    pub(crate) fn replacements(&self, from: usize) -> Vec<Arc<ShardReplacement>> {
        let assignments = self.inner.assignments.load();
        assignments
            .replacements
            .get(from..)
            .unwrap_or_default()
            .to_vec()
    }

    /// The hash ranges of all shards in the namespace, per the current
    /// assignments.
    pub(crate) fn get_shard_ranges(&self) -> Vec<ShardRange> {
        self.inner.assignments.load().ranges.clone()
    }

    /// The shards in the current assignments whose hash ranges overlap
    /// `removed`'s: once `removed` has been split or merged away, the shards
    /// that own its keys.
    pub(crate) fn get_successors(&self, removed: &ShardRange) -> Vec<ShardRange> {
        self.inner
            .assignments
            .load()
            .ranges
            .iter()
            .filter(|range| range.overlaps(removed))
            .copied()
            .collect()
    }

    /// A receiver that sees each replacement of the assignments from now on.
    pub(crate) fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.inner.version.subscribe()
    }

    /// Replaces the assignments with `shards`, as `(shard, min_hash,
    /// max_hash)`, as if the coordinator had published them.
    #[cfg(test)]
    pub(crate) fn set_assignments_for_tests(&self, shards: &[(i64, u32, u32)]) {
        use crate::proto::{Int32HashRange, ShardAssignment};
        let assignments = shards
            .iter()
            .map(|&(shard, min_hash, max_hash)| ShardAssignment {
                shard,
                leader: format!("server-{shard}:6648"),
                shard_boundaries: Some(ShardBoundaries::Int32HashRange(Int32HashRange {
                    min_hash_inclusive: min_hash,
                    max_hash_inclusive: max_hash,
                })),
            })
            .collect();
        let next = self
            .inner
            .assignments
            .load()
            .next(&NamespaceShardsAssignment {
                assignments,
                shard_key_router: ShardKeyRouter::Xxhash3 as i32,
                ..Default::default()
            });
        self.inner.set_assignments(next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(min: u32, max: u32, shard: i64) -> ShardRange {
        ShardRange {
            min_hash: min,
            max_hash: max,
            shard,
        }
    }

    #[test]
    fn shard_for_hash_binary_search() {
        let a = Assignments {
            shard_key_router: ShardKeyRouter::Xxhash3 as i32,
            key_order: KeyOrder::default(),
            ranges: vec![range(0, 99, 1), range(100, 199, 2), range(200, u32::MAX, 3)],
            leaders: HashMap::new(),
            replacements: Vec::new(),
        };
        assert_eq!(a.shard_for_hash(0).map(|r| r.shard), Some(1));
        assert_eq!(a.shard_for_hash(99).map(|r| r.shard), Some(1));
        assert_eq!(a.shard_for_hash(100).map(|r| r.shard), Some(2));
        assert_eq!(a.shard_for_hash(150).map(|r| r.shard), Some(2));
        assert_eq!(a.shard_for_hash(200).map(|r| r.shard), Some(3));
        assert_eq!(a.shard_for_hash(u32::MAX).map(|r| r.shard), Some(3));
    }

    #[test]
    fn shard_for_hash_reports_gaps() {
        // A gap between 100..=199 that no shard owns.
        let a = Assignments {
            shard_key_router: ShardKeyRouter::Xxhash3 as i32,
            key_order: KeyOrder::default(),
            ranges: vec![range(0, 99, 1), range(200, 299, 2)],
            leaders: HashMap::new(),
            replacements: Vec::new(),
        };
        assert_eq!(a.shard_for_hash(50).map(|r| r.shard), Some(1));
        assert_eq!(a.shard_for_hash(150).map(|r| r.shard), None);
        assert_eq!(a.shard_for_hash(250).map(|r| r.shard), Some(2));
    }

    #[test]
    fn empty_assignments_route_nowhere() {
        let a = Assignments::empty();
        assert_eq!(a.shard_for_hash(0).map(|r| r.shard), None);
    }

    /// The assignments keep the key sorting of the namespace, and a server
    /// that does not report it leaves the legacy order.
    #[test]
    fn assignments_keep_the_key_sorting() {
        use crate::proto::KeySorting;

        for (key_sorting, order) in [
            (KeySorting::Natural, KeyOrder::Natural),
            (KeySorting::Hierarchical, KeyOrder::Hierarchical),
            (KeySorting::Unknown, KeyOrder::Legacy),
        ] {
            let namespace = NamespaceShardsAssignment {
                key_sorting: key_sorting as i32,
                ..NamespaceShardsAssignment::default()
            };
            assert_eq!(Assignments::from_namespace(&namespace).key_order, order);
        }
        assert_eq!(Assignments::empty().key_order, KeyOrder::Legacy);
    }

    /// The assignments of `shards`, as `(shard, min_hash, max_hash)`.
    fn namespace(shards: &[(i64, u32, u32)]) -> NamespaceShardsAssignment {
        use crate::proto::{Int32HashRange, ShardAssignment};
        NamespaceShardsAssignment {
            assignments: shards
                .iter()
                .map(
                    |&(shard, min_hash_inclusive, max_hash_inclusive)| ShardAssignment {
                        shard,
                        leader: format!("server-{shard}:6648"),
                        shard_boundaries: Some(ShardBoundaries::Int32HashRange(Int32HashRange {
                            min_hash_inclusive,
                            max_hash_inclusive,
                        })),
                    },
                )
                .collect(),
            shard_key_router: ShardKeyRouter::Xxhash3 as i32,
            ..NamespaceShardsAssignment::default()
        }
    }

    fn replaced(assignments: &Assignments) -> Vec<(i64, Vec<i64>)> {
        assignments
            .replacements
            .iter()
            .map(|r| (r.removed, r.successors.iter().map(|s| s.shard).collect()))
            .collect()
    }

    /// An update records the shards it removes, each with the shards of the
    /// update that overlap its hash range: a split shard keeps its children
    /// as its successors even once one of them is split in turn.
    #[test]
    fn updates_record_the_replaced_shards() {
        let first = Assignments::empty().next(&namespace(&[(0, 0, 99), (1, 100, 199)]));
        assert_eq!(replaced(&first), []);
        // Shard 0 is split into shards 2 and 3.
        let second = first.next(&namespace(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]));
        assert_eq!(replaced(&second), [(0, vec![2, 3])]);
        // An update that removes no shard records nothing.
        let third = second.next(&namespace(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]));
        assert_eq!(replaced(&third), [(0, vec![2, 3])]);
        // Shard 3 is split into shards 4 and 5.
        let fourth = third.next(&namespace(&[
            (1, 100, 199),
            (2, 0, 49),
            (4, 50, 74),
            (5, 75, 99),
        ]));
        assert_eq!(replaced(&fourth), [(0, vec![2, 3]), (3, vec![4, 5])]);
        // A shard that no other shard replaced is not recorded.
        let fifth = fourth.next(&namespace(&[(2, 0, 49), (4, 50, 74), (5, 75, 99)]));
        assert_eq!(replaced(&fifth), [(0, vec![2, 3]), (3, vec![4, 5])]);
    }

    /// A replacement sends a key to the successor that the new assignments
    /// route it to.
    #[test]
    fn replacement_routes_a_key_to_its_successor() {
        let middle = u32::MAX / 2;
        let assignments = Assignments::empty()
            .next(&namespace(&[(0, 0, u32::MAX)]))
            .next(&namespace(&[(1, 0, middle), (2, middle + 1, u32::MAX)]));
        let replacement = &assignments.replacements[0];
        let mut successors = std::collections::HashSet::new();
        for key in (0..20).map(|i| format!("key-{i}")) {
            let routed = assignments
                .shard_for_hash(crate::hash::xxh3_32(&key))
                .map(|r| r.shard);
            let successor = replacement
                .successor_for(&key)
                .map(|i| replacement.successors[i].shard);
            assert_eq!(successor, routed, "key {key}");
            successors.extend(successor);
        }
        assert_eq!(successors.len(), 2, "the keys land on both successors");
    }

    #[test]
    fn replacements_are_listed_after_the_first_ones() {
        let manager = ShardManager::detached_for_tests();
        let assignments = Assignments::empty()
            .next(&namespace(&[(0, 0, 99), (1, 100, 199)]))
            .next(&namespace(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]))
            .next(&namespace(&[(2, 0, 49), (3, 50, 99), (4, 100, 199)]));
        manager.inner.assignments.store(Arc::new(assignments));
        let removed = |from| -> Vec<i64> {
            manager
                .replacements(from)
                .iter()
                .map(|r| r.removed)
                .collect()
        };
        assert_eq!(removed(0), [0, 1]);
        assert_eq!(removed(1), [1]);
        assert_eq!(removed(2), []);
        assert_eq!(removed(3), []);
    }

    fn range_of(manager: &ShardManager, shard: i64) -> ShardRange {
        manager
            .get_shard_ranges()
            .into_iter()
            .find(|r| r.shard == shard)
            .expect("shard is assigned")
    }

    fn successor_ids(manager: &ShardManager, removed: &ShardRange) -> Vec<i64> {
        let mut ids: Vec<i64> = manager
            .get_successors(removed)
            .iter()
            .map(|r| r.shard)
            .collect();
        ids.sort_unstable();
        ids
    }

    #[test]
    fn successors_are_the_shards_overlapping_a_removed_range() {
        let manager = ShardManager::detached_for_tests();
        manager.set_assignments_for_tests(&[(0, 0, 99), (1, 100, 199)]);
        let shard_0 = range_of(&manager, 0);
        let shard_1 = range_of(&manager, 1);

        // Shard 0 is split into shards 2 and 3.
        manager.set_assignments_for_tests(&[(1, 100, 199), (2, 0, 49), (3, 50, 99)]);
        assert_eq!(successor_ids(&manager, &shard_0), vec![2, 3]);
        let shard_3 = range_of(&manager, 3);

        // Shard 3 is split too: shard 0's successors are now 2, 4 and 5.
        manager.set_assignments_for_tests(&[(1, 100, 199), (2, 0, 49), (4, 50, 74), (5, 75, 99)]);
        assert_eq!(successor_ids(&manager, &shard_0), vec![2, 4, 5]);
        assert_eq!(successor_ids(&manager, &shard_3), vec![4, 5]);
        // A shard that still exists is its own only successor.
        assert_eq!(successor_ids(&manager, &shard_1), vec![1]);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_update_ends_on_update_or_timeout() {
        let manager = ShardManager::detached_for_tests();
        let version = manager.assignments_version();

        // No update: the wait lasts the whole timeout.
        let start = tokio::time::Instant::now();
        manager
            .wait_for_update(version, Duration::from_secs(5))
            .await;
        assert!(start.elapsed() >= Duration::from_secs(5));

        // An update past `version` ends the wait, even one made before the
        // wait began.
        manager.inner.version.send_modify(|version| *version += 1);
        let start = tokio::time::Instant::now();
        manager
            .wait_for_update(version, Duration::from_secs(5))
            .await;
        assert_eq!(start.elapsed(), Duration::ZERO);
    }
}
