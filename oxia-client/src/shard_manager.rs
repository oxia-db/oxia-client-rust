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
use tokio::sync::{Mutex, mpsc};
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
struct ShardRange {
    min_hash: u32,
    max_hash: u32,
    shard: i64,
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
}

impl Assignments {
    fn empty() -> Self {
        Assignments {
            shard_key_router: ShardKeyRouter::Xxhash3 as i32,
            key_order: KeyOrder::default(),
            ranges: Vec::new(),
            leaders: HashMap::new(),
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
        }
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
                                local_inner
                                    .assignments
                                    .store(Arc::new(Assignments::from_namespace(na)));
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
            }),
            context: CancellationToken::new(),
            assignment_handle: Mutex::new(None),
        })
    }

    pub fn get_leader(&self, shard_id: i64) -> Option<Node> {
        self.inner
            .assignments
            .load()
            .leaders
            .get(&shard_id)
            .map(|leader| Node {
                service_address: ensure_protocol(leader.clone()),
            })
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
}
