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

//! A byte budget for the writes the client holds in memory, and the
//! backpressure that keeps it: the per-shard in-flight window bounds how many
//! batches are *dispatched*, but the batches themselves are unbounded, so
//! without a budget a caller that issues writes faster than the cluster
//! acknowledges them grows the open batch until the process runs out of
//! memory.
//!
//! A write reserves its key and value bytes before it is queued for batching
//! and holds the reservation until its response arrives. While usage is at or
//! above the limit, a write waits, which is what pushes back on the caller:
//! `put(..).await` simply takes longer.
//!
//! A reservation is admitted when usage is *below* the limit and then charged
//! in full, so usage can overshoot by one write. That is deliberate: it makes
//! the budget a limit on what is *already* in flight rather than on what may
//! be, and it keeps a write larger than the whole budget from waiting forever.
//! Waiting writes are admitted strictly in the order they were issued, so the
//! budget never reorders the writes of a shard and a steady stream of small
//! writes cannot starve a large one.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use tokio::sync::oneshot;

/// Caps the bytes held for the writes in flight on one client.
pub(crate) struct MemoryLimiter {
    /// `0` means unlimited: every reservation is admitted untracked.
    limit: usize,
    state: RefCell<State>,
}

#[derive(Default)]
struct State {
    used: usize,
    /// Writes waiting for capacity, in the order they were issued. Each is
    /// charged by the release that admits it, so an admitted write never
    /// finds the capacity taken again.
    waiting: VecDeque<Waiter>,
}

struct Waiter {
    size: usize,
    /// Carries the already-charged reservation; dropping it (the caller went
    /// away while waiting) releases the bytes.
    admitted: oneshot::Sender<Reservation>,
}

impl MemoryLimiter {
    pub(crate) fn new(limit: usize) -> Rc<Self> {
        Rc::new(MemoryLimiter {
            limit,
            state: RefCell::new(State::default()),
        })
    }

    /// Reserves `size` bytes, waiting until the budget has room. With room
    /// available it returns without yielding, so a write that is not
    /// backpressured still reaches its batcher in the poll that issued it.
    pub(crate) async fn reserve(self: &Rc<Self>, size: usize) -> Reservation {
        if self.limit == 0 {
            return Reservation {
                limiter: None,
                size: 0,
            };
        }
        let waiting = {
            let mut st = self.state.borrow_mut();
            // Queue behind the writes already waiting, even with room
            // available, so writes are admitted in issue order.
            if st.waiting.is_empty() && st.used < self.limit {
                st.used += size;
                None
            } else {
                let (tx, rx) = oneshot::channel();
                st.waiting.push_back(Waiter { size, admitted: tx });
                Some(rx)
            }
        };
        match waiting {
            None => Reservation {
                limiter: Some(self.clone()),
                size,
            },
            // The reservation is built by the release that admits this write.
            // The sender lives in the limiter, which this `Rc` keeps alive, so
            // the receiver cannot be dropped first.
            Some(rx) => rx.await.expect("the limiter outlives the waiting write"),
        }
    }

    /// Returns `size` bytes to the budget and admits whatever now fits.
    fn release(self: &Rc<Self>, size: usize) {
        let admitted: Vec<Waiter> = {
            let mut st = self.state.borrow_mut();
            st.used -= size;
            let mut admitted = Vec::new();
            while st.used < self.limit {
                let Some(waiter) = st.waiting.pop_front() else {
                    break;
                };
                st.used += waiter.size;
                admitted.push(waiter);
            }
            admitted
        };
        for waiter in admitted {
            // Dropped on a send failure, which releases the bytes again.
            let _ = waiter.admitted.send(Reservation {
                limiter: Some(self.clone()),
                size: waiter.size,
            });
        }
    }

    #[cfg(test)]
    fn used(&self) -> usize {
        self.state.borrow().used
    }

    #[cfg(test)]
    fn waiting(&self) -> usize {
        self.state.borrow().waiting.len()
    }
}

/// Bytes charged to a client's write budget, returned when dropped.
pub(crate) struct Reservation {
    /// `None` when the client has no limit: nothing to return.
    limiter: Option<Rc<MemoryLimiter>>,
    size: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(limiter) = self.limiter.take() {
            limiter.release(self.size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::task::Poll;

    /// Polls `future` once, to tell a reservation that completes without
    /// waiting from one that is queued.
    fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
        let mut future = Box::pin(future);
        let waker = std::task::Waker::noop();
        future
            .as_mut()
            .poll(&mut std::task::Context::from_waker(waker))
    }

    #[test]
    fn a_reservation_under_the_limit_is_admitted_without_waiting() {
        let limiter = MemoryLimiter::new(100);
        let reservation = poll_once(limiter.reserve(30));
        assert!(matches!(reservation, Poll::Ready(_)));
        assert_eq!(limiter.used(), 30);
        drop(reservation);
        assert_eq!(limiter.used(), 0);
    }

    #[test]
    fn a_write_larger_than_the_limit_is_admitted_alone() {
        let limiter = MemoryLimiter::new(100);
        let big = poll_once(limiter.reserve(400));
        assert!(matches!(big, Poll::Ready(_)), "an empty budget takes it");
        assert_eq!(limiter.used(), 400);
        // Nothing else gets in until it completes.
        assert!(matches!(poll_once(limiter.reserve(1)), Poll::Pending));
        drop(big);
        assert_eq!(limiter.used(), 0);
    }

    #[test]
    fn no_limit_tracks_nothing() {
        let limiter = MemoryLimiter::new(0);
        let a = poll_once(limiter.reserve(usize::MAX));
        let b = poll_once(limiter.reserve(usize::MAX));
        assert!(matches!(a, Poll::Ready(_)));
        assert!(matches!(b, Poll::Ready(_)));
        assert_eq!(limiter.used(), 0);
    }

    #[compio::test]
    async fn a_release_admits_the_waiting_writes_in_issue_order() {
        let limiter = MemoryLimiter::new(100);
        let first = limiter.reserve(100).await;
        // Both wait: the budget is full.
        let admitted = Rc::new(RefCell::new(Vec::new()));
        let waiting: Vec<_> = [1usize, 2]
            .into_iter()
            .map(|n| {
                let limiter = limiter.clone();
                let admitted = admitted.clone();
                compio::runtime::spawn(async move {
                    let reservation = limiter.reserve(n * 10).await;
                    admitted.borrow_mut().push(n);
                    reservation
                })
            })
            .collect();
        compio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(limiter.waiting(), 2);
        assert!(admitted.borrow().is_empty());

        drop(first);
        let reservations: Vec<_> = futures::future::join_all(waiting)
            .await
            .into_iter()
            .map(|r| r.expect("reservation task"))
            .collect();
        assert_eq!(*admitted.borrow(), [1, 2], "admitted in issue order");
        assert_eq!(limiter.used(), 30);
        drop(reservations);
        assert_eq!(limiter.used(), 0);
    }

    #[compio::test]
    async fn a_write_that_stops_waiting_releases_its_bytes() {
        let limiter = MemoryLimiter::new(100);
        let first = limiter.reserve(100).await;
        let gone = {
            let limiter = limiter.clone();
            compio::runtime::spawn(async move { limiter.reserve(50).await })
        };
        compio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(limiter.waiting(), 1);
        // The caller goes away while queued; the admitting release hands its
        // bytes straight back.
        drop(gone);
        drop(first);
        compio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(limiter.used(), 0);
        assert_eq!(limiter.waiting(), 0);
    }
}
