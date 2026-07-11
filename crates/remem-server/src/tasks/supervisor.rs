use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::engine::StorageEngine;
use crate::services::{ConnectionManager, LifecycleManager};
use crate::tasks::TaskRegistry;

/// Shared health counters for discovery worker slots, readable from the task
/// status API without a reference to the supervisor.
#[derive(Clone)]
pub struct DiscoveryWorkerState {
    pub alive: Arc<AtomicUsize>,
    pub total: usize,
    pub restart_count: Arc<AtomicU32>,
    pub last_panic: Arc<Mutex<Option<String>>>,
}

impl DiscoveryWorkerState {
    pub fn new(total: usize) -> Self {
        Self {
            alive: Arc::new(AtomicUsize::new(0)),
            total,
            restart_count: Arc::new(AtomicU32::new(0)),
            last_panic: Arc::new(Mutex::new(None)),
        }
    }
}

/// Owns all app-level background workers and provides a single shutdown path.
///
/// Storage-engine-internal tasks (flush, compaction, checkpoint) are managed
/// by `StorageEngine::graceful_shutdown` and are NOT included here.
pub struct TaskSupervisor {
    cancel: CancellationToken,
    handles: Vec<(&'static str, JoinHandle<()>)>,
}

impl TaskSupervisor {
    pub fn new(cancel: CancellationToken) -> Self {
        Self { cancel, handles: Vec::new() }
    }

    /// Spawn the lifecycle schedule loop (expiry, decay, consolidation, etc.).
    pub fn spawn_lifecycle(
        &mut self,
        lifecycle: Arc<LifecycleManager>,
        registry: Arc<TaskRegistry>,
        engine: Arc<StorageEngine>,
        cfg: crate::config::TaskConfig,
    ) {
        let token = self.cancel.clone();
        let handle = tokio::spawn(async move {
            crate::tasks::lifecycle::run(lifecycle, registry, engine, cfg, token).await;
        });
        self.handles.push(("lifecycle", handle));
    }

    /// Spawn `n` discovery workers. Each slot has an outer restart loop that
    /// detects panics in the inner worker and retries with exponential backoff
    /// (capped at 8 s, max `max_restarts` retries per slot).
    pub fn spawn_discovery_workers(
        &mut self,
        n: usize,
        rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<crate::services::connection_manager::DiscoveryTask>>>,
        connection: Arc<ConnectionManager>,
        state: &DiscoveryWorkerState,
    ) {
        for _ in 0..n {
            let rx = Arc::clone(&rx);
            let conn = Arc::clone(&connection);
            self.spawn_restartable_worker("discovery", state, 5, move || {
                let rx = Arc::clone(&rx);
                let conn = Arc::clone(&conn);
                async move {
                    loop {
                        let task = {
                            let mut guard = rx.lock().await;
                            guard.recv().await
                        };
                        match task {
                            None => break, // channel closed — all senders dropped (server shutting down)
                            Some(t) => {
                                match conn
                                    .auto_discover(t.memory_id, &t.embedding, t.threshold, t.top_k)
                                    .await
                                {
                                    Ok(conns) => tracing::debug!(
                                        memory_id = %t.memory_id,
                                        count = conns.len(),
                                        "auto_discover completed"
                                    ),
                                    Err(e) => tracing::warn!(
                                        memory_id = %t.memory_id,
                                        error = %e,
                                        "auto_discover failed"
                                    ),
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    /// Cancel all workers and wait for them to finish.
    pub async fn shutdown(self) {
        self.cancel.cancel();
        for (name, handle) in self.handles {
            match handle.await {
                Ok(()) => tracing::debug!(worker = name, "worker stopped cleanly"),
                Err(e) => tracing::error!(
                    worker = name,
                    panic = %e,
                    "worker panicked during shutdown"
                ),
            }
        }
    }

    /// Wrap `make_worker` in an outer restart loop. On panic the outer loop
    /// logs the error, updates the shared counters, sleeps with exponential
    /// backoff (2^n seconds, capped at 8 s), and respawns — up to
    /// `max_restarts` times. On a clean exit or cancellation it just breaks.
    fn spawn_restartable_worker<F, Fut>(
        &mut self,
        name: &'static str,
        state: &DiscoveryWorkerState,
        max_restarts: u32,
        make_worker: F,
    ) where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let alive = Arc::clone(&state.alive);
        let restart_count = Arc::clone(&state.restart_count);
        let last_panic = Arc::clone(&state.last_panic);
        let cancel = self.cancel.clone();

        let outer = tokio::spawn(async move {
            let mut slot_restarts = 0u32;
            loop {
                alive.fetch_add(1, Ordering::Relaxed);
                let inner = tokio::spawn(make_worker());
                match inner.await {
                    Ok(()) => {
                        alive.fetch_sub(1, Ordering::Relaxed);
                        break; // clean exit
                    }
                    Err(e) => {
                        alive.fetch_sub(1, Ordering::Relaxed);
                        let msg = format!("{e}");
                        tracing::error!(
                            worker = name,
                            panic = %msg,
                            slot_restarts,
                            "worker panicked"
                        );
                        if let Ok(mut guard) = last_panic.lock() {
                            *guard = Some(msg);
                        }
                        restart_count.fetch_add(1, Ordering::Relaxed);
                        slot_restarts += 1;
                        if slot_restarts > max_restarts {
                            tracing::error!(
                                worker = name,
                                "giving up after {} restart(s)", max_restarts
                            );
                            break;
                        }
                        let delay = std::time::Duration::from_secs(
                            2u64.pow(slot_restarts.min(3))
                        );
                        tokio::select! {
                            _ = cancel.cancelled() => break,
                            _ = tokio::time::sleep(delay) => {}
                        }
                    }
                }
                if cancel.is_cancelled() {
                    break;
                }
            }
        });
        self.handles.push((name, outer));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_worker_state_initializes_correctly() {
        let s = DiscoveryWorkerState::new(2);
        assert_eq!(s.total, 2);
        assert_eq!(s.alive.load(Ordering::Relaxed), 0);
        assert_eq!(s.restart_count.load(Ordering::Relaxed), 0);
        assert!(s.last_panic.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn shutdown_awaits_all_handles() {
        let token = CancellationToken::new();
        let mut sup = TaskSupervisor::new(token);
        let cancel = sup.cancel.clone();
        let h = tokio::spawn(async move { cancel.cancelled().await; });
        sup.handles.push(("test_worker", h));

        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            sup.shutdown(),
        )
        .await
        .expect("shutdown must complete within 500ms");
    }

    #[tokio::test]
    async fn panic_updates_counters_and_gives_up_at_zero_max() {
        let token = CancellationToken::new();
        let state = DiscoveryWorkerState::new(1);
        let mut sup = TaskSupervisor::new(token.clone());

        sup.spawn_restartable_worker("test", &state, 0, || async {
            panic!("deliberate test panic");
        });

        // Yield enough times for: outer spawns inner → inner panics →
        // outer records panic → outer gives up (no sleep with max=0).
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        assert_eq!(state.restart_count.load(Ordering::Relaxed), 1, "restart_count");
        assert_eq!(state.alive.load(Ordering::Relaxed), 0, "alive");
        assert!(state.last_panic.lock().unwrap().is_some(), "last_panic set");

        sup.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn panic_triggers_restart_and_worker_recovers() {
        let token = CancellationToken::new();
        let state = DiscoveryWorkerState::new(1);
        let mut sup = TaskSupervisor::new(token.clone());

        let call_count = Arc::new(AtomicU32::new(0));
        let cc = Arc::clone(&call_count);

        sup.spawn_restartable_worker("test", &state, 5, move || {
            let c = Arc::clone(&cc);
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    panic!("first run panics");
                }
                // second run exits cleanly
            }
        });

        // Let inner task run and panic; outer starts sleep(2^1 = 2s).
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(state.restart_count.load(Ordering::Relaxed), 1);

        // Advance past the 2s backoff.
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        // Second run exits cleanly → alive back to 0.
        assert_eq!(state.alive.load(Ordering::Relaxed), 0);
        assert_eq!(call_count.load(Ordering::Relaxed), 2);

        sup.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn max_restarts_gives_up_after_n_retries() {
        // max_restarts = 2: original + 2 retries = 3 panics total, then gives up.
        let token = CancellationToken::new();
        let state = DiscoveryWorkerState::new(1);
        let mut sup = TaskSupervisor::new(token.clone());

        sup.spawn_restartable_worker("test", &state, 2, || async {
            panic!("always panics");
        });

        // Panic 1 → sleep 2s (2^1); panic 2 → sleep 4s (2^2); panic 3 → give up.
        for _ in 0..20 { tokio::task::yield_now().await; }
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        for _ in 0..20 { tokio::task::yield_now().await; }
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        for _ in 0..20 { tokio::task::yield_now().await; }

        assert_eq!(state.restart_count.load(Ordering::Relaxed), 3, "3 panics total");
        assert_eq!(state.alive.load(Ordering::Relaxed), 0, "no alive workers");

        sup.shutdown().await;
    }
}
