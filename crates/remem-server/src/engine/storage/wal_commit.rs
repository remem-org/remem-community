use super::wal::{WalRecord, WAL};
use crate::engine::error::{Result, StorageError};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

const DEFAULT_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_MAX_GROUP_BATCHES: usize = 256;

#[derive(Clone)]
pub(crate) struct WalCommitOptions {
    sync_writes: bool,
    queue_capacity: usize,
    max_group_batches: usize,
    #[cfg(test)]
    test_hooks: Option<Arc<WalCommitTestHooks>>,
}

impl WalCommitOptions {
    pub(crate) fn new(sync_writes: bool) -> Self {
        Self {
            sync_writes,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            max_group_batches: DEFAULT_MAX_GROUP_BATCHES,
            #[cfg(test)]
            test_hooks: Some(Arc::new(WalCommitTestHooks::default())),
        }
    }

    #[cfg(test)]
    fn durable() -> Self {
        Self::new(true)
    }

    #[cfg(test)]
    fn with_test_hooks(mut self, hooks: Arc<WalCommitTestHooks>) -> Self {
        self.test_hooks = Some(hooks);
        self
    }
}

#[cfg(test)]
#[derive(Default)]
struct WalCommitTestHooks {
    pause_next_drain: std::sync::atomic::AtomicBool,
    pause_next_sync: std::sync::atomic::AtomicBool,
    fail_next_sync: std::sync::atomic::AtomicBool,
    fail_next_append: std::sync::atomic::AtomicBool,
    sync_blocked: tokio::sync::Notify,
    drain_blocked: tokio::sync::Notify,
    release: (std::sync::Mutex<bool>, std::sync::Condvar),
}

#[cfg(test)]
impl WalCommitTestHooks {
    fn pause_next_drain(&self) {
        *self.release.0.lock().unwrap() = false;
        self.pause_next_drain.store(true, Ordering::Release);
    }

    async fn wait_until_drain_blocked(&self) {
        self.drain_blocked.notified().await;
    }

    fn pause_next_sync(&self) {
        *self.release.0.lock().unwrap() = false;
        self.pause_next_sync.store(true, Ordering::Release);
    }

    async fn wait_until_sync_blocked(&self) {
        self.sync_blocked.notified().await;
    }

    fn release_sync(&self) {
        *self.release.0.lock().unwrap() = true;
        self.release.1.notify_all();
    }

    fn fail_next_sync(&self) {
        self.fail_next_sync.store(true, Ordering::Release);
    }

    fn fail_next_append(&self) {
        self.fail_next_append.store(true, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct CommitPosition(NonZeroU64);

impl CommitPosition {
    pub(crate) fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[allow(dead_code)]
pub(crate) struct WalCommitStats {
    pub(crate) submitted_batches: usize,
    pub(crate) physical_syncs: usize,
    pub(crate) grouped_batches: usize,
    pub(crate) max_group_size: usize,
    pub(crate) failures: usize,
    pub(crate) queue_depth: usize,
    pub(crate) durable_position: u64,
}

#[derive(Default)]
struct SharedStats {
    submitted_batches: AtomicUsize,
    physical_syncs: AtomicUsize,
    grouped_batches: AtomicUsize,
    max_group_size: AtomicUsize,
    failures: AtomicUsize,
    queue_depth: AtomicUsize,
    durable_position: AtomicU64,
}

type Completion = oneshot::Sender<Result<CommitPosition>>;

pub(crate) struct PendingCommit {
    completion: oneshot::Receiver<Result<CommitPosition>>,
}

impl PendingCommit {
    pub(crate) async fn wait(self) -> Result<CommitPosition> {
        self.completion
            .await
            .map_err(|_| coordinator_unavailable())?
    }
}

enum Command {
    Write {
        position: CommitPosition,
        records: Vec<WalRecord>,
        completion: Completion,
    },
    Barrier(oneshot::Sender<Result<()>>),
    Size(oneshot::Sender<Result<u64>>),
    Truncate(oneshot::Sender<Result<()>>),
    Shutdown(oneshot::Sender<Result<()>>),
}

pub(crate) struct WalCommitCoordinator {
    tx: mpsc::Sender<Command>,
    next_position: AtomicU64,
    stats: Arc<SharedStats>,
    worker: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
    #[cfg(test)]
    test_hooks: Option<Arc<WalCommitTestHooks>>,
}

impl WalCommitCoordinator {
    pub(crate) fn start(wal: WAL, options: WalCommitOptions) -> Self {
        let (tx, rx) = mpsc::channel(options.queue_capacity);
        let stats = Arc::new(SharedStats::default());
        let worker_stats = Arc::clone(&stats);
        #[cfg(test)]
        let test_hooks = options.test_hooks.clone();
        let worker = std::thread::Builder::new()
            .name("remem-wal-commit".to_owned())
            .spawn(move || run_worker(wal, rx, options, worker_stats))
            .expect("failed to start WAL commit worker");
        Self {
            tx,
            next_position: AtomicU64::new(1),
            stats,
            worker: parking_lot::Mutex::new(Some(worker)),
            #[cfg(test)]
            test_hooks,
        }
    }

    #[cfg(test)]
    pub(crate) async fn submit(&self, records: Vec<WalRecord>) -> Result<CommitPosition> {
        self.enqueue(records).await?.wait().await
    }

    pub(crate) async fn enqueue(&self, records: Vec<WalRecord>) -> Result<PendingCommit> {
        if records.is_empty() {
            return Err(StorageError::InvalidArgument(
                "WAL commit batch must not be empty".to_owned(),
            ));
        }
        let raw = self.next_position.fetch_add(1, Ordering::Relaxed);
        let position = CommitPosition(NonZeroU64::new(raw).ok_or_else(|| {
            StorageError::InvalidArgument("WAL commit position exhausted".to_owned())
        })?);
        let (completion, rx) = oneshot::channel();
        self.stats.submitted_batches.fetch_add(1, Ordering::Relaxed);
        self.stats.queue_depth.fetch_add(1, Ordering::Relaxed);
        if self
            .tx
            .send(Command::Write {
                position,
                records,
                completion,
            })
            .await
            .is_err()
        {
            self.stats.queue_depth.fetch_sub(1, Ordering::Relaxed);
            return Err(coordinator_unavailable());
        }
        Ok(PendingCommit { completion: rx })
    }

    pub(crate) async fn barrier(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Barrier(tx))
            .await
            .map_err(|_| coordinator_unavailable())?;
        rx.await.map_err(|_| coordinator_unavailable())?
    }

    pub(crate) async fn size(&self) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Size(tx))
            .await
            .map_err(|_| coordinator_unavailable())?;
        rx.await.map_err(|_| coordinator_unavailable())?
    }

    pub(crate) async fn truncate(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Truncate(tx))
            .await
            .map_err(|_| coordinator_unavailable())?;
        rx.await.map_err(|_| coordinator_unavailable())?
    }

    pub(crate) async fn shutdown(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Shutdown(tx))
            .await
            .map_err(|_| coordinator_unavailable())?;
        let result = rx.await.map_err(|_| coordinator_unavailable())?;
        let worker = { self.worker.lock().take() };
        if let Some(handle) = worker {
            tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
                .map_err(|_| {
                    StorageError::Io(std::io::Error::other("WAL commit worker panicked"))
                })?;
        }
        result
    }

    #[allow(dead_code)]
    pub(crate) fn stats(&self) -> WalCommitStats {
        WalCommitStats {
            submitted_batches: self.stats.submitted_batches.load(Ordering::Relaxed),
            physical_syncs: self.stats.physical_syncs.load(Ordering::Relaxed),
            grouped_batches: self.stats.grouped_batches.load(Ordering::Relaxed),
            max_group_size: self.stats.max_group_size.load(Ordering::Relaxed),
            failures: self.stats.failures.load(Ordering::Relaxed),
            queue_depth: self.stats.queue_depth.load(Ordering::Relaxed),
            durable_position: self.stats.durable_position.load(Ordering::Acquire),
        }
    }

    #[cfg(test)]
    pub(crate) fn pause_next_sync(&self) {
        self.test_hooks().pause_next_sync();
    }

    #[cfg(test)]
    pub(crate) async fn wait_until_sync_blocked(&self) {
        self.test_hooks().wait_until_sync_blocked().await;
    }

    #[cfg(test)]
    pub(crate) fn release_sync(&self) {
        self.test_hooks().release_sync();
    }

    #[cfg(test)]
    pub(crate) fn fail_next_sync(&self) {
        self.test_hooks().fail_next_sync();
    }

    #[cfg(test)]
    fn test_hooks(&self) -> &Arc<WalCommitTestHooks> {
        // Every coordinator constructed in a test build gets hooks from
        // `WalCommitOptions::new`; focused unit tests may replace that Arc.
        self.test_hooks
            .as_ref()
            .expect("test WAL coordinator hooks are installed")
    }
}

fn coordinator_unavailable() -> StorageError {
    StorageError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "WAL commit coordinator is unavailable",
    ))
}

fn result_from_message<T>(result: &Result<T>) -> Result<()> {
    result
        .as_ref()
        .map(|_| ())
        .map_err(|error| StorageError::Io(std::io::Error::other(error.to_string())))
}

fn run_worker(
    mut wal: WAL,
    mut rx: mpsc::Receiver<Command>,
    options: WalCommitOptions,
    stats: Arc<SharedStats>,
) {
    let mut pending = None;
    loop {
        let command = match pending.take().or_else(|| rx.blocking_recv()) {
            Some(command) => command,
            None => break,
        };
        match command {
            Command::Write {
                position,
                records,
                completion,
            } => {
                let mut group = vec![(position, records, completion)];
                #[cfg(test)]
                if let Some(hooks) = &options.test_hooks {
                    if hooks.pause_next_drain.swap(false, Ordering::AcqRel) {
                        hooks.drain_blocked.notify_one();
                        let mut released = hooks.release.0.lock().unwrap();
                        while !*released {
                            released = hooks.release.1.wait(released).unwrap();
                        }
                    }
                }
                while group.len() < options.max_group_batches {
                    match rx.try_recv() {
                        Ok(Command::Write {
                            position,
                            records,
                            completion,
                        }) => {
                            group.push((position, records, completion));
                        }
                        Ok(other) => {
                            pending = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                stats.queue_depth.fetch_sub(group.len(), Ordering::Relaxed);
                stats
                    .max_group_size
                    .fetch_max(group.len(), Ordering::Relaxed);
                if group.len() > 1 {
                    stats
                        .grouped_batches
                        .fetch_add(group.len(), Ordering::Relaxed);
                }
                let mut operation: Result<()> = Ok(());
                #[cfg(test)]
                if let Some(hooks) = &options.test_hooks {
                    if hooks.fail_next_append.swap(false, Ordering::AcqRel) {
                        operation = Err(StorageError::Io(std::io::Error::other(
                            "injected WAL append failure",
                        )));
                    }
                }
                for (_, records, _) in &group {
                    if operation.is_ok() {
                        operation = wal.append_batch(records);
                    }
                }
                if operation.is_ok() && options.sync_writes {
                    stats.physical_syncs.fetch_add(1, Ordering::Relaxed);
                    operation = sync_wal(&mut wal, &options);
                    if operation.is_ok() {
                        let covered = group.last().expect("commit group is non-empty").0.get();
                        stats.durable_position.store(covered, Ordering::Release);
                    }
                }
                if operation.is_err() {
                    stats.failures.fetch_add(group.len(), Ordering::Relaxed);
                }
                for (position, _, completion) in group {
                    let response = result_from_message(&operation).map(|()| position);
                    let _ = completion.send(response);
                }
            }
            Command::Barrier(completion) => {
                let result = sync_wal(&mut wal, &options);
                if result.is_ok() {
                    stats.physical_syncs.fetch_add(1, Ordering::Relaxed);
                }
                let _ = completion.send(result);
            }
            Command::Size(completion) => {
                let _ = completion.send(Ok(wal.size()));
            }
            Command::Truncate(completion) => {
                let result = wal.truncate();
                if result.is_ok() {
                    stats.physical_syncs.fetch_add(1, Ordering::Relaxed);
                }
                let _ = completion.send(result);
            }
            Command::Shutdown(completion) => {
                let result = sync_wal(&mut wal, &options);
                if result.is_ok() {
                    stats.physical_syncs.fetch_add(1, Ordering::Relaxed);
                }
                let _ = completion.send(result);
                break;
            }
        }
    }
}

fn sync_wal(wal: &mut WAL, options: &WalCommitOptions) -> Result<()> {
    #[cfg(not(test))]
    let _ = options;
    #[cfg(test)]
    if let Some(hooks) = &options.test_hooks {
        if hooks.pause_next_sync.swap(false, Ordering::AcqRel) {
            hooks.sync_blocked.notify_one();
            let mut released = hooks.release.0.lock().unwrap();
            while !*released {
                released = hooks.release.1.wait(released).unwrap();
            }
        }
        if hooks.fail_next_sync.swap(false, Ordering::AcqRel) {
            return Err(StorageError::Io(std::io::Error::other(
                "injected WAL sync failure",
            )));
        }
    }
    wal.sync()
}

#[cfg(test)]
mod tests {
    use super::{WalCommitCoordinator, WalCommitOptions, WalCommitTestHooks};
    use crate::engine::storage::wal::{WalRecord, WAL};
    use bytes::Bytes;
    use std::sync::Arc;

    #[tokio::test]
    async fn non_durable_write_completes_without_a_physical_sync() {
        let dir = tempfile::tempdir().unwrap();
        let coordinator = WalCommitCoordinator::start(
            WAL::create(dir.path().join("current.wal")).unwrap(),
            WalCommitOptions::new(false),
        );

        coordinator
            .submit(vec![WalRecord::insert("key".into(), "value".into(), 1)])
            .await
            .unwrap();

        assert_eq!(coordinator.stats().physical_syncs, 0);
        coordinator.barrier().await.unwrap();
        assert_eq!(coordinator.stats().physical_syncs, 1);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn size_and_truncate_are_serialized_behind_prior_writes() {
        let dir = tempfile::tempdir().unwrap();
        let coordinator = WalCommitCoordinator::start(
            WAL::create(dir.path().join("current.wal")).unwrap(),
            WalCommitOptions::durable(),
        );
        coordinator
            .submit(vec![WalRecord::insert("key".into(), "value".into(), 1)])
            .await
            .unwrap();
        assert!(coordinator.size().await.unwrap() > super::super::wal::WAL_HEADER_LEN);
        coordinator.truncate().await.unwrap();
        assert_eq!(
            coordinator.size().await.unwrap(),
            super::super::wal::WAL_HEADER_LEN
        );
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_final_sync_still_stops_the_worker_and_fails_later_submissions() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = Arc::new(WalCommitTestHooks::default());
        hooks.fail_next_sync();
        let coordinator = WalCommitCoordinator::start(
            WAL::create(dir.path().join("current.wal")).unwrap(),
            WalCommitOptions::durable().with_test_hooks(hooks),
        );

        assert!(coordinator.shutdown().await.is_err());
        assert!(coordinator
            .submit(vec![WalRecord::insert("late".into(), "value".into(), 1)])
            .await
            .is_err());
        assert_eq!(coordinator.stats().queue_depth, 0);
    }

    #[tokio::test]
    async fn append_failure_fans_out_without_advancing_durable_position() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = Arc::new(WalCommitTestHooks::default());
        hooks.pause_next_drain();
        hooks.fail_next_append();
        let coordinator = Arc::new(WalCommitCoordinator::start(
            WAL::create(dir.path().join("current.wal")).unwrap(),
            WalCommitOptions::durable().with_test_hooks(Arc::clone(&hooks)),
        ));

        let first = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move {
                coordinator
                    .submit(vec![WalRecord::insert("a".into(), "1".into(), 1)])
                    .await
            })
        };
        hooks.wait_until_drain_blocked().await;
        let second = coordinator
            .enqueue(vec![WalRecord::insert("b".into(), "2".into(), 2)])
            .await
            .unwrap();
        hooks.release_sync();

        assert!(first.await.unwrap().is_err());
        assert!(second.wait().await.is_err());
        assert_eq!(coordinator.stats().durable_position, 0);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn records_from_each_submitted_batch_remain_contiguous() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("current.wal");
        let hooks = Arc::new(WalCommitTestHooks::default());
        hooks.pause_next_drain();
        let coordinator = WalCommitCoordinator::start(
            WAL::create(&path).unwrap(),
            WalCommitOptions::durable().with_test_hooks(Arc::clone(&hooks)),
        );
        let first = coordinator
            .enqueue(vec![
                WalRecord::insert("a1".into(), "1".into(), 1),
                WalRecord::insert("a2".into(), "2".into(), 1),
            ])
            .await
            .unwrap();
        hooks.wait_until_drain_blocked().await;
        let second = coordinator
            .enqueue(vec![
                WalRecord::insert("b1".into(), "3".into(), 2),
                WalRecord::insert("b2".into(), "4".into(), 2),
            ])
            .await
            .unwrap();
        hooks.release_sync();
        first.wait().await.unwrap();
        second.wait().await.unwrap();
        coordinator.shutdown().await.unwrap();

        let keys: Vec<_> = WAL::open(&path)
            .unwrap()
            .iter()
            .unwrap()
            .collect::<crate::engine::error::Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .map(|record| record.key)
            .collect();
        assert_eq!(keys, ["a1", "a2", "b1", "b2"].map(Bytes::from));
    }

    #[tokio::test]
    async fn bytes_from_a_failed_sync_can_be_covered_by_a_later_successful_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("current.wal");
        let hooks = Arc::new(WalCommitTestHooks::default());
        hooks.fail_next_sync();
        let coordinator = WalCommitCoordinator::start(
            WAL::create(&path).unwrap(),
            WalCommitOptions::durable().with_test_hooks(Arc::clone(&hooks)),
        );

        assert!(coordinator
            .submit(vec![WalRecord::insert("a".into(), "1".into(), 1)])
            .await
            .is_err());
        let position = coordinator
            .submit(vec![WalRecord::insert("b".into(), "2".into(), 2)])
            .await
            .unwrap();
        assert_eq!(position.get(), 2);
        assert_eq!(coordinator.stats().durable_position, 2);
        coordinator.shutdown().await.unwrap();

        let records: Vec<_> = WAL::open(&path)
            .unwrap()
            .iter()
            .unwrap()
            .collect::<crate::engine::error::Result<_>>()
            .unwrap();
        assert_eq!(records.len(), 2);
    }

    #[tokio::test]
    async fn isolated_durable_write_is_synced_without_a_batching_timer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("current.wal");
        let coordinator =
            WalCommitCoordinator::start(WAL::create(&path).unwrap(), WalCommitOptions::durable());

        let position = coordinator
            .submit(vec![WalRecord::insert(
                Bytes::from_static(b"key"),
                Bytes::from_static(b"value"),
                1,
            )])
            .await
            .unwrap();

        assert_eq!(position.get(), 1);
        assert_eq!(coordinator.stats().physical_syncs, 1);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn writes_queued_during_a_sync_share_the_next_physical_sync() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = Arc::new(WalCommitTestHooks::default());
        hooks.pause_next_sync();
        let coordinator = Arc::new(WalCommitCoordinator::start(
            WAL::create(dir.path().join("current.wal")).unwrap(),
            WalCommitOptions::durable().with_test_hooks(Arc::clone(&hooks)),
        ));

        let first = {
            let coordinator = Arc::clone(&coordinator);
            tokio::spawn(async move {
                coordinator
                    .submit(vec![WalRecord::insert("a".into(), "1".into(), 1)])
                    .await
            })
        };
        hooks.wait_until_sync_blocked().await;
        let second = coordinator
            .enqueue(vec![WalRecord::insert("b".into(), "2".into(), 2)])
            .await
            .unwrap();
        let third = coordinator
            .enqueue(vec![WalRecord::insert("c".into(), "3".into(), 3)])
            .await
            .unwrap();
        hooks.release_sync();

        assert_eq!(first.await.unwrap().unwrap().get(), 1);
        assert_eq!(second.wait().await.unwrap().get(), 2);
        assert_eq!(third.wait().await.unwrap().get(), 3);
        let stats = coordinator.stats();
        assert_eq!(stats.submitted_batches, 3);
        assert_eq!(stats.physical_syncs, 2);
        assert_eq!(stats.grouped_batches, 2);
        assert_eq!(stats.max_group_size, 2);
        assert_eq!(stats.durable_position, 3);
        assert_eq!(stats.queue_depth, 0);
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn one_failed_sync_fails_every_batch_in_its_group() {
        let dir = tempfile::tempdir().unwrap();
        let hooks = Arc::new(WalCommitTestHooks::default());
        hooks.pause_next_drain();
        let coordinator = Arc::new(WalCommitCoordinator::start(
            WAL::create(dir.path().join("current.wal")).unwrap(),
            WalCommitOptions::durable().with_test_hooks(Arc::clone(&hooks)),
        ));
        let first = coordinator
            .enqueue(vec![WalRecord::insert("a".into(), "1".into(), 1)])
            .await
            .unwrap();
        hooks.wait_until_drain_blocked().await;
        let second = coordinator
            .enqueue(vec![WalRecord::insert("b".into(), "2".into(), 2)])
            .await
            .unwrap();
        let third = coordinator
            .enqueue(vec![WalRecord::insert("c".into(), "3".into(), 3)])
            .await
            .unwrap();
        hooks.fail_next_sync();
        hooks.release_sync();

        assert!(first.wait().await.is_err());
        assert!(second.wait().await.is_err());
        assert!(third.wait().await.is_err());
        assert_eq!(coordinator.stats().failures, 3);
        assert_eq!(coordinator.stats().durable_position, 0);
        coordinator.shutdown().await.unwrap();
    }

    /// Repeatable local microbenchmark for REM-26. Run with:
    /// `cargo test -p remem-server benchmark_group_commit_concurrency_50 -- --ignored --nocapture`
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "local performance benchmark"]
    async fn benchmark_group_commit_concurrency_50() {
        const CONCURRENCY: usize = 50;
        const WRITES_PER_WORKER: usize = 100;
        let dir = tempfile::tempdir().unwrap();
        let coordinator = Arc::new(WalCommitCoordinator::start(
            WAL::create(dir.path().join("current.wal")).unwrap(),
            WalCommitOptions::durable(),
        ));
        let start_gate = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));
        let benchmark_start = std::time::Instant::now();
        let mut workers = Vec::with_capacity(CONCURRENCY);
        for worker in 0..CONCURRENCY {
            let coordinator = Arc::clone(&coordinator);
            let start_gate = Arc::clone(&start_gate);
            workers.push(tokio::spawn(async move {
                start_gate.wait().await;
                let mut latencies = Vec::with_capacity(WRITES_PER_WORKER);
                for sequence in 0..WRITES_PER_WORKER {
                    let started = std::time::Instant::now();
                    coordinator
                        .submit(vec![WalRecord::insert(
                            format!("benchmark:{worker}:{sequence}").into(),
                            Bytes::from_static(b"value"),
                            (worker * WRITES_PER_WORKER + sequence + 1) as u64,
                        )])
                        .await
                        .unwrap();
                    latencies.push(started.elapsed());
                }
                latencies
            }));
        }
        let mut latencies = Vec::with_capacity(CONCURRENCY * WRITES_PER_WORKER);
        for worker in workers {
            latencies.extend(worker.await.unwrap());
        }
        let elapsed = benchmark_start.elapsed();
        latencies.sort_unstable();
        let percentile = |percent: usize| {
            latencies[(latencies.len() - 1) * percent / 100].as_secs_f64() * 1_000.0
        };
        let stats = coordinator.stats();
        let throughput = latencies.len() as f64 / elapsed.as_secs_f64();
        eprintln!(
            "group-commit concurrency={CONCURRENCY} writes={} throughput={throughput:.1}/s \
             p50={:.3}ms p95={:.3}ms p99={:.3}ms submitted={} physical_syncs={} \
             grouped_batches={} max_group_size={}",
            latencies.len(),
            percentile(50),
            percentile(95),
            percentile(99),
            stats.submitted_batches,
            stats.physical_syncs,
            stats.grouped_batches,
            stats.max_group_size,
        );
        assert!(stats.physical_syncs < stats.submitted_batches);
        assert!(stats.max_group_size > 1);
        coordinator.shutdown().await.unwrap();
    }
}
