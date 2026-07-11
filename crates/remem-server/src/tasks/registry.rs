use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::services::types::now_ms;

#[derive(Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct RunLog {
    /// When the task started (0 for legacy entries written before this field existed).
    #[serde(default)]
    pub started_ms: u64,
    /// When the task finished.
    pub run_ms: u64,
    pub count: i64,
    pub error: Option<String>,
}

#[derive(Clone, Serialize, utoipa::ToSchema)]
pub struct TaskStatus {
    pub name: String,
    pub schedule: String,
    pub is_running: bool,
    #[serde(default)]
    pub is_paused: bool,
    pub last_started_ms: Option<u64>,
    pub last_run_ms: Option<u64>,
    pub last_count: Option<i64>,
    pub last_error: Option<String>,
}

struct TaskEntry {
    status: TaskStatus,
    /// Whether the task is currently paused (suppressed at next tick).
    is_paused: bool,
    /// Timestamp when the current (or last) run began — used to populate RunLog.
    current_started_ms: Option<u64>,
    /// Complete run history kept in memory — loaded from disk on startup.
    history: Vec<RunLog>,
}

/// Shared registry tracking background task status and persistent run history.
#[derive(Clone)]
pub struct TaskRegistry {
    inner: Arc<Mutex<HashMap<String, TaskEntry>>>,
    log_dir: PathBuf,
    started_at_ms: u64,
}

const TASKS: &[(&str, &str)] = &[
    ("expire_short_term", "Every 5 minutes"),
    ("apply_importance_decay", "Daily"),
    ("active_forgetting", "Daily"),
    ("consolidate_similar", "Weekly"),
    ("cleanup_archived", "Monthly"),
    ("discover_connections", "Every hour"),
    ("checkpoint", "On demand"),
];

impl TaskRegistry {
    pub fn new(data_dir: &PathBuf) -> Self {
        let log_dir = data_dir.join("task_logs");
        if let Err(e) = fs::create_dir_all(&log_dir) {
            tracing::warn!(dir = %log_dir.display(), error = %e, "could not create task_logs dir");
        }

        let mut map = HashMap::new();
        for (name, schedule) in TASKS {
            let history = Self::load_history(&log_dir, name);
            let (last_started_ms, last_run_ms, last_count, last_error) = history
                .last()
                .map(|l| (
                    if l.started_ms > 0 { Some(l.started_ms) } else { None },
                    Some(l.run_ms),
                    Some(l.count),
                    l.error.clone(),
                ))
                .unwrap_or((None, None, None, None));

            map.insert(
                name.to_string(),
                TaskEntry {
                    status: TaskStatus {
                        name: name.to_string(),
                        schedule: schedule.to_string(),
                        is_running: false,
                        is_paused: false,
                        last_started_ms,
                        last_run_ms,
                        last_count,
                        last_error,
                    },
                    is_paused: false,
                    current_started_ms: None,
                    history,
                },
            );
        }

        Self {
            inner: Arc::new(Mutex::new(map)),
            log_dir,
            started_at_ms: now_ms(),
        }
    }

    fn load_history(log_dir: &PathBuf, name: &str) -> Vec<RunLog> {
        let path = log_dir.join(format!("{name}.jsonl"));
        let Ok(file) = fs::File::open(&path) else {
            return Vec::new();
        };
        BufReader::new(file)
            .lines()
            .filter_map(|line| {
                let line = line.ok()?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    return None;
                }
                match serde_json::from_str::<RunLog>(trimmed) {
                    Ok(log) => Some(log),
                    Err(e) => {
                        tracing::warn!(task = name, error = %e, "skipping malformed log line");
                        None
                    }
                }
            })
            .collect()
    }

    fn append_to_disk(&self, name: &str, log: &RunLog) {
        let path = self.log_dir.join(format!("{name}.jsonl"));
        match serde_json::to_string(log) {
            Ok(json) => {
                match OpenOptions::new().create(true).append(true).open(&path) {
                    Ok(mut file) => {
                        if let Err(e) = writeln!(file, "{json}") {
                            tracing::warn!(task = name, error = %e, "failed to append run log");
                        }
                    }
                    Err(e) => tracing::warn!(task = name, error = %e, "failed to open log file"),
                }
            }
            Err(e) => tracing::warn!(task = name, error = %e, "failed to serialize run log"),
        }
    }

    pub fn set_running(&self, name: &str) {
        let started_ms = now_ms();
        if let Ok(mut map) = self.inner.lock() {
            if let Some(e) = map.get_mut(name) {
                e.status.is_running = true;
                e.status.last_started_ms = Some(started_ms);
                e.current_started_ms = Some(started_ms);
            }
        }
    }

    pub fn record_result(&self, name: &str, count: i64, error: Option<String>) {
        let run_ms = now_ms();
        let mut log = RunLog { started_ms: 0, run_ms, count, error: error.clone() };

        // Update in-memory state while holding the lock.
        if let Ok(mut map) = self.inner.lock() {
            if let Some(e) = map.get_mut(name) {
                log.started_ms = e.current_started_ms.unwrap_or(run_ms);
                e.status.is_running = false;
                e.status.last_run_ms = Some(run_ms);
                e.status.last_count = Some(count);
                e.status.last_error = error;
                e.history.push(log.clone());
            }
        }
        // Append to disk after releasing the lock.
        self.append_to_disk(name, &log);
    }

    pub fn is_known(&self, name: &str) -> bool {
        TASKS.iter().any(|(n, _)| *n == name)
    }

    pub fn pause(&self, name: &str) {
        if let Ok(mut map) = self.inner.lock() {
            if let Some(e) = map.get_mut(name) {
                e.is_paused = true;
                e.status.is_paused = true;
            }
        }
    }

    pub fn resume(&self, name: &str) {
        if let Ok(mut map) = self.inner.lock() {
            if let Some(e) = map.get_mut(name) {
                e.is_paused = false;
                e.status.is_paused = false;
            }
        }
    }

    pub fn is_paused(&self, name: &str) -> bool {
        self.inner
            .lock()
            .ok()
            .and_then(|map| map.get(name).map(|e| e.is_paused))
            .unwrap_or(false)
    }

    /// Returns true if the named task is currently executing.
    pub fn is_running_now(&self, name: &str) -> bool {
        self.inner
            .lock()
            .ok()
            .and_then(|map| map.get(name).map(|e| e.status.is_running))
            .unwrap_or(false)
    }

    /// Atomically check-and-set: marks the task as running and returns true.
    /// Returns false (without mutating) if the task is already running.
    /// Callers must call `record_result` when the task finishes.
    pub fn try_set_running(&self, name: &str) -> bool {
        let started_ms = now_ms();
        if let Ok(mut map) = self.inner.lock() {
            if let Some(e) = map.get_mut(name) {
                if e.status.is_running {
                    return false;
                }
                e.status.is_running = true;
                e.status.last_started_ms = Some(started_ms);
                e.current_started_ms = Some(started_ms);
                return true;
            }
        }
        false
    }

    pub fn list(&self) -> Vec<TaskStatus> {
        if let Ok(map) = self.inner.lock() {
            let mut tasks: Vec<TaskStatus> = map.values().map(|e| e.status.clone()).collect();
            tasks.sort_by_key(|t| t.name.clone());
            tasks
        } else {
            vec![]
        }
    }

    /// Returns all run logs for a task, most-recent first.
    pub fn get_history(&self, name: &str) -> Vec<RunLog> {
        if let Ok(map) = self.inner.lock() {
            map.get(name)
                .map(|e| {
                    let mut h = e.history.clone();
                    h.reverse();
                    h
                })
                .unwrap_or_default()
        } else {
            vec![]
        }
    }

    pub fn uptime_ms(&self) -> u64 {
        now_ms().saturating_sub(self.started_at_ms)
    }
}

#[cfg(test)]
mod pause_tests {
    use super::*;
    use std::path::PathBuf;

    fn test_registry() -> TaskRegistry {
        TaskRegistry::new(&PathBuf::from("/tmp/remem-test-registry"))
    }

    #[test]
    fn pause_and_resume() {
        let r = test_registry();
        assert!(!r.is_paused("expire_short_term"));
        r.pause("expire_short_term");
        assert!(r.is_paused("expire_short_term"));
        r.resume("expire_short_term");
        assert!(!r.is_paused("expire_short_term"));
    }

    #[test]
    fn pause_unknown_task_is_noop() {
        let r = test_registry();
        r.pause("does_not_exist"); // must not panic
        assert!(!r.is_paused("does_not_exist"));
    }

    #[test]
    fn paused_flag_reflected_in_task_status() {
        let r = test_registry();
        r.pause("checkpoint");
        let status = r.list();
        let ckpt = status.iter().find(|t| t.name == "checkpoint").unwrap();
        assert!(ckpt.is_paused);
    }

    #[test]
    fn is_running_now_reflects_set_running() {
        let tmp = tempfile::tempdir().unwrap();
        let r = TaskRegistry::new(&tmp.path().to_path_buf());

        assert!(!r.is_running_now("expire_short_term"));
        r.set_running("expire_short_term");
        assert!(r.is_running_now("expire_short_term"));
        r.record_result("expire_short_term", 0, None);
        assert!(!r.is_running_now("expire_short_term"));
    }
}
