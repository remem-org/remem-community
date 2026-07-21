use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use lru::LruCache;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use crate::error::{AppError, Result};

/// Maximum texts per ONNX call.
const MAX_BATCH_SIZE: usize = 32;

/// How long to wait for a batch to fill before flushing whatever has arrived.
const BATCH_TIMEOUT: Duration = Duration::from_millis(5);

struct PendingEmbed {
    text: String,
    reply: oneshot::Sender<Result<Vec<f32>>>,
}

pub struct EmbeddingService {
    tx: mpsc::Sender<PendingEmbed>,
    cache: Arc<Mutex<LruCache<[u8; 32], Vec<f32>>>>,
}

impl EmbeddingService {
    /// Initialise the embedding model and spawn `num_workers` background batch
    /// workers. Workers share a single `TextEmbedding` (ONNX sessions are
    /// thread-safe) and a single input channel; each independently collects a
    /// batch and runs a `spawn_blocking` inference in parallel with others.
    pub fn new(cache_size: usize) -> Result<Self> {
        // fastembed 3.14.1 does not read FASTEMBED_CACHE_PATH itself.
        let cache_dir = std::env::var("FASTEMBED_CACHE_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".fastembed_cache"));

        let model = Arc::new(
            TextEmbedding::try_new(InitOptions {
                model_name: EmbeddingModel::AllMiniLML6V2,
                show_download_progress: true,
                cache_dir,
                ..InitOptions::default()
            })
            .map_err(|e| AppError::Embedding(e.to_string()))?,
        );

        let capacity = NonZeroUsize::new(cache_size.max(1)).unwrap();
        let cache = Arc::new(Mutex::new(LruCache::new(capacity)));

        // Saturate available cores: each worker runs an independent ONNX
        // inference on a blocking thread while the next worker collects its
        // batch. E.g. on 8 cores: 4 workers → 3 inferences + 1 collecting at
        // any moment ≈ full utilisation.
        let num_workers = std::thread::available_parallelism()
            .map(|n| (n.get() / 2).max(2).min(8))
            .unwrap_or(4);

        // Channel depth: enough to absorb bursts without blocking callers.
        let (tx, rx) = mpsc::channel::<PendingEmbed>(1024);

        // Wrap the receiver in a tokio Mutex so multiple workers can take
        // turns collecting batches without polling.
        let shared_rx = Arc::new(tokio::sync::Mutex::new(rx));

        tracing::info!("embedding service: {} parallel workers", num_workers);

        for _ in 0..num_workers {
            tokio::spawn(embed_batch_worker(
                Arc::clone(&model),
                Arc::clone(&shared_rx),
                Arc::clone(&cache),
            ));
        }

        Ok(Self { tx, cache })
    }

    /// Lightweight constructor for unit tests that exercise service-layer
    /// code paths not requiring real embeddings (e.g.
    /// `LifecycleManager::active_forgetting`). Does not load the ONNX model
    /// or spawn batch workers -- calling `embed()` on the result will hang
    /// forever since nothing drains the channel.
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        let (tx, _rx) = mpsc::channel::<PendingEmbed>(1);
        let capacity = NonZeroUsize::new(1).unwrap();
        let cache = Arc::new(Mutex::new(LruCache::new(capacity)));
        Self { tx, cache }
    }

    /// Embed a single text, using the in-process LRU cache.
    ///
    /// The request is queued to the background workers, which coalesce
    /// concurrent calls into batches before calling ONNX.
    pub async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let key = cache_key(text);
        if let Some(hit) = self.cache.lock().get(&key).cloned() {
            return Ok(hit);
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PendingEmbed { text: text.to_owned(), reply: reply_tx })
            .await
            .map_err(|_| AppError::Embedding("embedding service unavailable".into()))?;

        reply_rx
            .await
            .map_err(|_| AppError::Embedding("embedding worker dropped reply".into()))?
    }
}

/// One parallel embedding worker. Holds the shared receiver lock only while
/// collecting a batch (≤ BATCH_TIMEOUT), then releases it before inference so
/// another worker can immediately start collecting the next batch.
async fn embed_batch_worker(
    model: Arc<TextEmbedding>,
    shared_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<PendingEmbed>>>,
    cache: Arc<Mutex<LruCache<[u8; 32], Vec<f32>>>>,
) {
    loop {
        // --- Collect a batch (holding the receiver lock) ---
        let batch: Vec<PendingEmbed> = {
            let mut rx = shared_rx.lock().await;

            // Block until at least one request arrives.
            let first = match rx.recv().await {
                Some(r) => r,
                None => break, // channel closed — service shut down
            };
            let mut batch = vec![first];

            // Drain more requests within the timeout window.
            let deadline = tokio::time::Instant::now() + BATCH_TIMEOUT;
            while batch.len() < MAX_BATCH_SIZE {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(req)) => batch.push(req),
                    _ => break,
                }
            }

            batch
            // Receiver lock released here — next worker immediately starts
            // collecting its own batch while this one runs ONNX below.
        };

        tracing::debug!("embedding batch of {} texts", batch.len());

        // --- Infer on a blocking thread (runs in parallel with other workers) ---
        let model_ref = Arc::clone(&model);
        let texts: Vec<String> = batch.iter().map(|r| r.text.clone()).collect();

        let embed_result = tokio::task::spawn_blocking(move || {
            let refs: Vec<&str> = texts.iter().map(String::as_str).collect();
            model_ref.embed(refs, None)
        })
        .await;

        match embed_result {
            Ok(Ok(mut embeddings)) => {
                let mut cache_guard = cache.lock();
                for (req, emb_slot) in batch.into_iter().zip(embeddings.iter_mut()) {
                    let mut emb = std::mem::take(emb_slot);
                    l2_normalize(&mut emb);
                    cache_guard.put(cache_key(&req.text), emb.clone());
                    let _ = req.reply.send(Ok(emb));
                }
            }
            Ok(Err(e)) => {
                let msg = e.to_string();
                tracing::error!("ONNX embed error: {}", msg);
                for req in batch {
                    let _ = req.reply.send(Err(AppError::Embedding(msg.clone())));
                }
            }
            Err(e) => {
                let msg = e.to_string();
                tracing::error!("embed spawn_blocking failed: {}", msg);
                for req in batch {
                    let _ = req.reply.send(Err(AppError::Internal(msg.clone())));
                }
            }
        }
    }
}

fn cache_key(text: &str) -> [u8; 32] {
    Sha256::digest(text.as_bytes()).into()
}

fn l2_normalize(v: &mut Vec<f32>) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > f32::EPSILON {
        v.iter_mut().for_each(|x| *x /= norm);
    }
}
