use axum::body::Body;
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::api::AppState;
use crate::error::AppError;

/// Bounded channel capacity for streaming tar.gz chunks out of the blocking
/// archive task. Small on purpose: it's what keeps peak memory for this
/// endpoint bounded instead of scaling with `data_dir` size (see below).
const BACKUP_CHANNEL_CAPACITY: usize = 16;

/// A `std::io::Write` sink that forwards each chunk it receives into a
/// bounded Tokio mpsc channel. `GzEncoder`/`tar::Builder` write through this
/// from a `spawn_blocking` thread; the async side reads the other end of the
/// channel as a `Stream` and hands it to `Body::from_stream`. This is what
/// lets the archive be streamed to the client chunk-by-chunk instead of being
/// fully materialized in memory first.
struct ChannelWriter {
    tx: mpsc::Sender<std::io::Result<Bytes>>,
}

impl std::io::Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let chunk = Bytes::copy_from_slice(buf);
        // `blocking_send` is the correct call here: this runs on a
        // spawn_blocking thread, not inside an async task, so it's fine to
        // block until channel capacity frees up (that's exactly what
        // provides the backpressure that bounds memory use). If the
        // receiver was dropped (client disconnected), report that as a
        // BrokenPipe so the tar/gzip writers unwind cleanly instead of
        // spinning.
        self.tx.blocking_send(Ok(chunk)).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "backup stream receiver dropped",
            )
        })?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// `POST /api/v1/backup` — triggers a checkpoint (flush memtables + save all
/// indexes, gated on every index save succeeding, per
/// `StorageEngine::checkpoint`), then streams a `tar.gz` of the data
/// directory. The checkpoint guarantees the directory is a consistent,
/// restartable snapshot at the moment it returns; any write landing after
/// that point just isn't included in this backup, it doesn't corrupt it.
///
/// The checkpoint briefly stalls *all* writes for the duration of its
/// memtable flush (every writer's first step is the same WAL lock this
/// holds -- a deliberate write barrier, see `StorageEngine::checkpoint`).
/// This endpoint makes that pre-existing background-checkpoint cost
/// externally, on-demand triggerable: on a large dataset, calling this
/// reads as a brief write freeze. The flush itself runs on a blocking-pool
/// thread rather than inline, so it no longer also blocks unrelated async
/// work sharing this request's executor thread -- but the write stall for
/// concurrent writers is inherent to the barrier, not an implementation
/// detail this endpoint can route around.
///
/// The archive is streamed rather than buffered: `tar::Builder` and
/// `GzEncoder` write into a `ChannelWriter` on a `spawn_blocking` thread, and
/// the receiving half of that channel is exposed to the client as the
/// response body via `Body::from_stream`. This keeps peak memory bounded by
/// the channel capacity (a handful of chunks) instead of by the size of
/// `data_dir`, which for a full-dataset-read endpoint like this one could
/// otherwise be arbitrarily large.
///
/// A failure while building the archive (e.g. a disk read error partway
/// through) can't be turned into a clean HTTP error once the response has
/// already started streaming with a 200 status and headers — that's
/// inherent to HTTP streaming. In that case the body is simply truncated;
/// the client can detect the incomplete gzip/tar and know the download
/// failed. The checkpoint step above still returns a proper error response
/// if it fails, since that happens before any bytes are sent.
///
/// Not documented via `#[utoipa::path]` / registered in `openapi.rs` — like
/// the business `/api/v1/metrics` endpoint, its response isn't a JSON body
/// the schema generator can describe.
pub async fn create_backup(State(state): State<AppState>) -> Result<Response, AppError> {
    state.services.engine.checkpoint().await?;

    let data_dir = state.services.engine.data_dir().to_path_buf();
    let (tx, rx) = mpsc::channel::<std::io::Result<Bytes>>(BACKUP_CHANNEL_CAPACITY);

    tokio::task::spawn_blocking(move || {
        let writer = ChannelWriter { tx: tx.clone() };
        let write_result: std::io::Result<()> = (|| {
            let gz = GzEncoder::new(writer, Compression::default());
            let mut builder = tar::Builder::new(gz);
            builder.append_dir_all(".", &data_dir)?;
            let gz = builder.into_inner()?;
            gz.finish()?;
            Ok(())
        })();

        // On success the channel is simply closed (both `tx` and its clone
        // drop here), ending the stream cleanly. On failure, best-effort
        // forward the error so the stream ends on an `Err` frame instead of
        // silently truncating; if the receiver is already gone (client
        // disconnected), there's nothing left to report to.
        if let Err(e) = write_result {
            tracing::warn!(error = %e, "backup archive build failed; response body will be truncated");
            let _ = tx.blocking_send(Err(e));
        }
    });

    let body = Body::from_stream(ReceiverStream::new(rx));

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/gzip"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"remem-backup.tar.gz\"",
            ),
        ],
        body,
    )
        .into_response())
}
