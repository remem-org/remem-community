package server

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/remem-org/remem-go/internal/config"
	"github.com/remem-org/remem-go/internal/embedding"
	"github.com/remem-org/remem-go/internal/embedding/onnx"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/obs"
	"github.com/remem-org/remem-go/internal/storage"
	"github.com/remem-org/remem-go/internal/storage/memkv"
	"github.com/remem-org/remem-go/internal/storage/pebble"
	"github.com/remem-org/remem-go/internal/vector"
)

// The adapters build opens and closes: the store, the embedder, and the vector
// index queue's shutdown. Kept apart from build so that it reads as wiring.

// maintainerOf asks an index whether it keeps a structure of its own beside the
// canonical vectors, the way memory.Service does. An exact index does not: it
// *is* a scan of those vectors, so there is nothing for a deletion to tidy.
func maintainerOf(index vector.Index) vector.Maintainer {
	if m, ok := index.(vector.Maintainer); ok {
		return m
	}
	return nil
}

func openStore(cfg config.Config) (storage.KV, error) {
	switch cfg.Storage.Engine {
	case "memory":
		return memkv.New(), nil
	case "pebble":
		return pebble.Open(cfg.Storage.Path, pebble.Options{
			CacheSizeBytes:    cfg.Storage.CacheSizeBytes,
			MemTableSizeBytes: cfg.Storage.MemTableSizeBytes,
			BloomBitsPerKey:   cfg.Storage.BloomBitsPerKey,
		})
	default:
		return nil, errs.E(errs.Invalid, "server.openStore",
			fmt.Errorf("storage.engine %q is not one this binary knows", cfg.Storage.Engine))
	}
}

// openEmbedder loads the model.
//
// A build without the onnx tag fails here, and that is on purpose: the
// alternative is a server that starts, produces meaningless vectors, and stamps
// the real model's name on every one of them. A durable corpus filled that way
// cannot be told from a good one afterwards.
func openEmbedder(cfg config.Config) (embedding.Embedder, error) {
	if cfg.Embedding.Model != embedding.Model {
		return nil, errs.E(errs.Invalid, "server.openEmbedder", fmt.Errorf(
			"embedding.model is %q; this binary produces vectors comparable only with %q",
			cfg.Embedding.Model, embedding.Model))
	}
	// Resolved here, not inside onnx, so the rule is testable without the
	// native libraries; see embedding.IntraOpThreads.
	threads, source := embedding.IntraOpThreads(cfg.Embedding.IntraOpThreads, runtime.GOMAXPROCS(0))
	obs.Logger(context.Background()).Info("embedding intra-op threads",
		"threads", threads, "source", source.String(),
		"embedding.intra_op_threads", cfg.Embedding.IntraOpThreads)
	return onnx.Open(onnx.Config{
		ModelPath:         cfg.Embedding.ModelPath,
		SharedLibraryPath: cfg.Embedding.ONNXLibraryPath,
		BatchSize:         cfg.Embedding.BatchSize,
		IntraOpThreads:    threads,
	})
}

// indexQueueDrainTimeout bounds the index queue's drain at shutdown.
//
// Thirty seconds, the same budget server.shutdown_timeout gives in-flight
// requests, and bounded for the same reason: what the drain is finishing is
// node records a materialisation would re-insert anyway, so a shutdown held
// open by it would trade a fast restart for work the next start does for free.
// A drain that runs out of time logs what it abandoned.
const indexQueueDrainTimeout = 30 * time.Second

// indexQueueExitTimeout is how much longer a worker that is still mid-apply
// when the drain's deadline passes is given to finish.
//
// A second budget rather than a longer first one, because the two bound
// different things. The drain's thirty seconds bound *work the queue still
// owes*, which a materialisation would redo for free, so it is right to give up
// on it. This bounds *a worker that is inside an apply right now*, and giving
// up on that one means closing the Pebble store under a goroutine that is
// about to commit a node record into it. That used to be a panic in
// batch.apply; it is Unavailable since the guard went in, and waiting here is
// what keeps it from happening at all.
const indexQueueExitTimeout = 30 * time.Second

// closeVectors drains the index queue, stops its workers and waits for them to
// be gone, if there is one.
//
// Its failure is logged rather than returned. The queue holds node records,
// which are derived: whatever it did not apply, the next materialisation of
// that tenant inserts from the canonical vector. Reporting a shutdown as
// failed over that would be a false alarm every time the deadline was tight.
//
// The wait is the part the store depends on. Queue.Close stops the workers
// whether or not the drain finished, so when its deadline passes a worker may
// still be inside an index insert — and both callers of this function close the
// store next. A worker that outlived them would be committing into a closed
// store.
//
// It may be called more than once — build's failure path and close both reach
// it — and leaves the field alone rather than clearing it, because Queue.Close
// is already idempotent and a write here would be a write to a field other
// goroutines read.
func (d *deps) closeVectors() {
	if d.indexQueue == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), indexQueueDrainTimeout)
	defer cancel()
	if err := d.indexQueue.Close(ctx); err != nil {
		obs.Logger(ctx).Warn("the vector index queue did not drain before the deadline; "+
			"the node records it abandoned are re-inserted when each tenant is next materialised",
			"error", err)
	}
	exit, cancelExit := context.WithTimeout(context.Background(), indexQueueExitTimeout)
	defer cancelExit()
	if err := d.indexQueue.Wait(exit); err != nil {
		obs.Logger(exit).Warn("a vector index worker is still applying an operation after the shutdown "+
			"asked it to stop; the store is closing under it and its write will be refused rather than applied",
			"error", err)
	}
}
