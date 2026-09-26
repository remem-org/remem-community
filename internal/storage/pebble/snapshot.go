package pebble

import (
	"context"

	pebbledb "github.com/cockroachdb/pebble/v2"
	"github.com/remem-org/remem-go/internal/errs"
	"github.com/remem-org/remem-go/internal/storage"
)

type snapshot struct {
	snap   *pebbledb.Snapshot
	closed bool
}

var _ storage.Snapshot = (*snapshot)(nil)

func (s *snapshot) Get(_ context.Context, key []byte) ([]byte, error) {
	if s.closed {
		return nil, errs.E(errs.Unavailable, "pebble.Snapshot.Get", errClosed)
	}
	value, closer, err := s.snap.Get(key)
	if err != nil {
		return nil, translate("pebble.Snapshot.Get", err)
	}
	return copyAndClose(value, closer, "pebble.Snapshot.Get")
}

func (s *snapshot) NewIterator(lower, upper []byte) storage.Iterator {
	if s.closed {
		return failedIter(errs.E(errs.Unavailable, "pebble.Snapshot.NewIterator", errClosed))
	}
	it, err := s.snap.NewIter(iterOptions(lower, upper))
	if err != nil {
		return failedIter(translate("pebble.Snapshot.NewIterator", err))
	}
	return &iterator{it: it}
}

func (s *snapshot) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return translate("pebble.Snapshot.Close", s.snap.Close())
}
