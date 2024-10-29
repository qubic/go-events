package store

import (
	"context"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
)

type PebbleStore struct {
	db *pebble.DB
}

func NewPebbleStore(db *pebble.DB) *PebbleStore {
	return &PebbleStore{
		db: db,
	}
}

func (ps *PebbleStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, closer, err := ps.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting value")
	}
	defer closer.Close()

	return value, nil
}

func (ps *PebbleStore) Set(ctx context.Context, key []byte, value []byte) error {
	return ps.db.Set(key, value, pebble.Sync)
}

func (ps *PebbleStore) BatchSet(ctx context.Context, kvPairs map[string][]byte) error {
	batch := ps.db.NewBatch()
	defer batch.Close()

	for key, value := range kvPairs {
		err := batch.Set([]byte(key), value, nil)
		if err != nil {
			return errors.Wrapf(err, "setting key-value pair. key: %s, value: %s", key, value)
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}
