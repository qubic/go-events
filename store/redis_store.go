package store

import (
	"context"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"strconv"
)

type RedisStore struct {
	db *redis.Client
}

func NewRedisStore(rdb *redis.Client) *RedisStore {
	return &RedisStore{
		db: rdb,
	}
}

func (rs *RedisStore) Get(ctx context.Context, key []byte) ([]byte, error) {
	value, err := rs.db.Get(ctx, string(key)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tick events")
	}

	return value, nil
}

func (rs *RedisStore) Set(ctx context.Context, key, value []byte) error {
	err := rs.db.Set(ctx, string(key), value, 0).Err()
	if err != nil {
		return errors.Wrap(err, "calling redis set")
	}

	return nil
}

func (rs *RedisStore) BatchSet(ctx context.Context, kvPairs map[string][]byte) error {
	pipe := rs.db.TxPipeline()

	for key, value := range kvPairs {
		pipe.Set(ctx, key, value, 0)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return errors.Wrap(err, "exec tx pipe")
	}

	return nil
}

// SetOrdered sets a member in the set and it overrides it if the score(index) already exists
func (rs *RedisStore) SetOrdered(ctx context.Context, collection []byte, index uint64, value []byte) error {
	score := strconv.FormatFloat(float64(index), 'f', -1, 64)
	err := rs.db.ZRemRangeByScore(ctx, string(collection), score, score).Err()
	if err != nil {
		return errors.Wrap(err, "calling zremrangebyscore")
	}

	err = rs.db.ZAdd(ctx, string(collection), redis.Z{Score: float64(index), Member: value}).Err()
	if err != nil {
		return errors.Wrap(err, "calling zadd")
	}

	return nil
}

func (rs *RedisStore) GetOrderedSingle(ctx context.Context, collection []byte, index uint64) ([]byte, error) {
	values, err := rs.GetOrderedRange(ctx, collection, index, index)
	if err != nil {
		return nil, errors.Wrap(err, "getting ordered range")
	}

	if len(values) == 0 {
		return nil, ErrNotFound
	}

	return values[0], nil
}

func (rs *RedisStore) GetOrderedRange(ctx context.Context, collection []byte, start, end uint64) ([][]byte, error) {
	values, err := rs.db.ZRangeByScore(ctx, string(collection), &redis.ZRangeBy{
		Min: strconv.FormatFloat(float64(start), 'f', -1, 64),
		Max: strconv.FormatFloat(float64(end), 'f', -1, 64),
	}).Result()
	if err != nil {
		return nil, errors.Wrap(err, "calling redis zrange")
	}

	var result [][]byte
	for _, v := range values {
		result = append(result, []byte(v))
	}

	return result, nil
}
