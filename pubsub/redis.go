package pubsub

import (
	"context"
	"github.com/pkg/errors"
	qubicpb "github.com/qubic/go-qubic/proto/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"strconv"
	"time"
)

type RedisPubSub struct {
	rdb *redis.Client
}

func NewRedisPubSub(addr string, password string) (*RedisPubSub, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})
	pong := rdb.Ping(ctx)
	if err := pong.Err(); err != nil {
		return nil, errors.Wrap(err, "pinging redis client")
	}
	if pong.String() != "ping: PONG" {
		return nil, errors.New("invalid ping pong response")
	}

	return &RedisPubSub{rdb: rdb}, nil
}

func (ps *RedisPubSub) PublishTickEvents(ctx context.Context, tickEvents *qubicpb.TickEvents) error {
	if len(tickEvents.TxEvents) == 0 {
		return nil
	}

	err := ps.publishAllTickEvents(ctx, tickEvents)
	if err != nil {
		return errors.Wrap(err, "publishing all tick events")
	}

	for _, txEvents := range tickEvents.TxEvents {
		err = ps.publishTxEventsByType(ctx, txEvents)
		if err != nil {
			return errors.Wrapf(err, "publishing transaction events for tx id: %s", txEvents.TxId)
		}
	}

	return nil
}

func (ps *RedisPubSub) publishAllTickEvents(ctx context.Context, tickEvents *qubicpb.TickEvents) error {
	data, err := protojson.Marshal(tickEvents)
	if err != nil {
		return errors.Wrap(err, "marshalling tick events")
	}

	err = ps.rdb.Publish(ctx, "tickevents", data).Err()
	if err != nil {
		return errors.Wrap(err, "publishing tick events")
	}

	return nil
}

func (ps *RedisPubSub) publishTxEventsByType(ctx context.Context, txEvents *qubicpb.TransactionEvents) error {
	for _, event := range txEvents.Events {
		channelName := "eventsbytype" + strconv.FormatInt(int64(event.EventType), 10)
		data, err := protojson.Marshal(event)
		if err != nil {
			return errors.Wrapf(err, "marshalling data for event with type: %d id: %d", event.EventType, event.Header.EventId)
		}

		err = ps.rdb.Publish(ctx, channelName, data).Err()
		if err != nil {
			return errors.Wrapf(err, "publishing event with type: %d id: %d", event.EventType, event.Header.EventId)
		}
	}

	return nil
}
