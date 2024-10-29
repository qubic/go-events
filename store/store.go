package store

import (
	"context"
	"encoding/binary"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	eventspb "github.com/qubic/go-events/proto"
	qubicpb "github.com/qubic/go-qubic/proto/v1"
	"google.golang.org/protobuf/proto"
)

const maxTickNumber, maxEpochNumber = ^uint64(0), ^uint64(0)

var ErrNotFound = errors.New("store resource not found")

type EventsStore struct {
	db KVStore
}

type KVStore interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key []byte, value []byte) error
	SetOrdered(ctx context.Context, collection []byte, index uint64, value []byte) error
	GetOrderedRange(ctx context.Context, collection []byte, start, end uint64) ([][]byte, error)
	GetOrderedSingle(ctx context.Context, collection []byte, index uint64) ([]byte, error)
	BatchSet(ctx context.Context, kvPairs map[string][]byte) error
}

func NewEventsStore(db KVStore) *EventsStore {
	return &EventsStore{
		db: db,
	}
}

func (s *EventsStore) GetTickEvents(ctx context.Context, tickNumber uint32) (*qubicpb.TickEvents, error) {
	key := tickEventsKey(tickNumber)
	value, err := s.db.GetOrderedSingle(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tick data")
	}

	var te qubicpb.TickEvents
	if err := proto.Unmarshal(value, &te); err != nil {
		return nil, errors.Wrap(err, "unmarshalling tick events")
	}

	return &te, nil
}

func (s *EventsStore) GetTickProcessTime(ctx context.Context, tickNumber uint32) (uint64, error) {
	value, err := s.db.GetOrderedSingle(ctx, []byte{TickProcessTimeCollection}, int64(tickNumber))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return 0, ErrNotFound
		}

		return 0, errors.Wrap(err, "getting tick process time")
	}

	return binary.LittleEndian.Uint64(value), nil
}

func (s *EventsStore) SetTickProcessTime(ctx context.Context, tickNumber uint32, processTime uint64) error {
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, processTime)

	err := s.db.SetOrdered(ctx, []byte{TickProcessTimeCollection}, int64(tickNumber), value)
	if err != nil {
		return errors.Wrap(err, "setting tick process time")
	}

	return nil
}

func (s *EventsStore) SetTickEvents(ctx context.Context, tickNumber uint32, te *qubicpb.TickEvents) error {
	value, err := proto.Marshal(te)
	if err != nil {
		return errors.Wrap(err, "serializing tick events proto")
	}

	err = s.db.SetOrdered(ctx, []byte{TickEventsCollection}, int64(tickNumber), value)
	if err != nil {
		return errors.Wrap(err, "setting tick events")
	}

	return nil
}

type StatusStore struct {
	db KVStore
}

func NewStatusStore(db KVStore) *StatusStore {
	return &StatusStore{
		db: db,
	}
}

func (s *StatusStore) SetLastProcessedTick(ctx context.Context, lastProcessedTick *eventspb.ProcessedTick) error {
	kvPairs := make(map[string][]byte)

	key := lastProcessedTickKeyPerEpoch(lastProcessedTick.Epoch)
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, lastProcessedTick.TickNumber)
	kvPairs[string(key)] = value

	key = lastProcessedTickKey()
	serialized, err := proto.Marshal(lastProcessedTick)
	if err != nil {
		return errors.Wrap(err, "serializing skipped tick proto")
	}
	kvPairs[string(key)] = serialized

	err = s.db.BatchSet(ctx, kvPairs)
	if err != nil {
		return errors.Wrap(err, "batch setting kv pairs")
	}

	ptie, err := s.getProcessedTickIntervalsPerEpoch(ctx, lastProcessedTick.Epoch)
	if err != nil {
		return errors.Wrap(err, "getting ptie")
	}

	if len(ptie.Intervals) == 0 {
		ptie = &eventspb.ProcessedTickIntervalsPerEpoch{Epoch: lastProcessedTick.Epoch, Intervals: []*eventspb.ProcessedTickInterval{{InitialProcessedTick: lastProcessedTick.TickNumber, LastProcessedTick: lastProcessedTick.TickNumber}}}
	} else {
		ptie.Intervals[len(ptie.Intervals)-1].LastProcessedTick = lastProcessedTick.TickNumber
	}

	err = s.SetProcessedTickIntervalPerEpoch(ctx, lastProcessedTick.Epoch, ptie)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *StatusStore) GetLastProcessedTick(ctx context.Context) (*eventspb.ProcessedTick, error) {
	key := lastProcessedTickKey()
	value, err := s.db.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting last processed tick")
	}

	var lpt eventspb.ProcessedTick
	if err := proto.Unmarshal(value, &lpt); err != nil {
		return nil, errors.Wrap(err, "unmarshalling lpt to protobuff type")
	}

	return &lpt, nil
}

func (s *StatusStore) AppendProcessedTickInterval(ctx context.Context, epoch uint32, pti *eventspb.ProcessedTickInterval) error {
	existing, err := s.getProcessedTickIntervalsPerEpoch(ctx, epoch)
	if err != nil {
		return errors.Wrap(err, "getting existing processed tick intervals")
	}

	existing.Intervals = append(existing.Intervals, pti)

	err = s.SetProcessedTickIntervalPerEpoch(ctx, epoch, existing)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *StatusStore) getProcessedTickIntervalsPerEpoch(ctx context.Context, epoch uint32) (*eventspb.ProcessedTickIntervalsPerEpoch, error) {
	key := processedTickIntervalsPerEpochKey(epoch)
	value, err := s.db.Get(ctx, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return &eventspb.ProcessedTickIntervalsPerEpoch{Intervals: make([]*eventspb.ProcessedTickInterval, 0), Epoch: epoch}, nil
		}

		return nil, errors.Wrap(err, "getting processed tick intervals per epoch from store")
	}

	var ptie eventspb.ProcessedTickIntervalsPerEpoch
	if err := proto.Unmarshal(value, &ptie); err != nil {
		return nil, errors.Wrap(err, "unmarshalling processed tick intervals per epoch")
	}

	return &ptie, nil
}

func (s *StatusStore) SetProcessedTickIntervalPerEpoch(ctx context.Context, epoch uint32, ptie *eventspb.ProcessedTickIntervalsPerEpoch) error {
	key := processedTickIntervalsPerEpochKey(epoch)
	serialized, err := proto.Marshal(ptie)
	if err != nil {
		return errors.Wrap(err, "serializing ptie proto")
	}

	err = s.db.Set(ctx, key, serialized)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *StatusStore) SetSkippedTicksInterval(ctx context.Context, skippedTick *eventspb.SkippedTicksInterval) error {
	newList := eventspb.SkippedTicksIntervalList{}
	current, err := s.GetSkippedTicksInterval(ctx)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return errors.Wrap(err, "getting skipped tick interval")
		}
	} else {
		newList.SkippedTicks = current.SkippedTicks
	}

	newList.SkippedTicks = append(newList.SkippedTicks, skippedTick)

	key := skippedTicksIntervalKey()
	serialized, err := proto.Marshal(&newList)
	if err != nil {
		return errors.Wrap(err, "serializing skipped tick proto")
	}

	err = s.db.Set(ctx, key, serialized)
	if err != nil {
		return errors.Wrap(err, "setting skipped tick interval")
	}

	return nil
}

func (s *StatusStore) GetSkippedTicksInterval(ctx context.Context) (*eventspb.SkippedTicksIntervalList, error) {
	key := skippedTicksIntervalKey()
	value, err := s.db.Get(ctx, key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting skipped tick interval")
	}

	var stil eventspb.SkippedTicksIntervalList
	if err := proto.Unmarshal(value, &stil); err != nil {
		return nil, errors.Wrap(err, "unmarshalling skipped tick interval to eventspb type")
	}

	return &stil, nil
}

func (s *StatusStore) GetProcessedTickIntervals(ctx context.Context) ([]*eventspb.ProcessedTickIntervalsPerEpoch, error) {
	start := 0
	end := maxTickNumber

	values, err := s.db.GetOrderedRange(ctx, []byte{ProcessedTickIntervals}, uint64(start), end)
	if err != nil {
		return nil, errors.Wrap(err, "getting processed tick intervals")
	}

	processedTickIntervals := make([]*eventspb.ProcessedTickIntervalsPerEpoch, 0)
	for _, value := range values {
		var ptie eventspb.ProcessedTickIntervalsPerEpoch
		err = proto.Unmarshal(value, &ptie)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling iter ptie")
		}
		processedTickIntervals = append(processedTickIntervals, &ptie)
	}

	return processedTickIntervals, nil
}

func (s *StatusStore) GetLastProcessedTicksPerEpoch(ctx context.Context) (map[uint32]uint32, error) {
	start := 0
	end := maxEpochNumber

	values, err := s.db.GetOrderedRange(ctx, []byte{LastProcessedTickPerEpoch}, uint64(start), end)
	if err != nil {
		return nil, errors.Wrap(err, "getting processed tick intervals")
	}

	ticksPerEpoch := make(map[uint32]uint32)
	for _, value := range values {
		key := iter.Key()

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting value from iter")
		}

		epochNumber := binary.BigEndian.Uint32(key[1:])
		tickNumber := binary.LittleEndian.Uint32(value)
		ticksPerEpoch[epochNumber] = tickNumber
	}

	return ticksPerEpoch, nil
}
