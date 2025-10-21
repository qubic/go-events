package store

import (
	"context"
	"encoding/binary"
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	eventspb "github.com/qubic/go-events/proto"
	qubicpb "github.com/qubic/go-qubic/proto/v1"
	"google.golang.org/protobuf/proto"
)

const maxTickNumber = ^uint64(0)

var ErrNotFound = errors.New("store resource not found")

type Store struct {
	db *pebble.DB
}

func NewStore(db *pebble.DB) *Store {
	return &Store{
		db: db,
	}
}

func (s *Store) GetTickEvents(tickNumber uint32) (*qubicpb.TickEvents, error) {
	key := tickEventsKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tick data")
	}
	defer closer.Close()

	var te qubicpb.TickEvents
	if err := proto.Unmarshal(value, &te); err != nil {
		return nil, errors.Wrap(err, "unmarshalling tick events")
	}

	return &te, nil
}

func (s *Store) GetTickProcessTime(tickNumber uint32) (uint64, error) {
	key := tickProcessTimeKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, ErrNotFound
		}

		return 0, errors.Wrap(err, "getting tick process time")
	}
	defer closer.Close()

	return binary.LittleEndian.Uint64(value), nil
}

func (s *Store) SetTickProcessTime(tickNumber uint32, processTime uint64) error {
	key := tickProcessTimeKey(tickNumber)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, processTime)

	err := s.db.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting tick process time")
	}

	return nil
}

func (s *Store) SetTickEvents(tickNumber uint32, te *qubicpb.TickEvents) error {
	key := tickEventsKey(tickNumber)
	serialized, err := proto.Marshal(te)
	if err != nil {
		return errors.Wrap(err, "serializing tick events proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting tick events")
	}

	return nil
}

func (s *Store) SetLastProcessedTick(ctx context.Context, lastProcessedTick *eventspb.ProcessedTick) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	key := lastProcessedTickKeyPerEpoch(lastProcessedTick.Epoch)
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, lastProcessedTick.TickNumber)

	err := batch.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting last processed tick")
	}

	key = lastProcessedTickKey()
	serialized, err := proto.Marshal(lastProcessedTick)
	if err != nil {
		return errors.Wrap(err, "serializing skipped tick proto")
	}

	err = batch.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting last processed tick")
	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
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

func (s *Store) GetLastProcessedTick() (*eventspb.ProcessedTick, error) {
	key := lastProcessedTickKey()
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting last processed tick")
	}
	defer closer.Close()

	var lpt eventspb.ProcessedTick
	if err := proto.Unmarshal(value, &lpt); err != nil {
		return nil, errors.Wrap(err, "unmarshalling lpt to protobuff type")
	}

	return &lpt, nil
}

func (s *Store) AppendProcessedTickInterval(ctx context.Context, epoch uint32, pti *eventspb.ProcessedTickInterval) error {
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

func (s *Store) getProcessedTickIntervalsPerEpoch(ctx context.Context, epoch uint32) (*eventspb.ProcessedTickIntervalsPerEpoch, error) {
	key := processedTickIntervalsPerEpochKey(epoch)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return &eventspb.ProcessedTickIntervalsPerEpoch{Intervals: make([]*eventspb.ProcessedTickInterval, 0), Epoch: epoch}, nil
		}

		return nil, errors.Wrap(err, "getting processed tick intervals per epoch from store")
	}
	defer closer.Close()

	var ptie eventspb.ProcessedTickIntervalsPerEpoch
	if err := proto.Unmarshal(value, &ptie); err != nil {
		return nil, errors.Wrap(err, "unmarshalling processed tick intervals per epoch")
	}

	return &ptie, nil
}

func (s *Store) SetProcessedTickIntervalPerEpoch(ctx context.Context, epoch uint32, ptie *eventspb.ProcessedTickIntervalsPerEpoch) error {
	key := processedTickIntervalsPerEpochKey(epoch)
	serialized, err := proto.Marshal(ptie)
	if err != nil {
		return errors.Wrap(err, "serializing ptie proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *Store) SetSkippedTicksInterval(ctx context.Context, skippedTick *eventspb.SkippedTicksInterval) error {
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

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting skipped tick interval")
	}

	return nil
}

func (s *Store) GetSkippedTicksInterval(ctx context.Context) (*eventspb.SkippedTicksIntervalList, error) {
	key := skippedTicksIntervalKey()
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting skipped tick interval")
	}
	defer closer.Close()

	var stil eventspb.SkippedTicksIntervalList
	if err := proto.Unmarshal(value, &stil); err != nil {
		return nil, errors.Wrap(err, "unmarshalling skipped tick interval to eventspb type")
	}

	return &stil, nil
}

func (s *Store) GetProcessedTickIntervals(ctx context.Context) ([]*eventspb.ProcessedTickIntervalsPerEpoch, error) {
	upperBound := append([]byte{ProcessedTickIntervals}, []byte(strconv.FormatUint(maxTickNumber, 10))...)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{ProcessedTickIntervals},
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	processedTickIntervals := make([]*eventspb.ProcessedTickIntervalsPerEpoch, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting value from iter")
		}

		var ptie eventspb.ProcessedTickIntervalsPerEpoch
		err = proto.Unmarshal(value, &ptie)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling iter ptie")
		}
		processedTickIntervals = append(processedTickIntervals, &ptie)
	}

	return processedTickIntervals, nil
}

func (s *Store) GetLastProcessedTicksPerEpoch(ctx context.Context) (map[uint32]uint32, error) {
	upperBound := append([]byte{LastProcessedTickPerEpoch}, []byte(strconv.FormatUint(maxTickNumber, 10))...)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{LastProcessedTickPerEpoch},
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	ticksPerEpoch := make(map[uint32]uint32)
	for iter.First(); iter.Valid(); iter.Next() {
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
