package server

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	eventspb "github.com/qubic/go-events/proto"
	"github.com/qubic/go-events/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

var _ eventspb.EventsServiceServer = &EventsService{}

type EventsService struct {
	eventspb.UnimplementedEventsServiceServer
	eventsStore *store.Store
}

func NewEventsService(eventsStore *store.Store) *EventsService {
	return &EventsService{eventsStore: eventsStore}
}

func (s *EventsService) GetTickEvents(ctx context.Context, req *eventspb.GetTickEventsRequest) (*eventspb.TickEvents, error) {
	lastProcessedTick, err := s.eventsStore.GetLastProcessedTick(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting last processed tick: %v", err)
	}
	if req.Tick > lastProcessedTick.TickNumber {
		st := status.Newf(codes.FailedPrecondition, "requested tick number %d is greater than last processed tick %d", req.Tick, lastProcessedTick.TickNumber)
		st, err = st.WithDetails(&eventspb.LastProcessedTick{LastProcessedTick: lastProcessedTick.TickNumber})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "creating custom status")
		}

		return nil, st.Err()
	}

	processedTickIntervalsPerEpoch, err := s.eventsStore.GetProcessedTickIntervals(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting processed tick intervals per epoch")
	}

	wasSkipped, nextAvailableTick := wasTickSkippedBySystem(req.Tick, processedTickIntervalsPerEpoch)
	if wasSkipped == true {
		st := status.Newf(codes.OutOfRange, "provided tick number %d was skipped by the system, next available tick is %d", req.Tick, nextAvailableTick)
		st, err = st.WithDetails(&eventspb.NextAvailableTick{NextTickNumber: nextAvailableTick})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "creating custom status")
		}

		return nil, st.Err()
	}

	te, err := s.eventsStore.GetTickEvents(req.Tick)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var localTe eventspb.TickEvents
	err = recast(te, &localTe)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "casting tick events to local with err: %s", err.Error())
	}

	return &localTe, nil
}

func (s *EventsService) GetStatus(ctx context.Context, _ *emptypb.Empty) (*eventspb.GetStatusResponse, error) {
	tick, err := s.eventsStore.GetLastProcessedTick(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting last processed tick: %v", err)
	}

	lastProcessedTicksPerEpoch, err := s.eventsStore.GetLastProcessedTicksPerEpoch(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting last processed tick: %v", err)
	}

	skippedTicks, err := s.eventsStore.GetSkippedTicksInterval(ctx)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return &eventspb.GetStatusResponse{LastProcessedTick: tick, LastProcessedTicksPerEpoch: lastProcessedTicksPerEpoch}, nil
		}

		return nil, status.Errorf(codes.Internal, "getting skipped ticks: %v", err)
	}

	ptie, err := s.eventsStore.GetProcessedTickIntervals(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting processed tick intervals")
	}

	var epochs []uint32
	for epoch, _ := range lastProcessedTicksPerEpoch {
		epochs = append(epochs, epoch)
	}

	return &eventspb.GetStatusResponse{
		LastProcessedTick:              tick,
		LastProcessedTicksPerEpoch:     lastProcessedTicksPerEpoch,
		SkippedTicks:                   skippedTicks.SkippedTicks,
		ProcessedTickIntervalsPerEpoch: ptie,
	}, nil
}

func (s *EventsService) GetTickProcessTime(ctx context.Context, req *eventspb.GetTickProcessTimeRequest) (*eventspb.GetTickProcessTimeResponse, error) {
	lastProcessedTick, err := s.eventsStore.GetLastProcessedTick(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting last processed tick: %v", err)
	}
	if req.Tick > lastProcessedTick.TickNumber {
		st := status.Newf(codes.FailedPrecondition, "requested tick number %d is greater than last processed tick %d", req.Tick, lastProcessedTick.TickNumber)
		st, err = st.WithDetails(&eventspb.LastProcessedTick{LastProcessedTick: lastProcessedTick.TickNumber})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "creating custom status")
		}

		return nil, st.Err()
	}

	processedTickIntervalsPerEpoch, err := s.eventsStore.GetProcessedTickIntervals(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting processed tick intervals per epoch")
	}

	wasSkipped, nextAvailableTick := wasTickSkippedBySystem(req.Tick, processedTickIntervalsPerEpoch)
	if wasSkipped == true {
		st := status.Newf(codes.OutOfRange, "provided tick number %d was skipped by the system, next available tick is %d", req.Tick, nextAvailableTick)
		st, err = st.WithDetails(&eventspb.NextAvailableTick{NextTickNumber: nextAvailableTick})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "creating custom status")
		}

		return nil, st.Err()
	}

	pt, err := s.eventsStore.GetTickProcessTime(req.Tick)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &eventspb.GetTickProcessTimeResponse{ProcessTimeSeconds: pt}, nil
}

func recast(a, b interface{}) error {
	js, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return json.Unmarshal(js, b)
}

func wasTickSkippedBySystem(tick uint32, processedTicksIntervalPerEpoch []*eventspb.ProcessedTickIntervalsPerEpoch) (bool, uint32) {
	if len(processedTicksIntervalPerEpoch) == 0 {
		return false, 0
	}
	for _, epochInterval := range processedTicksIntervalPerEpoch {
		for _, interval := range epochInterval.Intervals {
			if tick < interval.InitialProcessedTick {
				return true, interval.InitialProcessedTick
			}
			if tick >= interval.InitialProcessedTick && tick <= interval.LastProcessedTick {
				return false, 0
			}
		}
	}
	return false, 0
}
