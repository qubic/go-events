package server

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/pkg/errors"
	eventspb "github.com/qubic/go-events/proto"
	"github.com/qubic/go-events/store"
	"github.com/qubic/go-qubic/common"
	"github.com/qubic/go-qubic/sdk/events"
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
	lastProcessedTick, err := s.eventsStore.FetchLastProcessedTick()
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
	tick, err := s.eventsStore.FetchLastProcessedTick()
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
	lastProcessedTick, err := s.eventsStore.FetchLastProcessedTick()
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

func (s *EventsService) DecodeEvent(ctx context.Context, req *eventspb.DecodeEventRequest) (*eventspb.DecodeEventResponse, error) {
	decodedEventData, err := base64.StdEncoding.DecodeString(req.EventData)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decoding event data: %v", err)
	}

	decodedEvent, err := decodeEvent(uint8(req.EventType), decodedEventData)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decoding event: %v", err)
	}

	return &eventspb.DecodeEventResponse{DecodedEvent: decodedEvent}, nil
}

func decodeEvent(eventType uint8, eventData []byte) (*eventspb.DecodedEvent, error) {
	switch eventType {
	case events.EventTypeQuTransfer:
		var event events.QuTransferEvent
		err := event.UnmarshalBinary(eventData)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling qu transfer event")
		}

		sourceID, err := common.PubKeyToIdentity(event.SourceIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting source identity pubkey")
		}

		destID, err := common.PubKeyToIdentity(event.DestinationIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting destination identity pubkey")
		}

		pbEvent := eventspb.DecodedEvent_QuTransferEvent_{
			QuTransferEvent: &eventspb.DecodedEvent_QuTransferEvent{
				SourceId: sourceID.String(),
				DestId:   destID.String(),
				Amount:   event.Amount,
			},
		}

		return &eventspb.DecodedEvent{Event: &pbEvent}, nil
	case events.EventTypeAssetIssuance:
		var event events.AssetIssuanceEvent
		err := event.UnmarshalBinary(eventData)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling asset issuance event")
		}

		sourceID, err := common.PubKeyToIdentity(event.SourceIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting source identity pubkey")
		}

		pbEvent := eventspb.DecodedEvent_AssetIssuanceEvent_{
			AssetIssuanceEvent: &eventspb.DecodedEvent_AssetIssuanceEvent{
				SourceId:              sourceID.String(),
				AssetName:             string(event.AssetName[:]),
				NumberOfDecimals:      uint32(event.NumberOfDecimals),
				MeasurementUnit:       event.MeasurementUnit[:],
				NumberOfShares:        event.NumberOfShares,
				ManagingContractIndex: event.ManagingContractIndex,
			},
		}

		return &eventspb.DecodedEvent{Event: &pbEvent}, nil
	case events.EventTypeAssetOwnershipChange:
		var event events.AssetOwnershipChangeEvent
		err := event.UnmarshalBinary(eventData)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling asset ownership change event")
		}

		sourceID, err := common.PubKeyToIdentity(event.SourceIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting source identity pubkey")
		}

		destID, err := common.PubKeyToIdentity(event.DestinationIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting destination identity pubkey")
		}

		issuerID, err := common.PubKeyToIdentity(event.IssuerIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting issuer identity pubkey")
		}

		pbEvent := eventspb.DecodedEvent_AssetOwnershipChangeEvent_{
			AssetOwnershipChangeEvent: &eventspb.DecodedEvent_AssetOwnershipChangeEvent{
				SourceId:              sourceID.String(),
				DestId:                destID.String(),
				IssuerId:              issuerID.String(),
				AssetName:             string(event.AssetName[:]),
				NumberOfDecimals:      uint32(event.NumberOfDecimals),
				MeasurementUnit:       event.MeasurementUnit[:],
				NumberOfShares:        event.NumberOfShares,
				ManagingContractIndex: event.ManagingContractIndex,
			},
		}

		return &eventspb.DecodedEvent{Event: &pbEvent}, nil
	case events.EventTypeAssetPossessionChange:
		var event events.AssetPossessionChangeEvent
		err := event.UnmarshalBinary(eventData)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling asset possession change event")
		}

		sourceID, err := common.PubKeyToIdentity(event.SourceIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting source identity pubkey")
		}

		destID, err := common.PubKeyToIdentity(event.DestinationIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting destination identity pubkey")
		}

		issuerID, err := common.PubKeyToIdentity(event.IssuerIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting issuer identity pubkey")
		}

		pbEvent := eventspb.DecodedEvent_AssetPossessionChangeEvent_{
			AssetPossessionChangeEvent: &eventspb.DecodedEvent_AssetPossessionChangeEvent{
				SourceId:              sourceID.String(),
				DestId:                destID.String(),
				IssuerId:              issuerID.String(),
				AssetName:             string(event.AssetName[:]),
				NumberOfDecimals:      uint32(event.NumberOfDecimals),
				MeasurementUnit:       event.MeasurementUnit[:],
				NumberOfShares:        event.NumberOfShares,
				ManagingContractIndex: event.ManagingContractIndex,
			},
		}

		return &eventspb.DecodedEvent{Event: &pbEvent}, nil
	case events.EventTypeBurning:
		var event events.BurningEvent
		err := event.UnmarshalBinary(eventData)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling burning event")
		}

		sourceID, err := common.PubKeyToIdentity(event.SourceIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting source identity pubkey")
		}

		pbEvent := eventspb.DecodedEvent_BurnEvent_{
			BurnEvent: &eventspb.DecodedEvent_BurnEvent{
				SourceId: sourceID.String(),
				Amount:   event.Amount,
			},
		}

		return &eventspb.DecodedEvent{Event: &pbEvent}, nil
	case events.EventTypeDustBurning:
		var event events.DustBurningEvent
		err := event.UnmarshalBinary(eventData)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling dust burning event")
		}

		sourceID, err := common.PubKeyToIdentity(event.SourceIdentityPubKey)
		if err != nil {
			return nil, errors.Wrap(err, "converting source identity pubkey")
		}

		pbEvent := eventspb.DecodedEvent_DustBurnEvent_{
			DustBurnEvent: &eventspb.DecodedEvent_DustBurnEvent{
				NumberOfBurns: uint32(event.NumberOfBurns),
				SourceId:      sourceID.String(),
				Amount:        event.Amount,
			},
		}

		return &eventspb.DecodedEvent{Event: &pbEvent}, nil
	case events.EventTypeSpectrumStats:
		var event events.SpectrumStatsEvent
		err := event.UnmarshalBinary(eventData)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling spectrum stats event")
		}

		pbEvent := eventspb.DecodedEvent_SpectrumStatsEvent_{
			SpectrumStatsEvent: &eventspb.DecodedEvent_SpectrumStatsEvent{
				TotalAmount:               event.TotalAmount,
				DustThresholdBurnAll:      event.DustThresholdBurnAll,
				DustThresholdBurnHalf:     event.DustThresholdBurnHalf,
				NumberOfEntities:          event.NumberOfEntities,
				EntityCategoryPopulations: event.EntityCategoryPopulations[:],
			},
		}

		return &eventspb.DecodedEvent{Event: &pbEvent}, nil
	default:
		return nil, errors.Errorf("not supported event type: %d", eventType)
	}
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
