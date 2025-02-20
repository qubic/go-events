package processor

import (
	"context"
	"github.com/pkg/errors"
	eventspb "github.com/qubic/go-events/proto"
	"github.com/qubic/go-events/pubsub"
	"github.com/qubic/go-events/store"
	"github.com/qubic/go-qubic/connector"
	qubicpb "github.com/qubic/go-qubic/proto/v1"
	"github.com/qubic/go-qubic/sdk/core"
	"github.com/qubic/go-qubic/sdk/events"
	"log"
	"time"
)

func newTickInTheFutureError(requestedTick uint32, latestTick uint32) *TickInTheFutureError {
	return &TickInTheFutureError{requestedTick: requestedTick, latestTick: latestTick}
}

type TickInTheFutureError struct {
	requestedTick uint32
	latestTick    uint32
}

func (e *TickInTheFutureError) Error() string {
	return errors.Errorf("Requested tick %d is in the future. Latest tick is: %d", e.requestedTick, e.latestTick).Error()
}

type Processor struct {
	qubicConnector     *connector.Connector
	isPubSubEnabled    bool
	redisPubSubClient  *pubsub.RedisPubSub
	eventsStore        *store.Store
	passcodes          map[string][4]uint64
	processTickTimeout time.Duration
}

func NewProcessor(qubicConnector *connector.Connector, redisPubSubClient *pubsub.RedisPubSub, isPubSubEnabled bool, eventsStore *store.Store, processTickTimeout time.Duration, passcodes map[string][4]uint64) *Processor {
	return &Processor{
		qubicConnector:     qubicConnector,
		isPubSubEnabled:    isPubSubEnabled,
		redisPubSubClient:  redisPubSubClient,
		eventsStore:        eventsStore,
		processTickTimeout: processTickTimeout,
		passcodes:          passcodes,
	}
}

func (p *Processor) Start() error {
	for {
		err := p.processOneByOne()
		if err != nil {
			log.Printf("Processing failed: %s\n", err.Error())
			time.Sleep(1 * time.Second)
		}
	}
}

func (p *Processor) processOneByOne() error {
	f := func(rp connector.RequestPerformer) error {
		ctx, cancel := context.WithTimeout(context.Background(), p.processTickTimeout)
		defer cancel()

		coreClient := core.NewClient(rp)
		tickInfo, err := coreClient.GetTickInfo(ctx)
		if err != nil {
			return errors.Wrap(err, "getting tick info")
		}

		lastTick, err := p.getLastProcessedTick(ctx, tickInfo)
		if err != nil {
			return errors.Wrap(err, "getting last processed tick")
		}

		nextTick, err := p.getNextProcessingTick(ctx, lastTick, tickInfo)
		if err != nil {
			return errors.Wrap(err, "getting next processing tick")
		}
		log.Printf("Next tick to process: %d\n", nextTick.TickNumber)

		if tickInfo.Tick < nextTick.TickNumber {
			err = newTickInTheFutureError(nextTick.TickNumber, tickInfo.Tick)
			return err
		}

		eventsClient := events.NewClient(rp, p.passcodes)
		start := time.Now()
		tickEvents, err := eventsClient.GetTickEvents(context.Background(), nextTick.TickNumber)
		if err != nil {
			return errors.Wrap(err, "getting tick events")
		}
		end := time.Now()

		err = p.eventsStore.SetTickEvents(nextTick.TickNumber, tickEvents)
		if err != nil {
			return errors.Wrap(err, "setting tick events")
		}

		err = p.eventsStore.SetTickProcessTime(nextTick.TickNumber, uint64(end.Sub(start).Seconds()))
		if err != nil {
			return errors.Wrap(err, "setting tick process time")
		}

		err = p.processStatus(ctx, lastTick, nextTick)
		if err != nil {
			return errors.Wrapf(err, "processing status for lastTick %+v and nextTick %+v", lastTick, nextTick)
		}

		if p.isPubSubEnabled {
			err = p.redisPubSubClient.PublishTickEvents(ctx, tickEvents)
			if err != nil {
				return errors.Wrap(err, "publishing tick events")
			}
		}

		return nil
	}

	err := p.qubicConnector.WithConnection(f)
	if err != nil {
		return errors.Wrap(err, "performing WithConnection logic")
	}

	return nil
}

func (p *Processor) processStatus(ctx context.Context, lastTick *eventspb.ProcessedTick, nextTick *eventspb.ProcessedTick) error {
	err := p.processSkippedTicks(ctx, lastTick, nextTick)
	if err != nil {
		return errors.Wrap(err, "processing skipped ticks")
	}

	err = p.eventsStore.SetLastProcessedTick(ctx, nextTick)
	if err != nil {
		return errors.Wrapf(err, "setting last processed tick %d", nextTick.TickNumber)
	}

	return nil
}

func (p *Processor) getNextProcessingTick(ctx context.Context, lastTick *eventspb.ProcessedTick, currentTickInfo *qubicpb.TickInfo) (*eventspb.ProcessedTick, error) {
	//handles the case where the initial tick of epoch returned by the node is greater than the last processed tick
	// which means that we are in the next epoch and we should start from the initial tick of the current epoch
	if currentTickInfo.InitialTickOfEpoch > lastTick.TickNumber {
		return &eventspb.ProcessedTick{TickNumber: currentTickInfo.InitialTickOfEpoch, Epoch: currentTickInfo.Epoch}, nil
	}

	// otherwise we are in the same epoch and we should start from the last processed tick + 1
	return &eventspb.ProcessedTick{TickNumber: lastTick.TickNumber + 1, Epoch: lastTick.Epoch}, nil
}

func (p *Processor) getLastProcessedTick(ctx context.Context, currentTickInfo *qubicpb.TickInfo) (*eventspb.ProcessedTick, error) {
	lastTick, err := p.eventsStore.GetLastProcessedTick(ctx)
	if err != nil {
		//handles first run of the events processor where there is nothing in storage
		// in this case last tick is 0 and epoch is current tick info epoch
		if errors.Is(err, store.ErrNotFound) {
			return &eventspb.ProcessedTick{TickNumber: 0, Epoch: currentTickInfo.Epoch}, nil
		}

		return nil, errors.Wrap(err, "getting last processed tick")
	}

	return lastTick, nil
}

func (p *Processor) processSkippedTicks(ctx context.Context, lastTick *eventspb.ProcessedTick, nextTick *eventspb.ProcessedTick) error {
	// nothing to process, no skipped ticks
	if nextTick.TickNumber-lastTick.TickNumber == 1 {
		return nil
	}

	if nextTick.TickNumber-lastTick.TickNumber == 0 {
		return errors.Errorf("Next tick should not be equal to last tick %d", nextTick.TickNumber)
	}

	err := p.eventsStore.AppendProcessedTickInterval(ctx, nextTick.Epoch, &eventspb.ProcessedTickInterval{InitialProcessedTick: nextTick.TickNumber, LastProcessedTick: nextTick.TickNumber})
	if err != nil {
		return errors.Wrap(err, "appending processed tick interval")
	}

	err = p.eventsStore.SetSkippedTicksInterval(ctx, &eventspb.SkippedTicksInterval{
		StartTick: lastTick.TickNumber + 1,
		EndTick:   nextTick.TickNumber - 1,
	})
	if err != nil {
		return errors.Wrap(err, "setting skipped ticks interval")
	}

	return nil
}
