package metrics

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	eventspb "github.com/qubic/go-events/proto"
	"log"
	"time"
)

type Store interface {
	FetchLastProcessedTick() (*eventspb.ProcessedTick, error)
}

type Service struct {
	lastProcessedTickGauge prometheus.Gauge
	currentEpochGauge      prometheus.Gauge
	store                  Store
	updateInterval         time.Duration
}

func NewMetricsService(store Store, updateInterval time.Duration) *Service {
	return &Service{
		lastProcessedTickGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "qubic_events_last_processed_tick",
			Help: "The last tick processed by the events service.",
		}),
		currentEpochGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "qubic_events_current_epoch",
			Help: "The current epoch of the last processed tick.",
		}),
		store:          store,
		updateInterval: updateInterval,
	}
}

func (s *Service) Start() {

	go func() {
		log.Printf("Starting metrics service...\n")

		ticker := time.NewTicker(s.updateInterval)
		for {
			select {
			case <-ticker.C:
				lastProcessedTick, err := s.store.FetchLastProcessedTick()
				if err != nil {
					fmt.Printf("Error while fetching last processed tick: %s\n", err)
				}

				s.lastProcessedTickGauge.Set(float64(lastProcessedTick.TickNumber))
				s.currentEpochGauge.Set(float64(lastProcessedTick.Epoch))
			}
		}
	}()
}
