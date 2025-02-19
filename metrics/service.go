package metrics

import (
	"github.com/jellydator/ttlcache/v3"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	eventspb "github.com/qubic/go-events/proto"
	"log"
	"net/http"
	"time"
)

const lptCacheKey = "LPT"
const epochCacheKey = "EPOCH"

type Store interface {
	FetchLastProcessedTick() (*eventspb.ProcessedTick, error)
}

type Service struct {
	address string
	cache   *ttlcache.Cache[string, uint32]
	store   Store

	lastProcessedTickGauge prometheus.Gauge
	currentEpochGauge      prometheus.Gauge
}

func NewMetricsService(address string, store Store) *Service {
	return &Service{
		address: address,
		cache:   ttlcache.New[string, uint32](ttlcache.WithTTL[string, uint32](5 * time.Second)), // TODO: maybe find a better value?
		store:   store,
		lastProcessedTickGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "qubic_events_last_processed_tick",
			Help: "The last tick processed by the events service.",
		}),
		currentEpochGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "qubic_events_current_epoch",
			Help: "The current epoch of the last processed tick.",
		}),
	}
}

func (s *Service) Start() {

	go s.cache.Start()

	go func() {

		serverMux := http.NewServeMux()
		serverMux.Handle("/metrics", s.metricsEndpointHandler())

		var server = &http.Server{
			Addr:              s.address,
			Handler:           serverMux,
			ReadTimeout:       15 * time.Second,
			ReadHeaderTimeout: 15 * time.Second,
			WriteTimeout:      15 * time.Second,
		}

		if err := server.ListenAndServe(); err != nil {
			panic(err)
		}
	}()
}

func (s *Service) refreshCache() error {

	lpt, err := s.store.FetchLastProcessedTick()
	if err != nil {
		return errors.Wrap(err, "fetching last processed tick")
	}

	s.cache.Set(lptCacheKey, lpt.TickNumber, ttlcache.DefaultTTL)
	s.cache.Set(epochCacheKey, lpt.Epoch, ttlcache.DefaultTTL)

	return nil
}

func (s *Service) metricsEndpointHandler() http.Handler {

	if !s.cache.Has(lptCacheKey) || !s.cache.Has(epochCacheKey) {
		err := s.refreshCache()
		if err != nil {
			log.Printf("Failed to refresh metrics cache: %s\n", err)
		}
	}

	lastProcessedTick := s.cache.Get(lptCacheKey).Value()
	epoch := s.cache.Get(epochCacheKey).Value()

	s.lastProcessedTickGauge.Set(float64(lastProcessedTick))
	s.currentEpochGauge.Set(float64(epoch))
	return promhttp.Handler()
}
