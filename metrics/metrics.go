package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	lastProcessedTickGauge prometheus.Gauge
	currentEpochGauge      prometheus.Gauge
}

func NewMetrics() *Metrics {
	return &Metrics{
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

func (m *Metrics) SetLastProcessedTick(tickNumber uint32) {
	m.lastProcessedTickGauge.Set(float64(tickNumber))
}

func (m *Metrics) SetCurrentEpoch(epoch uint32) {
	m.currentEpochGauge.Set(float64(epoch))
}
