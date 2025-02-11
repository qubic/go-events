package main

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/go-events/processor"
	"github.com/qubic/go-events/pubsub"
	"github.com/qubic/go-events/server"
	"github.com/qubic/go-events/store"
	"github.com/qubic/go-qubic/common"
	"github.com/qubic/go-qubic/connector"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ardanlabs/conf"
	"github.com/pkg/errors"
)

const prefix = "QUBIC_EVENTS"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	var cfg struct {
		Server struct {
			ReadTimeout           time.Duration `conf:"default:5s"`
			WriteTimeout          time.Duration `conf:"default:5s"`
			ShutdownTimeout       time.Duration `conf:"default:5s"`
			HttpHost              string        `conf:"default:0.0.0.0:8000"`
			GrpcHost              string        `conf:"default:0.0.0.0:8001"`
			NodeSyncThreshold     int           `conf:"default:3"`
			MetricsUpdateInterval time.Duration `conf:"default:2s"`
			MetricsInstanceLabel  string
			MetricsAddress        string `conf:"default:0.0.0.0:2112"`
		}
		Pool struct {
			SingleNodeIP string `conf:"default:127.0.0.1"`
			//NodePasscodes uses a format of "ip:passcodeInBase64" which will be then decoded to raw passcode [4]uint64
			NodePasscodes      map[string]string `conf:"default:127.0.0.1:AAAAAAAAAAEAAAAAAAAAAQAAAAAAAAABAAAAAAAAAAE=;192.168.0.1:AAAAAAAAAAEAAAAAAAAAAgAAAAAAAAADAAAAAAAAAAQ="`
			NodeFetcherUrl     string            `conf:"default:http://127.0.0.1:8080/status"`
			NodeFetcherTimeout time.Duration     `conf:"default:2s"`
			InitialCap         int               `conf:"default:5"`
			MaxIdle            int               `conf:"default:20"`
			MaxCap             int               `conf:"default:30"`
			IdleTimeout        time.Duration     `conf:"default:15s"`
		}
		Qubic struct {
			NodePort              string        `conf:"default:21841"`
			StorageFolder         string        `conf:"default:store"`
			ConnectionTimeout     time.Duration `conf:"default:5s"`
			HandlerRequestTimeout time.Duration `conf:"default:5s"`
			ProcessTickTimeout    time.Duration `conf:"default:120s"`
		}
		PubSub struct {
			Enabled  bool   `conf:"default:false"`
			Addr     string `conf:"default:localhost:6379"`
			Password string `conf:"default:password"`
		}
	}

	if err := conf.Parse(os.Args[1:], prefix, &cfg); err != nil {
		switch {
		case errors.Is(err, conf.ErrHelpWanted):
			usage, err := conf.Usage(prefix, &cfg)
			if err != nil {
				return errors.Wrap(err, "generating config usage")
			}
			fmt.Println(usage)
			return nil
		case errors.Is(err, conf.ErrVersionWanted):
			version, err := conf.VersionString(prefix, &cfg)
			if err != nil {
				return errors.Wrap(err, "generating config version")
			}
			fmt.Println(version)
			return nil
		}
		return errors.Wrap(err, "parsing config")
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return errors.Wrap(err, "generating config for output")
	}
	log.Printf("main: Config :\n%v\n", out)

	createMetricsGauges(cfg.Server.MetricsInstanceLabel)

	pfConfig := connector.PoolFetcherConfig{
		URL:            cfg.Pool.NodeFetcherUrl,
		RequestTimeout: cfg.Pool.NodeFetcherTimeout,
	}
	cConfig := connector.Config{
		ConnectionPort:        cfg.Qubic.NodePort,
		ConnectionTimeout:     cfg.Qubic.ConnectionTimeout,
		HandlerRequestTimeout: cfg.Qubic.HandlerRequestTimeout,
	}
	pConfig := connector.PoolConfig{
		InitialCap:  cfg.Pool.InitialCap,
		MaxCap:      cfg.Pool.MaxCap,
		MaxIdle:     cfg.Pool.MaxIdle,
		IdleTimeout: cfg.Pool.IdleTimeout,
	}

	pConn, err := connector.NewPoolConnector(pfConfig, cConfig, pConfig)
	if err != nil {
		return errors.Wrap(err, "creating connector")
	}

	levelOptions := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.ZstdCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       2097152,
	}

	pebbleOptions := pebble.Options{
		Levels: []pebble.LevelOptions{levelOptions},
	}

	db, err := pebble.Open(cfg.Qubic.StorageFolder, &pebbleOptions)
	if err != nil {
		return errors.Wrap(err, "opening db with zstd compression")
	}
	defer db.Close()

	var pubSubClient *pubsub.RedisPubSub
	if cfg.PubSub.Enabled {
		redisPubSubClient, err := pubsub.NewRedisPubSub(cfg.PubSub.Addr, cfg.PubSub.Password)
		if err != nil {
			return errors.Wrap(err, "creating redis pubsub client")
		}

		pubSubClient = redisPubSubClient
	}

	eventsStore := store.NewStore(db)

	passcodes, err := convertPasscodesMapFromBase64ToRaw(cfg.Pool.NodePasscodes)
	if err != nil {
		return errors.Wrap(err, "converting passcodes from base64 to raw")
	}

	proc := processor.NewProcessor(pConn, pubSubClient, cfg.PubSub.Enabled, eventsStore, cfg.Qubic.ProcessTickTimeout, passcodes)

	setupMetricsRoutines(cfg.Server.MetricsAddress, cfg.Server.MetricsUpdateInterval, proc)

	srv := server.NewServer(cfg.Server.GrpcHost, cfg.Server.HttpHost, eventsStore)
	err = srv.Start()
	if err != nil {
		return errors.Wrap(err, "starting server")
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	procErrors := make(chan error, 1)

	// Start the service listening for requests.
	go func() {
		procErrors <- proc.Start()
	}()

	for {
		select {
		case <-shutdown:
			return errors.New("shutting down")
		case err := <-procErrors:
			return errors.Wrap(err, "events service error")
		}
	}

	return nil
}

func convertPasscodesMapFromBase64ToRaw(passcodesMap map[string]string) (map[string][4]uint64, error) {
	passcodes := make(map[string][4]uint64)

	for k, v := range passcodesMap {
		decodedPasscode, err := common.DecodePasscodeFromBase64(v)
		if err != nil {
			return nil, errors.Wrapf(err, "decoding passcode for node %s", k)
		}

		passcodes[k] = decodedPasscode
	}

	return passcodes, nil
}

var (
	registry = prometheus.NewRegistry()
	factory  = promauto.With(registry)

	lastProcessedTick prometheus.Gauge
	currentEpoch      prometheus.Gauge
)

func createMetricsGauges(instanceLabel string) {

	var labels prometheus.Labels

	if len(instanceLabel) != 0 {
		labels = make(prometheus.Labels)
		labels["name"] = instanceLabel
	}

	lastProcessedTick = factory.NewGauge(prometheus.GaugeOpts{
		Name:        "qubic_events_last_processed_tick",
		Help:        "The last tick processed by the events service.",
		ConstLabels: labels,
	})

	currentEpoch = factory.NewGauge(prometheus.GaugeOpts{
		Name:        "qubic_events_current_epoch",
		Help:        "The current epoch of the last processed tick.",
		ConstLabels: labels,
	})
}

func setupMetricsRoutines(metricsAddress string, updateInterval time.Duration, proc *processor.Processor) {
	go func() {
		ticker := time.NewTicker(updateInterval)
		for {
			select {
			case <-ticker.C:
				lastTick := proc.LastProcessedTick.Get()
				lastProcessedTick.Set(float64(lastTick.TickNumber))
				currentEpoch.Set(float64(lastTick.Epoch))
			}
		}
	}()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.InstrumentMetricHandler(registry, promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))

		metricsServer := http.Server{
			Addr:    metricsAddress,
			Handler: mux,
		}
		err := metricsServer.ListenAndServe()
		if err != nil {
			log.Printf("Metrics server failed: %s\n", err)
		}
	}()
}
