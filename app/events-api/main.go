package main

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/qubic/go-events/processor"
	"github.com/qubic/go-events/server"
	"github.com/qubic/go-events/store"
	"github.com/qubic/go-qubic/connector"
	"log"
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
			ReadTimeout       time.Duration `conf:"default:5s"`
			WriteTimeout      time.Duration `conf:"default:5s"`
			ShutdownTimeout   time.Duration `conf:"default:5s"`
			HttpHost          string        `conf:"default:0.0.0.0:8000"`
			GrpcHost          string        `conf:"default:0.0.0.0:8001"`
			NodeSyncThreshold int           `conf:"default:3"`
			ChainTickFetchUrl string        `conf:"default:http://127.0.0.1:8080/max-tick"`
		}
		Pool struct {
			SingleNodeIP       string        `conf:"default:127.0.0.1"`
			NodePasscode       []uint64      `conf:"default:1;1;1;1"`
			NodeFetcherUrl     string        `conf:"default:http://127.0.0.1:8080/status"`
			NodeFetcherTimeout time.Duration `conf:"default:2s"`
			InitialCap         int           `conf:"default:5"`
			MaxIdle            int           `conf:"default:20"`
			MaxCap             int           `conf:"default:30"`
			IdleTimeout        time.Duration `conf:"default:15s"`
		}
		Qubic struct {
			NodePort           string        `conf:"default:21841"`
			StorageFolder      string        `conf:"default:store"`
			ProcessTickTimeout time.Duration `conf:"default:120s"`
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

	connectorConfig := connector.Config{
		ConnectionPort:        cfg.Qubic.NodePort,
		ConnectionTimeout:     5 * time.Second,
		HandlerRequestTimeout: 5 * time.Second,
	}

	conn, err := connector.NewConnector(cfg.Pool.SingleNodeIP, connectorConfig)
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

	eventsStore := store.NewStore(db)

	var passcode [4]uint64
	copy(passcode[:], cfg.Pool.NodePasscode)
	proc := processor.NewProcessor(conn, eventsStore, cfg.Qubic.ProcessTickTimeout, passcode)

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
