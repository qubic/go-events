package server

import (
	"context"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/pkg/errors"
	eventspb "github.com/qubic/go-events/proto"
	"github.com/qubic/go-events/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	"net"
	"net/http"
)

type Server struct {
	listenAddrGRPC string
	listenAddrHTTP string
	grpcServer     *grpc.Server
}

func NewServer(listenAddrGRPC, listenAddrHTTP string, eventsStore *store.EventsStore) *Server {
	grpcServer := createGrpcServerAndRegisterServices(eventsStore)
	server := Server{
		listenAddrGRPC: listenAddrGRPC,
		listenAddrHTTP: listenAddrHTTP,
		grpcServer:     grpcServer,
	}

	return &server
}

func createGrpcServerAndRegisterServices(eventsStore *store.EventsStore) *grpc.Server {
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(600*1024*1024),
		grpc.MaxSendMsgSize(600*1024*1024),
	)

	eventsService := NewEventsService(eventsStore)
	eventspb.RegisterEventsServiceServer(srv, eventsService)

	reflection.Register(srv)

	return srv
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.listenAddrGRPC)
	if err != nil {
		return errors.Wrap(err, "grpc failed to listen to tcp port")
	}

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()

	if s.listenAddrHTTP != "" {
		go func() {
			mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
				MarshalOptions: protojson.MarshalOptions{EmitDefaultValues: true, EmitUnpopulated: false},
			}))
			opts := []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultCallOptions(
					grpc.MaxCallRecvMsgSize(600*1024*1024),
					grpc.MaxCallSendMsgSize(600*1024*1024),
				),
			}

			if err := eventspb.RegisterEventsServiceHandlerFromEndpoint(
				context.Background(),
				mux,
				s.listenAddrGRPC,
				opts,
			); err != nil {
				panic(err)
			}

			if err := http.ListenAndServe(s.listenAddrHTTP, mux); err != nil {
				panic(err)
			}
		}()
	}

	return nil
}
