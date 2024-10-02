#Check for Macos arm64

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_S),Darwin)
    ifeq ($(UNAME_M),arm64)
        OPT_ARGS := --proto_path=/opt/homebrew/Cellar/protobuf/25.3_1/include
    else
        OPT_ARGS :=
    endif
else
    OPT_ARGS :=
endif



PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)
PWD = $(pwd)


all: $(GO)

%.pb.go: %.proto
		protoc -I=. --go-grpc_out=paths=source_relative:. \
		--grpc-gateway_out . \
    --grpc-gateway_opt logtostderr=true \
    --grpc-gateway_opt paths=source_relative \
    --grpc-gateway_opt generate_unbound_methods=true $(OPT_ARGS) \
	--go_out=paths=source_relative:. *.proto

openapi:
		protoc -I=. --openapiv2_out . \
				--openapiv2_opt allow_merge=true,merge_file_name=qubic-events-rpc $(OPT_ARGS) \
			   *.proto
		jq '. += {"host":"api.qubic.org", "schemes": ["http", "https"]}' qubic-events-rpc.swagger.json > tmp && mv tmp qubic-events-rpc.swagger.json

clean:
		rm -f *.pb.go
		rm -f *.go