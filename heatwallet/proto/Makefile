PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)
GW = $(PB:.proto=.pb.gw.go)

all: $(GO) $(GW)

%.pb.go %.pb.gw.go: %.proto
	protoc -I=. -I../../protobuff \
		--go_out=paths=source_relative,Marchive.proto=github.com/qubic/go-archiver/protobuff:. \
		--go-grpc_out=paths=source_relative,Marchive.proto=github.com/qubic/go-archiver/protobuff:. \
		--grpc-gateway_out=paths=source_relative,Marchive.proto=github.com/qubic/go-archiver/protobuff:. \
		--grpc-gateway_opt logtostderr=true \
		--grpc-gateway_opt paths=source_relative \
		--grpc-gateway_opt generate_unbound_methods=true \
		$<

clean:
	rm -f *.pb.go *.pb.gw.go
