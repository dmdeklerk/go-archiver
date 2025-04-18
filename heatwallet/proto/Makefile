PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)

all: $(GO)

%.pb.go: %.proto
	protoc -I=. -I../../protobuff --go-grpc_out=paths=source_relative:. \
		--go_out=paths=source_relative:. $<

clean:
	rm -f *.pb.go
