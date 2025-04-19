package rpc

import (
	"github.com/dmdeklerk/go-archiver/heatwallet/hw_store"
	pb "github.com/dmdeklerk/go-archiver/heatwallet/proto"
)

// HeatServer implements the HeatwalletService gRPC interface.
type HeatServer struct {
	pb.UnimplementedHeatwalletServiceServer
	Store *hw_store.HeatPebbleStore // Capital S to match the reference in v2_endpoints.go
}

// NewHeatServer creates a new HeatServer with given store.
func NewHeatServer(store *hw_store.HeatPebbleStore) *HeatServer {
	return &HeatServer{Store: store}
}
