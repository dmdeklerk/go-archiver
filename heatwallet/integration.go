// Package heatwallet provides integration points for the parent module
package heatwallet

import (
	"context"
	"fmt"
	"github.com/dmdeklerk/go-archiver/heatwallet/hw_store"
	"github.com/dmdeklerk/go-archiver/heatwallet/migrations"
	"github.com/dmdeklerk/go-archiver/heatwallet/processor"
	"github.com/qubic/go-archiver/store"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	rpc "github.com/dmdeklerk/go-archiver/heatwallet/rpc"
	pb "github.com/dmdeklerk/go-archiver/heatwallet/proto"
)

// RegisterServices sets up all Heatwallet components.
// Returns the HeatPebbleStore for any additional operations.
func RegisterServices(grpcServer *grpc.Server, parentStore *store.PebbleStore) *hw_store.HeatPebbleStore {
	// 1. Create HeatPebbleStore
	heatStore := hw_store.NewHeatPebbleStore(parentStore)

	// 1a. Run migrations
	if err := migrations.PerformMigrations(heatStore); err != nil {
		panic(fmt.Errorf("heatwallet: migrations failed: %w", err))
	}

	// 2. Register processors
	processor.RegisterProcessor(&processor.AssetTransactionProcessor{})

	// 3. Register the Heatwallet gRPC service
	heatServer := rpc.NewHeatServer(heatStore)
	pb.RegisterHeatwalletServiceServer(grpcServer, heatServer)

	// 4. Return the store for migrations or other operations
	return heatStore
}

// RegisterHTTPHandlers registers the Heatwallet HTTP/JSON handlers on the given mux.
func RegisterHTTPHandlers(mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) error {
	return pb.RegisterHeatwalletServiceHandlerFromEndpoint(context.Background(), mux, endpoint, opts)
}
