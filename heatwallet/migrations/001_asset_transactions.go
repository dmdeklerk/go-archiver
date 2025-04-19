package migrations

import (
	"context"
	"log"
	"time"

	"github.com/dmdeklerk/go-archiver/heatwallet/hw_store"
	"github.com/dmdeklerk/go-archiver/heatwallet/processor"
	"github.com/dmdeklerk/go-archiver/heatwallet/utils"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/go-archiver/store"
)

func AssetTransactionMigration(ps *hw_store.HeatPebbleStore) error {
	log.Println("performing asset transaction migration...")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	if err := ps.ClearKeysByPrefix(hw_store.IdentityAssetTransactions); err != nil {
		return errors.Wrap(err, "deleting asset transactions")
	}

	lastTick, err := ps.Store.GetLastProcessedTick(ctx)
	if err != nil {
		return errors.Wrap(err, "getting last processed tick")
	}

	firstTick, err := ps.FindFirstTickNumber()
	if err != nil {
		return errors.Wrap(err, "find first tick number")
	}
	log.Printf("[migration/001_asset_transaction] first tick is %d", firstTick)

	tickCounter := 0
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			log.Printf("[migration/001_asset_transaction] processed %d ticks so far...", tickCounter)
		}
	}()

	for tickNumber := firstTick; tickNumber <= lastTick.TickNumber; tickNumber++ {
		tickTransactions, err := ps.Store.GetTickTransactions(ctx, tickNumber)
		if errors.Is(err, store.ErrNotFound) {
			continue
		} else if err != nil {
			log.Printf("error retrieving tick data for tick number: %d: %v", tickNumber, err)
			continue
		}
		err = processTickData(ctx, ps, tickNumber, tickTransactions)
		if err != nil {
			return errors.Wrap(err, "failed to proces tick")
		}
		tickCounter++
	}

	ticker.Stop()

	log.Printf("[migration/001_asset_transaction] done processing ticks")

	keyCount, err := ps.CountKeysInRange(hw_store.IdentityAssetTransactions)
	if err != nil {
		return errors.Wrap(err, "cant count keys")
	}
	log.Printf("[migration/001_asset_transaction] number of asset transaction keys after migration %d", keyCount)

	return nil
}

func processTickData(ctx context.Context, ps *hw_store.HeatPebbleStore, tickNumber uint32, tickTransactions []*protobuff.Transaction) error {
	transactions, err := utils.ProtoToQubic(tickTransactions)
	if err != nil {
		return err
	}

	err = (&processor.AssetTransactionProcessor{}).Process(ctx, ps, tickNumber, transactions)
	if err != nil {
		return errors.Wrap(err, "failed to store asset transaction")
	}
	return nil
}
