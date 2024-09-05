package migrations

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/go-archiver/store"
	"github.com/qubic/go-archiver/validator/tx"
)

func AssetTransferMigration(ps *store.PebbleStore) error {
	log.Println("performing asset transfer migration...")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	if err := ps.ClearKeysByPrefix(store.QxIdentityAssetTransfers); err != nil {
		return errors.Wrap(err, "deleting asset transfers")
	}

	lastTick, err := ps.GetLastProcessedTick(ctx)
	if err != nil {
		return errors.Wrap(err, "getting last processed tick")
	}

	firstTick, err := ps.FindFirstTickNumber()
	if err != nil {
		return errors.Wrap(err, "getting last processed tick")
	}
	log.Printf("[migration/001_asset_transfer] first tick is %d", firstTick)

	tickCounter := 0
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			log.Printf("[migration/001_asset_transfer] processed %d ticks so far...", tickCounter)
		}
	}()

	for tickNumber := firstTick; tickNumber <= lastTick.TickNumber; tickNumber++ {
		tickTransactions, err := ps.GetTickTransactions(ctx, tickNumber)
		if errors.Is(err, store.ErrNotFound) {
			continue
		} else if err != nil {
			// Log and handle other errors, possibly continue or break based on your error policy
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

	log.Printf("[migration/001_asset_transfer] done processing ticks")

	keyCount, err := ps.CountKeysInRange(store.QxIdentityAssetTransfers)
	if err != nil {
		return errors.Wrap(err, "cant count keys")
	}
	log.Printf("[migration/001_asset_transfer] number of asset transfer keys after migration %d", keyCount)

	return nil
}

// processTickData handles the processing of each tick's data
func processTickData(ctx context.Context, ps *store.PebbleStore, tickNumber uint32, tickTransactions []*protobuff.Transaction) error {
	transactions, err := tx.ProtoToQubic(tickTransactions)
	if err != nil {
		return err
	}

	err = tx.StoreQxAssetTransfers(ctx, ps, tickNumber, transactions)
	if err != nil {
		return errors.Wrap(err, "failed to store asset transfers")
	}
	return nil
}
