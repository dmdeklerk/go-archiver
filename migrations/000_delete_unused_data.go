package migrations

import (
	"context"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/store"
)

// This is a special migration that runs on every startup.
// Its goal is to delete database data that is not used or needed for our use case.
// Use the config option to determine what data is deleted and what is kept

var DeleteQuorumData = true

func DeleteUnusedDataMigration(ps *store.PebbleStore) error {
	_, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	if DeleteQuorumData {
		log.Println("deleting all quorum data...")

		if err := ps.ClearKeysByPrefix(store.QuorumData); err != nil {
			return errors.Wrap(err, "deleting quorum data")
		}
	}
	return nil
}
