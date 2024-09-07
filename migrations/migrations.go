package migrations

import (
	"log"

	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/store"
)

type MigrationFunc func(ps *store.PebbleStore) error

var migrations = []MigrationFunc{
	AssetTransactionMigration,
}

func PerformMigrations(ps *store.PebbleStore) error {
	// We always run the deletion task to delete the quorum data as we dont use that.
	err := DeleteUnusedDataMigration(ps)
	if err != nil {
		return errors.Wrap(err, "deleting unused data")
	}

	currentVersion, err := ps.GetMigrationVersion()
	if err != nil {
		// Handle the case where the migration version cannot be fetched
		// Assuming 0 as the default version if not set
		if err != store.ErrNotFound {
			return errors.Wrap(err, "unable to fetch current migration version")
		}
		currentVersion = 0
	}

	// // // DEBUG!! REMOVE THIS !!
	// currentVersion = 0
	// log.Printf("DEBUG!! FORCE SET MIGRATION VERSION TO 0")

	for idx, migrate := range migrations {
		migrationIndex := uint32(idx + 1) // Migration versions start at 1
		if migrationIndex > currentVersion {
			log.Printf("running migration %d...\n", migrationIndex)
			if err := migrate(ps); err != nil {
				return errors.Wrapf(err, "error running migration %d", migrationIndex)
			}
			// Update the migration version after each successful migration
			if err := ps.SetMigrationVersion(migrationIndex); err != nil {
				return errors.Wrapf(err, "error updating migration version after migration %d", migrationIndex)
			}
		}
	}

	log.Println("all applicable migrations completed successfully.")
	return nil
}
