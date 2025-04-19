package hw_store

import (
	"encoding/binary"
)

const (
	IdentityAssetTransactions = 0x00
	DbMigrationVersion        = 0xFF
)

const modulePrefix = "HW"

func identityAssetTransactionKey(identity string, assetId string) []byte {
	key := []byte(modulePrefix)
	key = append(key, IdentityAssetTransactions)
	key = append(key, []byte(identity)...)
	key = append(key, []byte(assetId)...)
	return key
}

func identityAssetTransactionKeyWithTickNumber(baseKey []byte, tickNumber uint32) []byte {
	key := make([]byte, len(baseKey))
	copy(key, baseKey)
	key = binary.BigEndian.AppendUint64(key, uint64(tickNumber))
	return key
}
