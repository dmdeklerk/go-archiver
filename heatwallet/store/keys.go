package store

import (
	"encoding/binary"
)

const (
	// These are from the parent project
	// TickData                     = 0x00
	// QuorumData                   = 0x01
	// ComputorList                 = 0x02
	// Transaction                  = 0x03
	// LastProcessedTick            = 0x04
	// LastProcessedTickPerEpoch    = 0x05
	// SkippedTicksInterval         = 0x06
	// IdentityTransferTransactions = 0x07
	// ChainDigest                  = 0x08
	// ProcessedTickIntervals       = 0x09
	// TickTransactionsStatus       = 0x10
	// TransactionStatus            = 0x11
	// StoreDigest                  = 0x12
	// EmptyTicksPerEpoch           = 0x13

	// This needs to have a unique signature likely the upstream code started using this number for their own next key

	IdentityAssetTransactions = 0x14
	// Database Migration
	DbMigrationVersion = 0xFF
)

func identityAssetTransactionKey(identity string, assetId string) []byte {
	key := []byte{IdentityAssetTransactions}
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
