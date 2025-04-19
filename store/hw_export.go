package store

import "github.com/cockroachdb/pebble"

// Exported wrapper for tickDataKey
func TickDataKey(tickNumber uint32) []byte {
	return tickDataKey(tickNumber)
}

// Exported wrapper for emptyTickListPerEpochKey
func EmptyTickListPerEpochKey(epoch uint32) []byte {
	return emptyTickListPerEpochKey(epoch)
}

// Exported DB accessor for PebbleStore
func (s *PebbleStore) DB() *pebble.DB {
	return s.db
}
