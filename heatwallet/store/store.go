// Coding Agent instructions START
// The code below saves data to the pebble database
// Coding Agent instructions END

func (s *PebbleStore) GetMigrationVersion() (uint32, error) {
	var migrationVersionKey = []byte{DbMigrationVersion}
	value, closer, err := s.db.Get(migrationVersionKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, ErrNotFound
		}
		return 0, errors.Wrap(err, "retrieving migration version")
	}
	defer closer.Close()

	if len(value) < 4 {
		return 0, errors.New("migration version data is corrupted")
	}
	version := binary.LittleEndian.Uint32(value)
	return version, nil
}

func (s *PebbleStore) SetMigrationVersion(version uint32) error {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], version)
	var migrationVersionKey = []byte{DbMigrationVersion}
	err := s.db.Set(migrationVersionKey, buf[:], pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting migration version")
	}
	return nil
}

// CountKeysInRange counts all the keys in the Pebble database.
func (s *PebbleStore) CountKeysInRange(prefixID byte) (int, error) {
	startKey := []byte{prefixID}
	endKey := make([]byte, len(startKey))
	copy(endKey, startKey)
	endKey[len(endKey)-1]++

	log.Printf("start counting keys in range...")
	count := 0
	iter, err := s.db.NewIter(&pebble.IterOptions{
		UpperBound: endKey,
		LowerBound: startKey,
	}) // nil IterOptions means iterate over the entire database
	if err != nil {
		return 0, errors.Wrap(err, "creating iterator")
	}
	defer iter.Close()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			log.Printf("still counting [%d]...", count)
		}
	}()

	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	ticker.Stop()

	if err := iter.Error(); err != nil {
		return 0, err
	}

	log.Printf("done counting keys in range, there are %d keys", count)
	return count, nil
}

// ClearKeysByPrefix deletes all keys starting with the specified prefix identifier.
func (s *PebbleStore) ClearKeysByPrefix(prefixID byte) error {
	startKey := []byte{prefixID}
	endKey := make([]byte, len(startKey))
	copy(endKey, startKey)
	endKey[len(endKey)-1]++

	keyCountBefore, err := s.CountKeysInRange(prefixID)
	if err != nil {
		return errors.Wrap(err, "cant count keys")
	}

	log.Printf("start key range deletion...")

	if err := s.db.DeleteRange(startKey, endKey, pebble.Sync); err != nil {
		return errors.Wrap(err, "deleting key range in batch")
	}

	log.Printf("done deleting key range, starting key analysis...")

	keyCountAfter, err := s.CountKeysInRange(prefixID)
	if err != nil {
		return errors.Wrap(err, "cant count keys")
	}

	log.Printf("a total of %d keys have been deleted", keyCountBefore-keyCountAfter)

	return nil
}

func (s *PebbleStore) FindFirstTickNumber() (uint32, error) {
	startKey := tickDataKey(0) // Generates the lowest possible key
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
	})
	if err != nil {
		return 0, errors.Wrap(err, "cant create iterator")
	}
	defer iter.Close()

	// Advance the iterator to the first key in the specified range
	if iter.First() {
		if len(iter.Key()) > 8 { // The key should be at least 1 byte prefix + 8 bytes uint64
			// Parse the tick number from the key
			// Key structure is [prefix][8-byte tickNumber]
			tickNumber := binary.BigEndian.Uint64(iter.Key()[1:])
			return uint32(tickNumber), nil // Convert uint64 to uint32, assuming the value fits into uint32
		}
	}

	if err := iter.Error(); err != nil {
		return 0, errors.Wrap(err, "iterator exited with error")
	}

	return 0, errors.New("no tick data keys found")
}

func (s *PebbleStore) PutAssetTransactionsPerTick(identity string, assetId string, tickNumber uint32, txs *protobuff.AssetTransactionsPerTickDB) error {
	baseKey := identityAssetTransactionKey(identity, assetId)
	key := identityAssetTransactionKeyWithTickNumber(baseKey, tickNumber)
	serialized, err := proto.Marshal(txs)
	if err != nil {
		return errors.Wrap(err, "serializing asset transaction proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting asset transactions per tick")
	}

	return nil
}

func (s *PebbleStore) PutAssetTransactionsPerTickBatch(identityMap map[string]map[string][]string, tickNumber uint32) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for identity, assetIdMap := range identityMap {
		for assetId, transactionIds := range assetIdMap {
			baseKey := identityAssetTransactionKey(identity, assetId)
			key := identityAssetTransactionKeyWithTickNumber(baseKey, tickNumber)
			serialized, err := proto.Marshal(&protobuff.AssetTransactionsPerTickDB{
				Transactions: transactionIds,
			})
			if err != nil {
				return errors.Wrap(err, "serializing asset transaction proto")
			}
			err = batch.Set(key, serialized, nil)
			if err != nil {
				return errors.Wrap(err, "setting asset transactions per tick")
			}
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}
	return nil
}

type IdetityAssetTransactions struct {
	Transaction *protobuff.Transaction
	MoneyFlew   bool
	Timestamp   uint64
	Payload     asset_transactions.TransactionWithAssetPayload
}

func extractTickNumberFromIdentityAssetTransactionKey(key []byte) (uint32, error) {
	if len(key) < 8 {
		return 0, errors.New("invalid key length")
	}
	tickNumberBytes := key[len(key)-8:]
	tickNumber := binary.BigEndian.Uint64(tickNumberBytes)
	return uint32(tickNumber), nil
}

func (s *PebbleStore) GetIdetityAssetTransactionsFromEnd(ctx context.Context, includeFailedTransactions bool, identity, assetId string, endTick uint32, txnIndexStart, maxTransactions int) ([]*IdetityAssetTransactions, uint32, uint32, uint32, error) {
	lastProcessedTick, err := s.GetLastProcessedTick(ctx)
	if err != nil {
		return nil, 0, 0, 0, errors.Wrap(err, "fetching last processed tick")
	}
	// The user can omit the {endTick} parameter in which case we start at the last processed tick
	if endTick == 0 {
		endTick = lastProcessedTick.TickNumber
	}

	// The user can omit the {maxTransactions} parameter in which case we default to 1000
	if maxTransactions == 0 {
		maxTransactions = 1000
	}

	baseKey := identityAssetTransactionKey(identity, assetId)
	startKey := identityAssetTransactionKeyWithTickNumber(baseKey, 0)
	endKey := identityAssetTransactionKeyWithTickNumber(baseKey, endTick+1)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err != nil {
		return nil, 0, 0, 0, errors.Wrap(err, "creating iterator")
	}
	defer iter.Close()

	var transactions []*IdetityAssetTransactions
	firstTick := true // this is the first tick we process, this affects if we consider the start index in the transaction array, or start at index 0
	nextEndTick := uint32(0)
	nextTxnIndexStart := uint32(0)

	// Start from the last entry within bounds and iterate backwards
	for ok := iter.Last(); ok; ok = iter.Prev() {

		// The tickNumber is in the key
		key := iter.Key()
		tickNumber, err := extractTickNumberFromIdentityAssetTransactionKey(key)
		if err != nil {
			return nil, 0, 0, 0, errors.Wrap(err, "extracting tickNumber from key")
		}
		// TODO: This fetches all transactions for the tick but all we want is the timestamp
		tickData, err := s.GetTickData(ctx, tickNumber)
		if err != nil {
			return nil, 0, 0, 0, errors.Wrap(err, "getting tick data")
		}
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, 0, 0, 0, errors.Wrap(err, "getting value from iterator")
		}

		var perTick protobuff.AssetTransactionsPerTickDB
		err = proto.Unmarshal(value, &perTick)
		if err != nil {
			return nil, 0, 0, 0, errors.Wrap(err, "unmarshalling asset transactions per tick")
		}
		nextEndTick = tickNumber

		if firstTick && txnIndexStart >= len(perTick.Transactions) {
			firstTick = false
			continue // Skip this tick if txnIndexStart is out of bounds
		}

		// For simpler processing logic we reverse the array of transaction ids in place
		for i, j := 0, len(perTick.Transactions)-1; i < j; i, j = i+1, j-1 {
			perTick.Transactions[i], perTick.Transactions[j] = perTick.Transactions[j], perTick.Transactions[i]
		}

		// If its not the first tick we start processing at index 0
		if !firstTick {
			txnIndexStart = 0
		}

		for i := txnIndexStart; i < len(perTick.Transactions); i++ {
			transactionId := perTick.Transactions[i]

			txStatus, err := s.GetTransactionStatus(ctx, transactionId)
			if err != nil {
				return nil, 0, 0, 0, errors.Wrap(err, "getting transaction status")
			}

			// Filter says we only want valid transfers
			if !includeFailedTransactions && !txStatus.MoneyFlew {
				continue
			}

			transaction, err := s.GetTransaction(ctx, transactionId)
			if err != nil {
				return nil, 0, 0, 0, errors.Wrap(err, "get transaction by id")
			}

			transactions = append(transactions, &IdetityAssetTransactions{
				Transaction: transaction,
				MoneyFlew:   txStatus.MoneyFlew,
				Timestamp:   tickData.Timestamp,
			})

			if len(transactions) >= maxTransactions {
				// We might have stopped processing transactions mid-range, this means that the next pagination should
				// start where we have now left off. Thats unless we reached the end of the array
				if i < (len(perTick.Transactions) - 1) {
					nextTxnIndexStart = uint32(i + 1)
				}
				return transactions, nextEndTick, nextTxnIndexStart, lastProcessedTick.TickNumber, nil
			}
		}

		// We fully processed the current tick so we can safely move to the next
		if nextEndTick > 0 {
			nextTxnIndexStart = 0
			nextEndTick--
		}

		firstTick = false // Reset firstTick flag after processing the first tick
	}
	return transactions, nextEndTick, nextTxnIndexStart, lastProcessedTick.TickNumber, nil
}