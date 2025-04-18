// Coding Agent instructions START

// The function below lives in validator/tx/validator.go in the parent package.
// The Store function runs for every transaction being processed by the parent package and we use it to inject our
// StoreAssetTransactions function whose task it is to store the specific per asset transfer information in the
// parent pebble database at the keys we defined in heatwallet/keys.go.

// We have to somehow plugin our transaction processing into the parent processor code but do so in a way that its a minimal
// change and will not lead to merge problems later on.

// We expect our module to have more transaction processors in the future as we want to store specific indexes based on transaction parsing.

// Coding Agent instructions END

// func Store(ctx context.Context, store *store.PebbleStore, tickNumber uint32, transactions types.Transactions) error {
// 	err := storeTickTransactions(ctx, store, transactions)
// 	if err != nil {
// 		return errors.Wrap(err, "storing tick transactions")
// 	}

// 	err = storeTransferTransactions(ctx, store, tickNumber, transactions)
// 	if err != nil {
// 		return errors.Wrap(err, "storing transfer transactions")
// 	}
// 	err = StoreAssetTransactions(ctx, store, tickNumber, transactions)
// 	if err != nil {
// 		return errors.Wrap(err, "storing asset transfer transactions")
// 	}
// 	return nil
// }

func StoreAssetTransactions(ctx context.Context, store *store.PebbleStore, tickNumber uint32, transactions types.Transactions) error {
	transactionWithAssetPayloads, err := removeNonTransactionWithAssetPayloadsAndConvert(transactions)
	if err != nil {
		return errors.Wrap(err, "removing non transactions with asset payload")
	}
	identityMap, err := createIdentityMap(transactionWithAssetPayloads)
	if err != nil {
		return errors.Wrap(err, "grouping transactions with asset payload per identity and asset id")
	}

	err = store.PutAssetTransactionsPerTickBatch(identityMap, tickNumber)
	if err != nil {
		return errors.Wrap(err, "storing asset transactions")
	}

	return nil
}

// Removes unsupported transactions, parses the payload based on the input type, returns a struct containing
// the transaction and the parsed payload
func removeNonTransactionWithAssetPayloadsAndConvert(transactions []types.Transaction) ([]*asset_transactions.TransactionWithAssetPayload, error) {
	transactionWithAssetPayloads := make([]*asset_transactions.TransactionWithAssetPayload, 0)
	for _, tx := range transactions {

		transactionWithAssetPayload, err := asset_transactions.ParseAssetTransaction(tx)
		if err != nil {
			if err == asset_transactions.ErrNotValidTransaction {
				continue
			}
			return nil, errors.Wrap(err, "parse asset transaction")
		}

		if transactionWithAssetPayload != nil {
			transactionWithAssetPayloads = append(transactionWithAssetPayloads, transactionWithAssetPayload)
		}
	}

	return transactionWithAssetPayloads, nil
}

// We want to group transactions per identity and asset id
// The map looks like this
//
//	{
//	  "identity-1": {
//				"asset-1" : ["transaction id 1", "transaction id 2"],
//				"asset-2" : ["transaction id 2", "transaction id 3"],
//	  }
//	  "identity-2": {
//				"asset-1" : ["transaction id 1", "transaction id 2"],
//				"asset-2" : ["transaction id 2", "transaction id 3"],
//	  }
//	}
//
// Example: if we pass a single Qubic transfer transaction the result will be a map with two entries, one for the
// sourceId and one for the destId. Both entries will have a copy of the same transaction
func createIdentityMap(txs []*asset_transactions.TransactionWithAssetPayload) (map[string]map[string][]string, error) {

	// Define the map structure: map[identity]map[assetId][]transactionId
	identityMap := make(map[string]map[string][]string)

	for _, tx := range txs {

		transactionData, err := asset_transactions.FindTransactionIdParticipantsAndCurrency(*tx)
		if err != nil {
			if err == asset_transactions.ErrNotValidTransaction {
				log.Printf("no transaction id, particpants and currency, skipping %s", tx.Transaction.TxId)
				continue
			}
			return nil, errors.Wrap(err, "finding transaction id, particpants and currency")
		}

		// for each participant
		for _, identity := range transactionData.Identities {
			// prepare the identity entry
			_, ok := identityMap[identity]
			if !ok {
				identityMap[identity] = make(map[string][]string)
			}

			// prepare the asset id entry
			assetId := transactionData.Currency.AssetIssuer + transactionData.Currency.AssetName
			identityMap[identity][assetId] = append(identityMap[identity][assetId], tx.Transaction.TxId)
		}
	}

	return identityMap, nil
}