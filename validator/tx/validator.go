package tx

import (
	"context"
	"encoding/hex"
	"log"

	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/asset_transactions"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/go-archiver/store"
	"github.com/qubic/go-archiver/utils"
	"github.com/qubic/go-node-connector/types"
)

var emptyTxDigest [32]byte

func Validate(ctx context.Context, sigVerifierFunc utils.SigVerifierFunc, transactions []types.Transaction, tickData types.TickData) ([]types.Transaction, error) {
	digestsMap := createTxDigestsMap(tickData)
	// handles empty tick but with transactions
	if len(digestsMap) == 0 {
		return []types.Transaction{}, nil
	}

	if len(transactions) != len(digestsMap) {
		return nil, errors.Errorf("tx count mismatch. tx count: %d, digests count: %d", len(transactions), len(digestsMap))
	}

	validTxs, err := validateTransactions(ctx, sigVerifierFunc, transactions, digestsMap)
	if err != nil {
		return nil, errors.Wrap(err, "validating transactions")
	}

	return validTxs, nil
}

func validateTransactions(ctx context.Context, sigVerifierFunc utils.SigVerifierFunc, transactions []types.Transaction, digestsMap map[string]struct{}) ([]types.Transaction, error) {
	validTransactions := make([]types.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		txDigest, err := getDigestFromTransaction(tx)
		if err != nil {
			return nil, errors.Wrap(err, "getting digest from tx data")
		}

		txId, err := tx.ID()
		if err != nil {
			return nil, errors.Wrap(err, "getting tx id")
		}

		hexDigest := hex.EncodeToString(txDigest[:])
		if _, ok := digestsMap[hexDigest]; !ok {
			return nil, errors.Errorf("tx id: %s not found in digests map", txId)
		}

		txDataBytes, err := tx.MarshallBinary()
		if err != nil {
			return nil, errors.Wrap(err, "marshalling tx data")
		}

		constructedDigest, err := utils.K12Hash(txDataBytes[:len(txDataBytes)-64])
		if err != nil {
			return nil, errors.Wrap(err, "constructing digest from tx data")
		}

		err = sigVerifierFunc(ctx, tx.SourcePublicKey, constructedDigest, tx.Signature)
		if err != nil {
			return nil, errors.Wrap(err, "verifying tx signature")
		}
		validTransactions = append(validTransactions, tx)

		//log.Printf("Validated tx: %s. Count: %d\n", hexDigest, index)
	}

	return validTransactions, nil
}

func getDigestFromTransaction(tx types.Transaction) ([32]byte, error) {
	txDataMarshalledBytes, err := tx.MarshallBinary()
	if err != nil {
		return [32]byte{}, errors.Wrap(err, "marshalling")
	}

	digest, err := utils.K12Hash(txDataMarshalledBytes)
	if err != nil {
		return [32]byte{}, errors.Wrap(err, "hashing tx tx")
	}

	return digest, nil
}

func createTxDigestsMap(tickData types.TickData) map[string]struct{} {
	digestsMap := make(map[string]struct{})

	for _, digest := range tickData.TransactionDigests {
		if digest == emptyTxDigest {
			continue
		}

		hexDigest := hex.EncodeToString(digest[:])
		digestsMap[hexDigest] = struct{}{}
	}

	return digestsMap
}

func Store(ctx context.Context, store *store.PebbleStore, tickNumber uint32, transactions types.Transactions) error {
	err := storeTickTransactions(ctx, store, transactions)
	if err != nil {
		return errors.Wrap(err, "storing tick transactions")
	}

	err = storeTransferTransactions(ctx, store, tickNumber, transactions)
	if err != nil {
		return errors.Wrap(err, "storing transfer transactions")
	}

	err = StoreAssetTransactions(ctx, store, tickNumber, transactions)
	if err != nil {
		return errors.Wrap(err, "storing asset transfer transactions")
	}

	return nil
}

func storeTickTransactions(ctx context.Context, store *store.PebbleStore, transactions types.Transactions) error {
	protoModel, err := qubicToProto(transactions)
	if err != nil {
		return errors.Wrap(err, "converting to proto")
	}

	err = store.SetTransactions(ctx, protoModel)
	if err != nil {
		return errors.Wrap(err, "storing tick transactions")
	}

	return nil
}

func storeTransferTransactions(ctx context.Context, store *store.PebbleStore, tickNumber uint32, transactions types.Transactions) error {
	transferTransactions, err := removeNonTransferTransactionsAndConvert(transactions)
	if err != nil {
		return errors.Wrap(err, "removing non transfer transactions")
	}
	txsPerIdentity, err := createTransferTransactionsIdentityMap(ctx, transferTransactions)
	if err != nil {
		return errors.Wrap(err, "filtering transfer transactions")
	}

	for id, txs := range txsPerIdentity {
		err = store.PutTransferTransactionsPerTick(ctx, id, tickNumber, &protobuff.TransferTransactionsPerTick{TickNumber: uint32(tickNumber), Identity: id, Transactions: txs})
		if err != nil {
			return errors.Wrap(err, "storing transfer transactions")
		}
	}

	return nil
}

func removeNonTransferTransactionsAndConvert(transactions []types.Transaction) ([]*protobuff.Transaction, error) {
	transferTransactions := make([]*protobuff.Transaction, 0)
	for _, tx := range transactions {
		if tx.Amount == 0 {
			continue
		}

		protoTx, err := txToProto(tx)
		if err != nil {
			return nil, errors.Wrap(err, "converting to proto")
		}

		transferTransactions = append(transferTransactions, protoTx)
	}

	return transferTransactions, nil
}

func createTransferTransactionsIdentityMap(ctx context.Context, txs []*protobuff.Transaction) (map[string][]*protobuff.Transaction, error) {
	txsPerIdentity := make(map[string][]*protobuff.Transaction)
	for _, tx := range txs {
		_, ok := txsPerIdentity[tx.DestId]
		if !ok {
			txsPerIdentity[tx.DestId] = make([]*protobuff.Transaction, 0)
		}

		_, ok = txsPerIdentity[tx.SourceId]
		if !ok {
			txsPerIdentity[tx.SourceId] = make([]*protobuff.Transaction, 0)
		}

		txsPerIdentity[tx.DestId] = append(txsPerIdentity[tx.DestId], tx)
		txsPerIdentity[tx.SourceId] = append(txsPerIdentity[tx.SourceId], tx)
	}

	return txsPerIdentity, nil
}

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
