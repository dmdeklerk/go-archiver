package processor

import (
	"context"
	"log"

	"github.com/dmdeklerk/go-archiver/heatwallet/hw_store"
	"github.com/dmdeklerk/go-archiver/heatwallet/utils"
	"github.com/pkg/errors"
	"github.com/qubic/go-node-connector/types"
)

// TransactionProcessor defines an interface for transaction processors.
type TransactionProcessor interface {
	Process(ctx context.Context, store *hw_store.HeatPebbleStore, tickNumber uint32, transactions types.Transactions) error
}

// processors holds all registered transaction processors.
var processors []TransactionProcessor

// RegisterProcessor registers a transaction processor.
func RegisterProcessor(p TransactionProcessor) {
	processors = append(processors, p)
}

// ProcessAll runs all registered processors.
func ProcessAll(ctx context.Context, store *hw_store.HeatPebbleStore, tickNumber uint32, transactions types.Transactions) error {
	for _, p := range processors {
		err := p.Process(ctx, store, tickNumber, transactions)
		if err != nil {
			log.Printf("processor %T failed: %v", p, err)
			return err
		}
	}
	return nil
}

// AssetTransactionProcessor implements TransactionProcessor for asset transactions.
type AssetTransactionProcessor struct{}

func (p *AssetTransactionProcessor) Process(ctx context.Context, store *hw_store.HeatPebbleStore, tickNumber uint32, transactions types.Transactions) error {
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
		return errors.Wrap(err, "putting asset transactions per tick batch")
	}
	return nil
}

// Removes unsupported transactions, parses the payload based on the input type, returns a struct containing
// the transaction and the parsed payload
func removeNonTransactionWithAssetPayloadsAndConvert(transactions []types.Transaction) ([]*utils.TransactionWithAssetPayload, error) {
	transactionWithAssetPayloads := make([]*utils.TransactionWithAssetPayload, 0)
	for _, tx := range transactions {

		transactionWithAssetPayload, err := utils.ParseAssetTransaction(tx)
		if err != nil {
			if err == utils.ErrNotValidTransaction {
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
func createIdentityMap(txs []*utils.TransactionWithAssetPayload) (map[string]map[string][]string, error) {

	// Define the map structure: map[identity]map[assetId][]transactionId
	identityMap := make(map[string]map[string][]string)

	for _, tx := range txs {

		transactionData, err := utils.FindTransactionIdParticipantsAndCurrency(*tx)
		if err != nil {
			if err == utils.ErrNotValidTransaction {
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
