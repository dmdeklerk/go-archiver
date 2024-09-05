package tx

import (
	"context"
	"encoding/hex"
	"log"

	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/go-archiver/qx"
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

	err = StoreQxAssetTransfers(ctx, store, tickNumber, transactions)
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

// Qx Asset Transfers
type QxTransactionWithTransferAssetPayload struct {
	Transaction     types.Transaction
	TransferPayload qx.QxTransferAssetPayload
}

type QxAssetTransfer struct {
	AssetIssuer string
	AssetName   string
	SourceId    string
	DestId      string
	Amount      int64
	TxId        string
	TickNumber  uint32
}

func StoreQxAssetTransfers(ctx context.Context, store *store.PebbleStore, tickNumber uint32, transactions types.Transactions) error {
	assetTransferTransactions, err := removeNonQxAssetTransferTransactionsAndConvert(transactions)
	if err != nil {
		return errors.Wrap(err, "removing non asset transfer transactions")
	}
	assetTransfersPerIdentity, err := createQxAssetTransfersIdentityMap(assetTransferTransactions, tickNumber)
	if err != nil {
		return errors.Wrap(err, "grouping asset transfers per identity")
	}

	for id, transfersOuterLoop := range assetTransfersPerIdentity {

		assetTransfersPerIdentityPerAsset, err := createQxAssetTransferAssetIdMap(transfersOuterLoop)
		if err != nil {
			return errors.Wrap(err, "grouping asset transfers per asset id")
		}

		for assetId, transfers := range assetTransfersPerIdentityPerAsset {

			err = store.PutQxAssetTransfersPerTick(ctx, id, assetId, tickNumber, &protobuff.QxAssetTransfersPerTickDB{
				TickNumber: tickNumber,
				Transfers:  transfers,
			})
			if err != nil {
				return errors.Wrap(err, "storing asset transfers")
			}
		}
	}

	return nil
}

// Removes non Qx Asset Transfer transactions and converts to {QxTransactionWithTransferAssetPayload}
func removeNonQxAssetTransferTransactionsAndConvert(transactions []types.Transaction) ([]*QxTransactionWithTransferAssetPayload, error) {
	assetTransferTransactions := make([]*QxTransactionWithTransferAssetPayload, 0)
	for _, tx := range transactions {
		if tx.InputType != 2 {
			continue
		}

		var transferAssetOwnershipAndPossessionInput qx.QxTransferAssetOwnershipAndPossessionInput
		err := transferAssetOwnershipAndPossessionInput.UnmarshalBinary(tx.Input)
		if err != nil {
			log.Printf("failed to unmarshal transaction from input: %v", err)
			continue
		}

		transferPayload, err := transferAssetOwnershipAndPossessionInput.GetAssetTransfer()
		if err != nil {
			log.Printf("failed to get asset transfer from input: %v", err)
			continue
		}

		assetTransferTransactions = append(assetTransferTransactions, &QxTransactionWithTransferAssetPayload{
			Transaction:     tx,
			TransferPayload: *transferPayload,
		})
	}

	return assetTransferTransactions, nil
}

// Groups {QxTransactionWithTransferAssetPayload} per identity
func createQxAssetTransfersIdentityMap(txs []*QxTransactionWithTransferAssetPayload, tickNumber uint32) (map[string][]*QxAssetTransfer, error) {
	assetTransferTxsPerIdentity := make(map[string][]*QxAssetTransfer)
	for _, tx := range txs {

		digest, err := tx.Transaction.Digest()
		if err != nil {
			return nil, errors.Wrap(err, "getting tx digest")
		}
		var txID types.Identity
		txID, err = txID.FromPubKey(digest, true)
		if err != nil {
			return nil, errors.Wrap(err, "getting tx id")
		}

		var sourceIdentity types.Identity
		sourceIdentity, err = sourceIdentity.FromPubKey(tx.Transaction.SourcePublicKey, false)
		if err != nil {
			return nil, errors.Wrap(err, "getting source id")
		}

		var destId = tx.TransferPayload.DestId.String()
		var sourceId = sourceIdentity.String()

		_, ok := assetTransferTxsPerIdentity[destId]
		if !ok {
			assetTransferTxsPerIdentity[destId] = make([]*QxAssetTransfer, 0)
		}

		_, ok = assetTransferTxsPerIdentity[sourceId]
		if !ok {
			assetTransferTxsPerIdentity[sourceId] = make([]*QxAssetTransfer, 0)
		}

		assetTransfer := &QxAssetTransfer{
			AssetIssuer: tx.TransferPayload.Issuer.String(),
			AssetName:   tx.TransferPayload.AssetName,
			SourceId:    sourceId,
			DestId:      destId,
			Amount:      tx.TransferPayload.Amount,
			TxId:        txID.String(),
			TickNumber:  tickNumber,
		}

		assetTransferTxsPerIdentity[destId] = append(assetTransferTxsPerIdentity[destId], assetTransfer)
		assetTransferTxsPerIdentity[sourceId] = append(assetTransferTxsPerIdentity[sourceId], assetTransfer)
	}

	return assetTransferTxsPerIdentity, nil
}

// Groups {QxAssetTransfer} per AssetId (asset id is assetIssuer + assetName) and converts the result into {QxAssetTransferDB}
func createQxAssetTransferAssetIdMap(transfers []*QxAssetTransfer) (map[string][]*protobuff.QxAssetTransferDB, error) {
	assetTransferPerAssetId := make(map[string][]*protobuff.QxAssetTransferDB)
	for _, t := range transfers {
		assetId := t.AssetIssuer + t.AssetName

		_, ok := assetTransferPerAssetId[assetId]
		if !ok {
			assetTransferPerAssetId[assetId] = make([]*protobuff.QxAssetTransferDB, 0)
		}

		assetTransferPerAssetId[assetId] = append(assetTransferPerAssetId[assetId], &protobuff.QxAssetTransferDB{
			SourceId: t.SourceId,
			DestId:   t.DestId,
			Amount:   uint64(t.Amount),
			TxId:     t.TxId,
		})

	}
	return assetTransferPerAssetId, nil
}
