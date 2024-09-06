package asset_transactions

import (
	"encoding/hex"
	"log"

	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/go-archiver/qx"
	"github.com/qubic/go-node-connector/types"
)

var ErrNotValidTransaction = errors.New("not a valid transaction")

var NATIVE_QUBIC_ASSET_ISSUER = "0"
var NATIVE_QUBIC_ASSET_NAME = "0"
var SMART_CONTRACT_QX = "BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID"
var SMART_CONTRACT_QUTIL = "EAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAVWRF"

// Asset Transactions - Transactions related to an identity and grouped by currency
// Examples are:
//		- Qubic Transfers (normal or SendMany)
//		- Qx Asset Transfers
//		- Qx Asset Issuance
//		- Qx Placing/Cancelling Buy/Sell orders
//		- QEarn Transactions

type TransactionIdParticipantsAndCurrency struct {
	TxId       string
	Currency   TransactionCurrency
	Identities []string
}

type TransactionCurrency struct {
	AssetIssuer string
	AssetName   string
}

type LeanTransaction struct {
	SourceId  string
	DestId    string
	TxId      string
	InputType uint32
	Input     []byte
}

type TransactionWithAssetPayload struct {
	Transaction LeanTransaction

	// Transaction has an Asset Transfer payload or nil
	QxTransferAssetPayload *qx.QxTransferAssetPayload

	// Transaction has a Send Many payload
	SendManyTransaction *protobuff.SendManyTransaction

	// Add other payload types here ...
}

// Parses the payload based on the input type, returns a struct containing
// the transaction and the parsed payload
//
// #define QX_ISSUE_ASSET 1
// #define QX_TRANSFER_SHARE 2
// #define QX_PLACEHOLDER0 3
// #define QX_PLACEHOLDER1 4
// #define QX_ADD_ASK_ORDER 5
// #define QX_ADD_BID_ORDER 6
// #define QX_REMOVE_ASK_ORDER 7
// #define QX_REMOVE_BID_ORDER 8
func ParseAssetTransaction(tx types.Transaction) (*TransactionWithAssetPayload, error) {
	transaction, err := txToLeanTransaction(tx)
	if err != nil {
		return nil, errors.Wrap(err, "converting to proto")
	}

	if tx.InputType == 0 {
		return &TransactionWithAssetPayload{
			Transaction: *transaction,
		}, nil
	} else if transaction.DestId == SMART_CONTRACT_QUTIL {
		switch tx.InputType {
		// send many
		case 1:
			{
				if transaction.DestId != types.QutilAddress {
					log.Printf("sendmany transaction not send to qutil sc")
					return nil, ErrNotValidTransaction
				}

				var sendManyPayload types.SendManyTransferPayload
				err = sendManyPayload.UnmarshallBinary(tx.Input)
				if err != nil {
					log.Printf("failed to unmarshall payload data")
					return nil, ErrNotValidTransaction
				}

				sendManyTransfers := make([]*protobuff.SendManyTransfer, 0)

				transfers, err := sendManyPayload.GetTransfers()
				if err != nil {
					log.Printf("getting send many transfers")
					return nil, ErrNotValidTransaction
				}

				for _, transfer := range transfers {
					sendManyTransfers = append(sendManyTransfers, &protobuff.SendManyTransfer{
						DestId: transfer.AddressID.String(),
						Amount: transfer.Amount,
					})
				}

				sendManyTransaction := &protobuff.SendManyTransaction{
					SourceId:     transaction.SourceId,
					Transfers:    sendManyTransfers,
					TotalAmount:  sendManyPayload.GetTotalAmount(),
					TickNumber:   tx.Tick,
					TxId:         transaction.TxId,
					SignatureHex: "",
				}

				log.Printf("Send Many sender %s", transaction.SourceId)

				return &TransactionWithAssetPayload{
					Transaction:         *transaction,
					SendManyTransaction: sendManyTransaction,
				}, nil
			}
		}

	} else if transaction.DestId == SMART_CONTRACT_QX {
		switch tx.InputType {
		// transfer asset share
		case 2:
			{
				var transferAssetOwnershipAndPossessionInput qx.QxTransferAssetOwnershipAndPossessionInput
				err := transferAssetOwnershipAndPossessionInput.UnmarshalBinary(tx.Input)
				if err != nil {
					log.Printf("failed to unmarshal transaction from input: %v", err)
					return nil, ErrNotValidTransaction
				}

				transferPayload, err := transferAssetOwnershipAndPossessionInput.GetAssetTransfer()
				if err != nil {
					log.Printf("failed to get asset transfer from input: %v", err)
					return nil, ErrNotValidTransaction
				}

				log.Printf("Qx Asset sender %s, issuer=%s, assetName=%s", transaction.SourceId, transferPayload.Issuer.String(), transferPayload.AssetName)

				return &TransactionWithAssetPayload{
					Transaction:            *transaction,
					QxTransferAssetPayload: transferPayload,
				}, nil
			}
		}
	}

	return nil, ErrNotValidTransaction
}

func FindTransactionIdParticipantsAndCurrency(tx TransactionWithAssetPayload) (*TransactionIdParticipantsAndCurrency, error) {
	participants, err := findTransactionParticipants(tx)
	if err != nil {
		if err == ErrNotValidTransaction {
			log.Printf("no participants for transaction %s", tx.Transaction.TxId)
			return nil, err
		}
		return nil, errors.Wrap(err, "reading participants")
	}

	currency, err := findTransactionCurrency(tx)
	if err != nil {
		if err == ErrNotValidTransaction {
			log.Printf("no currency for transaction %s", tx.Transaction.TxId)
			return nil, err
		}
		return nil, errors.Wrap(err, "reading currency")
	}

	return &TransactionIdParticipantsAndCurrency{
		TxId:       tx.Transaction.TxId,
		Currency:   *currency,
		Identities: participants,
	}, nil
}

// Returns an array of all identities involved in this transaction
func findTransactionParticipants(tx TransactionWithAssetPayload) ([]string, error) {
	var result []string
	seen := make(map[string]bool)

	if tx.Transaction.InputType == 0 {
		if !seen[tx.Transaction.SourceId] {
			result = append(result, tx.Transaction.SourceId)
			seen[tx.Transaction.SourceId] = true
		}
		if !seen[tx.Transaction.DestId] {
			result = append(result, tx.Transaction.DestId)
			seen[tx.Transaction.DestId] = true
		}
	} else if tx.Transaction.DestId == SMART_CONTRACT_QUTIL {
		switch tx.Transaction.InputType {
		// send many
		case 1:
			{
				if tx.SendManyTransaction == nil {
					return nil, errors.New("send many payload is nil")
				}
				if !seen[tx.SendManyTransaction.SourceId] {
					result = append(result, tx.SendManyTransaction.SourceId)
					seen[tx.SendManyTransaction.SourceId] = true
				}
				for _, transfer := range tx.SendManyTransaction.Transfers {
					if !seen[transfer.DestId] {
						result = append(result, transfer.DestId)
						seen[transfer.DestId] = true
					}
				}
			}
		}

	} else if tx.Transaction.DestId == SMART_CONTRACT_QX {
		switch tx.Transaction.InputType {
		// transfer asset share
		case 2:
			{
				if tx.QxTransferAssetPayload == nil {
					return nil, errors.New("qx transfer asset payload is nil")
				}
				if !seen[tx.Transaction.SourceId] {
					result = append(result, tx.Transaction.SourceId)
					seen[tx.Transaction.SourceId] = true
				}
				if !seen[tx.QxTransferAssetPayload.DestId.String()] {
					result = append(result, tx.QxTransferAssetPayload.DestId.String())
					seen[tx.QxTransferAssetPayload.DestId.String()] = true
				}
			}
		}
	}

	return result, nil
}

func findTransactionCurrency(tx TransactionWithAssetPayload) (*TransactionCurrency, error) {

	if tx.Transaction.InputType == 0 {
		return &TransactionCurrency{
			AssetIssuer: NATIVE_QUBIC_ASSET_ISSUER,
			AssetName:   NATIVE_QUBIC_ASSET_NAME,
		}, nil
	} else if tx.Transaction.DestId == SMART_CONTRACT_QUTIL {
		switch tx.Transaction.InputType {
		// send many
		case 1:
			{
				if tx.SendManyTransaction == nil {
					return nil, errors.New("send many payload is nil")
				}
				return &TransactionCurrency{
					AssetIssuer: NATIVE_QUBIC_ASSET_ISSUER,
					AssetName:   NATIVE_QUBIC_ASSET_NAME,
				}, nil
			}
		}

	} else if tx.Transaction.DestId == SMART_CONTRACT_QX {
		switch tx.Transaction.InputType {
		// transfer asset share
		case 2:
			{
				if tx.QxTransferAssetPayload == nil {
					return nil, errors.New("qx transfer asset payload is nil")
				}
				return &TransactionCurrency{
					AssetIssuer: tx.QxTransferAssetPayload.Issuer.String(),
					AssetName:   tx.QxTransferAssetPayload.AssetName,
				}, nil
			}
		}
	}

	return nil, ErrNotValidTransaction
}

func txToLeanTransaction(tx types.Transaction) (*LeanTransaction, error) {
	digest, err := tx.Digest()
	if err != nil {
		return nil, errors.Wrap(err, "getting tx digest")
	}
	var txID types.Identity
	txID, err = txID.FromPubKey(digest, true)
	if err != nil {
		return nil, errors.Wrap(err, "getting tx id")
	}

	var sourceID types.Identity
	sourceID, err = sourceID.FromPubKey(tx.SourcePublicKey, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting source id")
	}

	var destID types.Identity
	destID, err = destID.FromPubKey(tx.DestinationPublicKey, false)
	if err != nil {
		return nil, errors.Wrap(err, "getting dest id")
	}

	return &LeanTransaction{
		SourceId:  sourceID.String(),
		DestId:    destID.String(),
		InputType: uint32(tx.InputType),
		TxId:      txID.String(),
		Input:     tx.Input,
	}, nil
}

func ProtoToQubic(protoTxs []*protobuff.Transaction) (types.Transactions, error) {
	txs := make(types.Transactions, len(protoTxs))
	for i, protoTx := range protoTxs {
		tx, err := ProtoToTx(protoTx)
		if err != nil {
			return nil, errors.Wrapf(err, "converting proto tx to qubic tx")
		}
		txs[i] = tx
	}

	return txs, nil
}

func ProtoToTx(protoTx *protobuff.Transaction) (types.Transaction, error) {
	var tx types.Transaction

	sourcePublicKey, err := identityToPublicKeyBytes(protoTx.SourceId)
	if err != nil {
		return tx, err
	}

	destinationPublicKey, err := identityToPublicKeyBytes(protoTx.DestId)
	if err != nil {
		return tx, err
	}

	inputBytes, err := hex.DecodeString(protoTx.InputHex)
	if err != nil {
		return tx, errors.Wrap(err, "decoding input hex")
	}

	signatureBytes, err := hex.DecodeString(protoTx.SignatureHex)
	if err != nil {
		return tx, errors.Wrap(err, "decoding signature hex")
	}
	if len(signatureBytes) != 64 {
		return tx, errors.New("signature must be exactly 64 bytes")
	}
	var signatureArray [64]byte
	copy(signatureArray[:], signatureBytes)

	// Assuming types.Transaction has a constructor or can be directly constructed
	tx = types.Transaction{
		SourcePublicKey:      sourcePublicKey,
		DestinationPublicKey: destinationPublicKey,
		Amount:               protoTx.Amount,
		Tick:                 protoTx.TickNumber,
		InputType:            uint16(protoTx.InputType),
		InputSize:            uint16(protoTx.InputSize),
		Input:                inputBytes,
		Signature:            signatureArray,
	}

	return tx, nil
}

func identityToPublicKeyBytes(identity string) ([32]byte, error) {
	var pubKeyBytes [32]byte
	id := types.Identity(identity)
	pubKeyBytes, err := id.ToPubKey(false)
	if err != nil {
		return pubKeyBytes, err
	}
	return pubKeyBytes, nil
}
