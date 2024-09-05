package tx

import (
	"encoding/hex"

	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/protobuff"
	"github.com/qubic/go-node-connector/types"
)

func qubicToProto(txs types.Transactions) ([]*protobuff.Transaction, error) {
	protoTxs := make([]*protobuff.Transaction, len(txs))
	for i, tx := range txs {
		txProto, err := txToProto(tx)
		if err != nil {
			return nil, errors.Wrapf(err, "converting tx to proto")
		}
		protoTxs[i] = txProto
	}

	return protoTxs, nil
}

func txToProto(tx types.Transaction) (*protobuff.Transaction, error) {
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

	return &protobuff.Transaction{
		SourceId:     sourceID.String(),
		DestId:       destID.String(),
		Amount:       tx.Amount,
		TickNumber:   tx.Tick,
		InputType:    uint32(tx.InputType),
		InputSize:    uint32(tx.InputSize),
		InputHex:     hex.EncodeToString(tx.Input[:]),
		SignatureHex: hex.EncodeToString(tx.Signature[:]),
		TxId:         txID.String(),
	}, nil
}

func ProtoToQubic(protoTxs []*protobuff.Transaction) (types.Transactions, error) {
	txs := make(types.Transactions, len(protoTxs))
	for i, protoTx := range protoTxs {
		tx, err := protoToTx(protoTx)
		if err != nil {
			return nil, errors.Wrapf(err, "converting proto tx to qubic tx")
		}
		txs[i] = tx
	}

	return txs, nil
}

func protoToTx(protoTx *protobuff.Transaction) (types.Transaction, error) {
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
