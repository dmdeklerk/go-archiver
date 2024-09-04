package qx

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
	"github.com/qubic/go-node-connector/types"
)

type QxTransferAssetOwnershipAndPossessionInput struct {
	Issuer               [32]byte
	NewOwnerAndPossessor [32]byte
	AssetName            uint64
	NumberOfUnits        int64
}

type QxTransferAssetPayload struct {
	Issuer    types.Identity
	DestId    types.Identity
	AssetName string
	Amount    int64
}

func (input *QxTransferAssetOwnershipAndPossessionInput) GetAssetTransfer() (*QxTransferAssetPayload, error) {

	var issuerId types.Identity
	issuerId, err := issuerId.FromPubKey(input.Issuer, false)
	if err != nil {
		return nil, errors.Wrapf(err, "getting issuer identity from bytes %v", input.Issuer)
	}
	var newOwnerAndPossessorId types.Identity
	newOwnerAndPossessorId, err = newOwnerAndPossessorId.FromPubKey(input.NewOwnerAndPossessor, false)
	if err != nil {
		return nil, errors.Wrapf(err, "getting newOwnerAndPossessor identity from bytes %v", input.NewOwnerAndPossessor)
	}

	payload := &QxTransferAssetPayload{
		Issuer:    issuerId,
		DestId:    newOwnerAndPossessorId,
		AssetName: Uint64ToString(input.AssetName),
		Amount:    input.NumberOfUnits,
	}

	return payload, nil
}

func (t *QxTransferAssetOwnershipAndPossessionInput) MarshalBinary() ([]byte, error) {
	buffer := new(bytes.Buffer)

	// Write Issuer to buffer
	if err := binary.Write(buffer, binary.LittleEndian, t.Issuer); err != nil {
		return nil, errors.Wrap(err, "writing issuer to buffer")
	}

	// Write NewOwnerAndPossessor to buffer
	if err := binary.Write(buffer, binary.LittleEndian, t.NewOwnerAndPossessor); err != nil {
		return nil, errors.Wrap(err, "writing new owner and possessor to buffer")
	}

	// Write AssetName to buffer
	if err := binary.Write(buffer, binary.LittleEndian, t.AssetName); err != nil {
		return nil, errors.Wrap(err, "writing asset name to buffer")
	}

	// Write NumberOfUnits to buffer
	if err := binary.Write(buffer, binary.LittleEndian, t.NumberOfUnits); err != nil {
		return nil, errors.Wrap(err, "writing number of units to buffer")
	}

	return buffer.Bytes(), nil
}

func (t *QxTransferAssetOwnershipAndPossessionInput) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)

	// Read Issuer from data
	if err := binary.Read(reader, binary.LittleEndian, &t.Issuer); err != nil {
		return errors.Wrap(err, "reading issuer")
	}

	// Read NewOwnerAndPossessor from data
	if err := binary.Read(reader, binary.LittleEndian, &t.NewOwnerAndPossessor); err != nil {
		return errors.Wrap(err, "reading new owner and possessor")
	}

	// Read AssetName from data
	if err := binary.Read(reader, binary.LittleEndian, &t.AssetName); err != nil {
		return errors.Wrap(err, "reading asset name")
	}

	// Read NumberOfUnits from data
	if err := binary.Read(reader, binary.LittleEndian, &t.NumberOfUnits); err != nil {
		return errors.Wrap(err, "reading number of units")
	}

	return nil
}

// Uint64ToString takes a uint64 value and converts it to a UTF-8 string
// assuming the uint64 holds bytes of a UTF-8 encoded string.
func Uint64ToString(value uint64) string {
	buf := make([]byte, 8)
	// Ensure you choose the correct endianness; adjust if necessary.
	binary.LittleEndian.PutUint64(buf, value)

	// Strip null bytes if they are considered padding
	// This step is critical if your data might include padded zero bytes that are not part of the original string
	buf = bytes.Trim(buf, "\x00")

	// Convert bytes to string, assuming UTF-8 encoding
	return string(buf)
}
