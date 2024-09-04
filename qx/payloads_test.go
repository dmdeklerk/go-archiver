package qx_test

import (
	"testing"

	"encoding/hex"

	"github.com/qubic/go-archiver/qx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseAssetTransfer(t *testing.T) {
	rawPayload, err := hex.DecodeString("0830bb63bf7d5e164ac8cbd38680630ff7670a1ebf39f7210b40bcdca253d05f2fc8a29a7a4a6969cd3a57244c48c5027b5b6940ed11f739d052b40e9dd357fa43464200000000007ea8450000000000")
	require.NoError(t, err, "Decoding should not produce an error")

	var transferAssetOwnershipAndPossessionInput qx.QxTransferAssetOwnershipAndPossessionInput
	err = transferAssetOwnershipAndPossessionInput.UnmarshalBinary(rawPayload)
	require.NoError(t, err, "Unmarshal should not produce an error")

	transferPayload, err := transferAssetOwnershipAndPossessionInput.GetAssetTransfer()
	require.NoError(t, err, "Unmarshal should not produce an error")

	assert.NotNil(t, transferPayload, "The transfer payload should not be nil")

	assert.Equal(t, transferPayload.Issuer.String(), "CFBMEMZOIDEXQAUXYYSZIURADQLAPWPMNJXQSNVQZAHYVOPYUKKJBJUCTVJL")
	assert.Equal(t, transferPayload.DestId.String(), "VFWIEWBYSIMPBDHBXYFJVMLGKCCABZKRYFLQJVZTRBUOYSUHOODPVAHHKXPJ")
	assert.Equal(t, transferPayload.AssetName, "CFB")
	assert.Equal(t, transferPayload.Amount, int64(4565118))
}
