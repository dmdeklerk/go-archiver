package rpc

import (
	"context"
	"log"

	"github.com/dmdeklerk/go-archiver/heatwallet/proto"
	"github.com/dmdeklerk/go-archiver/heatwallet/utils"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Coding Agent instructions START
// This is the implementation for the rpc endpoint GetIdentityAssetTransactionsV2 as defined in heatwallet.proto
// Coding Agent instructions END

func (s *HeatServer) GetIdentityAssetTransactionsV2(ctx context.Context, req *proto.GetIdentityAssetTransactionsRequestV2) (*proto.GetIdentityAssetTransactionsResponseV2, error) {
	assetId := req.AssetIssuer + req.AssetName
	txData, nextEndTick, nextTxnIndexStart, currentTick, err := s.Store.GetIdetityAssetTransactionsFromEnd(ctx, req.IncludeFailedTransactions, req.Identity, assetId, req.GetEndTick(), int(req.GetTxnIndexStart()), int(req.GetMaxTransactions()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting asset transactions: %v", err)
	}

	var transactions []*proto.AssetTransaction
	for _, identityAssetTransaction := range txData {
		if identityAssetTransaction.Transaction.InputType == 0 {
			transactions = append(transactions, &proto.AssetTransaction{
				TransactionType: proto.AssetTransactionType_NATIVE_TRANSFER,
				Transaction:     identityAssetTransaction.Transaction,
				Timestamp:       identityAssetTransaction.Timestamp,
				MoneyFlew:       identityAssetTransaction.MoneyFlew,
			})
		} else {
			tx, err := utils.ProtoToTx(identityAssetTransaction.Transaction)
			if err != nil {
				return nil, errors.Wrap(err, "convert proto to types")
			}

			assetTransaction, err := utils.ParseAssetTransaction(tx)
			if err != nil {
				if err == utils.ErrNotValidTransaction {
					log.Printf("invalid transaction in database with id %s", identityAssetTransaction.Transaction.TxId)
					continue
				}
				return nil, errors.Wrap(err, "parse asset transaction")
			}

			if assetTransaction.QxTransferAssetPayload != nil {
				transactions = append(transactions, &proto.AssetTransaction{
					TransactionType: proto.AssetTransactionType_QX_ASSET_TRANSFER,
					Transaction:     identityAssetTransaction.Transaction,
					Timestamp:       identityAssetTransaction.Timestamp,
					MoneyFlew:       identityAssetTransaction.MoneyFlew,
					Payload: &proto.AssetTransaction_QxAssetTransfer{
						QxAssetTransfer: &proto.AssetTransactionQxTransfer{
							AssetIssuer: assetTransaction.QxTransferAssetPayload.Issuer.String(),
							AssetName:   assetTransaction.QxTransferAssetPayload.AssetName,
							SourceId:    identityAssetTransaction.Transaction.SourceId,
							DestId:      assetTransaction.QxTransferAssetPayload.DestId.String(),
							Amount:      uint64(assetTransaction.QxTransferAssetPayload.Amount),
						},
					},
				})
			} else if assetTransaction.SendManyTransaction != nil {
				transactions = append(transactions, &proto.AssetTransaction{
					TransactionType: proto.AssetTransactionType_QUTIL_SEND_MANY,
					Transaction:     identityAssetTransaction.Transaction,
					Timestamp:       identityAssetTransaction.Timestamp,
					MoneyFlew:       identityAssetTransaction.MoneyFlew,
					Payload: &proto.AssetTransaction_QutilSendMany{
						QutilSendMany: assetTransaction.SendManyTransaction,
					},
				})
			}
		}
	}

	return &proto.GetIdentityAssetTransactionsResponseV2{
		CurrentTick:       currentTick,
		NextEndTick:       nextEndTick,
		NextTxnIndexStart: nextTxnIndexStart,
		Transactions:      transactions,
	}, nil
}
