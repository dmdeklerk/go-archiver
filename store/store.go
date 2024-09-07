package store

import (
	"context"
	"encoding/binary"
	"log"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/asset_transactions"
	"github.com/qubic/go-archiver/protobuff"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const maxTickNumber = ^uint64(0)

var ErrNotFound = errors.New("store resource not found")

type PebbleStore struct {
	db     *pebble.DB
	logger *zap.Logger
}

func NewPebbleStore(db *pebble.DB, logger *zap.Logger) *PebbleStore {
	return &PebbleStore{db: db, logger: logger}
}

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

func (s *PebbleStore) GetTickData(ctx context.Context, tickNumber uint32) (*protobuff.TickData, error) {
	key := tickDataKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tick data")
	}
	defer closer.Close()

	var td protobuff.TickData
	if err := proto.Unmarshal(value, &td); err != nil {
		return nil, errors.Wrap(err, "unmarshalling tick data to protobuff type")
	}

	return &td, err
}

func (s *PebbleStore) SetTickData(ctx context.Context, tickNumber uint32, td *protobuff.TickData) error {
	key := tickDataKey(tickNumber)
	serialized, err := proto.Marshal(td)
	if err != nil {
		return errors.Wrap(err, "serializing td proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting tick data")
	}

	return nil
}

func (s *PebbleStore) GetQuorumTickData(ctx context.Context, tickNumber uint32) (*protobuff.QuorumTickData, error) {
	key := quorumTickDataKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting quorum tick data")
	}
	defer closer.Close()

	var qtd protobuff.QuorumTickData
	if err := proto.Unmarshal(value, &qtd); err != nil {
		return nil, errors.Wrap(err, "unmarshalling quorum tick data to protobuf type")
	}

	return &qtd, err
}

func (s *PebbleStore) SetQuorumTickData(ctx context.Context, tickNumber uint32, qtd *protobuff.QuorumTickData) error {
	key := quorumTickDataKey(tickNumber)
	serialized, err := proto.Marshal(qtd)
	if err != nil {
		return errors.Wrap(err, "serializing qtd proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting quorum tick data")
	}

	return nil
}

func (s *PebbleStore) GetComputors(ctx context.Context, epoch uint32) (*protobuff.Computors, error) {
	key := computorsKey(epoch)

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting quorum tick data")
	}
	defer closer.Close()

	var computors protobuff.Computors
	if err := proto.Unmarshal(value, &computors); err != nil {
		return nil, errors.Wrap(err, "unmarshalling computors to protobuff type")
	}

	return &computors, nil
}

func (s *PebbleStore) SetComputors(ctx context.Context, epoch uint32, computors *protobuff.Computors) error {
	key := computorsKey(epoch)

	serialized, err := proto.Marshal(computors)
	if err != nil {
		return errors.Wrap(err, "serializing computors proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting computors")
	}

	return nil
}

func (s *PebbleStore) SetTransactions(ctx context.Context, txs []*protobuff.Transaction) error {
	batch := s.db.NewBatchWithSize(len(txs))
	defer batch.Close()

	for _, tx := range txs {
		key, err := tickTxKey(tx.TxId)
		if err != nil {
			return errors.Wrapf(err, "creating tx key for id: %s", tx.TxId)
		}

		serialized, err := proto.Marshal(tx)
		if err != nil {
			return errors.Wrap(err, "serializing tx proto")
		}

		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "getting tick data")
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}

func (s *PebbleStore) GetTickTransactions(ctx context.Context, tickNumber uint32) ([]*protobuff.Transaction, error) {
	td, err := s.GetTickData(ctx, tickNumber)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tick data")
	}

	txs := make([]*protobuff.Transaction, 0, len(td.TransactionIds))
	for _, txID := range td.TransactionIds {
		tx, err := s.GetTransaction(ctx, txID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, ErrNotFound
			}

			return nil, errors.Wrapf(err, "getting tx for id: %s", txID)
		}

		txs = append(txs, tx)
	}

	return txs, nil
}

func (s *PebbleStore) GetTickTransferTransactions(ctx context.Context, tickNumber uint32) ([]*protobuff.Transaction, error) {
	td, err := s.GetTickData(ctx, tickNumber)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tick data")
	}

	txs := make([]*protobuff.Transaction, 0, len(td.TransactionIds))
	for _, txID := range td.TransactionIds {
		tx, err := s.GetTransaction(ctx, txID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, ErrNotFound
			}

			return nil, errors.Wrapf(err, "getting tx for id: %s", txID)
		}
		if tx.Amount <= 0 {
			continue
		}

		txs = append(txs, tx)
	}

	return txs, nil
}

func (s *PebbleStore) GetTransaction(ctx context.Context, txID string) (*protobuff.Transaction, error) {
	key, err := tickTxKey(txID)
	if err != nil {
		return nil, errors.Wrap(err, "getting tx key")
	}

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting tx")
	}
	defer closer.Close()

	var tx protobuff.Transaction
	if err := proto.Unmarshal(value, &tx); err != nil {
		return nil, errors.Wrap(err, "unmarshalling tx to protobuff type")
	}

	return &tx, nil
}

func (s *PebbleStore) SetLastProcessedTick(ctx context.Context, lastProcessedTick *protobuff.ProcessedTick) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	key := lastProcessedTickKeyPerEpoch(lastProcessedTick.Epoch)
	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, lastProcessedTick.TickNumber)

	err := batch.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting last processed tick")
	}

	key = lastProcessedTickKey()
	serialized, err := proto.Marshal(lastProcessedTick)
	if err != nil {
		return errors.Wrap(err, "serializing skipped tick proto")
	}

	err = batch.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting last processed tick")
	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	ptie, err := s.getProcessedTickIntervalsPerEpoch(ctx, lastProcessedTick.Epoch)
	if err != nil {
		return errors.Wrap(err, "getting ptie")
	}

	if len(ptie.Intervals) == 0 {
		ptie = &protobuff.ProcessedTickIntervalsPerEpoch{Epoch: lastProcessedTick.Epoch, Intervals: []*protobuff.ProcessedTickInterval{{InitialProcessedTick: lastProcessedTick.TickNumber, LastProcessedTick: lastProcessedTick.TickNumber}}}
	} else {
		ptie.Intervals[len(ptie.Intervals)-1].LastProcessedTick = lastProcessedTick.TickNumber
	}

	err = s.SetProcessedTickIntervalPerEpoch(ctx, lastProcessedTick.Epoch, ptie)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *PebbleStore) GetLastProcessedTick(ctx context.Context) (*protobuff.ProcessedTick, error) {
	key := lastProcessedTickKey()
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting last processed tick")
	}
	defer closer.Close()

	// handle old data format, to be removed in the future
	if len(value) == 8 {
		tickNumber := uint32(binary.LittleEndian.Uint64(value))
		ticksPerEpoch, err := s.GetLastProcessedTicksPerEpoch(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "getting last processed ticks per epoch")
		}
		var epoch uint32
		for e, tick := range ticksPerEpoch {
			if tick == tickNumber {
				epoch = e
				break
			}
		}
		return &protobuff.ProcessedTick{
			TickNumber: tickNumber,
			Epoch:      epoch,
		}, nil
	}

	var lpt protobuff.ProcessedTick
	if err := proto.Unmarshal(value, &lpt); err != nil {
		return nil, errors.Wrap(err, "unmarshalling lpt to protobuff type")
	}

	return &lpt, nil
}

func (s *PebbleStore) GetLastProcessedTicksPerEpoch(ctx context.Context) (map[uint32]uint32, error) {
	upperBound := append([]byte{LastProcessedTickPerEpoch}, []byte(strconv.FormatUint(maxTickNumber, 10))...)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{LastProcessedTickPerEpoch},
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	ticksPerEpoch := make(map[uint32]uint32)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()

		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting value from iter")
		}

		epochNumber := binary.BigEndian.Uint32(key[1:])
		tickNumber := binary.LittleEndian.Uint32(value)
		ticksPerEpoch[epochNumber] = tickNumber
	}

	return ticksPerEpoch, nil
}

func (s *PebbleStore) SetSkippedTicksInterval(ctx context.Context, skippedTick *protobuff.SkippedTicksInterval) error {
	newList := protobuff.SkippedTicksIntervalList{}
	current, err := s.GetSkippedTicksInterval(ctx)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return errors.Wrap(err, "getting skipped tick interval")
		}
	} else {
		newList.SkippedTicks = current.SkippedTicks
	}

	newList.SkippedTicks = append(newList.SkippedTicks, skippedTick)

	key := skippedTicksIntervalKey()
	serialized, err := proto.Marshal(&newList)
	if err != nil {
		return errors.Wrap(err, "serializing skipped tick proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting skipped tick interval")
	}

	return nil
}

func (s *PebbleStore) GetSkippedTicksInterval(ctx context.Context) (*protobuff.SkippedTicksIntervalList, error) {
	key := skippedTicksIntervalKey()
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting skipped tick interval")
	}
	defer closer.Close()

	var stil protobuff.SkippedTicksIntervalList
	if err := proto.Unmarshal(value, &stil); err != nil {
		return nil, errors.Wrap(err, "unmarshalling skipped tick interval to protobuff type")
	}

	return &stil, nil
}

func (s *PebbleStore) PutTransferTransactionsPerTick(ctx context.Context, identity string, tickNumber uint32, txs *protobuff.TransferTransactionsPerTick) error {
	key := identityTransferTransactionsPerTickKey(identity, tickNumber)

	serialized, err := proto.Marshal(txs)
	if err != nil {
		return errors.Wrap(err, "serializing tx proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting transfer tx")
	}

	return nil
}

func (s *PebbleStore) GetTransferTransactions(ctx context.Context, identity string, startTick, endTick uint64) ([]*protobuff.TransferTransactionsPerTick, error) {
	partialKey := identityTransferTransactions(identity)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: binary.BigEndian.AppendUint64(partialKey, startTick),
		UpperBound: binary.BigEndian.AppendUint64(partialKey, endTick+1),
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	transferTxs := make([]*protobuff.TransferTransactionsPerTick, 0)

	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting value from iter")
		}

		var perTick protobuff.TransferTransactionsPerTick

		err = proto.Unmarshal(value, &perTick)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling transfer tx per tick to protobuff type")
		}

		transferTxs = append(transferTxs, &perTick)
	}

	return transferTxs, nil
}

func (s *PebbleStore) PutChainDigest(ctx context.Context, tickNumber uint32, digest []byte) error {
	key := chainDigestKey(tickNumber)

	err := s.db.Set(key, digest, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting chain digest")
	}

	return nil
}

func (s *PebbleStore) GetChainDigest(ctx context.Context, tickNumber uint32) ([]byte, error) {
	key := chainDigestKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting chain digest")
	}
	defer closer.Close()

	return value, nil
}

func (s *PebbleStore) PutStoreDigest(ctx context.Context, tickNumber uint32, digest []byte) error {
	key := storeDigestKey(tickNumber)

	err := s.db.Set(key, digest, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting chain digest")
	}

	return nil
}

func (s *PebbleStore) GetStoreDigest(ctx context.Context, tickNumber uint32) ([]byte, error) {
	key := storeDigestKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting chain digest")
	}
	defer closer.Close()

	return value, nil
}

func (s *PebbleStore) GetTickTransactionsStatus(ctx context.Context, tickNumber uint64) (*protobuff.TickTransactionsStatus, error) {
	key := tickTxStatusKey(tickNumber)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting transactions status")
	}
	defer closer.Close()

	var tts protobuff.TickTransactionsStatus
	if err := proto.Unmarshal(value, &tts); err != nil {
		return nil, errors.Wrap(err, "unmarshalling tick transactions status")
	}

	return &tts, err
}

func (s *PebbleStore) GetTransactionStatus(ctx context.Context, txID string) (*protobuff.TransactionStatus, error) {
	key := txStatusKey(txID)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}

		return nil, errors.Wrap(err, "getting transaction status")
	}
	defer closer.Close()

	var ts protobuff.TransactionStatus
	if err := proto.Unmarshal(value, &ts); err != nil {
		return nil, errors.Wrap(err, "unmarshalling transaction status")
	}

	return &ts, err
}

func (s *PebbleStore) SetTickTransactionsStatus(ctx context.Context, tickNumber uint64, tts *protobuff.TickTransactionsStatus) error {
	key := tickTxStatusKey(tickNumber)
	batch := s.db.NewBatchWithSize(len(tts.Transactions) + 1)
	defer batch.Close()

	serialized, err := proto.Marshal(tts)
	if err != nil {
		return errors.Wrap(err, "serializing tts proto")
	}

	err = batch.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting tts data")
	}

	for _, tx := range tts.Transactions {
		key := txStatusKey(tx.TxId)

		serialized, err := proto.Marshal(tx)
		if err != nil {
			return errors.Wrap(err, "serializing tx status proto")
		}

		err = batch.Set(key, serialized, nil)
		if err != nil {
			return errors.Wrap(err, "setting tx status data")
		}
	}

	err = batch.Commit(pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "committing batch")
	}

	return nil
}

func (s *PebbleStore) getProcessedTickIntervalsPerEpoch(ctx context.Context, epoch uint32) (*protobuff.ProcessedTickIntervalsPerEpoch, error) {
	key := processedTickIntervalsPerEpochKey(epoch)
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return &protobuff.ProcessedTickIntervalsPerEpoch{Intervals: make([]*protobuff.ProcessedTickInterval, 0), Epoch: epoch}, nil
		}

		return nil, errors.Wrap(err, "getting processed tick intervals per epoch from store")
	}
	defer closer.Close()

	var ptie protobuff.ProcessedTickIntervalsPerEpoch
	if err := proto.Unmarshal(value, &ptie); err != nil {
		return nil, errors.Wrap(err, "unmarshalling processed tick intervals per epoch")
	}

	return &ptie, nil
}

func (s *PebbleStore) SetProcessedTickIntervalPerEpoch(ctx context.Context, epoch uint32, ptie *protobuff.ProcessedTickIntervalsPerEpoch) error {
	key := processedTickIntervalsPerEpochKey(epoch)
	serialized, err := proto.Marshal(ptie)
	if err != nil {
		return errors.Wrap(err, "serializing ptie proto")
	}

	err = s.db.Set(key, serialized, pebble.Sync)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *PebbleStore) AppendProcessedTickInterval(ctx context.Context, epoch uint32, pti *protobuff.ProcessedTickInterval) error {
	existing, err := s.getProcessedTickIntervalsPerEpoch(ctx, epoch)
	if err != nil {
		return errors.Wrap(err, "getting existing processed tick intervals")
	}

	existing.Intervals = append(existing.Intervals, pti)

	err = s.SetProcessedTickIntervalPerEpoch(ctx, epoch, existing)
	if err != nil {
		return errors.Wrap(err, "setting ptie")
	}

	return nil
}

func (s *PebbleStore) GetProcessedTickIntervals(ctx context.Context) ([]*protobuff.ProcessedTickIntervalsPerEpoch, error) {
	upperBound := append([]byte{ProcessedTickIntervals}, []byte(strconv.FormatUint(maxTickNumber, 10))...)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{ProcessedTickIntervals},
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, errors.Wrap(err, "creating iter")
	}
	defer iter.Close()

	processedTickIntervals := make([]*protobuff.ProcessedTickIntervalsPerEpoch, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, errors.Wrap(err, "getting value from iter")
		}

		var ptie protobuff.ProcessedTickIntervalsPerEpoch
		err = proto.Unmarshal(value, &ptie)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshalling iter ptie")
		}
		processedTickIntervals = append(processedTickIntervals, &ptie)
	}

	return processedTickIntervals, nil
}

func (s *PebbleStore) SetEmptyTicksForEpoch(epoch uint32, emptyTicksCount uint32) error {
	key := emptyTicksPerEpochKey(epoch)

	value := make([]byte, 4)
	binary.LittleEndian.PutUint32(value, emptyTicksCount)

	err := s.db.Set(key, value, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "saving emptyTickCount for epoch %d", epoch)
	}
	return nil
}

func (s *PebbleStore) GetEmptyTicksForEpoch(epoch uint32) (uint32, error) {
	key := emptyTicksPerEpochKey(epoch)

	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, err
		}

		return 0, errors.Wrapf(err, "getting emptyTickCount for epoch %d", epoch)
	}
	defer closer.Close()

	emptyTicksCount := binary.LittleEndian.Uint32(value)

	return emptyTicksCount, nil
}

func (s *PebbleStore) GetEmptyTicksForEpochs(epochs []uint32) (map[uint32]uint32, error) {

	emptyTickMap := make(map[uint32]uint32, len(epochs))

	for _, epoch := range epochs {
		emptyTicks, err := s.GetEmptyTicksForEpoch(epoch)
		if err != nil {
			if !errors.Is(err, pebble.ErrNotFound) {
				return nil, errors.Wrapf(err, "getting empty ticks for epoch %d", epoch)
			}
		}
		emptyTickMap[epoch] = emptyTicks
	}

	return emptyTickMap, nil

}

func (s *PebbleStore) DeleteEmptyTicksKeyForEpoch(epoch uint32) error {
	key := emptyTicksPerEpochKey(epoch)

	err := s.db.Delete(key, pebble.Sync)
	if err != nil {
		return errors.Wrapf(err, "deleting empty ticks key for epoch %d", epoch)
	}
	return nil
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

			tickData, err := s.GetTickData(ctx, tickNumber)
			if err != nil {
				return nil, 0, 0, 0, errors.Wrap(err, "getting tick data")
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
