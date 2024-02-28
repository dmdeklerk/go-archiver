package validator

import (
	"context"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/store"
	"github.com/qubic/go-archiver/validator/computors"
	"github.com/qubic/go-archiver/validator/quorum"
	"github.com/qubic/go-archiver/validator/tick"
	"github.com/qubic/go-archiver/validator/tx"
	qubic "github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
	"log"
	"time"
)

type Validator struct {
	qu    *qubic.Connection
	store *store.PebbleStore
}

func NewValidator(qu *qubic.Connection, store *store.PebbleStore) *Validator {
	return &Validator{qu: qu, store: store}
}

func (v *Validator) ValidateTick(ctx context.Context, tickNumber uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	quorumVotes, err := v.qu.GetQuorumVotes(ctx, uint32(tickNumber))
	if err != nil {
		return errors.Wrap(err, "getting quorum tick data")
	}

	if len(quorumVotes) == 0 {
		return errors.New("not quorum votes fetched")
	}

	//getting computors from storage, otherwise get it from a node
	epoch := quorumVotes[0].Epoch
	var comps types.Computors
	comps, err = computors.Get(ctx, v.store, uint64(epoch))
	if err != nil {
		if errors.Cause(err) != store.ErrNotFound {
			return errors.Wrap(err, "getting computors from store")
		}

		comps, err = v.qu.GetComputors(ctx)
		if err != nil {
			return errors.Wrap(err, "getting computors from qubic")
		}
	}

	err = computors.Validate(ctx, comps)
	if err != nil {
		return errors.Wrap(err, "validating comps")
	}
	//log.Println("Computors validated")
	err = computors.Store(ctx, v.store, comps)
	if err != nil {
		return errors.Wrap(err, "storing computors")
	}

	err = quorum.Validate(ctx, quorumVotes, comps)
	if err != nil {
		return errors.Wrap(err, "validating quorum")
	}

	// if the quorum votes have an empty tick data, it means that POTENTIALLY there is no tick data, it doesn't for
	// validation, but we may need to fetch it in the future ?!
	if quorumVotes[0].TxDigest == [32]byte{} {
		return nil
	}

	log.Println("Quorum validated")

	tickData, err := v.qu.GetTickData(ctx, uint32(tickNumber))
	if err != nil {
		return errors.Wrap(err, "getting tick data")
	}
	log.Println("Got tick data")

	err = tick.Validate(ctx, tickData, quorumVotes[0], comps)
	if err != nil {
		return errors.Wrap(err, "validating tick data")
	}

	log.Println("Tick data validated")

	transactions, err := v.qu.GetTickTransactions(ctx, uint32(tickNumber))
	if err != nil {
		return errors.Wrap(err, "getting tick transactions")
	}

	log.Printf("Validating %d transactions\n", len(transactions))

	validTxs, err := tx.Validate(ctx, transactions, tickData)
	if err != nil {
		return errors.Wrap(err, "validating transactions")
	}

	log.Printf("Validated %d transactions\n", len(validTxs))

	// proceed to storing tick information
	err = quorum.Store(ctx, v.store, quorumVotes)
	if err != nil {
		return errors.Wrap(err, "storing quorum votes")
	}

	log.Printf("Stored %d quorum votes\n", len(quorumVotes))

	err = tick.Store(ctx, v.store, tickData)
	if err != nil {
		return errors.Wrap(err, "storing tick data")
	}

	log.Printf("Stored tick data\n")

	err = tx.Store(ctx, v.store, validTxs)
	if err != nil {
		return errors.Wrap(err, "storing transactions")
	}

	log.Printf("Stored %d transactions\n", len(transactions))

	return nil
}

func getComputorsAndValidate(ctx context.Context, qu *qubic.Connection) (types.Computors, error) {
	comps, err := qu.GetComputors(ctx)
	if err != nil {
		return types.Computors{}, errors.Wrap(err, "getting comps")
	}

	err = computors.Validate(ctx, comps)
	if err != nil {
		return types.Computors{}, errors.Wrap(err, "validating comps")
	}
	log.Println("Computors validated")

	return comps, nil
}

//func (v *Validator) ValidateTickParallel(ctx context.Context, nodeIP string, tickNumber uint64) error {
//	comps, err := getComputorsAndValidate(ctx, v.qu)
//
//	var quorumVotes []types.QuorumTickData
//	var tickData types.TickData
//	var transactions []types.Transaction
//	var wg sync.WaitGroup
//	var errChan = make(chan error, 3)
//
//	wg.Add(3)
//
//	go func() {
//		defer wg.Done()
//		client, err := qubic.NewClient(context.Background(), nodeIP, "21841")
//		if err != nil {
//			errChan <- errors.Wrap(err, "creating qubic client")
//			return
//		}
//		defer client.Close()
//
//		log.Println("Fetching Quorum votes")
//		data, err := client.GetQuorumTickData(ctx, uint32(tickNumber))
//		if err != nil {
//			log.Println("err quorum")
//			errChan <- errors.Wrap(err, "getting quorum tick data")
//			return
//		}
//
//		quorumVotes = data.QuorumData
//		log.Println("Quorum Tick data fetched")
//	}()
//
//	go func() {
//		defer wg.Done()
//		client, err := qubic.NewClient(context.Background(), nodeIP, "21841")
//		if err != nil {
//			errChan <- errors.Wrap(err, "creating qubic client")
//			return
//		}
//		defer client.Close()
//
//		log.Println("Fetching tick data")
//		data, err := client.GetTickData(ctx, uint32(tickNumber))
//		if err != nil {
//			log.Println("err tick")
//			errChan <- errors.Wrap(err, "getting tick data")
//			return
//		}
//
//		tickData = data
//		log.Println("Tick data fetched")
//	}()
//
//	 go func() {
//		 defer wg.Done()
//		 client, err := qubic.NewClient(context.Background(), nodeIP, "21841")
//		 if err != nil {
//			 errChan <- errors.Wrap(err, "creating qubic client")
//			 return
//		 }
//		 defer client.Close()
//
//		 log.Println("Fetching tick transaction")
//		 data, err := client.GetTickTransactions(ctx, uint32(tickNumber))
//		 if err != nil {
//			 log.Println("err tx")
//			 errChan <- errors.Wrap(err, "getting tick transactions")
//			 return
//		 }
//
//		 transactions = data
//		 log.Println("Tick transaction data fetched")
//	 }()
//
//	go func() {
//		wg.Wait()
//		log.Println("Work done")
//		close(errChan) // Close channel after all goroutines report back
//	}()
//
//	for err := range errChan {
//		if err != nil {
//			fmt.Println("Error received:", err)
//			return err    // Exit the loop on the first error
//		}
//	}
//
//	err = quorum.Validate(ctx, quorumVotes, comps)
//	if err != nil {
//		return errors.Wrap(err, "validating quorum")
//	}
//
//	log.Println("Quorum validated")
//
//	err = tick.Validate(ctx, tickData, quorumVotes[0], comps)
//	if err != nil {
//		return errors.Wrap(err, "validating tick data")
//	}
//
//	log.Println("Tick validated")
//
//
//	err = tx.Validate(ctx, transactions, tickData)
//	if err != nil {
//		return errors.Wrap(err, "validating transactions")
//	}
//
//	return nil
//}
