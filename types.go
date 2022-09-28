package gethdatalayer

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"

	"github.com/umbracle/fastrlp"
)

type Block struct {
	Number   uint64
	Header   *Header
	Body     *Body
	Receipts Receipts
}

type hash [32]byte

func (h hash) String() string {
	return "0x" + hex.EncodeToString(h[:])
}

type address [20]byte

func (a address) String() string {
	return "0x" + hex.EncodeToString(a[:])
}

type Receipt struct {
	PostStateOrStatus []byte
	CumulativeGasUsed uint64
	Logs              []*Log
}

type Log struct {
	Address address
	Topics  []hash
	Data    []byte
}

type Receipts []*Receipt

func (r *Receipts) UnmarshalRLP(input []byte) error {
	return unmarshalRlp(r.UnmarshalRLPFrom, input)
}

func (r *Receipts) UnmarshalRLPFrom(p *fastrlp.Parser, v *fastrlp.Value) error {
	elems, err := v.GetElems()
	if err != nil {
		return err
	}
	for _, elem := range elems {
		rr := &Receipt{}
		if err := rr.UnmarshalRLPFrom(p, elem); err != nil {
			return err
		}
		(*r) = append(*r, rr)
	}
	return nil
}

func (r *Receipt) UnmarshalRLP(input []byte) error {
	return unmarshalRlp(r.UnmarshalRLPFrom, input)
}

// UnmarshalRLP unmarshals a Receipt in RLP format
func (r *Receipt) UnmarshalRLPFrom(p *fastrlp.Parser, v *fastrlp.Value) error {
	elems, err := v.GetElems()
	if err != nil {
		return err
	}
	if len(elems) != 3 {
		return fmt.Errorf("expected 3 elements")
	}

	// root or status
	buf, err := elems[0].Bytes()
	if err != nil {
		return err
	}
	r.PostStateOrStatus = buf

	// cumulativeGasUsed
	if r.CumulativeGasUsed, err = elems[1].GetUint64(); err != nil {
		return err
	}

	// logs
	logsElems, err := v.Get(2).GetElems()
	if err != nil {
		return err
	}
	for _, elem := range logsElems {
		log := &Log{}
		if err := log.UnmarshalRLPFrom(p, elem); err != nil {
			return err
		}
		r.Logs = append(r.Logs, log)
	}
	return nil
}

func (l *Log) UnmarshalRLPFrom(p *fastrlp.Parser, v *fastrlp.Value) error {
	elems, err := v.GetElems()
	if err != nil {
		return err
	}
	if len(elems) != 3 {
		return fmt.Errorf("bad elems")
	}

	// address
	if err := elems[0].GetAddr(l.Address[:]); err != nil {
		return err
	}
	// topics
	topicElems, err := elems[1].GetElems()
	if err != nil {
		return err
	}
	l.Topics = make([]hash, len(topicElems))
	for indx, topic := range topicElems {
		if err := topic.GetHash(l.Topics[indx][:]); err != nil {
			return err
		}
	}
	// data
	if l.Data, err = elems[2].GetBytes(l.Data[:0]); err != nil {
		return err
	}
	return nil
}

type unmarshalRLPFunc func(p *fastrlp.Parser, v *fastrlp.Value) error

func unmarshalRlp(obj unmarshalRLPFunc, input []byte) error {
	pr := &fastrlp.Parser{}

	v, err := pr.Parse(input)
	if err != nil {
		return err
	}
	if err := obj(pr, v); err != nil {
		return err
	}
	return nil
}

func exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

type Body struct {
	Transactions []*Transaction
	Uncles       []*Header
}

func (b *Body) UnmarshalRLP(input []byte) error {
	return unmarshalRlp(b.UnmarshalRLPFrom, input)
}

func (b *Body) UnmarshalRLPFrom(p *fastrlp.Parser, v *fastrlp.Value) error {
	tuple, err := v.GetElems()
	if err != nil {
		return err
	}
	if len(tuple) != 2 {
		return fmt.Errorf("not enough elements to decode header, expected 15 but found %d", len(tuple))
	}

	// transactions
	txns, err := tuple[0].GetElems()
	if err != nil {
		return err
	}
	for _, txn := range txns {
		bTxn := &Transaction{}
		if err := bTxn.UnmarshalRLPFrom(p, txn); err != nil {
			return err
		}
		b.Transactions = append(b.Transactions, bTxn)
	}

	// uncles
	uncles, err := tuple[1].GetElems()
	if err != nil {
		return err
	}
	for _, uncle := range uncles {
		bUncle := &Header{}
		if err := bUncle.UnmarshalRLPFrom(p, uncle); err != nil {
			return err
		}
		b.Uncles = append(b.Uncles, bUncle)
	}

	return nil
}

type Transaction struct {
	Hash     hash
	Nonce    uint64
	GasPrice *big.Int
	Gas      uint64
	To       *address
	Value    *big.Int
	Input    []byte
	V        byte
	R        []byte
	S        []byte
}

func (t *Transaction) UnmarshalRLP(input []byte) error {
	return unmarshalRlp(t.UnmarshalRLPFrom, input)
}

// UnmarshalRLP unmarshals a Transaction in RLP format
func (t *Transaction) UnmarshalRLPFrom(p *fastrlp.Parser, v *fastrlp.Value) error {
	elems, err := v.GetElems()
	if err != nil {
		return err
	}
	if num := len(elems); num != 9 {
		return fmt.Errorf("not enough elements to decode transaction, expected 9 but found %d", num)
	}

	p.Hash(t.Hash[:0], v)

	// nonce
	if t.Nonce, err = elems[0].GetUint64(); err != nil {
		return err
	}
	// gasPrice
	t.GasPrice = new(big.Int)
	if err := elems[1].GetBigInt(t.GasPrice); err != nil {
		return err
	}
	// gas
	if t.Gas, err = elems[2].GetUint64(); err != nil {
		return err
	}
	// to
	vv, err := v.Get(3).Bytes()
	if err != nil {
		return err
	}
	if len(vv) == 20 {
		// address
		var addr address
		copy(addr[:], vv)
		t.To = &addr
	} else {
		// reset To
		t.To = nil
	}
	// value
	t.Value = new(big.Int)
	if err := elems[4].GetBigInt(t.Value); err != nil {
		return err
	}
	// input
	if t.Input, err = elems[5].GetBytes(t.Input[:0]); err != nil {
		return err
	}
	// v
	vv, err = v.Get(6).Bytes()
	if err != nil {
		return err
	}
	if len(vv) != 1 {
		t.V = 0x0
	} else {
		t.V = byte(vv[0])
	}
	// R
	if t.R, err = elems[7].GetBytes(t.R[:0]); err != nil {
		return err
	}
	// S
	if t.S, err = elems[8].GetBytes(t.S[:0]); err != nil {
		return err
	}
	return nil
}

type Header struct {
	Hash         hash
	ParentHash   hash
	Sha3Uncles   hash
	Miner        address
	StateRoot    hash
	TxRoot       hash
	ReceiptsRoot hash
	LogsBloom    [32]byte
	Difficulty   uint64
	Number       uint64
	GasLimit     uint64
	GasUsed      uint64
	Timestamp    uint64
	ExtraData    []byte
	MixHash      hash
	Nonce        [8]byte
}

func (h *Header) UnmarshalRLP(input []byte) error {
	return unmarshalRlp(h.UnmarshalRLPFrom, input)
}

func (h *Header) UnmarshalRLPFrom(p *fastrlp.Parser, v *fastrlp.Value) error {
	elems, err := v.GetElems()
	if err != nil {
		return err
	}
	if num := len(elems); num != 15 {
		return fmt.Errorf("not enough elements to decode header, expected 15 but found %d", num)
	}

	p.Hash(h.Hash[:0], v)

	// parentHash
	if err = elems[0].GetHash(h.ParentHash[:]); err != nil {
		return err
	}
	// sha3uncles
	if err = elems[1].GetHash(h.Sha3Uncles[:]); err != nil {
		return err
	}
	// miner
	if err = elems[2].GetAddr(h.Miner[:]); err != nil {
		return err
	}
	// stateroot
	if err = elems[3].GetHash(h.StateRoot[:]); err != nil {
		return err
	}
	// txroot
	if err = elems[4].GetHash(h.TxRoot[:]); err != nil {
		return err
	}
	// receiptroot
	if err = elems[5].GetHash(h.ReceiptsRoot[:]); err != nil {
		return err
	}
	// logsBloom
	if _, err = elems[6].GetBytes(h.LogsBloom[:0], 256); err != nil {
		return err
	}
	// difficulty
	if h.Difficulty, err = elems[7].GetUint64(); err != nil {
		return err
	}
	// number
	if h.Number, err = elems[8].GetUint64(); err != nil {
		return err
	}
	// gasLimit
	if h.GasLimit, err = elems[9].GetUint64(); err != nil {
		return err
	}
	// gasused
	if h.GasUsed, err = elems[10].GetUint64(); err != nil {
		return err
	}
	// timestamp
	if h.Timestamp, err = elems[11].GetUint64(); err != nil {
		return err
	}
	// extraData
	if h.ExtraData, err = elems[12].GetBytes(h.ExtraData[:0]); err != nil {
		return err
	}
	// mixHash
	if err = elems[13].GetHash(h.MixHash[:0]); err != nil {
		return err
	}
	// nonce
	nonce, err := elems[14].GetUint64()
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(h.Nonce[:], nonce)

	return err
}
