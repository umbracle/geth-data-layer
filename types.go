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

type TransactionType int

const (
	TransactionLegacy TransactionType = 0
	// eip-2930
	TransactionAccessList TransactionType = 1
	// eip-1559
	TransactionDynamicFee TransactionType = 2
)

type Transaction struct {
	Type TransactionType

	// legacy values
	Hash     hash
	From     address
	To       *address
	Input    []byte
	GasPrice uint64
	Gas      uint64
	Value    *big.Int
	Nonce    uint64
	V        []byte
	R        []byte
	S        []byte

	// eip-2930 values
	ChainID    *big.Int
	AccessList AccessList

	// eip-1559 values
	MaxPriorityFeePerGas *big.Int
	MaxFeePerGas         *big.Int
}

type AccessEntry struct {
	Address address
	Storage []hash
}

type AccessList []AccessEntry

func (t *Transaction) UnmarshalRLP(buf []byte) error {
	return unmarshalRlp(t.UnmarshalRLPFrom, buf)
}

func (t *Transaction) UnmarshalRLPFrom(p *fastrlp.Parser, v *fastrlp.Value) error {
	keccak := fastrlp.NewKeccak256()

	if v.Type() == fastrlp.TypeBytes {
		// typed transaction
		buf, err := v.Bytes()
		if err != nil {
			return err
		}

		switch typ := buf[0]; typ {
		case 1:
			t.Type = TransactionAccessList
		case 2:
			t.Type = TransactionDynamicFee
		default:
			return fmt.Errorf("type byte %d not found", typ)
		}
		buf = buf[1:]

		pp := fastrlp.Parser{}
		if v, err = pp.Parse(buf); err != nil {
			return err
		}

		keccak.Write([]byte{byte(t.Type)})
		keccak.Write(pp.Raw(v))
	} else {
		keccak.Write(p.Raw(v))
	}

	keccak.Sum(t.Hash[:0])

	elems, err := v.GetElems()
	if err != nil {
		return err
	}

	getElem := func() *fastrlp.Value {
		v := elems[0]
		elems = elems[1:]
		return v
	}

	var num int
	switch t.Type {
	case TransactionLegacy:
		num = 9
	case TransactionAccessList:
		// legacy + chain id + access list
		num = 11
	case TransactionDynamicFee:
		// access list txn + gas fee 1 + gas fee 2 - gas price
		num = 12
	default:
		return fmt.Errorf("transaction type %d not found", t.Type)
	}
	if numElems := len(elems); numElems != num {
		return fmt.Errorf("not enough elements to decode transaction, expected %d but found %d", num, numElems)
	}

	if t.Type != 0 {
		t.ChainID = new(big.Int)
		if err := getElem().GetBigInt(t.ChainID); err != nil {
			return err
		}
	}

	// nonce
	if t.Nonce, err = getElem().GetUint64(); err != nil {
		return err
	}

	if t.Type == TransactionDynamicFee {
		// dynamic fee uses
		t.MaxPriorityFeePerGas = new(big.Int)
		if err := getElem().GetBigInt(t.MaxPriorityFeePerGas); err != nil {
			return err
		}
		t.MaxFeePerGas = new(big.Int)
		if err := getElem().GetBigInt(t.MaxFeePerGas); err != nil {
			return err
		}
	} else {
		// legacy and access type use gas price
		if t.GasPrice, err = getElem().GetUint64(); err != nil {
			return err
		}
	}

	// gas
	if t.Gas, err = getElem().GetUint64(); err != nil {
		return err
	}
	// to
	vv, _ := getElem().Bytes()
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
	if err := getElem().GetBigInt(t.Value); err != nil {
		return err
	}
	// input
	if t.Input, err = getElem().GetBytes(t.Input[:0]); err != nil {
		return err
	}

	if t.Type != 0 {
		if err := t.AccessList.UnmarshalRLPWith(getElem()); err != nil {
			return err
		}
	}

	// V
	if t.V, err = getElem().GetBytes(t.V); err != nil {
		return err
	}
	// R
	if t.R, err = getElem().GetBytes(t.R); err != nil {
		return err
	}
	// S
	if t.S, err = getElem().GetBytes(t.S); err != nil {
		return err
	}

	return nil
}

func (a *AccessList) UnmarshalRLPWith(v *fastrlp.Value) error {
	if v.Type() == fastrlp.TypeArrayNull {
		// empty
		return nil
	}

	elems, err := v.GetElems()
	if err != nil {
		return err
	}
	for _, elem := range elems {
		entry := AccessEntry{}

		acctElems, err := elem.GetElems()
		if err != nil {
			return err
		}
		if len(acctElems) != 2 {
			return fmt.Errorf("two elems expected but %d found", len(acctElems))
		}

		// decode 'address'
		if err = acctElems[0].GetAddr(entry.Address[:]); err != nil {
			return err
		}

		// decode 'storage'
		if acctElems[1].Type() != fastrlp.TypeArrayNull {
			storageElems, err := acctElems[1].GetElems()
			if err != nil {
				return err
			}

			entry.Storage = make([]hash, len(storageElems))
			for indx, storage := range storageElems {
				// decode storage
				if err = storage.GetHash(entry.Storage[indx][:]); err != nil {
					return err
				}
			}
		}
		(*a) = append((*a), entry)
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
	BaseFee      *big.Int
}

func (h *Header) UnmarshalRLP(input []byte) error {
	return unmarshalRlp(h.UnmarshalRLPFrom, input)
}

func (h *Header) UnmarshalRLPFrom(p *fastrlp.Parser, v *fastrlp.Value) error {
	elems, err := v.GetElems()
	if err != nil {
		return err
	}
	num := len(elems)
	if num != 15 && num != 16 {
		return fmt.Errorf("not enough elements to decode header, expected 15 or 16 but found %d", num)
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

	if num == 16 {
		// base fee
		h.BaseFee = new(big.Int)
		if err := elems[15].GetBigInt(h.BaseFee); err != nil {
			return err
		}
	}

	return err
}
