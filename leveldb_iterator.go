package gethdatalayer

import (
	"encoding/binary"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type levelDbStore struct {
	db *leveldb.DB
}

func NewLevelDBStore(path string) (*levelDbStore, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to load leveldb: %v", err)
	}
	store := &levelDbStore{
		db: db,
	}
	return store, nil
}

func (l *levelDbStore) Iterator(num int) Iterator {
	firstNum := uint64(0)
	if num >= 0 {
		firstNum = uint64(num)
	}

	iter := &levelDbIterator{
		db:  l.db,
		num: firstNum,
	}
	return iter
}

type levelDbIterator struct {
	db    *leveldb.DB
	num   uint64
	block *Block
}

func (l *levelDbIterator) decodeBlock(num uint64) (*Block, error) {
	// find the canonical chain for 'num' to resolve
	// the hash
	hashB, err := l.db.Get(headerHashKey(l.num), nil)
	if err != nil {
		return nil, err
	}
	if len(hashB) != 32 {
		return nil, fmt.Errorf("incorrect hash length: %d", len(hashB))
	}

	// header
	headerRaw, err := l.db.Get(headerKey(l.num, hashB), nil)
	if err != nil {
		return nil, err
	}
	header := new(Header)
	if err := header.UnmarshalRLP(headerRaw); err != nil {
		return nil, fmt.Errorf("failed to decode header: %v", err)
	}

	// body
	bodyRaw, err := l.db.Get(blockBodyKey(num, hashB), nil)
	if err != nil {
		return nil, err
	}
	body := new(Body)
	if err := body.UnmarshalRLP(bodyRaw); err != nil {
		return nil, fmt.Errorf("failed to decode body: %v", err)
	}

	// receipts
	receiptsRaw, err := l.db.Get(blockReceiptsKey(num, hashB), nil)
	if err != nil {
		return nil, err
	}
	receipts := new(Receipts)
	if err := receipts.UnmarshalRLP(receiptsRaw); err != nil {
		return nil, fmt.Errorf("failed to decode receipts: %v", err)
	}

	if len(body.Transactions) != len(*receipts) {
		return nil, fmt.Errorf("incorrect match")
	}

	resp := &Block{
		Number:   num,
		Header:   header,
		Body:     body,
		Receipts: *receipts,
	}
	return resp, nil
}

func (l *levelDbIterator) Next() bool {
	block, err := l.decodeBlock(l.num)
	if err != nil {
		return false
	}

	l.block = block
	l.num++

	return true
}

func (l *levelDbIterator) Value() (*Block, error) {
	return l.block, nil
}

var (
	// headerPrefix + num (uint64 big endian) + hash -> header
	headerPrefix = []byte("h")

	// blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockBodyPrefix = []byte("b")

	// blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts
	blockReceiptsPrefix = []byte("r")

	// headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerHashSuffix = []byte("n")
)

func marshalUint64(num uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, num)
	return buf
}

func unmarshalUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func decodeKey(b []byte) (uint64, []byte) {
	return unmarshalUint64(b[1:9]), b[9:]
}

func headerKey(number uint64, hash []byte) []byte {
	return append(append(headerPrefix, marshalUint64(number)...), hash...)
}

func blockBodyKey(number uint64, hash []byte) []byte {
	return append(append(blockBodyPrefix, marshalUint64(number)...), hash[:]...)
}

func blockReceiptsKey(number uint64, hash []byte) []byte {
	return append(append(blockReceiptsPrefix, marshalUint64(number)...), hash[:]...)
}

func headerHashKey(number uint64) []byte {
	return append(append(headerPrefix, marshalUint64(number)...), headerHashSuffix...)
}
