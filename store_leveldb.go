package gethdatalayer

import (
	"encoding/binary"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type LevelDbStore struct {
	db *leveldb.DB
}

func NewLevelDBStore(path string) (*LevelDbStore, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("failed to load leveldb: %v", err)
	}
	store := &LevelDbStore{
		db: db,
	}
	return store, nil
}

func (l *LevelDbStore) Iterator() Iterator {
	iter := &levelDbIterator{
		db: l,
	}
	iter.Seek(0)
	return iter
}

type levelDbIterator struct {
	db    *LevelDbStore
	num   uint64
	block *Block
}

type kvDb interface {
	Get([]byte) ([]byte, error)
}

func decodeBlock(db kvDb, num uint64) (*Block, error) {
	// find the canonical chain for 'num' to resolve
	// the hash
	hashB, err := db.Get(headerHashKey(num))
	if err != nil {
		return nil, err
	}
	if len(hashB) != 32 {
		return nil, fmt.Errorf("incorrect hash length: %d", len(hashB))
	}

	// header
	headerRaw, err := db.Get(headerKey(num, hashB))
	if err != nil {
		return nil, err
	}
	header := new(Header)
	if err := header.UnmarshalRLP(headerRaw); err != nil {
		return nil, fmt.Errorf("failed to decode header: %v", err)
	}

	// body
	bodyRaw, err := db.Get(blockBodyKey(num, hashB))
	if err != nil {
		return nil, err
	}
	body := new(Body)
	if err := body.UnmarshalRLP(bodyRaw); err != nil {
		return nil, fmt.Errorf("failed to decode body: %v", err)
	}

	// receipts
	receiptsRaw, err := db.Get(blockReceiptsKey(num, hashB))
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

func (l *LevelDbStore) Get(k []byte) ([]byte, error) {
	return l.db.Get(k, nil)
}

func (l *levelDbIterator) Seek(num uint64) {
	l.num = num
}

func (l *levelDbIterator) Next() bool {
	block, err := decodeBlock(l.db, l.num)
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

	headBlockKey = []byte("LastBlock")

	headerNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian)
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

func headerNumberKey(hash []byte) []byte {
	return append(headerNumberPrefix, hash...)
}
