package gethdatalayer

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/golang/snappy"
)

var _ Iterator = &ancientIterator{}

type ancientStore struct {
	receipts *ancientTable
	headers  *ancientTable
	bodies   *ancientTable
}

func NewAncientStore(path string) (*ancientStore, error) {
	receiptsTable, err := newAncientTable(path, "receipts")
	if err != nil {
		return nil, err
	}
	headerTable, err := newAncientTable(path, "headers")
	if err != nil {
		return nil, err
	}
	bodiesTable, err := newAncientTable(path, "bodies")
	if err != nil {
		return nil, err
	}

	// all the tables should have the same number of items
	if headerTable.numItems != bodiesTable.numItems {
		return nil, fmt.Errorf("header and bodies table do not have same num of items")
	}
	if headerTable.numItems != receiptsTable.numItems {
		return nil, fmt.Errorf("header and receipts table do not have same num of items")
	}

	store := &ancientStore{
		receipts: receiptsTable,
		headers:  headerTable,
		bodies:   bodiesTable,
	}
	return store, nil
}

func (a *ancientStore) LastNum() uint64 {
	return a.headers.numItems
}

func (a *ancientStore) Iterator(num int) Iterator {
	iter := &ancientIterator{
		rIter: a.receipts.Iter(num),
		hIter: a.headers.Iter(num),
		bIter: a.bodies.Iter(num),
	}
	return iter
}

type ancientIterator struct {
	rIter *ancientTableIterator
	hIter *ancientTableIterator
	bIter *ancientTableIterator
}

func (i *ancientIterator) Next() bool {
	next := i.rIter.Next()
	i.hIter.Next()
	i.bIter.Next()

	return next
}

func (i *ancientIterator) Value() (*Block, error) {
	receipts := Receipts{}
	if err := i.rIter.Value(&receipts); err != nil {
		return nil, err
	}
	header := Header{}
	if err := i.hIter.Value(&header); err != nil {
		return nil, err
	}
	body := Body{}
	if err := i.bIter.Value(&body); err != nil {
		return nil, err
	}
	if len(body.Transactions) != len(receipts) {
		return nil, fmt.Errorf("incorrect match")
	}

	block := &Block{
		Number:   header.Number,
		Header:   &header,
		Body:     &body,
		Receipts: receipts,
	}
	return block, nil
}

type ancientTable struct {
	path       string
	name       string
	compressed bool

	// store the file of index and offsets
	index *os.File

	// data files
	data map[uint16]*os.File

	// number of items in the table
	numItems uint64
}

func newAncientTable(path, name string) (*ancientTable, error) {
	t := &ancientTable{
		path: path,
		name: name,
		data: map[uint16]*os.File{},
	}
	err := t.checkIndex()
	if err != nil {
		return nil, err
	}

	// open index file
	if t.index, err = os.Open(t.getIndexName(t.compressed)); err != nil {
		return nil, err
	}

	stat, err := t.index.Stat()
	if err != nil {
		return nil, err
	}

	t.numItems = uint64(stat.Size() / indexEntrySize)

	// preopen all the data files
	if err := t.openDataFiles(); err != nil {
		return nil, err
	}
	return t, nil
}

func (a *ancientTable) readTable(fileNum uint16, from uint32, size uint32) []byte {
	buf := make([]byte, size)
	_, err := a.data[fileNum].ReadAt(buf, int64(from))
	if err != nil {
		panic(err)
	}

	if buf, err = snappy.Decode(nil, buf); err != nil {
		panic(err)
	}
	return buf
}

func (a *ancientTable) checkIndex() error {
	hasCompr, err := exists(a.getIndexName(true))
	if err != nil {
		return err
	}
	hasNormal, err := exists(a.getIndexName(false))
	if err != nil {
		return err
	}
	if !hasCompr && !hasNormal {
		return fmt.Errorf("table not found")
	}
	if hasCompr && hasNormal {
		return fmt.Errorf("both compress and uncompress index found")
	}
	a.compressed = hasCompr
	return nil
}

func (a *ancientTable) getDataName(indx uint16, compressed bool) string {
	ext := ""
	if compressed {
		ext = "cdat"
	} else {
		ext = "rdat"
	}
	return filepath.Join(a.path, fmt.Sprintf("%s.%04d.%s", a.name, indx, ext))
}

func (a *ancientTable) getIndexName(compressed bool) string {
	ext := ""
	if compressed {
		ext = "cidx"
	} else {
		ext = "ridx"
	}
	return filepath.Join(a.path, a.name+"."+ext)
}

func (a *ancientTable) openDataFiles() error {
	stat, err := a.index.Stat()
	if err != nil {
		return err
	}

	var firstEntry, lastEntry indexEntry
	buf := make([]byte, indexEntrySize)

	readAt := func(entry *indexEntry, pos int64) error {
		if _, err := a.index.ReadAt(buf, pos); err != nil {
			return err
		}
		entry.Unmarshal(buf)
		return nil
	}

	// read the first entry
	if err := readAt(&firstEntry, 0); err != nil {
		return err
	}
	// read last entry
	if err := readAt(&lastEntry, stat.Size()-indexEntrySize); err != nil {
		return err
	}

	// open the files
	for i := firstEntry.FileNum; i <= lastEntry.FileNum; i++ {
		f, err := os.Open(a.getDataName(i, a.compressed))
		if err != nil {
			return err
		}
		a.data[i] = f
	}
	return nil
}

func (a *ancientTable) Iter(num int) *ancientTableIterator {
	a.index.Seek(int64(num*int(indexEntrySize)), 0)

	i := &ancientTableIterator{
		table:     a,
		indexFile: a.index,
	}
	i.ptr = i.readEntry()
	return i
}

type ancientTableIterator struct {
	table     *ancientTable
	indexFile *os.File
	ptr       indexEntry
	val       []byte
}

func (i *ancientTableIterator) readEntry() indexEntry {
	buf := make([]byte, indexEntrySize)

	if _, err := i.indexFile.Read(buf); err != nil {
		panic(err)
	}

	var entry indexEntry
	entry.Unmarshal(buf)

	return entry
}

func (i *ancientTableIterator) Next() bool {
	// read next entry
	next := i.readEntry()

	if i.ptr.FileNum != next.FileNum {
		// start from the next item
		i.val = i.table.readTable(next.FileNum, 0, next.Offset)
	} else {
		// follow the sequence
		i.val = i.table.readTable(i.ptr.FileNum, i.ptr.Offset, next.Offset-i.ptr.Offset)
	}

	i.ptr = next
	return true
}

type rlpObj interface {
	UnmarshalRLP(v []byte) error
}

func (i *ancientTableIterator) Value(obj rlpObj) error {
	return obj.UnmarshalRLP(i.val)
}

const indexEntrySize = int64(6)

type indexEntry struct {
	FileNum uint16 // 2 bytes
	Offset  uint32 // 4 bytes
}

func (i *indexEntry) Unmarshal(b []byte) {
	i.FileNum = binary.BigEndian.Uint16(b[:2])
	i.Offset = binary.BigEndian.Uint32(b[2:])
}
