package gethdatalayer

import (
	"path/filepath"
)

type Store struct {
	leveldbStore *LevelDbStore
	ancientStore *AncientStore
}

func NewStore(path string) (*Store, error) {
	// load the kv store
	leveldbStore, err := NewLevelDBStore(path)
	if err != nil {
		return nil, err
	}

	// load the ancient store
	ancientStore, err := NewAncientStore(filepath.Join(path, "ancient/chain"))
	if err != nil {
		return nil, err
	}

	// figure out if there is a stream between the last block we can
	// read from ancient store and the leveldb
	lastAncientBlock := ancientStore.LastNum()

	if _, err := decodeBlock(leveldbStore, lastAncientBlock+1); err != nil {
		return nil, err
	}

	s := &Store{
		leveldbStore: leveldbStore,
		ancientStore: ancientStore,
	}
	return s, nil
}

func (s *Store) Iterator() Iterator {
	iter := &storeIterator{
		store:          s,
		lastAncientNum: s.ancientStore.LastNum(),
	}
	return iter
}

type storeIterator struct {
	store          *Store
	num            uint64
	lastAncientNum uint64
	iterLevelDb    Iterator
	iterAncient    Iterator
}

func (s *storeIterator) Seek(num uint64) {
	s.num = num
}

func (s *storeIterator) Next() bool {
	if s.num+1 >= s.lastAncientNum {
		// use the leveldb store
		if s.iterLevelDb == nil {
			s.iterLevelDb = s.store.leveldbStore.Iterator()
			s.iterLevelDb.Seek(s.num)
		}
		if s.iterAncient != nil {
			// reset the ancient iterator (TODO: Close it)
			s.iterAncient = nil
		}
		s.num++
		return s.iterLevelDb.Next()
	}

	// use the ancient store
	if s.iterAncient == nil {
		s.iterAncient = s.store.ancientStore.Iterator()
		s.iterAncient.Seek(s.num)
	}
	s.num++
	return s.iterAncient.Next()
}

func (s *storeIterator) Value() (*Block, error) {
	if s.iterAncient != nil {
		return s.iterAncient.Value()
	}
	return s.iterLevelDb.Value()
}
