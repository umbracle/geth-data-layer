package gethdatalayer

type Iterator interface {
	Seek(num uint64)
	Next() bool
	Value() (*Block, error)
}
