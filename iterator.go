package gethdatalayer

type Iterator interface {
	Next() bool
	Value() (*Block, error)
}
