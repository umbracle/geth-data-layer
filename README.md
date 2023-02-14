# Geth-data-layer

Go library to access the [`geth`](https://github.com/ethereum/go-ethereum) stored data.

## Usage

```go
package main

import (
	"fmt"

	gethdatalayer "github.com/umbracle/geth-data-layer"
)

func main() {
	path := "..../chaindata" // path to the storage data

	store, err := gethdatalayer.NewStore(path)
	if err != nil {
		panic(err)
	}

	iter := store.Iterator()
	// iter.Seek(1000000)

	for iter.Next() {
		val, _ := iter.Value()
		fmt.Println(val.Number)
	}
}
```

There are three storage interaces:

- `NewAncientStore`: Access the `ancient` store data.
- `NewLevelDbStore`: Access the `leveldb` store data.
- `NewStore`: Abstraction on top of the `leveldb` and `ancient` data.
