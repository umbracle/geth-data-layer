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
    path := "..../chaindata/ancient/chain" // path to the ancient data storage

	store, err := gethdatalayer.NewAncientStore(path)
	if err != nil {
		panic(err)
	}
	iter := store.Iterator()
	for iter.Next() {
		fmt.Println(iter.Value())
	}
}
```
