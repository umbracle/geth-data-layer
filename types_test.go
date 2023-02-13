package gethdatalayer

import (
	"bytes"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"testing"
)

//go:embed fixtures/transactions.json
var transactionsFixtures string

func TestTypesTxn(t *testing.T) {
	var cases []struct {
		Raw  string
		Hash string
	}
	if err := json.Unmarshal([]byte(transactionsFixtures), &cases); err != nil {
		t.Fatal(err)
	}

	for _, c := range cases {
		rlpHex, _ := hex.DecodeString(c.Raw)
		hashHex, _ := hex.DecodeString(c.Hash)

		var txn Transaction
		if err := txn.UnmarshalRLP(rlpHex); err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(hashHex, txn.Hash[:]) {
			t.Fatal("not equal")
		}
	}
}
