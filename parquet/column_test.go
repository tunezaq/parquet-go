package parquet

import (
	"fmt"
	"os"
	"testing"
)

func TestColumnReader(t *testing.T) {
	f, err := os.Open("../../parquet-test/harness/input/Booleans.parquet")
	//f, err := os.Open("/home/ksh/downloads/nation.impala.parquet")
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	err = getColumnReader(f, 0)
	fmt.Printf("Error: %s\n", err)
}
