package GoMM

// Benchmakrs, can be run with:
// go test -bench . -run "NONE" github.com/swpecht/DUP/

import (
	"fmt"
	"testing"
)

func BenchmarkScatter(b *testing.B) {
	for i := 0; i < b.N; i++ {
		fmt.Sprintf("hello")
	}
}
