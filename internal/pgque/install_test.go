package pgque

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestBundledSQLMatchesUpstream(t *testing.T) {
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(schema))); got != "e7ffe1c941666e6789e5c91a7d0b28196bb8a883ebaf407b13794271926a5b2f" {
		t.Fatal("bundled PgQue differs from pinned upstream 0.2.0")
	}
}
