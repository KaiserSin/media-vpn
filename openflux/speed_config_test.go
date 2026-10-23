package main

import (
	"crypto/sha256"
	"strings"
	"testing"
)

func TestPoolIdentityPreservesDefaultAndIsolatesExperiments(t *testing.T) {
	urls := []string{"https://example.invalid/test-a", "https://example.invalid/test-b"}
	legacy := sha256.Sum256([]byte(strings.Join(urls, "\n")))
	base, err := poolIdentity(urls, "")
	if err != nil || string(base[:]) != string(legacy[:16]) {
		t.Fatal("default changed")
	}
	first, err := poolIdentity(urls, "speed-v1")
	if err != nil || first == base {
		t.Fatal("experiment is not isolated")
	}
	repeated, _ := poolIdentity(urls, "speed-v1")
	second, _ := poolIdentity(urls, "speed-v2")
	if repeated != first || second == first {
		t.Fatal("namespace identity mismatch")
	}
	for _, invalid := range []string{"bad\nname", "bad\x00name", strings.Repeat("a", 65)} {
		if _, err := poolIdentity(urls, invalid); err == nil {
			t.Fatal("invalid namespace accepted")
		}
	}
}
