package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDocumentListCanonicalAndBounded(t *testing.T) {
	p := filepath.Join(t.TempDir(), "urls")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(p, []byte(s), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(" https://disk.yandex.ru/i/b\r\n\nhttps://disk.yandex.ru/i/a\n")
	got, err := readDocumentURLs("", p, "")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"https://disk.yandex.ru/i/a", "https://disk.yandex.ru/i/b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("not canonical: %v", got)
	}
	for _, content := range []string{"", "\n", "same\nsame", strings.Repeat("x", 8193), "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\nk", strings.Repeat("x", 10*8193+1)} {
		write(content)
		if _, err := readDocumentURLs("", p, ""); err == nil {
			t.Fatal("accepted invalid list")
		}
	}
	if _, err := readDocumentURLs(p, p, ""); err == nil {
		t.Fatal("accepted ambiguous sources")
	}
}

func TestSingleDocumentCompatibility(t *testing.T) {
	p := filepath.Join(t.TempDir(), "url")
	if err := os.WriteFile(p, []byte(" https://disk.yandex.ru/i/test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := readDocumentURLs(p, "", "ignored")
	if err != nil || len(got) != 1 || got[0] != "https://disk.yandex.ru/i/test" {
		t.Fatalf("single file: %v %v", got, err)
	}
	got, err = readDocumentURLs("", "", " https://disk.yandex.ru/i/env ")
	if err != nil || got[0] != "https://disk.yandex.ru/i/env" {
		t.Fatal("env compatibility broken")
	}
}
