package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDocumentBootstrapExactMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap.json")
	raw := []byte(`{"schemaVersion":1,"documents":[{"url":"https://disk.yandex.ru/i/b","editorUrl":"https://docs.yandex.ru/edit/d/b","clientConfig":{"fixture":"second"},"dialAddress":"87.250.251.212"},{"url":"https://disk.yandex.ru/i/a","editorUrl":"https://docs.yandex.ru/edit/d/a","clientConfig":{"fixture":"first"},"dialAddress":"2a02:6b8::1"}]}`)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := readDocumentBootstrap(path, []string{"https://disk.yandex.ru/i/a", "https://disk.yandex.ru/i/b"})
	if err != nil || len(got) != 2 {
		t.Fatalf("mapping failed: %v", err)
	}
	var first map[string]string
	if json.Unmarshal(got["https://disk.yandex.ru/i/a"].ClientConfig, &first) != nil || first["fixture"] != "first" ||
		got["https://disk.yandex.ru/i/a"].DialAddress != "2a02:6b8::1" || got["https://disk.yandex.ru/i/b"].DialAddress != "87.250.251.212" {
		t.Fatal("bootstrap entries were mapped by order instead of exact URL")
	}
	if got, err := readDocumentBootstrap("", nil); got != nil || err != nil {
		t.Fatal("normal HTTP mode changed")
	}
	if validateBootstrapMode("private", false) == nil || validateBootstrapMode("private", true) != nil || validateBootstrapMode("", false) != nil {
		t.Fatal("bootstrap mode restriction failed")
	}
}

func TestDocumentBootstrapRejectsInvalidBundlesWithoutSecrets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "PRIVATE_PATH_MARKER")
	entry := `{"url":"https://disk.yandex.ru/i/a","editorUrl":"https://docs.yandex.ru/edit/d/a","clientConfig":{"fixture":"SECRET_MARKER"}}`
	wrap := func(entries string) string { return `{"schemaVersion":1,"documents":[` + entries + `]}` }
	for _, tc := range []struct {
		name, body string
		requested  []string
	}{
		{"empty", "", []string{"https://disk.yandex.ru/i/a"}},
		{"malformed", `{"SECRET_MARKER":`, []string{"https://disk.yandex.ru/i/a"}},
		{"wrong version", strings.Replace(wrap(entry), `"schemaVersion":1`, `"schemaVersion":2`, 1), []string{"https://disk.yandex.ru/i/a"}},
		{"no entries", wrap(""), []string{"https://disk.yandex.ru/i/a"}},
		{"eleven entries", wrap(strings.Repeat(entry+",", 10) + entry), []string{"https://disk.yandex.ru/i/a"}},
		{"missing", wrap(entry), []string{"https://disk.yandex.ru/i/a", "https://disk.yandex.ru/i/b"}},
		{"duplicate", wrap(entry + "," + entry), []string{"https://disk.yandex.ru/i/a", "https://disk.yandex.ru/i/b"}},
		{"unknown", wrap(strings.Replace(entry, "/i/a", "/i/SECRET_MARKER", 1)), []string{"https://disk.yandex.ru/i/a"}},
		{"not exact", wrap(entry), []string{"https://disk.yandex.ru/i/a/"}},
		{"null config", wrap(strings.Replace(entry, `{"fixture":"SECRET_MARKER"}`, `null`, 1)), []string{"https://disk.yandex.ru/i/a"}},
		{"array config", wrap(strings.Replace(entry, `{"fixture":"SECRET_MARKER"}`, `[]`, 1)), []string{"https://disk.yandex.ru/i/a"}},
		{"missing config", wrap(`{"url":"https://disk.yandex.ru/i/a","editorUrl":"https://docs.yandex.ru/edit/d/a"}`), []string{"https://disk.yandex.ru/i/a"}},
		{"missing editor metadata", wrap(`{"url":"https://disk.yandex.ru/i/a","clientConfig":{}}`), []string{"https://disk.yandex.ru/i/a"}},
		{"non-string dial address", wrap(strings.Replace(entry, `"editorUrl":`, `"dialAddress":123,"editorUrl":`, 1)), []string{"https://disk.yandex.ru/i/a"}},
		{"duplicate requested", wrap(entry + "," + entry), []string{"https://disk.yandex.ru/i/a", "https://disk.yandex.ru/i/a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := readDocumentBootstrap(path, tc.requested)
			if err == nil {
				t.Fatal("invalid bundle accepted")
			}
			if strings.Contains(err.Error(), "SECRET_MARKER") || strings.Contains(err.Error(), "PRIVATE_PATH_MARKER") || strings.Contains(err.Error(), "https://") {
				t.Fatal("bootstrap error exposed private input")
			}
		})
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := readDocumentBootstrap(path, []string{"a"}); err == nil || strings.Contains(err.Error(), "PRIVATE_PATH_MARKER") {
		t.Fatal("file error was not sanitized")
	}
}

func TestDocumentBootstrapFileLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap.json")
	raw := []byte(`{"schemaVersion":1,"documents":[{"url":"a","editorUrl":"https://docs.yandex.ru/edit/d/a","clientConfig":{}}]}`)
	raw = append(raw, bytes.Repeat([]byte(" "), maxBootstrapFileBytes-len(raw))...)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readDocumentBootstrap(path, []string{"a"}); err != nil {
		t.Fatal("bundle at size limit rejected:", err)
	}
	if err := os.WriteFile(path, append(raw, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readDocumentBootstrap(path, []string{"a"}); err == nil {
		t.Fatal("oversized bundle accepted")
	}
}
