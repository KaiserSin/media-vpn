package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
)

const maxBootstrapFileBytes = 16 << 20

type documentBootstrap struct {
	ClientConfig json.RawMessage
	DialAddress  string
}

func validateBootstrapMode(path string, streamMode bool) error {
	if path != "" && !streamMode {
		return errors.New("doc-bootstrap-file requires stream-relay")
	}
	return nil
}

// Bundle URLs are exact keys, not aliases. Nothing in the bundle is logged.
func readDocumentBootstrap(path string, requested []string) (map[string]documentBootstrap, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open document bootstrap file")
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, maxBootstrapFileBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxBootstrapFileBytes {
		return nil, errors.New("document bootstrap file exceeds size limit or cannot be read")
	}
	var bundle struct {
		SchemaVersion int `json:"schemaVersion"`
		Documents     []struct {
			URL          string          `json:"url"`
			EditorURL    string          `json:"editorUrl"`
			ClientConfig json.RawMessage `json:"clientConfig"`
			DialAddress  string          `json:"dialAddress,omitempty"`
		} `json:"documents"`
	}
	if json.Unmarshal(raw, &bundle) != nil || bundle.SchemaVersion != 1 || len(bundle.Documents) < 1 || len(bundle.Documents) > 10 {
		return nil, errors.New("invalid document bootstrap bundle or schema version")
	}
	if len(requested) < 1 || len(requested) > 10 || len(bundle.Documents) != len(requested) {
		return nil, errors.New("document bootstrap bundle must match the requested documents")
	}
	wanted := make(map[string]bool, len(requested))
	for _, docURL := range requested {
		if docURL == "" || wanted[docURL] {
			return nil, errors.New("invalid requested document set for bootstrap")
		}
		wanted[docURL] = true
	}
	configs := make(map[string]documentBootstrap, len(requested))
	for _, doc := range bundle.Documents {
		config := bytes.TrimSpace(doc.ClientConfig)
		if !wanted[doc.URL] || configs[doc.URL].ClientConfig != nil || doc.EditorURL == "" || len(config) == 0 || config[0] != '{' {
			return nil, errors.New("invalid, duplicate, missing, or unknown document bootstrap entry")
		}
		configs[doc.URL] = documentBootstrap{ClientConfig: append(json.RawMessage(nil), config...), DialAddress: doc.DialAddress}
	}
	return configs, nil
}
