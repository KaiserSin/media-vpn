package yandex

import (
	"encoding/json"
	"errors"
)

// SetBootstrapConfig opts into an operator-supplied, browser-captured client
// configuration. It accepts no cookies or executable JavaScript. Configure it
// before Start; the immutable copy is revalidated for each new session.
func (t *YandexDocsTransport) SetBootstrapConfig(raw []byte) error {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	if t.done != nil {
		return errors.New("configure document bootstrap before transport start")
	}
	if t.bootstrapDialAddress != "" {
		return errors.New("configure document bootstrap before its dial address")
	}
	if len(raw) == 0 || len(raw) > maxPageBytes {
		return errors.New("invalid document bootstrap config size")
	}
	var config map[string]interface{}
	if json.Unmarshal(raw, &config) != nil || config == nil {
		return errors.New("invalid document bootstrap config")
	}
	// Marshal also escapes HTML delimiters so values cannot terminate the script.
	copyJSON, err := json.Marshal(config)
	if err != nil {
		return errors.New("invalid document bootstrap config")
	}
	body := append([]byte(`<script id="client-config" type="application/json">`), copyJSON...)
	body = append(body, []byte(`</script>`)...)
	if _, err := parseDocInfo(body, t.baseUserID, t.allowTestURLs); err != nil {
		return errors.New("invalid document bootstrap config fields or endpoints")
	}
	t.bootstrapBody = body
	return nil
}
