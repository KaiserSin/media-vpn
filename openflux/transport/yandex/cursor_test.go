package yandex

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"strings"
	"testing"

	"universal-bypass-tool/transport"
)

func TestCursorDeclaredLengthBoundedAndExact(t *testing.T) {
	for _, size := range []int{1, 13, 18, 72, 4096, 32068, 32 << 10, maxPacketBytes} {
		payload := bytes.Repeat([]byte{0xA5}, size)
		wire := encodeCursor(payload)
		declared, encoded, ok := strings.Cut(wire, ";")
		if !ok || declared != strconv.Itoa(size) {
			t.Fatal("outgoing cursor did not declare the actual byte length")
		}
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || !bytes.Equal(decoded, payload) {
			t.Fatal("outgoing cursor payload changed")
		}
		decoded, err = decodeCursor(wire)
		if err != nil || !bytes.Equal(decoded, payload) {
			t.Fatal("valid cursor frame rejected")
		}
	}
	for _, wire := range []string{
		"", ";YQ==", "0;", "01;YQ==", "+1;YQ==", "-1;YQ==", " 1;YQ==", "1 ;YQ==", "1.0;YQ==", "1e0;YQ==",
		"999999999999999999999;YQ==", "65537;YQ==", "1;YQ==\n", "1;YR==", "1;not-base64", "1;YQ==;",
		"18;" + base64.StdEncoding.EncodeToString([]byte("hello")),
		"13;" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'a'}, 18)),
		encodeCursor(bytes.Repeat([]byte{'a'}, maxPacketBytes+1)),
	} {
		if _, err := decodeCursor(wire); err == nil {
			t.Fatal("invalid, unbounded or mismatched cursor frame accepted")
		}
	}
}

func TestCursorReceiveKeepsHeartbeatOutOfRelay(t *testing.T) {
	tr := NewYandexDocsTransport("https://docs.yandex.ru/fixture", transport.DefaultConfig())
	delivered := 0
	tr.Receive(func([]byte) { delivered++ })
	tr.receiveCursor("18;---KA---")
	tr.receiveCursor(encodeCursor([]byte(keepAlivePayload)))
	if delivered != 0 || tr.Stats().PacketsRecv != 0 {
		t.Fatal("heartbeat entered relay or payload counters")
	}
	tr.receiveCursor("18;" + base64.StdEncoding.EncodeToString([]byte("hello")))
	if delivered != 0 {
		t.Fatal("mismatched historical length delivered")
	}
	tr.receiveCursor(encodeCursor(bytes.Repeat([]byte{'x'}, 18)))
	if delivered != 1 {
		t.Fatal("correct legacy 18-byte cursor no longer works")
	}
}
