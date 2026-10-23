package yandex

import (
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
)

const keepAlivePayload = "---KA---"

var errCursor = errors.New("invalid document cursor frame")

// OnlyOffice cursor framing declares the decoded byte length before ';'.
func encodeCursor(data []byte) string {
	return strconv.Itoa(len(data)) + ";" + base64.StdEncoding.EncodeToString(data)
}

func decodeCursor(cursor string) ([]byte, error) {
	if len(cursor) > 6+base64.StdEncoding.EncodedLen(maxPacketBytes) {
		return nil, errCursor
	}
	separator := strings.IndexByte(cursor, ';')
	if separator < 1 || separator > 5 || cursor[0] == '0' {
		return nil, errCursor
	}
	for _, c := range cursor[:separator] {
		if c < '0' || c > '9' {
			return nil, errCursor
		}
	}
	declared, err := strconv.Atoi(cursor[:separator])
	if err != nil || declared < 1 || declared > maxPacketBytes {
		return nil, errCursor
	}
	encoded := cursor[separator+1:]
	if len(encoded) != base64.StdEncoding.EncodedLen(declared) {
		return nil, errCursor
	}
	data, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(data) != declared {
		return nil, errCursor
	}
	return data, nil
}
