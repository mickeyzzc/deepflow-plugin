package main_test

import (
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/segmentio/kafka-go/protocol"
)

func TestTopicBase(t *testing.T) {
	hexString := "000000490001000c000000140021636f6e73756d65722d636f6e736f6c652d636f6e73756d65722d38343130372d3100ffffffff000001f4000000010320000000029d2b8e0000000901010100"
	payload, _ := hex.DecodeString(hexString)
	b := protocol.NewBytes(payload)
	k, err := protocol.ReadAll(b)
	if err != nil {
		t.Errorf("read payload fail: %v", err)
	}
	var length = int32(binary.BigEndian.Uint32(k[:4]))
	t.Log("len", length)
}
