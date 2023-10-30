package main

import (
	"encoding/binary"
	"strconv"

	"github.com/deepflowio/deepflow-wasm-go-sdk/sdk"
	"github.com/segmentio/kafka-go/protocol"
)

const WASM_KAFKA_PROTOCOL uint8 = 100

type kafkaParser struct{}

func (p kafkaParser) HookIn() []sdk.HookBitmap {
	return []sdk.HookBitmap{
		sdk.HOOK_POINT_PAYLOAD_PARSE,
	}
}

func (p kafkaParser) OnHttpReq(ctx *sdk.HttpReqCtx) sdk.Action {
	return sdk.ActionNext()
}

func (p kafkaParser) OnHttpResp(ctx *sdk.HttpRespCtx) sdk.Action {
	return sdk.ActionNext()
}

func (p kafkaParser) OnCheckPayload(ctx *sdk.ParseCtx) (uint8, string) {
	if ctx.L4 != sdk.UDP || ctx.DstPort != 53 {
		return 0, ""
	}

	payload, err := ctx.GetPayload()
	if err != nil {
		sdk.Error("get payload fail: %v", err)
		return 0, ""
	}

	b := protocol.NewBytes(payload)
	k, err := protocol.ReadAll(b)
	if err != nil {
		sdk.Error("read payload fail: %v", err)
		return 0, ""
	}
	if len(k) < 14 {
		return 0, ""
	}
	var length = readInt32(k[:4])
	if length != int32(len(k))-4 {
		return 0, ""
	}
	var apikey = readInt16(k[4:6])
	var apiversion = readInt16(k[6:8])
	if protocol.ApiKey(apikey).String() == strconv.Itoa(int(apikey)) {
		return 0, ""
	}
	if apiversion < protocol.ApiKey(apikey).MinVersion() || apiversion > protocol.ApiKey(apikey).MaxVersion() {
		return 0, ""
	}

	return WASM_KAFKA_PROTOCOL, "kafka"
}

func (p kafkaParser) OnParsePayload(ctx *sdk.ParseCtx) sdk.Action {
	return sdk.ActionNext()
}

// /
func readInt16(b []byte) int16 {
	return int16(binary.BigEndian.Uint16(b))
}

func readInt32(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

func main() {
	sdk.Warn("wasm register dns parser")
	sdk.SetParser(kafkaParser{})
}
