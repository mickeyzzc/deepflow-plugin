package main

import (
	"encoding/binary"

	"github.com/deepflowio/deepflow-wasm-go-sdk/sdk"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/apiversions"
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
	var msg protocol.Message
	var req = &apiversions.Request{}
	var resp = &apiversions.Response{}
	b := protocol.NewBytes(payload)
	k, err := protocol.ReadAll(b)
	if err != nil {
		sdk.Error("read payload fail: %v", err)
		return 0, ""
	}
	if len(k) < 4 {
		return 0, ""
	}
	var version = readInt16(k[:2])
	var typ = readInt16(k[2:])

	//var kafka protocol.Header

	/*
		if err := dns.Unpack(payload); err != nil {
			return 0, ""
		}
		if dns.Response {
			return 0, ""
		}
	*/
	return WASM_KAFKA_PROTOCOL, "kafka"
}

// /
func readInt16(b []byte) int16 {
	return int16(binary.BigEndian.Uint16(b))
}
