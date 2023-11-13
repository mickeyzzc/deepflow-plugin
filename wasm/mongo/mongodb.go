package main

import (
	"encoding/binary"

	"github.com/deepflowio/deepflow-wasm-go-sdk/sdk"
	"go.mongodb.org/mongo-driver/x/mongo/driver/wiremessage"
)

const WASM_MONGODB_PROTOCOL uint8 = 81
const MONGO_HEADER_SIZE = 16

type mongoParser struct{}

func (p mongoParser) HookIn() []sdk.HookBitmap {
	return []sdk.HookBitmap{
		sdk.HOOK_POINT_PAYLOAD_PARSE,
	}
}

func (p mongoParser) OnHttpReq(ctx *sdk.HttpReqCtx) sdk.Action {
	return sdk.ActionNext()
}

func (p mongoParser) OnHttpResp(ctx *sdk.HttpRespCtx) sdk.Action {
	return sdk.ActionNext()
}

func (p mongoParser) OnCheckPayload(ctx *sdk.ParseCtx) (uint8, string) {
	if ctx.L4 != sdk.TCP {
		return 0, ""
	}
	payload, err := ctx.GetPayload()
	if err != nil {
		sdk.Error("get payload fail: %v", err)
		return 0, ""
	}
	ok, header := decodeHeader(payload)
	if !ok {
		return 0, ""
	}
	switch header.opCode {
	case wiremessage.OpMsg | wiremessage.OpReply:
		return WASM_MONGODB_PROTOCOL, "MongoDB"
	}
	return 0, ""
}

func (p mongoParser) OnParsePayload(ctx *sdk.ParseCtx) sdk.Action {
	if ctx.L4 != sdk.TCP || ctx.L7 != WASM_MONGODB_PROTOCOL {
		return sdk.ActionNext()
	}
	payload, err := ctx.GetPayload()
	if err != nil {
		return sdk.ActionAbortWithErr(err)
	}

	ok, header := decodeHeader(payload)
	if !ok {
		return sdk.ActionNext()
	}
	var (
		req  *sdk.Request
		resp *sdk.Response
		id   = uint32(header.requestID)
	)
	length := int(header.length)

	var header_offset = MONGO_HEADER_SIZE
	var res = ""
	switch header.opCode {
	case wiremessage.OpMsg:
		st, src, ok := wiremessage.ReadMsgSectionType(payload[header_offset:])
		if !ok {
			return sdk.ActionNext()
		}
		switch st {
		case wiremessage.SingleDocument:
			doc, _, ok := wiremessage.ReadMsgSectionSingleDocument(src)
			if !ok {
				return sdk.ActionNext()
			}
			res = doc.String()
		case wiremessage.DocumentSequence:
			_, docs, _, ok := wiremessage.ReadMsgSectionDocumentSequence(src)
			if !ok {
				return sdk.ActionNext()
			}
			res = docs[0].String()
		default:
			return sdk.ActionNext()
		}
	case wiremessage.OpReply:
		flag, _, ok := wiremessage.ReadReplyFlags(payload[header_offset:])
		if !ok {
			return sdk.ActionNext()
		}
		var status = sdk.RespStatusOk
		switch flag {
		case wiremessage.CursorNotFound | wiremessage.QueryFailure:
			status = sdk.RespStatusServerErr
		}
		resp = &sdk.Response{
			Result: flag.String(),
			Status: &status,
		}
	default:
		return sdk.ActionNext()
	}
	req = &sdk.Request{
		ReqType:  header.opCodeName,
		Resource: res,
	}
	return sdk.ParseActionAbortWithL7Info([]*sdk.L7ProtocolInfo{
		{
			RequestID: &id,
			ReqLen:    &length,
			Req:       req,
			Resp:      resp,
		},
	})
}

// /
func readUint16(b []byte) uint16 {
	return binary.LittleEndian.Uint16(b)
}

func readUint32(b []byte) uint32 {
	return binary.LittleEndian.Uint32(b)
}

func readInt32(b []byte) int32 {
	return int32(readUint32(b))
}

type mongoHeader struct {
	length     int32
	requestID  int32
	responseTO int32
	opCode     wiremessage.OpCode
	opCodeName string
	data       []byte
}

func decodeHeader(b []byte) (bool, *mongoHeader) {
	length, request_id, response_to, op_code, data, ok := wiremessage.ReadHeader(b)
	if !ok {
		return false, nil
	}
	if length != int32(len(b)) {
		sdk.Warn("length not match: %d, %d", length, len(b))
		return false, nil
	}

	switch op_code.String() {
	case "<invalid opcode>":
		sdk.Warn("invalid op_code: %d", op_code)
		return false, nil
	default:
		if request_id <= 0 || response_to <= 0 {
			return false, nil
		}
	}
	return true, &mongoHeader{
		length:     length,
		requestID:  request_id,
		responseTO: response_to,
		opCode:     op_code,
		opCodeName: op_code.String(),
		data:       data,
	}
}

func main() {
	sdk.Warn("wasm register mongo parser")
	sdk.SetParser(mongoParser{})
}
