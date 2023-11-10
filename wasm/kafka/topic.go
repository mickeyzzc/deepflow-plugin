package main

import (
	"encoding/binary"
	"strconv"

	"github.com/deepflowio/deepflow-wasm-go-sdk/sdk"
	"github.com/segmentio/kafka-go/protocol"
)

const WASM_KAFKA_PROTOCOL uint8 = 1

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
	if ctx.L4 != sdk.TCP {
		return 0, ""
	}
	if ctx.DstPort < 9092 || ctx.DstPort > 9093 {
		return 0, ""
	}
	payload, err := ctx.GetPayload()
	if err != nil {
		sdk.Error("get payload fail: %v", err)
		return 0, ""
	}
	bl, err := protocol.ReadAll(protocol.NewBytes(payload))
	if err != nil {
		sdk.Error("read payload fail: %v", err)
		return 0, ""
	}
	b, _ := decodeHeader(bl)
	if !b {
		return 0, ""
	}
	return WASM_KAFKA_PROTOCOL, "kafka"
}

func (p kafkaParser) OnParsePayload(ctx *sdk.ParseCtx) sdk.Action {
	if ctx.L4 != sdk.TCP || ctx.L7 != WASM_KAFKA_PROTOCOL {
		return sdk.ActionNext()
	}
	payload, err := ctx.GetPayload()
	if err != nil {
		return sdk.ActionAbortWithErr(err)
	}

	b, header := decodeHeader(payload)
	if !b {
		return sdk.ActionNext()
	}
	var (
		req *sdk.Request
		//resp *sdk.Response
		id = uint32(header.correlationID)
	)
	length := int(header.length)

	// header base size :
	// req_len(int32) + api_key(int16) + api_ver(int16) + c_id(int32) + client_len(int16)
	// = 14
	var header_offset = 14 + header.clientLen
	var topic_size int16 = 0
	var topic_name = ""
	switch protocol.ApiKey(header.apikey) {
	case protocol.Produce:
		topic_size, topic_name = decodeProduce(header.apiversion, payload[header_offset:])
	case protocol.Fetch:
		topic_size, topic_name = decodeFetch(header.apiversion, payload[header_offset:])
	}
	if topic_size == 0 {
		return sdk.ActionNext()
	}
	req = &sdk.Request{
		ReqType:  protocol.ApiKey(header.apikey).String() + "_v" + strconv.Itoa(int(header.apiversion)),
		Resource: topic_name,
	}
	return sdk.ParseActionAbortWithL7Info([]*sdk.L7ProtocolInfo{
		{
			RequestID: &id,
			ReqLen:    &length,
			Req:       req,
		},
	})
}

// /
func readInt16(b []byte) int16 {
	return int16(binary.BigEndian.Uint16(b))
}

func readInt32(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

func readJavaString(b []byte) (int16, string) {
	var size = readInt16(b[:2])
	if size <= 0 {
		return 0, ""
	}
	if len(b) < int(2+size) {
		return -1, ""
	}
	return size, string(b[2 : 2+size])
}

func readJavaNullableString(b []byte) (int16, string) {
	return readJavaString(b)
}

// size => INT32
// header => Request Header
//
// Request Header => request_api_key request_api_version correlation_id client_id
//
//	request_api_key => INT16
//	request_api_version => INT16
//	correlation_id => INT32
//	client_id => NULLABLE_STRING
type reqHeader struct {
	length        int32
	apikey        int16
	apiversion    int16
	correlationID int32
	clientLen     int16
	clientID      string
}

func decodeHeader(b []byte) (bool, *reqHeader) {
	if len(b) < 14 {
		return false, nil
	}
	var length = readInt32(b[:4])
	if length != int32(len(b))-4 {
		sdk.Warn("length not match: %d, %d", length, len(b))
		return false, nil
	}
	var apikey = readInt16(b[4:6])
	var apiversion = readInt16(b[6:8])
	if protocol.ApiKey(apikey).String() == strconv.Itoa(int(apikey)) {
		sdk.Warn("invalid apikey: %d", apikey)
		return false, nil
	}
	/*
		if apiversion < protocol.ApiKey(apikey).MinVersion() || apiversion > protocol.ApiKey(apikey).MaxVersion() {
			sdk.Warn("invalid %d apiversion: %d, minversion: %d, maxversion: %d", apikey, apiversion, protocol.ApiKey(apikey).MinVersion(), protocol.ApiKey(apikey).MaxVersion())
			return false, nil
		}
	*/
	var correlationID = readInt32(b[8:12])
	cLen, cID := readJavaNullableString(b[12:])
	if cLen == -1 {
		sdk.Warn("invalid clientid: %s", cID)
		return false, nil
	}
	return true, &reqHeader{
		length:        length,
		apikey:        apikey,
		apiversion:    apiversion,
		correlationID: correlationID,
		clientLen:     cLen,
		clientID:      cID,
	}
}

func decodeFetch(apiver int16, payload []byte) (int16, string) {
	var fixed_offset = 0
	switch {
	case apiver <= 2:
		// Fetch Request (Version: 0-2) => replica_id max_wait_ms min_bytes [topics]
		//  replica_id => INT32
		//  max_wait_ms => INT32
		//  min_bytes => INT32
		//  topics => topic [partitions]
		//    topic => STRING
		//
		// int32 + int32 + int32 + int32 = 16
		fixed_offset = 16
	case apiver == 3:
		// Fetch Request (Version: 3) => replica_id max_wait_ms min_bytes max_bytes [topics]
		//  replica_id => INT32
		//  max_wait_ms => INT32
		//  min_bytes => INT32
		//  max_bytes => INT32
		//  topics => topic [partitions]
		//    topic => STRING
		fixed_offset = 20
	case apiver <= 6:
		// Fetch Request (Version: 4-6) => replica_id max_wait_ms min_bytes max_bytes isolation_level [topics]
		//  replica_id => INT32
		//  max_wait_ms => INT32
		//  min_bytes => INT32
		//  max_bytes => INT32
		//  isolation_level => INT8
		//  topics => topic [partitions]
		//    topic => STRING
		fixed_offset = 21
	case apiver <= 11:
		// Fetch Request (Version: 7-11) => replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data]
		//  replica_id => INT32
		//  max_wait_ms => INT32
		//  min_bytes => INT32
		//  max_bytes => INT32
		//  isolation_level => INT8
		//  session_id => INT32
		//  session_epoch => INT32
		//  topics => topic [partitions]
		//    topic => STRING
		fixed_offset = 29
	}
	return readJavaString(payload[fixed_offset:])
}

func decodeProduce(apiver int16, payload []byte) (int16, string) {
	var fixed_offset = 0
	switch {
	case apiver <= 2:
		// Produce Request (Version: 0-2) => acks timeout_ms [topic_data]
		//
		//	acks => INT16
		//	timeout_ms => INT32
		//	topic_data => name [partition_data]
		//	  name => STRING
		//
		// int16 + int32 + int32 = 10
		fixed_offset = 10

	case apiver <= 8:
		// Produce Request (Version: 3-8) => transactional_id acks timeout_ms [topic_data]
		//
		//	transactional_id => NULLABLE_STRING
		//	acks => INT16
		//	timeout_ms => INT32
		//	topic_data => name [partition_data]
		//	  name => STRING
		var tid_len, _ = readJavaNullableString(payload)
		if tid_len < 0 {
			return 0, ""
		}
		fixed_offset = 12 + int(tid_len)
	}
	return readJavaString(payload[fixed_offset:])
}

func main() {
	sdk.Warn("wasm register kafka parser")
	sdk.SetParser(kafkaParser{})
}
