package rpc

import (
	"bufio"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

type clientCodecMsgpack struct {
	conn         io.ReadWriteCloser
	decoder      *msgpack.Decoder
	encoder      *msgpack.Encoder
	encodeBuffer *bufio.Writer
}

func newClientCodecMsgpack(conn io.ReadWriteCloser) clientCodecMsgpack {
	buf := bufio.NewWriter(conn)

	decoder := msgpack.NewDecoder(conn)
	decoder.UsePreallocateValues(true)
	decoder.DisableAllocLimit(true)

	encoder := msgpack.NewEncoder(buf)

	return clientCodecMsgpack{
		conn:         conn,
		decoder:      decoder,
		encoder:      encoder,
		encodeBuffer: buf,
	}
}

func (self clientCodecMsgpack) WriteRequest(reqHeader *requestHeader, reqBody any) error {
	if err := self.encoder.Encode(reqHeader); err != nil {
		return err
	}

	if err := self.encoder.Encode(reqBody); err != nil {
		return err
	}

	return self.encodeBuffer.Flush()
}

func (self clientCodecMsgpack) ReadResponseHeader(resHeader *responseHeader) error {
	return self.decoder.Decode(resHeader)
}

func (self clientCodecMsgpack) ReadResponseBody(resBody any) error {
	return self.decoder.Decode(resBody)
}

func (self clientCodecMsgpack) ReadRawResponseBody() ([]byte, error) {
	return self.decoder.DecodeRaw()
}

func (_ clientCodecMsgpack) Unmarshal(data []byte, value any) error {
	return msgpack.Unmarshal(data, value)
}

func (self clientCodecMsgpack) Close() error {
	return self.conn.Close()
}
