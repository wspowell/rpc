package rpc

import (
	"bufio"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

type ServerCodecMsgpack struct {
	conn         io.ReadWriteCloser
	decoder      *msgpack.Decoder
	encoder      *msgpack.Encoder
	encodeBuffer *bufio.Writer
	closed       bool
}

func NewServerCodecMsgpack(conn io.ReadWriteCloser) *ServerCodecMsgpack {
	buf := bufio.NewWriter(conn)

	return &ServerCodecMsgpack{
		conn:         conn,
		decoder:      msgpack.NewDecoder(conn),
		encoder:      msgpack.NewEncoder(buf),
		encodeBuffer: buf,
		closed:       false,
	}
}

func (self *ServerCodecMsgpack) ReadRequestHeader(reqHeader *requestHeader) error {
	return self.decoder.Decode(reqHeader)
}

func (self *ServerCodecMsgpack) ReadRequestBody(reqBody any) error {
	return self.decoder.Decode(reqBody)
}

func (self *ServerCodecMsgpack) WriteResponse(resHeader *responseHeader, resBody any) error {
	var err error

	if err = self.encoder.Encode(resHeader); err != nil {
		if self.encodeBuffer.Flush() == nil {
			// Msgpack couldn't encode the header. Should not happen, so if it does,
			// shut down the connection to signal that the connection is broken.
			err = self.Close()
		}

		return err
	}

	if err = self.encoder.Encode(resBody); err != nil {
		if self.encodeBuffer.Flush() == nil {
			// Was a msgpack problem encoding the body but the header has been written.
			// Shut down the connection to signal that the connection is broken.
			err = self.Close()
		}

		return err
	}

	return self.encodeBuffer.Flush()
}

func (self *ServerCodecMsgpack) Close() error {
	if self.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}

	self.closed = true

	return self.conn.Close()
}
