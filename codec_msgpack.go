package rpc

import (
	"bufio"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

type clientCodecMsgpack struct {
	rwc    io.ReadWriteCloser
	dec    *msgpack.Decoder
	enc    *msgpack.Encoder
	encBuf *bufio.Writer
}

func newClientCodecMsgpack(conn io.ReadWriteCloser) clientCodecMsgpack {
	buf := bufio.NewWriter(conn)

	decoder := msgpack.NewDecoder(conn)
	// decoder.UsePreallocateValues(true)
	// decoder.DisableAllocLimit(true)

	encoder := msgpack.NewEncoder(buf)

	return clientCodecMsgpack{
		rwc:    conn,
		dec:    decoder,
		enc:    encoder,
		encBuf: buf,
	}
}

func (self clientCodecMsgpack) WriteRequest(r *requestHeader, body any) error {
	if err := self.enc.Encode(r); err != nil {
		return err
	}
	if err := self.enc.Encode(body); err != nil {
		return err
	}
	return self.encBuf.Flush()
}

func (self clientCodecMsgpack) ReadResponseHeader(r *responseHeader) error {
	return self.dec.Decode(r)
}

func (self clientCodecMsgpack) ReadResponseBody(body any) error {
	return self.dec.Decode(body)
}

func (self clientCodecMsgpack) ReadRawResponseBody() ([]byte, error) {
	return self.dec.DecodeRaw()
}

func (self clientCodecMsgpack) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

func (self clientCodecMsgpack) Close() error {
	return self.rwc.Close()
}

type serverCodecMsgpack struct {
	rwc    io.ReadWriteCloser
	dec    *msgpack.Decoder
	enc    *msgpack.Encoder
	encBuf *bufio.Writer
	closed bool
}

func NewServerCodecMsgpack(conn io.ReadWriteCloser) *serverCodecMsgpack {
	buf := bufio.NewWriter(conn)
	return &serverCodecMsgpack{
		rwc:    conn,
		dec:    msgpack.NewDecoder(conn),
		enc:    msgpack.NewEncoder(buf),
		encBuf: buf,
	}
}

func (c *serverCodecMsgpack) ReadRequestHeader(r *requestHeader) error {
	return c.dec.Decode(r)
}

func (c *serverCodecMsgpack) ReadRequestBody(body any) error {
	return c.dec.Decode(body)
}

func (c *serverCodecMsgpack) WriteResponse(r *responseHeader, body any) (err error) {
	if err = c.enc.Encode(r); err != nil {
		if c.encBuf.Flush() == nil {
			// Msgpack couldn't encode the header. Should not happen, so if it does,
			// shut down the connection to signal that the connection is broken.
			// log.Println("rpc: msgpack error encoding response:", err)
			c.Close()
		}
		return
	}
	if err = c.enc.Encode(body); err != nil {
		if c.encBuf.Flush() == nil {
			// Was a msgpack problem encoding the body but the header has been written.
			// Shut down the connection to signal that the connection is broken.
			// log.Println("rpc: msgpack error encoding body:", err)
			c.Close()
		}
		return
	}
	return c.encBuf.Flush()
}

func (c *serverCodecMsgpack) Close() error {
	if c.closed {
		// Only call c.rwc.Close once; otherwise the semantics are undefined.
		return nil
	}
	c.closed = true
	return c.rwc.Close()
}
