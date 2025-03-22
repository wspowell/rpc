package rpc

import (
	"fmt"
	"io"
	"net"
	"time"
)

const (
	defaultCommandTimeout  = 3 * time.Second
	defaultKeepAlivePeriod = 10 * time.Second
)

var (
	ErrTimeout  = fmt.Errorf("command timed out")
	ErrShutdown = fmt.Errorf("connection is shut down")
)

// A ClientCodec implements writing of RPC requests and
// reading of RPC responses for the client side of an RPC session.
// The client calls [ClientCodec.WriteRequest] to write a request to the connection
// and calls [ClientCodec.ReadResponseHeader] and [ClientCodec.ReadResponseBody] in pairs
// to read responses. The client calls [ClientCodec.Close] when finished with the
// connection. ReadResponseBody may be called with a nil
// argument to force the body of the response to be read and then
// discarded.
// See [NewClient]'s comment for information about concurrent access.
type ClientCodec interface {
	WriteRequest(*requestHeader, any) error
	ReadResponseHeader(*responseHeader) error
	ReadResponseBody(any) error

	Close() error

	ReadRawResponseBody() ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type Client struct {
	tcpConnection    *net.TCPConn
	pendingRequests  chan clientRequest
	pendingResponses chan clientResponse
	rpcCodec         ClientCodec
	host             string
	port             string
	address          string
	commandTimeout   time.Duration
}

func NewClient(host string, port string) *Client {
	return &Client{
		host:             host,
		port:             port,
		address:          host + ":" + port,
		tcpConnection:    nil,
		commandTimeout:   defaultCommandTimeout,
		pendingRequests:  make(chan clientRequest),
		pendingResponses: make(chan clientResponse),
	}
}

func NewWithConnection(tcpConnection *net.TCPConn) *Client {
	return &Client{
		address:        tcpConnection.RemoteAddr().String(),
		tcpConnection:  tcpConnection,
		commandTimeout: defaultCommandTimeout,
	}
}

func (self *Client) Close() {
	if self.tcpConnection != nil {
		_ = self.tcpConnection.Close()
		self.tcpConnection = nil
	}
}

func (self *Client) Host() string {
	return self.host
}

func (self *Client) Port() string {
	return self.port
}

func (self *Client) Address() string {
	return self.address
}

func (self *Client) Connect() error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", self.address)
	if err != nil {
		return err
	}

	tcpConnection, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}

	self.tcpConnection = tcpConnection
	if keepAliveErr := self.tcpConnection.SetKeepAlive(true); keepAliveErr != nil {
		return keepAliveErr
	}
	if keepAliveErr := self.tcpConnection.SetKeepAlivePeriod(defaultKeepAlivePeriod); keepAliveErr != nil {
		return keepAliveErr
	}
	if keepAliveErr := self.tcpConnection.SetNoDelay(false); keepAliveErr != nil {
		return keepAliveErr
	}

	self.rpcCodec = newClientCodecMsgpack(self.tcpConnection)

	go self.processPendingCommands()
	go self.processCommandResponses()

	return nil
}

func (self *Client) send(cmd *call) error {
	if self.tcpConnection == nil {
		return ErrShutdown
	}

	if self.rpcCodec == nil {
		return ErrShutdown
	}

	if cmd.handleId == 0 {
		return fmt.Errorf("handle id cannot be 0")
	}

	timeout := acquireTimeoutTicker(self.commandTimeout)

	done := acquireDone()

	select {
	case self.pendingRequests <- clientRequest{
		cmd:  cmd,
		done: done,
	}:
	case <-timeout.C:
		releaseTimeoutTicker(timeout)
		// Do not release "done" since we do not know if it is still in use.
		return ErrTimeout
	}

	select {
	case err := <-done:
		releaseTimeoutTicker(timeout)
		releaseDone(done)
		return err
	case <-timeout.C:
		releaseTimeoutTicker(timeout)
		// Do not release "done" since we do not know if it is still in use.
		return ErrTimeout
	}
}

func (self *Client) processPendingCommands() {
	requestsInFlight := map[uint64]clientRequest{}
	var nextSequenceId uint64

	defer func() {
		for index := range requestsInFlight {
			requestsInFlight[index].done <- ErrShutdown
		}
	}()

	for {
		select {
		case pendingRequest := <-self.pendingRequests:
			nextSequenceId++
			sequenceId := nextSequenceId
			pendingRequest.sequenceId = sequenceId

			requestsInFlight[pendingRequest.sequenceId] = pendingRequest

			reqHeader := acquireRpcRequestHeader()
			reqHeader.SequenceId = sequenceId
			reqHeader.HandleId = pendingRequest.cmd.handleId

			if err := self.rpcCodec.WriteRequest(reqHeader, pendingRequest.cmd.Req); err != nil {
				pendingRequest.done <- err
				close(pendingRequest.done)
			}

			releaseRpcRequestHeader(reqHeader)
		case pendingResponse, open := <-self.pendingResponses:
			if !open {
				// Any pending requests will get a shutdown error during the defer.
				return
			}

			if pendingResponse.callErr == io.EOF {
				// Connection is closed.
				return
			}

			// Request is guaranteed to be in the map.
			pendingRequest := requestsInFlight[pendingResponse.sequenceId]
			if pendingRequest.sequenceId == 0 {
				panic("sequence id is 0, something is very wrong")
			}

			delete(requestsInFlight, pendingResponse.sequenceId)
			// Only bother with the Unmarshal if there was no error and if there was actually a response body to process.
			if pendingResponse.err == nil && pendingResponse.callErr == nil && len(pendingResponse.replyBytes) != 0 {
				if err := self.rpcCodec.Unmarshal(pendingResponse.replyBytes, pendingRequest.cmd.Res); err != nil {
					pendingResponse.callErr = err
				} else {
					pendingResponse.callErr = nil
				}
			}

			if pendingResponse.callErr != nil {
				pendingRequest.done <- pendingResponse.callErr
			} else if pendingResponse.err != nil {
				pendingRequest.done <- pendingResponse.err
			} else {
				pendingRequest.done <- nil
			}

			// Do not close "done" since it is handed back to a sync.Pool.
		}
	}
}

type clientRequest struct {
	done       chan error
	cmd        *call
	sequenceId uint64
}

type clientResponse struct {
	err        error
	callErr    error
	replyBytes []byte
	sequenceId uint64
}

func (self *Client) processCommandResponses() {
	for {
		resHeader := acquireRpcResponseHeader()
		if err := self.rpcCodec.ReadResponseHeader(resHeader); err != nil {
			releaseRpcResponseHeader(resHeader)
			close(self.pendingResponses)
			return
		}

		response := clientResponse{
			sequenceId: resHeader.SequenceId,
			replyBytes: nil,
			err:        nil,
			callErr:    nil,
		}
		switch {
		case resHeader.Error != "":
			// We've got an error response. Give this to the request;
			// any subsequent requests will get the ReadResponseBody
			// error if there is one.
			response.err = fmt.Errorf("%s", resHeader.Error)
			_ = self.rpcCodec.ReadResponseBody(nil)
		default:
			response.replyBytes, response.callErr = self.rpcCodec.ReadRawResponseBody()
		}

		releaseRpcResponseHeader(resHeader)
		self.pendingResponses <- response
	}
}
