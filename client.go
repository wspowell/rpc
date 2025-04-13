package rpc

import (
	"errors"
	"io"
	"net"
	"time"
)

const (
	connTypeTcp = "tcp"

	defaultCommandTimeout  = 3 * time.Second
	defaultKeepAlivePeriod = 10 * time.Second
)

var (
	ErrTimeout         = errors.New("command timed out")
	ErrShutdown        = errors.New("connection is shut down")
	ErrInvalidHandleId = errors.New("handle id cannot be 0")
)

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
	// WriteRequest to connection.
	WriteRequest(reqHeader *requestHeader, reqBody any) error
	// ReadResponseHeader from the connection.
	ReadResponseHeader(resHeader *responseHeader) error
	// ReadResponseBody from the connection.
	ReadResponseBody(resBody any) error

	// Close the underlying connection.
	Close() error

	// ReadRawResponseBody to avoid unmarshaling data prematurely.
	ReadRawResponseBody() ([]byte, error)
	// Unmarshal data into a value using the underlying codec's method.
	Unmarshal(data []byte, value any) error
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
		rpcCodec:         nil,
	}
}

func (self *Client) Close() error {
	if self.tcpConnection == nil {
		return nil
	}

	err := self.tcpConnection.Close()

	self.tcpConnection = nil

	return err
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
	if tcpConnection, err := openTcpConnectionToAddress(self.address); err != nil {
		return err
	} else {
		self.tcpConnection = tcpConnection
	}

	if connSettingErr := setTcpConnectionSettings(self.tcpConnection); connSettingErr != nil {
		return connSettingErr
	}

	self.rpcCodec = newClientCodecMsgpack(self.tcpConnection)

	go self.processPendingCommands()
	go self.processCommandResponses()

	return nil
}

func openTcpConnectionToAddress(address string) (*net.TCPConn, error) {
	tcpAddr, err := net.ResolveTCPAddr(connTypeTcp, address)
	if err != nil {
		return nil, err
	}

	return net.DialTCP(connTypeTcp, nil, tcpAddr)
}

func setTcpConnectionSettings(tcpConnection *net.TCPConn) error {
	if keepAliveErr := tcpConnection.SetKeepAlive(true); keepAliveErr != nil {
		return keepAliveErr
	}

	if keepAlivePeriodErr := tcpConnection.SetKeepAlivePeriod(defaultKeepAlivePeriod); keepAlivePeriodErr != nil {
		return keepAlivePeriodErr
	}

	return tcpConnection.SetNoDelay(false)
}

func (self *Client) send(cmd *call) error {
	if self.tcpConnection == nil {
		return ErrShutdown
	}

	if self.rpcCodec == nil {
		return ErrShutdown
	}

	if cmd.handleId == 0 {
		return ErrInvalidHandleId
	}

	timeout := acquireTimeoutTicker(self.commandTimeout)

	done := acquireDone()

	select {
	case self.pendingRequests <- clientRequest{
		sequenceId: 0, // Sequence ID is assigned later.
		cmd:        cmd,
		done:       done,
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

			if errors.Is(pendingResponse.callErr, io.EOF) {
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
				pendingResponse.callErr = self.rpcCodec.Unmarshal(pendingResponse.replyBytes, pendingRequest.cmd.Res)
			}

			switch {
			case pendingResponse.callErr != nil:
				pendingRequest.done <- pendingResponse.callErr
			case pendingResponse.err != nil:
				pendingRequest.done <- pendingResponse.err
			default:
				pendingRequest.done <- nil
			}
			// Do not close "done" since it is handed back to a sync.Pool.
		}
	}
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
			response.err = errors.New(resHeader.Error) //nolint:err113 // reason: This is creating a brand new dynamic error from a response, so this is fine.
			// Discard the response body.
			_ = self.rpcCodec.ReadResponseBody(nil) //nolint:errcheck // reason: We always want to respond with the header error so ignore this error.
		default:
			response.replyBytes, response.callErr = self.rpcCodec.ReadRawResponseBody()
		}

		releaseRpcResponseHeader(resHeader)
		self.pendingResponses <- response
	}
}
