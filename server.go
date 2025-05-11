package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

var (
	ErrHandleNotRegistered = errors.New("handle not registered")
	ErrListenerInvalid     = errors.New("invalid listener")
)

type ServerCodec interface {
	// ReadRequestHeader from the connection.
	ReadRequestHeader(reqHeader *requestHeader) error
	// ReadRequestBody from the connection.
	ReadRequestBody(reqBody any) error
	// WriteResponse to the connection.
	WriteResponse(resHeader *responseHeader, resBody any) error

	// Close the connection.
	// Close can be called multiple times and must be idempotent.
	Close() error
}

type Server struct {
	netListener net.Listener
	handlerFns  map[uint64]serverHandleFunc
	cancelFn    context.CancelFunc
	host        string
	port        string
	address     string
	keepAlive   time.Duration
}

func NewServer(host string, port string) *Server {
	return &Server{
		host:        host,
		port:        port,
		address:     host + ":" + port,
		keepAlive:   defaultKeepAlivePeriod,
		handlerFns:  map[uint64]serverHandleFunc{},
		cancelFn:    nil,
		netListener: nil,
	}
}

func (self *Server) Close() error {
	var err error

	if self.netListener != nil {
		err = self.netListener.Close()
		self.netListener = nil
	}

	if self.cancelFn != nil {
		self.cancelFn()

		self.cancelFn = nil
	}

	return err
}

func (_ *Server) Ping(struct{}) (bool, error) {
	return true, nil
}

func (self *Server) Address() string {
	return self.address
}

func (self *Server) Host() string {
	return self.host
}

func (self *Server) Port() string {
	return self.port
}

type serverResponse struct {
	err          error
	callErr      error
	responseBody any
	sequenceId   uint64
	handleId     uint64
}

func (self *Server) ListenTcp(ctx context.Context) (*net.TCPListener, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	self.cancelFn = cancelFn

	listenConfig := &net.ListenConfig{
		Control:   nil,
		KeepAlive: self.keepAlive,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     15 * time.Second, //nolint:mnd,revive // reason: This is the default value.
			Interval: 15 * time.Second, //nolint:mnd,revive // reason: This is the default value.
			Count:    9,                //nolint:mnd,revive // reason: This is the default value.
		},
	}

	netListener, err := listenConfig.Listen(ctx, connTypeTcp, self.address)
	if err != nil {
		return nil, err
	}

	if asTcpListener, ok := netListener.(*net.TCPListener); ok {
		return asTcpListener, nil
	}

	return nil, fmt.Errorf("%w: listener is not a *net.TCPListener", ErrListenerInvalid)
}

func (self *Server) AcceptTcpConnections(tcpListener *net.TCPListener) {
	self.netListener = tcpListener

	closed := make(chan struct{})

	for {
		tcpConnection, err := tcpListener.AcceptTCP()
		if err != nil {
			break
		}

		if keepAliveErr := tcpConnection.SetKeepAlive(true); keepAliveErr != nil {
			continue
		}

		if keepAliveErr := tcpConnection.SetKeepAlivePeriod(defaultKeepAlivePeriod); keepAliveErr != nil {
			continue
		}

		if keepAliveErr := tcpConnection.SetNoDelay(false); keepAliveErr != nil {
			continue
		}

		go func() {
			<-closed
			tcpConnection.Close()
		}()
		go self.ServeCodec(NewServerCodecMsgpack(tcpConnection))
	}

	closed <- struct{}{}
}

func (self *Server) ServeCodec(codec ServerCodec) {
	pendingResponses := make(chan serverResponse)
	doneProcessingResponses := processResponses(codec, pendingResponses)

	waitGroup := &sync.WaitGroup{}

	for {
		reqHeader := acquireRpcRequestHeader()
		if err := codec.ReadRequestHeader(reqHeader); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				releaseRpcRequestHeader(reqHeader)

				break
			}

			releaseRpcRequestHeader(reqHeader)

			break
		}

		// Process the request by calling the handle function.
		if handleFn, exists := self.handlerFns[reqHeader.HandleId]; exists {
			pendingResponse := serverResponse{
				handleId:     reqHeader.HandleId,
				sequenceId:   reqHeader.SequenceId,
				responseBody: nil,
				callErr:      nil,
				err:          nil,
			}

			waitGroup.Add(1)
			handleFn(codec, pendingResponses, pendingResponse, waitGroup)
		} else {
			// Discard body.
			_ = codec.ReadRequestBody(nil) //nolint:errcheck // reason: We just want to discard the body so ignore the error.

			waitGroup.Add(1)

			pendingResponses <- serverResponse{
				handleId:     reqHeader.HandleId,
				sequenceId:   reqHeader.SequenceId,
				responseBody: struct{}{},
				callErr:      ErrHandleNotRegistered,
				err:          nil,
			}

			waitGroup.Done()
		}

		releaseRpcRequestHeader(reqHeader)
	}

	waitGroup.Wait()

	close(pendingResponses)

	<-doneProcessingResponses

	_ = codec.Close()
}

func processResponses(codec ServerCodec, pendingResponses <-chan serverResponse) <-chan struct{} {
	done := make(chan struct{})

	go func(codec ServerCodec, pendingResponses <-chan serverResponse) {
		for pendingResponse := range pendingResponses {
			resHeader := acquireRpcResponseHeader()
			resHeader.HandleId = pendingResponse.handleId
			resHeader.SequenceId = pendingResponse.sequenceId

			if pendingResponse.callErr != nil {
				resHeader.Error = pendingResponse.callErr.Error()
			} else if pendingResponse.err != nil {
				resHeader.Error = pendingResponse.err.Error()
			}

			_ = codec.WriteResponse(resHeader, pendingResponse.responseBody)

			releaseRpcResponseHeader(resHeader)
		}

		done <- struct{}{}
	}(codec, pendingResponses)

	return done
}
