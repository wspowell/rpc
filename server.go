package rpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type ServerCodec interface {
	ReadRequestHeader(*requestHeader) error
	ReadRequestBody(any) error
	WriteResponse(*responseHeader, any) error

	// Close can be called multiple times and must be idempotent.
	Close() error
}

type Server struct {
	handlerFns      map[uint64]serverHandleFunc
	netListener     net.Listener
	host            string
	port            string
	address         string
	keepAlive       time.Duration
	commandTimeout  time.Duration
	cancelFn        context.CancelFunc
	openConnections sync.WaitGroup
}

func NewServer(host string, port string) *Server {
	return &Server{
		host:            host,
		port:            port,
		address:         host + ":" + port,
		keepAlive:       defaultKeepAlivePeriod,
		commandTimeout:  defaultCommandTimeout,
		handlerFns:      map[uint64]serverHandleFunc{},
		cancelFn:        nil,
		openConnections: sync.WaitGroup{},
		netListener:     nil,
	}
}

func (self *Server) Close() {
	if self.netListener != nil {
		_ = self.netListener.Close()
		self.netListener = nil
	}

	if self.cancelFn != nil {
		self.cancelFn()
		self.cancelFn = nil
	}

	self.openConnections.Wait()
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

func (self *Server) Listen(ctx context.Context) (net.Listener, error) {
	ctx, cancelFn := context.WithCancel(ctx)
	self.cancelFn = cancelFn

	listenConfig := &net.ListenConfig{
		Control:   nil,
		KeepAlive: self.keepAlive,
	}

	netListener, err := listenConfig.Listen(ctx, "tcp", self.address)
	if err != nil {
		return nil, err
	}

	return netListener, nil
}

func (self *Server) AcceptConnections(netListener net.Listener) {
	self.netListener = netListener

	var connectionId uint64
	var netConnection net.Conn
	var err error

	netConnections := []net.Conn{}
	for {
		netConnection, err = netListener.Accept()
		if err != nil {
			break
		}

		netConnections = append(netConnections, netConnection)

		asTcpConnection, ok := netConnection.(*net.TCPConn)
		if !ok {
			continue
		}

		connectionId++

		if keepAliveErr := asTcpConnection.SetKeepAlive(true); keepAliveErr != nil {
			continue
		}
		if keepAliveErr := asTcpConnection.SetKeepAlivePeriod(defaultKeepAlivePeriod); keepAliveErr != nil {
			continue
		}
		if keepAliveErr := asTcpConnection.SetNoDelay(false); keepAliveErr != nil {
			continue
		}

		go self.ServeCodec(NewServerCodecMsgpack(asTcpConnection))
	}

	for index := range netConnections {
		_ = netConnections[index].Close()
	}
}

func (self *Server) ServeCodec(codec ServerCodec) {
	pendingResponses := make(chan serverResponse)
	doneProcessingResponses := processResponses(codec, pendingResponses)

	waitGroup := &sync.WaitGroup{}

	for {
		reqHeader := acquireRpcRequestHeader()
		if err := codec.ReadRequestHeader(reqHeader); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
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
			// Discard body
			_ = codec.ReadRequestBody(nil)

			waitGroup.Add(1)

			pendingResponses <- serverResponse{
				handleId:     reqHeader.HandleId,
				sequenceId:   reqHeader.SequenceId,
				responseBody: struct{}{},
				callErr:      fmt.Errorf("handle not registered"),
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
