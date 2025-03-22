package rpc

import (
	"fmt"
	"sync"
)

type serverHandleFunc func(codec ServerCodec, pendingResponses chan<- serverResponse, pendingResponse serverResponse, waitGroup *sync.WaitGroup)

type Handle[Req any, Res any] struct {
	handleFn func(Req) (Res, error)
	Call     func(*Client, Req) (Res, error)
	handleId uint64
}

func NewHandle[Req any, Res any](handleId uint64) *Handle[Req, Res] {
	return &Handle[Req, Res]{
		handleId: handleId,
		handleFn: func(Req) (Res, error) {
			var zero Res
			return zero, fmt.Errorf("not implemented: call SetHandler()")
		},
		Call: func(client *Client, req Req) (Res, error) {
			var res Res

			rpcCall := acquireCall()
			rpcCall.handleId = handleId
			rpcCall.Req = req
			rpcCall.Res = &res
			rpcCall.Err = nil

			err := client.send(rpcCall)

			releaseCall(rpcCall)

			return res, err
		},
	}
}

func (self *Handle[Req, Res]) SetHandler(handlerFn func(Req) (Res, error)) {
	self.handleFn = handlerFn
}

func (self *Handle[Req, Res]) RegisterServer(server *Server) {
	if _, exists := server.handlerFns[self.handleId]; exists {
		panic(fmt.Sprintf("handler id already registered: %d", self.handleId))
	}
	server.handlerFns[self.handleId] = func(codec ServerCodec, pendingResponses chan<- serverResponse, pendingResponse serverResponse, waitGroup *sync.WaitGroup) {
		var req Req
		if err := codec.ReadRequestBody(&req); err != nil {
			pendingResponse.callErr = err
			pendingResponses <- pendingResponse
			waitGroup.Done()
			return
		}

		go func(req Req, pendingResponses chan<- serverResponse, pendingResponse serverResponse, waitGroup *sync.WaitGroup) {
			defer waitGroup.Done() // Defer because we have no control over handler().
			pendingResponse.responseBody, pendingResponse.err = self.handleFn(req)
			pendingResponses <- pendingResponse
		}(req, pendingResponses, pendingResponse, waitGroup)
	}
}

type call struct {
	Req      any
	Res      any
	Err      error
	handleId uint64
}

// requestHeader is a header written before every RPC call. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type requestHeader struct {
	HandleId   uint64
	SequenceId uint64 // sequence number chosen by client
}

// responseHeader is a header written before every RPC return. It is used internally
// but documented here as an aid to debugging, such as when analyzing
// network traffic.
type responseHeader struct {
	Error      string // error, if any.
	HandleId   uint64 // echoes that of the request header
	SequenceId uint64 // echoes that of the request header
}
