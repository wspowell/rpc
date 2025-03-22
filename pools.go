package rpc

import (
	"sync"
	"time"
)

var (
	poolDone = sync.Pool{
		New: func() any {
			return make(chan error, 1)
		},
	}

	poolTimeoutTicker = sync.Pool{
		New: func() any {
			return time.NewTicker(time.Second)
		},
	}

	poolRpcRequestHeaders = sync.Pool{
		New: func() any {
			return &requestHeader{}
		},
	}

	poolRpcResponseHeaders = sync.Pool{
		New: func() any {
			return &responseHeader{}
		},
	}

	poolCall = sync.Pool{
		New: func() any {
			return &call{}
		},
	}
)

func acquireDone() chan error {
	done, ok := poolDone.Get().(chan error)
	if !ok {
		panic("chan error pool encountered unexpected type")
	}

	// Drain any messages from the channel.
	draining := true
	for draining {
		select {
		case <-done:
		default:
			draining = false
		}
	}

	return done
}

func releaseDone(done chan error) {
	poolDone.Put(done)
}

func acquireTimeoutTicker(timeout time.Duration) *time.Ticker {
	ticker, ok := poolTimeoutTicker.Get().(*time.Ticker)
	if !ok {
		panic("*time.Ticker pool encountered unexpected type")
	}

	// Reset data for a clean object.
	ticker.Reset(timeout)

	return ticker
}

func releaseTimeoutTicker(ticker *time.Ticker) {
	ticker.Stop()
	poolTimeoutTicker.Put(ticker)
}

func acquireRpcRequestHeader() *requestHeader {
	request, ok := poolRpcRequestHeaders.Get().(*requestHeader)
	if !ok {
		panic("*requestHeader pool encountered unexpected type")
	}

	// Reset data for a clean object.
	request.SequenceId = 0
	request.HandleId = 0

	return request
}

func releaseRpcRequestHeader(request *requestHeader) {
	poolRpcRequestHeaders.Put(request)
}

func acquireRpcResponseHeader() *responseHeader {
	response, ok := poolRpcResponseHeaders.Get().(*responseHeader)
	if !ok {
		panic("*responseHeader pool encountered unexpected type")
	}

	// Reset data for a clean object.
	response.SequenceId = 0
	response.HandleId = 0
	response.Error = ""

	return response
}

func releaseRpcResponseHeader(response *responseHeader) {
	poolRpcResponseHeaders.Put(response)
}

func acquireCall() *call {
	rpcCall, ok := poolCall.Get().(*call)
	if !ok {
		panic("*call pool encountered unexpected type")
	}

	// Reset data for a clean object.
	rpcCall.handleId = 0
	rpcCall.Err = nil
	rpcCall.Req = nil
	rpcCall.Res = nil

	return rpcCall
}

func releaseCall(rpcCall *call) {
	poolCall.Put(rpcCall)
}
