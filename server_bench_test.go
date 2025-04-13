package rpc_test

import (
	"context"
	"errors"
	"log"
	"net"
	gorpc "net/rpc" //nolint:importas // reason: Import "net/rpc" for benchmark comparisons.
	"sync"
	"testing"

	"github.com/wspowell/rpc"
)

// Codec: gob.
// goos: linux
// goarch: amd64
// pkg: diskey/pkg/server
// cpu: AMD Ryzen 9 4900HS with Radeon Graphics
// BenchmarkTest-8            	    7822	    145434 ns/op	     502 B/op	      16 allocs/op
// BenchmarkPing-8            	    7725	    157496 ns/op	    2480 B/op	      24 allocs/op
// BenchmarkPing_parallel-8   	   35712	     34134 ns/op	    2478 B/op	      24 allocs/op

// Codec: msgpack.
// goos: linux
// goarch: amd64
// pkg: diskey/pkg/server
// cpu: AMD Ryzen 9 4900HS with Radeon Graphics
// BenchmarkTest-8            	    8336	    140708 ns/op	     501 B/op	      16 allocs/op
// BenchmarkPing-8            	    8246	    144255 ns/op	    2416 B/op	      20 allocs/op
// BenchmarkPing_parallel-8   	   35959	     32518 ns/op	    2424 B/op	      20 allocs/op

// I re-implemented the rpc.Client to improve how it works.
// Codec: msgpack
// goos: linux
// goarch: amd64
// pkg: diskey/pkg/rpc
// cpu: AMD Ryzen 9 4900HS with Radeon Graphics
// BenchmarkTest-8                     9289            130344 ns/op             514 B/op         16 allocs/op
// BenchmarkPing-8                     9007            129679 ns/op             273 B/op          9 allocs/op
// BenchmarkPing_parallel-8           36468             30683 ns/op             273 B/op          9 allocs/op

// Completely reimplemented the rpc package.
// goos: linux
// goarch: amd64
// pkg: diskey/pkg/rpc
// cpu: AMD Ryzen 9 4900HS with Radeon Graphics
// BenchmarkTest-8                     8799            118338 ns/op             515 B/op         16 allocs/op
// BenchmarkPing-8                    13536            105715 ns/op             184 B/op          5 allocs/op
// BenchmarkPing_parallel-8           43266             27022 ns/op             181 B/op          5 allocs/op

// go test -timeout 30s -benchmem -bench=. -run ^$ ./
// goos: linux
// goarch: amd64
// pkg: diskey/pkg/rpc
// cpu: AMD Ryzen 9 4900HS with Radeon Graphics
// BenchmarkTest-8                     9681            133869 ns/op             515 B/op         16 allocs/op
// BenchmarkPing-8                     9212            120137 ns/op             185 B/op          5 allocs/op
// BenchmarkPing_parallel-8           38565             30547 ns/op             181 B/op          5 allocs/op
// PASS
// ok      diskey/pkg/rpc  4.625s

// go test -timeout 30s -benchmem -bench=. -run ^$ ./
// goos: linux
// goarch: amd64
// pkg: github.com/wspowell/rpc
// cpu: AMD Ryzen 9 4900HS with Radeon Graphics
// BenchmarkTest-8                     7603            137472 ns/op             515 B/op         16 allocs/op
// BenchmarkPing-8                    10000            124340 ns/op             177 B/op          4 allocs/op
// BenchmarkPing_parallel-8           39692             29686 ns/op             173 B/op          4 allocs/op
// PASS
// ok      github.com/wspowell/rpc 4.471s

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (_ *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (_ *Arith) Divide(args *Args, quo *Quotient) error {
	if args.B == 0 {
		return errors.New("divide by zero")
	}
	quo.Quo = args.A / args.B
	quo.Rem = args.A % args.B
	return nil
}

var registerOnce = &sync.Once{}

func BenchmarkTest(b *testing.B) {
	arith := new(Arith)

	registerOnce.Do(func() {
		gorpc.Register(arith)
	})

	l, err := net.Listen("tcp", "localhost:1236")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	defer l.Close()
	go func() {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		gorpc.ServeConn(conn)
	}()

	client, err := gorpc.Dial("tcp", "localhost:1236")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Synchronous call
		args := &Args{7, 8}
		var reply int
		err = client.Call("Arith.Multiply", args, &reply)
		if err != nil {
			log.Fatal("arith error:", err)
		}
	}
	b.StopTimer()
}

var (
	_testServer *rpc.Server
	_testClient *rpc.Client

	_pingHandler = rpc.NewHandle[struct{}, bool](1)
)

func init() {
	ctx := context.Background()
	port := "7100"

	_testServer = rpc.NewServer("localhost", port)

	_pingHandler.SetHandler(_testServer.Ping)
	_pingHandler.RegisterServer(_testServer)

	listener, listenErr := _testServer.ListenTcp(ctx)
	if listenErr != nil {
		panic(listenErr)
	}

	go _testServer.AcceptTcpConnections(listener)

	_testClient = rpc.NewClient("localhost", port)
	if connectErr := _testClient.Connect(); connectErr != nil {
		panic(connectErr)
	}
}

func BenchmarkPing(b *testing.B) {
	if _testClient == nil {
		panic("client is nil")
	}

	b.ResetTimer()

	for range b.N {
		result, err := _pingHandler.Call(_testClient, struct{}{})
		if err != nil {
			panic(err)
		}

		if !result {
			panic("ping failed")
		}
	}

	b.StopTimer()
}

func BenchmarkPing_parallel(b *testing.B) {
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result, err := _pingHandler.Call(_testClient, struct{}{})
			if err != nil {
				panic(err)
			}

			if !result {
				panic("ping failed")
			}
		}
	})

	b.StopTimer()
}
