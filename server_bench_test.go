package rpc_test

import (
	"context"
	"errors"
	"log"
	"net"
	gorpc "net/rpc"
	"sync"
	"testing"

	"github.com/wspowell/rpc"
)

// Codec: gob
// goos: linux
// goarch: amd64
// pkg: diskey/pkg/server
// cpu: AMD Ryzen 9 4900HS with Radeon Graphics
// BenchmarkTest-8            	    7822	    145434 ns/op	     502 B/op	      16 allocs/op
// BenchmarkPing-8            	    7725	    157496 ns/op	    2480 B/op	      24 allocs/op
// BenchmarkPing_parallel-8   	   35712	     34134 ns/op	    2478 B/op	      24 allocs/op

// Codec: msgpack
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

type Args struct {
	A, B int
}

type Quotient struct {
	Quo, Rem int
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *int) error {
	*reply = args.A * args.B
	return nil
}

func (t *Arith) Divide(args *Args, quo *Quotient) error {
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
	// fmt.Printf("Arith: %d*%d=%d", args.A, args.B, reply)
}

var (
	testServer *rpc.Server
	testClient *rpc.Client

	pingHandler = rpc.NewHandle[struct{}, bool](1)
)

func init() {
	ctx := context.Background()
	port := "7100"

	testServer = rpc.NewServer("localhost", port)

	pingHandler.SetHandler(testServer.Ping)
	pingHandler.RegisterServer(testServer)

	listener, listenErr := testServer.Listen(ctx)
	if listenErr != nil {
		panic(listenErr)
	}

	go testServer.AcceptConnections(listener)

	testClient = rpc.NewClient("localhost", port)
	if connectErr := testClient.Connect(); connectErr != nil {
		panic(connectErr)
	}
}

func BenchmarkPing(b *testing.B) {
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	// // Start CPU profiling
	// fileCpu, err := os.Create("cpu.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// fileGoroutine, err := os.Create("goroutine.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// fileHeap, err := os.Create("heap.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// fileAllocs, err := os.Create("allocs.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// fileThreadcreate, err := os.Create("threadcreate.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// fileBlock, err := os.Create("block.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// fileMutex, err := os.Create("mutex.pprof")
	// if err != nil {
	// 	panic(err)
	// }

	if testClient == nil {
		panic("client is nil")
	}

	// pprof.StartCPUProfile(fileCpu)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := pingHandler.Call(testClient, struct{}{})
		if err != nil {
			panic(err)
		}

		if !result {
			panic("ping failed")
		}
	}

	b.StopTimer()
	// pprof.StopCPUProfile()
	// pprof.Lookup("goroutine").WriteTo(fileGoroutine, 0)
	// pprof.Lookup("heap").WriteTo(fileHeap, 0)
	// pprof.Lookup("allocs").WriteTo(fileAllocs, 0)
	// pprof.Lookup("threadcreate").WriteTo(fileThreadcreate, 0)
	// pprof.Lookup("block").WriteTo(fileBlock, 0)
	// pprof.Lookup("mutex").WriteTo(fileMutex, 0)
}

func BenchmarkPing_parallel(b *testing.B) {
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	// // Start CPU profiling
	// f, err := os.Create("cpu.pprof")
	// if err != nil {
	// 	panic(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result, err := pingHandler.Call(testClient, struct{}{})
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
