package rpc_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wspowell/rpc"
)

func Test_Server_Handler(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	port := "7000"

	testServer := rpc.NewServer("localhost", port)
	defer testServer.Close()

	pingHandler := rpc.NewHandle[struct{}, bool](1)
	pingHandler.SetHandler(testServer.Ping)
	pingHandler.RegisterServer(testServer)

	testExtension := rpc.NewHandle[testHandlerArgs, testHandlerReply](2)
	testExtension.SetHandler(extHandler)
	testExtension.RegisterServer(testServer)

	listener, listenErr := testServer.ListenTcp(ctx)
	require.NoError(t, listenErr)

	go testServer.AcceptTcpConnections(listener)

	testClient := rpc.NewClient("localhost", port)
	connectErr := testClient.Connect()
	require.NoError(t, connectErr)

	// Test Case: ping.
	{
		response, err := pingHandler.Call(testClient, struct{}{})
		require.NoError(t, err)
		assert.True(t, response)
	}

	// Test Case: extension.
	{
		response, err := testExtension.Call(testClient, testHandlerArgs{
			Input: "test",
		})
		require.NoError(t, err)
		assert.Equal(t, testHandlerReply{
			Output: "success",
		}, response)
	}
}

type testHandlerArgs struct {
	Input string
}

type testHandlerReply struct {
	Output string
}

func extHandler(req testHandlerArgs) (testHandlerReply, error) {
	if req.Input == "test" {
		return testHandlerReply{
			Output: "success",
		}, nil
	}

	if req.Input == "error" {
		return testHandlerReply{
			Output: "failure",
		}, fmt.Errorf("error")
	}

	return testHandlerReply{
		Output: "failure",
	}, nil
}

func Test_Server_close(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	port := "7001"

	waitGroup := &sync.WaitGroup{}

	{
		testServer := rpc.NewServer("localhost", port)

		pingHandler := rpc.NewHandle[struct{}, bool](1)
		pingHandler.SetHandler(testServer.Ping)
		pingHandler.RegisterServer(testServer)

		listener, listenErr := testServer.ListenTcp(ctx)
		require.NoError(t, listenErr)

		go testServer.AcceptTcpConnections(listener)

		testClient := rpc.NewClient("localhost", port)
		connectErr := testClient.Connect()
		require.NoError(t, connectErr)

		_, err := pingHandler.Call(testClient, struct{}{})
		require.NoError(t, err)

		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			_, pingErr := pingHandler.Call(testClient, struct{}{})
			assert.NoError(t, pingErr)
		}()

		testServer.Close()
	}

	{
		testServer := rpc.NewServer("localhost", port)

		pingHandler := rpc.NewHandle[struct{}, bool](1)
		pingHandler.SetHandler(testServer.Ping)
		pingHandler.RegisterServer(testServer)

		listener, listenErr := testServer.ListenTcp(ctx)
		require.NoError(t, listenErr)

		go testServer.AcceptTcpConnections(listener)

		testClient := rpc.NewClient("localhost", port)
		connectErr := testClient.Connect()
		require.NoError(t, connectErr)

		_, err := pingHandler.Call(testClient, struct{}{})
		require.NoError(t, err)

		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			_, pingErr := pingHandler.Call(testClient, struct{}{})
			assert.NoError(t, pingErr)
		}()

		testServer.Close()
	}

	waitGroup.Wait()
}

func Test_Server_error(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	port := "7002"

	testServer := rpc.NewServer("localhost", port)

	testExtension := rpc.NewHandle[testHandlerArgs, testHandlerReply](2)
	testExtension.SetHandler(extHandler)
	testExtension.RegisterServer(testServer)

	listener, listenErr := testServer.ListenTcp(ctx)
	require.NoError(t, listenErr)

	go testServer.AcceptTcpConnections(listener)

	testClient := rpc.NewClient("localhost", port)
	connectErr := testClient.Connect()
	require.NoError(t, connectErr)

	_, err := testExtension.Call(testClient, testHandlerArgs{
		Input: "error",
	})
	assert.Equal(t, "error", err.Error())

	testServer.Close()

	time.Sleep(time.Second)
}
