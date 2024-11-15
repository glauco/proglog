package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/glauco/proglog/api/v1"
	"github.com/glauco/proglog/internal/config"
	"github.com/glauco/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// TestServer runs multiple scenarios to verify the behavior of the gRPC server.
func TestServer(t *testing.T) {
	// Define scenarios and their associated test functions
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		// Run each scenario as a sub-test for better isolation and reporting
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown() // Ensure the server and resources are properly cleaned up after the test
			fn(t, client, config)
		})
	}
}

// setupTest sets up a test environment for the server.
// It starts a gRPC server, creates a log client, and returns a teardown function to clean up resources.
func setupTest(t *testing.T, fn func(*Config)) (client api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	// Start a TCP listener on a random available port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile: config.CAFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)

	// Set up gRPC dial options for connecting to the server with TLS encryption
	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
	// Create a new gRPC client connection
	cc, err := grpc.NewClient(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	client = api.NewLogClient(cc)

	// Create a temporary directory for the log files
	dir := t.TempDir()
	defer os.RemoveAll(dir)

	// Initialize a new log instance using the temporary directory
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	// Set up the server configuration with the initialized log
	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg) // If provided, apply additional configuration modifications
	}

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	// Create the gRPC server using the configuration
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	// Start the server in a separate goroutine
	go func() {
		server.Serve(l)
	}()

	// Create the gRPC log client
	client = api.NewLogClient(cc)

	// Return the client, configuration, and a teardown function to clean up resources
	return client, cfg, func() {
		server.Stop() // Stop the gRPC server
		cc.Close()    // Close the client connection
		l.Close()     // Close the network listener
		clog.Remove() // Remove the log files
	}
}

// testProduceConsume tests that a record can be produced to the log and then successfully consumed.
func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	// Define the record that will be appended to the log
	want := &api.Record{
		Value: []byte("hello world"),
	}

	// Produce the record to the log using the gRPC client
	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	// Consume the record from the log using the returned offset
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)

	// Verify that the consumed value matches the produced value
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, produce.Offset, consume.Record.Offset)
}

// testProduceConsumeStream tests that records can be produced and consumed using gRPC streaming.
func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	// Define multiple records to be appended to the log
	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	// Test producing records using a stream
	{
		// Create a new stream for producing records
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		// Send each record and verify the response
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Offset, uint64(offset)) // Verify the offset matches the expected value
		}
	}

	// Test consuming records using a stream
	{
		// Create a new stream for consuming records starting at offset 0
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		// Receive each record and verify the response
		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			}) // Verify the received record matches the expected value
		}
	}
}

// testConsumePastBoundary tests that consuming a record past the end of the log returns an error.
func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	// Produce a single record to the log
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	// Attempt to consume a record at an offset beyond the current highest offset
	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	require.Nil(t, consume) // Ensure no record is returned
	got := status.Code(err) // Get the gRPC error code
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got) // Ensure the error code matches "offset out of range"
}
