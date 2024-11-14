package server

import (
	"context"

	api "github.com/glauco/proglog/api/v1"
	"google.golang.org/grpc"
)

// Config contains the dependencies required by the gRPC server.
type Config struct {
	CommitLog CommitLog // CommitLog is an interface used to append and read log records.
}

// Ensure grpcServer implements the api.LogServer interface.
// This helps catch implementation errors during compile time.
var _ api.LogServer = (*grpcServer)(nil)

// grpcServer implements the gRPC log server API.
// It embeds the generated UnimplementedLogServer for forward compatibility.
type grpcServer struct {
	api.UnimplementedLogServer // Provides default implementations of the LogServer methods.
	*Config                    // Embeds the configuration, including the CommitLog interface.
}

// newgrpcServer creates a new gRPC server instance.
// It takes a Config object and returns a pointer to a grpcServer.
func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config, // Assign the provided configuration
	}
	return srv, nil
}

// Produce handles producing (adding) a record to the commit log.
// It returns the offset at which the record was stored.
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	// Append the record to the commit log
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err // Return an error if the append fails
	}
	// Return the offset of the new record in the ProduceResponse
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume handles reading a record from the commit log at a given offset.
// It returns the record in a ConsumeResponse.
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	// Read the record from the commit log at the given offset
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err // Return an error if reading fails
	}
	// Return the record in a ConsumeResponse
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream handles a bidirectional stream where the client sends multiple ProduceRequests,
// and the server responds with multiple ProduceResponses.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		// Receive the next ProduceRequest from the stream
		req, err := stream.Recv()
		if err != nil {
			return err // Return error if the client closes the stream or any other error occurs
		}
		// Produce the record and get a response
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err // Return error if appending to the log fails
		}
		// Send the ProduceResponse back to the client
		if err = stream.Send(res); err != nil {
			return err // Return error if sending the response fails
		}
	}
}

// ConsumeStream handles a server-side streaming RPC where the client requests a stream
// starting at a specific offset, and the server keeps sending new records as they arrive.
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil // If the client's context is done, terminate the stream
		default:
			// Attempt to consume a record from the requested offset
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
				// If no error, proceed to send the response
			case api.ErrOffsetOutOfRange:
				// If the offset is out of range, continue and wait for more records
				continue
			default:
				return err // For any other error, terminate the stream
			}
			// Send the response back to the client
			if err = stream.Send(res); err != nil {
				return err // Return error if sending fails
			}
			// Increment the offset for the next read
			req.Offset++
		}
	}
}

// CommitLog is an interface that defines the methods required to interact with a log.
// It includes methods for appending records and reading records by offset.
type CommitLog interface {
	Append(*api.Record) (uint64, error) // Append adds a record to the log and returns its offset.
	Read(uint64) (*api.Record, error)   // Read retrieves a record at the given offset.
}

// NewGRPCServer creates a new gRPC server instance, registers the LogServer service, and returns it.
// It is responsible for setting up the gRPC server and linking the server logic.
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	// Create a new gRPC server instance
	gsrv := grpc.NewServer()

	// Create a new grpcServer instance using the provided configuration
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err // Return an error if the server initialization fails
	}

	// Register the grpcServer as the implementation of the LogServer
	api.RegisterLogServer(gsrv, srv)

	// Return the configured gRPC server
	return gsrv, nil
}
