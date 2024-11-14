package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	status "google.golang.org/grpc/status"
)

// ErrOffsetOutOfRange is a custom error type used to indicate that
// a requested offset is not available in the log.
type ErrOffsetOutOfRange struct {
	Offset uint64 // The out-of-range offset that triggered the error
}

// GRPCStatus converts the ErrOffsetOutOfRange into a gRPC status, which can be sent to a client.
// This function returns a status that contains the error code and a localized error message.
func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	// Create a new gRPC status with a status code (404) and a descriptive error message
	st := status.New(
		404, // HTTP 404 equivalent error code for "not found"
		fmt.Sprintf("The requested offset is outside the log's range: %d", e.Offset),
	)

	// Create a localized error message for additional details
	msg := fmt.Sprintf("The requested offset is outside the log's range: %d", e.Offset)
	d := &errdetails.LocalizedMessage{
		Locale:  "en-US", // Locale for the message, set to English (US)
		Message: msg,     // The descriptive error message
	}

	// Attach the localized message as additional details to the gRPC status
	// This provides more context to clients when they receive the error
	std, err := st.WithDetails(d)
	if err != nil {
		// If there was an error adding the details, return the original status without additional details
		return st
	}

	// Return the status with additional details
	return std
}

// Error implements the standard error interface for ErrOffsetOutOfRange.
// It returns a string representation of the gRPC status containing the error.
func (e ErrOffsetOutOfRange) Error() string {
	// Get the error message from the gRPC status and return it as a string
	return e.GRPCStatus().Err().Error()
}
