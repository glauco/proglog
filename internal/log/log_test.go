package log

import (
	"io"
	"os"
	"testing"

	api "github.com/glauco/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	// Define a set of test scenarios, each represented by a function that takes a *testing.T and *Log
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		// Run each scenario using t.Run for better isolation and test reporting
		t.Run(scenario, func(t *testing.T) {
			// Create a temporary directory for the test log files
			dir := t.TempDir()
			defer os.RemoveAll(dir) // Clean up after the test

			// Set up a log configuration with a small max store size for testing
			c := Config{}
			c.Segment.MaxStoreBytes = 32

			// Create a new log instance with the configured settings
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			// Run the specific test function
			fn(t, log)
		})
	}
}

// testAppendRead tests that appending a record to the log and then reading it back works correctly.
func testAppendRead(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	// Append the record to the log
	off, err := log.Append(append)
	require.NoError(t, err)          // Ensure the append operation succeeds
	require.Equal(t, uint64(0), off) // The offset of the first record should be 0

	// Read the record back from the log using the returned offset
	read, err := log.Read(off)
	require.NoError(t, err)                    // Ensure the read operation succeeds
	require.Equal(t, append.Value, read.Value) // Verify the value read matches the value appended
}

// testOutOfRangeErr tests reading an offset that is out of range, expecting an error.
func testOutOfRangeErr(t *testing.T, log *Log) {
	// Attempt to read from an offset that doesn't exist (offset 1 in an empty log)
	read, err := log.Read(1)
	require.Nil(t, read) // No record should be returned
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset) // Ensure an error is returned
}

// testInitExisting tests initializing a log with existing segments.
func testInitExisting(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}

	// Append multiple records to the log
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}
	// Close the log to simulate shutting down
	require.NoError(t, log.Close())

	// Verify the lowest and highest offsets before re-initializing the log
	off, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	// Re-open the log and verify that the offsets remain correct
	n, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

// testReader tests the Reader method of the log, which allows for sequential reading from all segments.
func testReader(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	// Append a record to the log
	off, err := log.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	// Use the log's Reader to read all records sequentially
	reader := log.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Unmarshal the read bytes into a record
	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value) // Ensure the value read matches what was appended
}

// testTruncate tests truncating the log by removing segments below a specified offset.
func testTruncate(t *testing.T, log *Log) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	// Append multiple records to the log
	for i := 0; i < 3; i++ {
		_, err := log.Append(append)
		require.NoError(t, err)
	}

	// Truncate the log to remove segments below offset 1
	err := log.Truncate(1)
	require.NoError(t, err)

	// Attempt to read a truncated offset (offset 0) and expect an error
	_, err = log.Read(0)
	require.Error(t, err)
}
