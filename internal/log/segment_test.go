package log

import (
	"io"
	"os"
	"testing"

	api "github.com/glauco/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	// Create a temporary directory to store the segment files during the test
	dir := t.TempDir()
	defer os.RemoveAll(dir) // Ensure all files are removed after the test to clean up

	// Define the record to be written to the segment during the test
	want := &api.Record{
		Value: []byte("hello world"),
	}

	// Create a Config object with limits for store and index sizes
	c := Config{}
	c.Segment.MaxStoreBytes = 1024         // Maximum allowed bytes for the store file
	c.Segment.MaxIndexBytes = entWidth * 3 // Limit the index to three entries

	// Create a new segment with baseOffset 16 and the specified configuration
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)

	// Verify that the initial next offset of the segment is set to the baseOffset (16)
	require.Equal(t, uint64(16), s.nextOffset)

	// Verify that the segment is not maxed out initially
	require.False(t, s.IsMaxed())

	// Append three records to the segment, checking each time that the data can be read back correctly
	for i := uint64(0); i < 3; i++ {
		// Append the record to the segment
		off, err := s.Append(want)
		require.NoError(t, err) // Ensure no error during append
		// The offset should match baseOffset + i for each appended record
		require.Equal(t, 16+i, off)

		// Read the record back from the segment to verify it was stored correctly
		got, err := s.Read(off)
		require.NoError(t, err)                 // Ensure no error during read
		require.Equal(t, want.Value, got.Value) // The value read should match what was written
	}

	// Attempt to append another record, which should fail as the index has reached its limit
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err) // Expect an EOF error indicating that the index is full

	// Confirm that the segment is now maxed out (index has reached maximum capacity)
	require.True(t, s.IsMaxed())

	// Update the configuration to limit store capacity, allowing only space for three records
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3) // Adjust the store limit to fit only three records
	c.Segment.MaxIndexBytes = 1024                        // Set index size to a larger value to avoid being maxed by index

	// Create a new segment with the updated configuration
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// The store should now be maxed out (limited by store size)
	require.True(t, s.IsMaxed())

	// Remove the segment, which involves closing and deleting its files from the filesystem
	err = s.Remove()
	require.NoError(t, err) // Ensure no error during segment removal

	// Create a new segment with the same configuration after removing the previous one
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// After recreating the segment, it should not be maxed out
	require.False(t, s.IsMaxed())
}
