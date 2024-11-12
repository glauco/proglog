package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	// Create a temporary file for testing the index
	f, err := os.CreateTemp("", "store_append_read_text")
	require.NoError(t, err)
	defer os.Remove(f.Name()) // Clean up file after test

	// Set up a configuration with a maximum index size
	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	// Initialize a new index with the temporary file and configuration
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	// Try reading from an empty index; expecting an error
	_, _, err = idx.Read(-1)
	require.Error(t, err)

	// Verify the index file's name is correct
	require.Equal(t, f.Name(), idx.Name())

	// Define entries to write to the index and later read for verification
	entries := []struct {
		Off uint32 // Offset of the entry
		Pos uint64 // Position in the log
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	// Write entries to the index and verify they can be read back accurately
	for _, want := range entries {
		// Write the entry to the index
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		// Read back the entry and ensure it matches the written values
		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// Attempt to read past the last entry, expecting an io.EOF error
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)

	// Close the index to flush and save its state
	_ = idx.Close()

	// Reopen the index to verify it correctly loads the state from the file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)

	// Read the last entry in the reopened index to verify persistence
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
