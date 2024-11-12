package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	// Data to write to the store
	write = []byte("hello world")
	// Width of each record, including the length prefix
	width = uint64(len(write)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	// Create a temporary file for testing the store
	f, err := os.CreateTemp("", "store_append_read_text")
	require.NoError(t, err)
	defer os.Remove(f.Name()) // Clean up file after test

	// Initialize a new store with the temporary file
	s, err := newStore(f)
	require.NoError(t, err)

	// Run append, read, and read-at tests on the store
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// Reopen the store and verify data can still be read correctly
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

// testAppend writes multiple records to the store and verifies that
// each record's position aligns as expected.
func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		// Append the record and check the returned position and size
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		// Ensure the position + size matches the expected width
		require.Equal(t, pos+n, width*i)
	}
}

// testRead reads records sequentially from the store and verifies
// that the content matches what was written.
func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		// Read a record at the current position
		read, err := s.Read(pos)
		require.NoError(t, err)
		// Verify the data read matches the original data written
		require.Equal(t, write, read)
		// Increment position by width to read the next record
		pos += width
	}
}

// testReadAt reads records from specific positions in the store file
// and verifies both the length and content of each record.
func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		// Read the length prefix of the record
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		// Ensure the number of bytes read matches the length width
		require.Equal(t, lenWidth, n)
		off += int64(n)

		// Read the actual data using the size obtained from the prefix
		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		// Verify the data matches the written content and size
		require.Equal(t, write, b)
		require.Equal(t, int(size), n)
		// Move offset forward by the number of bytes read
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	// Create a temporary file for testing the store's close behavior
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name()) // Clean up file after test

	// Initialize a new store with the temporary file
	s, err := newStore(f)
	require.NoError(t, err)

	// Append a record to the store
	_, _, err = s.Append(write)
	require.NoError(t, err)

	// Check the file size before closing the store
	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	// Close the store to flush any buffered data to the file
	err = s.Close()
	require.NoError(t, err)

	// Reopen the file and check the size after closing
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	// Verify that the file size has increased after flushing
	require.True(t, afterSize > beforeSize)
}

// openFile opens a file by name and returns the file, its size, and any errors.
// It is used here to inspect the file size after closing the store.
func openFile(name string) (file *os.File, size int64, err error) {
	// Open the file in read-write mode with creation and append flags
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}

	// Retrieve the file's size
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
