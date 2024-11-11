package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	// specifies the number of bytes to store the record length
	lenWidth = 8
)

// store represents a log-backed storage with thread-safe access.
// It buffers writes to improve performance and tracks the current size.
type store struct {
	*os.File               // underlying file for storage
	mu       sync.Mutex    // mutex to ensure thread-safe operations
	buf      *bufio.Writer // buffered writer to reduce file I/O
	size     uint64        // current size of the file
}

// newStore creates a new store for the provided file.
// It sets up buffering for efficient writing and retrieves the initial file size.
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append adds data to the store. It writes the length of the data followed by the data itself.
// Returns the number of bytes written, the starting position, and any error encountered.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size

	// Write the length of p as an 8-byte integer, followed by the actual data
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth // Total bytes written includes the length prefix

	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read retrieves a record from the store at the specified position.
// It reads the length of the record, then reads the record data based on the length.
// Returns the record data or any error encountered.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush any buffered data to ensure the latest data is on disk
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Read the record length from the specified position
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Allocate a slice for the record data and read it from disk
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt reads directly from the file at a specified offset into p.
// Ensures buffered data is flushed before reading to maintain consistency.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush buffer to ensure consistency for direct read
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close flushes any buffered data to disk and closes the file.
// Ensures all data is safely written and resources are released.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
