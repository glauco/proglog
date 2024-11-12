package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/glauco/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// segment is a data structure that ties together a store and an index for a specific segment
// of the log. It keeps track of the base offset (starting point) and the next available offset.
type segment struct {
	store                  *store // The store file for holding log records
	index                  *index // The index file for keeping track of offsets
	baseOffset, nextOffset uint64 // Base offset and next available offset for the segment
	config                 Config // Configuration options for the segment
}

// newSegment creates a new segment at the given directory with a specified base offset.
// It sets up both the store and index files for the segment.
// Each segment manages its store (data storage) and index (offset metadata).
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error

	// Open the store file in the specified directory.
	// The filename follows the pattern "<baseOffset>.store".
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		// If there is an error opening or creating the store file, return the error.
		return nil, err
	}

	// Create a new store object using the store file.
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// Open the index file in the specified directory.
	// The filename follows the pattern "<baseOffset>.index".
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		// If there is an error opening or creating the index file, return the error.
		return nil, err
	}

	// Create a new index object using the index file.
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// Determine the next offset to be used in the segment.
	// If reading the last offset in the index fails (e.g., because it is empty),
	// set the next offset to the base offset. Otherwise, calculate it based on the last offset read.
	if off, _, err := s.index.Read(-1); err != nil {
		// If there's an error reading the last offset (e.g., empty index), use baseOffset.
		s.nextOffset = baseOffset
	} else {
		// Set nextOffset to one past the last offset in the index.
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// Set the current offset to be the next available offset in the segment
	cur := s.nextOffset
	// Assign the current offset to the record
	record.Offset = cur

	// Marshal the record into a byte slice using protocol buffers for storage
	p, err := proto.Marshal(record)
	if err != nil {
		// Return an error if the marshaling fails
		return 0, err
	}

	// Append the marshaled record to the store
	// The store returns the number of bytes written and the position where the record starts
	_, pos, err := s.store.Append(p)
	if err != nil {
		// Return an error if appending to the store fails
		return 0, err
	}

	// Write the offset and the position of the record to the index
	// Index offsets are always relative to the baseOffset of the segment
	if err = s.index.Write(
		uint32(s.nextOffset-uint64(s.baseOffset)), pos,
	); err != nil {
		// Return an error if writing to the index fails
		return 0, err
	}

	// Increment the nextOffset to prepare for the next append
	s.nextOffset++

	// Return the current offset where the record was appended
	return cur, nil
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	// Calculate the relative offset to read from the index.
	// Since the offset given is absolute (i.e., across all segments), subtract the baseOffset
	// of the current segment to get the relative offset within this segment.
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		// If reading from the index fails, return the error.
		return nil, err
	}

	// Use the position obtained from the index to read the corresponding data from the store.
	p, err := s.store.Read(pos)
	if err != nil {
		// If reading from the store fails, return the error.
		return nil, err
	}

	// Create a new api.Record instance to unmarshal the data read from the store.
	record := &api.Record{}

	// Unmarshal the byte slice into a Record using protocol buffers.
	err = proto.Unmarshal(p, record)

	// Return the unmarshaled record and any potential error from the unmarshaling process.
	return record, err
}

// Checks whether the segment has reached its maximum allowed size.
// A segment is considered "maxed out" if either the store or index size exceeds their respective limits.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Gracefully closes both the store and index files associated with the segment.
// It ensures that all data is flushed to disk and resources are released.
func (s *segment) Close() error {
	// Attempt to close the index first.
	if err := s.index.Close(); err != nil {
		return err // Return the error if closing the index fails.
	}
	// Attempt to close the store.
	if err := s.store.Close(); err != nil {
		return err // Return the error if closing the store fails.
	}
	return nil // If both operations succeed, return nil.
}

// Deletes both the store and index files associated with the segment.
// This method first closes the files, ensuring data is flushed, before removing them.
func (s *segment) Remove() error {
	// Close the segment before attempting to remove the files.
	if err := s.Close(); err != nil {
		return err // Return the error if closing the segment fails.
	}
	// Remove the index file from the filesystem.
	if err := os.Remove(s.index.Name()); err != nil {
		return err // Return the error if removing the index file fails.
	}
	// Remove the store file from the filesystem.
	if err := os.Remove(s.store.Name()); err != nil {
		return err // Return the error if removing the store file fails.
	}
	return nil // If both files are successfully removed, return nil.
}
