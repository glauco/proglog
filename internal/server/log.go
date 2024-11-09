package server

import (
	"fmt"
	"sync"
)

// Log represents a thread-safe log that stores a sequence of records.
// It uses a mutex to synchronize access to the records.
type Log struct {
	mu      sync.Mutex // Mutex to ensure thread-safe access to records
	records []Record   // Slice to hold log records
}

// NewLog creates and returns a new instance of Log.
func NewLog() *Log {
	return &Log{}
}

// Append adds a new record to the log and returns its offset (index in the log).
// This method is thread-safe, locking the log during the append operation.
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()         // Lock to ensure thread-safe access
	defer c.mu.Unlock() // Unlock after the function returns

	// Set the offset of the new record to the current length of the records slice
	record.Offset = uint64(len(c.records))
	// Append the new record to the log
	c.records = append(c.records, record)
	// Return the offset of the appended record
	return record.Offset, nil
}

// Read retrieves a record from the log by its offset.
// Returns an error if the offset is out of bounds.
// This method is thread-safe, locking the log during the read operation.
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()         // Lock to ensure thread-safe access
	defer c.mu.Unlock() // Unlock after the function returns

	// Check if the offset is within the bounds of the log
	if offset >= uint64(len(c.records)) {
		// Return an error if the offset is not found
		return Record{}, ErrOffsetNotFound
	}
	// Return the record at the specified offset
	return c.records[offset], nil
}

// Record represents a log record with a value and an offset.
// The Value field stores the record data, and the Offset field indicates its position in the log.
type Record struct {
	Value  []byte `json:"value"`  // The actual content of the record
	Offset uint64 `json:"offset"` // The position of the record in the log
}

// The error returned when a record at a given offset does not exist in the log.
var ErrOffsetNotFound = fmt.Errorf("offset not found")
