package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/glauco/proglog/api/v1"
)

// Log represents the entire log consisting of multiple segments.
// It provides a thread-safe interface to append and read records.
type Log struct {
	mu            sync.RWMutex // Read-write lock to handle concurrent access to the log
	Dir           string       // Directory where the log files are stored
	Config        Config       // Configuration for the log, including max store/index sizes
	activeSegment *segment     // Currently active segment for writing new records
	segments      []*segment   // List of all segments in the log
}

// NewLog creates a new Log instance with the given directory and configuration.
// It initializes default configuration values if necessary and calls setup to initialize segments.
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024 // Set default max store bytes if not provided
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024 // Set default max index bytes if not provided
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	// Initialize segments by scanning the directory
	return l, l.setup()
}

// newSegment creates a new segment starting at the given offset and adds it to the log.
// It also sets the new segment as the active segment for appending new records.
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s) // Add the new segment to the list of segments
	l.activeSegment = s                // Set the new segment as the active one
	return nil
}

// setup scans the directory for existing segment files and initializes segments for each.
// If no segments exist, it creates a new initial segment.
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	// Collect base offsets from all segment files in the directory
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	// Sort the offsets in ascending order
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	// Create segments based on the sorted base offsets
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// Skip duplicate entries for index and store files
		i++
	}
	// If no segments exist, create an initial segment
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// Append adds a new record to the log. If the active segment is full, it creates a new segment.
// Returns the offset where the record was appended.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Append the record to the active segment
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	// If the active segment is maxed out, create a new segment
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read fetches a record from the log at the specified offset.
// It finds the correct segment based on the offset and reads the record from it.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var s *segment
	// Find the segment that contains the given offset
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	// If no segment contains the offset, return an error
	if s == nil {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}
	return s.Read(off)
}

// Close gracefully closes all segments in the log, ensuring all data is flushed to disk.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Close all segments in the log
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove deletes the entire log directory, including all segment files.
func (l *Log) Remove() error {
	// First close all segments to ensure data is flushed
	if err := l.Close(); err != nil {
		return err
	}
	// Remove all files in the log directory
	return os.RemoveAll(l.Dir)
}

// Reset deletes the log and recreates it, effectively resetting its state.
func (l *Log) Reset() error {
	// Remove the log and then set it up again
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// Truncate removes all segments whose offsets are less than or equal to the specified value.
// Used to trim old data from the log.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	// Iterate through segments and remove those whose nextOffset is less than or equal to the given value
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		// Keep segments that should not be removed
		segments = append(segments, s)
	}
	l.segments = segments // Update the list of segments to only include retained ones
	return nil
}

// originReader is a wrapper around a store that keeps track of its reading position.
type originReader struct {
	*store       // Embedded store to read from
	off    int64 // Current offset for reading
}

// Reader creates a multi-segment reader that reads from all segments sequentially.
func (l *Log) Reader() io.Reader {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Create a reader for each segment starting at offset 0
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{
			store: segment.store,
			off:   0,
		}
	}
	// Combine all segment readers into a single reader
	return io.MultiReader(readers...)
}

// Read implements the io.Reader interface for the originReader.
// It reads data from the current offset and then updates the offset accordingly.
func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off) // Read from the current offset
	o.off += int64(n)            // Update the offset to reflect the bytes read
	return n, err
}

// LowestOffset returns the base offset of the oldest segment in the log.
// This represents the lowest available offset within the entire log.
func (l *Log) LowestOffset() (uint64, error) {
	// Acquire a read lock to safely access the list of segments
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Return the baseOffset of the first segment
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the highest offset currently in the log.
// This is the offset of the last record written to the latest segment.
func (l *Log) HighestOffset() (uint64, error) {
	// Acquire a read lock to safely access the list of segments
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Retrieve the next offset of the last segment
	off := l.segments[len(l.segments)-1].nextOffset

	// If the nextOffset is 0, that means no records have been appended yet, return 0
	if off == 0 {
		return 0, nil
	}

	// The highest offset is the last used offset, which is nextOffset - 1
	return off - 1, nil
}
