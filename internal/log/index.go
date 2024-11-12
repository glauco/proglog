package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// Width of an offset entry in bytes
	offWidth uint64 = 4
	// Width of a position entry in bytes
	posWidth uint64 = 8
	// Total width of each index entry (offset + position)
	entWidth uint64 = offWidth + posWidth
)

// index represents a memory-mapped file index used to store offsets and positions
// of records in the log. This index allows fast lookup and access through mmap.
type index struct {
	file *os.File    // file used for storing the index
	mmap gommap.MMap // memory-mapped file for fast access
	size uint64      // current size of the index file
}

// newIndex initializes an index for the given file and configures it with the
// maximum number of bytes allowed by MaxIndexBytes in the Config.
// It truncates the file to the maximum allowed bytes and maps it into memory.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	// Retrieve the current size of the file
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// Truncate the file to the maximum allowed index size specified in config
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	// Map the file into memory with read-write permissions and shared visibility
	// PROT_READ | PROT_WRITE - allows reading and writing to the memory-mapped region
	// MAP_SHARED - changes to the memory-mapped file are visible to other processes
	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}
	return idx, nil
}

// Close flushes the memory-mapped file and synchronizes it to disk,
// then truncates the file to the current size and closes the file descriptor.
func (i *index) Close() error {
	// Sync changes to the memory-mapped file to disk
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	// Sync the file descriptor to ensure all data is written
	if err := i.file.Sync(); err != nil {
		return err
	}
	// Truncate the file to the actual size used by entries
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Read retrieves the record's offset and position at a given index entry.
// If in == -1, it returns the last entry. Returns io.EOF if the requested
// index is out of bounds or no entries are available.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		// No entries available
		return 0, 0, io.EOF
	}

	// If in == -1, read the last entry; otherwise, use the specified index
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// Calculate position in the memory-mapped file for the entry
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		// If requested position is out of bounds, return EOF
		return 0, 0, io.EOF
	}

	// Read the offset and position from the memory-mapped file
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends a new entry to the index with the given offset and position.
// Returns io.EOF if there is insufficient space in the memory-mapped file.
func (i *index) Write(off uint32, pos uint64) error {
	// Check if there is enough space in the mmap for a new entry
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// Write the offset and position to the memory-mapped file at the current size
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// Increment the index size by the entry width
	i.size += uint64(entWidth)
	return nil
}

// Name returns the name of the file associated with the index.
func (i *index) Name() string {
	return i.file.Name()
}
