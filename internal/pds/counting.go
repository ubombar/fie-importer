package pds

import (
	"io"
	"sync/atomic"
)

// countingReader counts the bytes read through it; the progress display
// reads the count from another goroutine.
type countingReader struct {
	r io.Reader
	n atomic.Int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}
