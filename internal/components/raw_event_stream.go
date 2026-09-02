package components

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"sort"
)

type RawEventStream interface {
	Events() iter.Seq2[[]byte, error]
	Len() int
}

type rawEventStream struct {
	files []string
	len   int
}

var _ RawEventStream = (*rawEventStream)(nil)

func NewRawEventStream(eventsDir string) (*rawEventStream, error) {
	files, err := filepath.Glob(filepath.Join(eventsDir, "events-*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("glob event files: %w", err)
	}
	sort.Strings(files)

	count := 0
	for _, filename := range files {
		f, err := os.Open(filename) //nolint:gosec
		if err != nil {
			return nil, fmt.Errorf("open %q: %w", filename, err)
		}

		reader := bufio.NewReader(f)
		for {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				count++
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("read %q: %w", filename, err)
			}
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("close %q: %w", filename, err)
		}
	}

	return &rawEventStream{
		files: files,
		len:   count,
	}, nil
}

func (s *rawEventStream) Len() int {
	return s.len
}

func (s *rawEventStream) Events() iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for _, filename := range s.files {
			if !s.readFile(filename, yield) {
				return
			}
		}
	}
}

func (s *rawEventStream) readFile(filename string, yield func([]byte, error) bool) bool {
	f, err := os.Open(filename) //nolint:gosec
	if err != nil {
		yield(nil, fmt.Errorf("open %q: %w", filename, err))
		return false
	}
	defer func() { _ = f.Close() }()

	reader := bufio.NewReader(f)

	for {
		line, err := reader.ReadBytes('\n')

		if len(line) > 0 {
			line = trimNewline(line)

			if !yield(line, nil) {
				return false
			}
		}

		if errors.Is(err, io.EOF) {
			return true
		}
		if err != nil {
			yield(nil, fmt.Errorf("read %q: %w", filename, err))
			return false
		}
	}
}

func trimNewline(b []byte) []byte {
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	if len(b) > 0 && b[len(b)-1] == '\r' {
		b = b[:len(b)-1]
	}
	return b
}
