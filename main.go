package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	eventsDir = "./captures/20260826_145652/events/"
	fiesDir   = "./captures/20260826_145652/fies/"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() (retErr error) {
	extractor, err := NewCaptureExtractor(CaptureExtractorConfig{
		EventsDir: eventsDir,
		FIEsDir:   fiesDir,
	})
	if err != nil {
		return fmt.Errorf("create extractor: %w", err)
	}

	if err := extractor.Load(); err != nil {
		return fmt.Errorf("load extractor: %w", err)
	}

	defer func() {
		if err := extractor.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("close extractor: %w", err)
		}
	}()

	out := bufio.NewWriter(os.Stdout)

	defer func() {
		if err := out.Flush(); err != nil && retErr == nil {
			retErr = fmt.Errorf("flush stdout: %w", err)
		}
	}()

	encoder := json.NewEncoder(out)

	for {
		fie, err := extractor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("extract FIE: %w", err)
		}

		if err := encoder.Encode(fie); err != nil {
			return fmt.Errorf("encode FIE: %w", err)
		}
	}
}
