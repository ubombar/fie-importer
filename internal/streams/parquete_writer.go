package streams

import (
	"fie-importer/internal/api"
	"fmt"
	"io"
	"iter"
	"net"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"
)

type LiteFIEParquetIngester struct {
	writer    *parquet.GenericWriter[api.ParquetLiteFIE]
	batchSize int
	count     atomic.Uint64
}

func NewLiteFIEParquetIngester(writer io.Writer, batchSize int) (*LiteFIEParquetIngester, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be greater than 0, got %d", batchSize)
	}

	return &LiteFIEParquetIngester{
		writer:    parquet.NewGenericWriter[api.ParquetLiteFIE](writer),
		batchSize: batchSize,
	}, nil
}

func (w *LiteFIEParquetIngester) Count() uint64 {
	return w.count.Load()
}

func (w *LiteFIEParquetIngester) Ingest(stream iter.Seq2[*api.CompressedFIE, error]) error {
	rows := make([]api.ParquetLiteFIE, 0, w.batchSize)
	compactIP := func(ip []byte) []byte {
		v := net.IP(ip)

		if v4 := v.To4(); v4 != nil {
			return v4
		}

		if v6 := v.To16(); v6 != nil {
			return v6
		}

		return nil
	}

	for fie, err := range stream {
		if err != nil {
			return err
		}

		rows = append(rows, api.ParquetLiteFIE{
			SequenceNumber:     fie.SequenceNumber,
			ProbingDirectiveID: fie.ProbingDirectiveID,
			NearReplyAddress:   compactIP(fie.NearReplyAddress),
			FarReplyAddress:    compactIP(fie.FarReplyAddress),
			CaptureTimestamp:   uint32(fie.CaptureBaseTime.Add(time.Duration(fie.CaptureSecond) * time.Second).Unix()), //nolint:gosec
		})

		if len(rows) >= w.batchSize {
			if err := w.write(rows); err != nil {
				return err
			}
			rows = rows[:0]
		}
	}

	if len(rows) > 0 {
		if err := w.write(rows); err != nil {
			return err
		}
	}

	return nil
}

func (w *LiteFIEParquetIngester) write(rows []api.ParquetLiteFIE) error {
	n, err := w.writer.Write(rows)
	if err != nil {
		return err
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}

	w.count.Add(uint64(n)) //nolint
	return nil
}

func (w *LiteFIEParquetIngester) Close() error {
	return w.writer.Close()
}
