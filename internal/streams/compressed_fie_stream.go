package streams

import (
	"database/sql"
	"fmt"
	"iter"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/marcboeker/go-duckdb/v2"

	"fie-importer/internal/api"
)

type CompressedFIEStream interface {
	Events() iter.Seq2[*api.CompressedFIE, error]
	Len() int
}

type compressedFIEStream struct {
	where string
	files []string
	len   int
}

var _ CompressedFIEStream = (*compressedFIEStream)(nil)

func NewCompressedFIEStream(fiesDir, where string) (*compressedFIEStream, error) {
	files, err := filepath.Glob(filepath.Join(fiesDir, "fies-*.duckdb"))
	if err != nil {
		return nil, fmt.Errorf("glob FIE files: %w", err)
	}
	sort.Strings(files)

	total := 0

	for _, filename := range files {
		connector, err := duckdb.NewConnector(filename, nil)
		if err != nil {
			return nil, fmt.Errorf("open DuckDB connector %q: %w", filename, err)
		}

		db := sql.OpenDB(connector)

		var count int
		err = db.QueryRow(`SELECT count(*) FROM fies`).Scan(&count)

		closeErr := db.Close()
		connectorErr := connector.Close()

		if err != nil {
			return nil, fmt.Errorf("count FIEs in %q: %w", filename, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close DuckDB %q: %w", filename, closeErr)
		}
		if connectorErr != nil {
			return nil, fmt.Errorf("close DuckDB connector %q: %w", filename, connectorErr)
		}

		total += count
	}

	return &compressedFIEStream{
		where: where,
		files: files,
		len:   total,
	}, nil
}

func (s *compressedFIEStream) Events() iter.Seq2[*api.CompressedFIE, error] {
	return func(yield func(*api.CompressedFIE, error) bool) {
		var sequenceNumber uint64 = 0
		for _, filename := range s.files {
			if !s.readFile(filename, yield, &sequenceNumber) {
				return
			}
		}
	}
}

func (s *compressedFIEStream) Len() int {
	return s.len
}

func (s *compressedFIEStream) readFile(filename string, yield func(*api.CompressedFIE, error) bool, sequenceNumber *uint64) bool {
	baseTime, err := parseFIEBaseTime(filename)
	if err != nil {
		yield(nil, err)
		return false
	}

	connector, err := duckdb.NewConnector(filename, nil)
	if err != nil {
		yield(nil, fmt.Errorf("open DuckDB connector %q: %w", filename, err))
		return false
	}

	db := sql.OpenDB(connector)

	query := `
		SELECT
			probing_directive_id,
			near_reply_address,
			far_reply_address,
			capture_second,
			time_deltas
		FROM fies
	`
	if s.where != "" {
		query = fmt.Sprintf("%s WHERE %s", query, s.where)
	}
	query = fmt.Sprintf("%s ORDER BY rowid", query)

	rows, err := db.Query(query)
	if err != nil {
		_ = db.Close()
		_ = connector.Close()
		yield(nil, fmt.Errorf("query FIE file %q: %w", filename, err))
		return false
	}

	defer func() {
		_ = rows.Close()
		_ = db.Close()
		_ = connector.Close()
	}()

	for rows.Next() {
		var record api.CompressedFIE
		if err := rows.Scan(
			&record.ProbingDirectiveID,
			&record.NearReplyAddress,
			&record.FarReplyAddress,
			&record.CaptureSecond,
			&record.TimeDeltas,
		); err != nil {
			yield(nil, fmt.Errorf("scan FIE row from %q: %w", filename, err))
			return false
		}

		record.CaptureBaseTime = baseTime
		record.SequenceNumber = *sequenceNumber
		*sequenceNumber++

		if !yield(&record, nil) {
			return false
		}
	}

	if err := rows.Err(); err != nil {
		yield(nil, fmt.Errorf("iterate FIE rows from %q: %w", filename, err))
		return false
	}

	return true
}

func (s *compressedFIEStream) Close() error {
	return nil
}

func parseFIEBaseTime(filename string) (time.Time, error) {
	name := filepath.Base(filename)

	const prefix = "fies-"
	const suffix = ".duckdb"

	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return time.Time{}, fmt.Errorf("invalid FIE filename %q", name)
	}

	timestamp := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)

	t, err := time.Parse("20060102T150405Z", timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse FIE timestamp from %q: %w", name, err)
	}

	return t.UTC(), nil
}
