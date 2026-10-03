package fies

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"slices"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func u8(v uint8) *uint8 { return &v }

func TestConvert2a(t *testing.T) {
	start := time.Date(2026, 10, 3, 11, 0, 0, 0, time.UTC)
	row := Convert2a(&Raw2a{
		PDID: 7, CaptureSecond: 65,
		NearReplyAddr: []byte{192, 0, 2, 1}, FarReplyAddr: net.ParseIP("2001:db8::1"),
		NearReplyAgeS: u8(1), FarReplyAgeS: u8(2), FIETransitS: u8(3),
	}, start)
	capture := start.Add(65 * time.Second)
	if row.SourceFormat != "2a" || row.PDID != 7 || !row.CaptureTime.Equal(capture) || *row.FIETransitS != 3 {
		t.Fatalf("row = %+v", row)
	}
	if got := row.NearReplyAddr.String(); got != "192.0.2.1" || len(*row.NearReplyAddr) != 16 {
		t.Fatalf("near = %v (%d bytes), want IPv4-mapped 192.0.2.1", got, len(*row.NearReplyAddr))
	}
	if got := row.FarReplyAddr.String(); got != "2001:db8::1" {
		t.Fatalf("far = %v", got)
	}
	if !row.NearReplyRecvTime.Equal(capture.Add(-4*time.Second)) || !row.FarReplyRecvTime.Equal(capture.Add(-5*time.Second)) {
		t.Fatalf("recv times = %v, %v", row.NearReplyRecvTime, row.FarReplyRecvTime)
	}
	if row.NearProbeSentTime != nil || row.FarProbeSentTime != nil {
		t.Fatal("fies2a has no sent times")
	}
}

func TestConvert2a_Nulls(t *testing.T) {
	start := time.Date(2026, 10, 3, 11, 0, 0, 0, time.UTC)
	// No reply, or an unknown transit, leaves the received time NULL.
	row := Convert2a(&Raw2a{PDID: 1, NearReplyAddr: []byte{192, 0, 2, 1}, NearReplyAgeS: u8(0)}, start)
	if row.NearReplyRecvTime != nil || row.FarReplyAddr != nil || row.FarReplyRecvTime != nil || row.FIETransitS != nil {
		t.Fatalf("row = %+v, want NULL received times and far reply", row)
	}
}

func TestWindow(t *testing.T) {
	files := []File{{Path: "a", Rows: 10}, {Path: "b", Rows: 0}, {Path: "c", Rows: 5}, {Path: "d", Rows: 7}}
	type read struct {
		path          string
		offset, limit int64
	}
	reads := func(rs []Read) []read {
		out := make([]read, len(rs))
		for i, r := range rs {
			out[i] = read{r.File.Path, r.Offset, r.Limit}
		}
		return out
	}
	for _, tc := range []struct {
		offset, limit int64
		want          []read
	}{
		{0, -1, []read{{"a", 0, 10}, {"c", 0, 5}, {"d", 0, 7}}},
		{12, -1, []read{{"c", 2, 3}, {"d", 0, 7}}},
		{8, 4, []read{{"a", 8, 2}, {"c", 0, 2}}},
		{15, 3, []read{{"d", 0, 3}}},
		{22, -1, nil},
		{0, 0, nil},
	} {
		if got := reads(Window(files, tc.offset, tc.limit)); !slices.Equal(got, tc.want) {
			t.Errorf("Window(offset %d, limit %d) = %v, want %v", tc.offset, tc.limit, got, tc.want)
		}
	}
}

// writeFiles writes two 10-minute fies2a files: PDs 1..50, each captured
// at seconds 0, 100, ..., 500 of each file.
func writeFiles(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for k, start := range []string{"2026-10-03T11:00:00Z", "2026-10-03T11:10:00Z"} {
		path := filepath.Join(dir, fmt.Sprintf("fies2a-20261003T11%d000Z.parquet", k))
		//nolint:gosec // test data
		q := fmt.Sprintf(`COPY (
			SELECT p::UINTEGER AS pd_id, s::USMALLINT AS capture_second,
				'\xC0\x00\x02\x01'::BLOB AS near_reply_addr, NULL::BLOB AS far_reply_addr,
				0::UTINYINT AS near_reply_age_s, NULL::UTINYINT AS far_reply_age_s, 1::UTINYINT AS fie_transit_s
			FROM range(1, 51) a(p), range(0, 600, 100) b(s) ORDER BY pd_id, capture_second)
			TO '%s' (FORMAT parquet, KV_METADATA {'retina.fies.format': '2a',
				'retina.fies.interval_start': '%s', 'retina.fies.interval_seconds': '600'})`, path, start)
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func plan(t *testing.T, dir string, f *Filter) (*Source, int64) {
	t.Helper()
	src, err := Open(context.Background(), dir, f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })
	if err := src.Plan(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, f := range src.Files() {
		n += f.Rows
	}
	return src, n
}

func TestSource_Filters(t *testing.T) {
	dir := writeFiles(t)
	at := func(s string) time.Time { v, _ := time.Parse(time.RFC3339, s); return v }
	for name, tc := range map[string]struct {
		filter Filter
		rows   int64
		files  int
	}{
		"none":          {Filter{}, 600, 2},
		"pd list":       {Filter{PDIDs: []uint32{3, 3, 7, 999}}, 24, 2},
		"start":         {Filter{Start: at("2026-10-03T11:08:20Z")}, 350, 2}, // second 500 of file 1, then file 2
		"window":        {Filter{Start: at("2026-10-03T11:10:00Z"), End: at("2026-10-03T11:13:20Z")}, 100, 1},
		"before files":  {Filter{End: at("2026-10-03T11:00:00Z")}, 0, 0},
		"end inclusive": {Filter{End: at("2026-10-03T11:00:00.5Z")}, 50, 1}, // only second 0
	} {
		t.Run(name, func(t *testing.T) {
			src, rows := plan(t, dir, &tc.filter)
			if rows != tc.rows || len(src.Files()) != tc.files {
				t.Fatalf("got %d rows in %d files, want %d in %d", rows, len(src.Files()), tc.rows, tc.files)
			}
		})
	}

	// The sample is deterministic: same seed, same PDs; another seed differs.
	pds := func(seed uint64) []uint32 {
		src, _ := plan(t, dir, &Filter{Percent: 30, Seed: seed})
		var ids []uint32
		for _, r := range Window(src.Files(), 0, -1) {
			if err := src.Rows(context.Background(), r, func(raw *Raw2a) error {
				ids = append(ids, raw.PDID)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
		return slices.Compact(slices.Sorted(slices.Values(ids)))
	}
	a, b, c := pds(1), pds(1), pds(2)
	if len(a) == 0 || len(a) == 50 || !slices.Equal(a, b) || slices.Equal(a, c) {
		t.Fatalf("sample: seed 1 %v, again %v, seed 2 %v", a, b, c)
	}
}

func TestSource_RowsAcrossFiles(t *testing.T) {
	dir := writeFiles(t)
	src, _ := plan(t, dir, &Filter{})
	var got []string
	for _, r := range Window(src.Files(), 298, 4) {
		if err := src.Rows(context.Background(), r, func(raw *Raw2a) error {
			row := Convert2a(raw, r.File.IntervalStart)
			got = append(got, fmt.Sprintf("%d@%s", row.PDID, row.CaptureTime.Format("15:04:05")))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Rows 298..301 in file order: the last two of file 1, the first two of file 2.
	want := []string{"50@11:06:40", "50@11:08:20", "1@11:10:00", "1@11:11:40"}
	if !slices.Equal(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}

	if n, err := src.DistinctPDs(context.Background()); err != nil || n != 50 {
		t.Fatalf("DistinctPDs = %d, %v", n, err)
	}
}

func TestOpen_Errors(t *testing.T) {
	if _, err := Open(context.Background(), t.TempDir(), &Filter{}); err == nil {
		t.Fatal("empty directory: got nil error")
	}
	if _, err := Open(context.Background(), writeFiles(t), &Filter{PDIDs: []uint32{1}, Percent: 5}); err == nil {
		t.Fatal("list and sample: got nil error")
	}
}
