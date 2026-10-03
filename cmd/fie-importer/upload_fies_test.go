package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestGroup(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 20486494: "20,486,494"} {
		if got := group(n); got != want {
			t.Errorf("group(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestUploadOptions_Filter(t *testing.T) {
	file := filepath.Join(t.TempDir(), "pds.txt")
	if err := os.WriteFile(file, []byte("# sample\n5\n\n9\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	o := uploadOptions{pdids: []string{"1", " 2"}, pdidsFile: file, start: "2026-10-03T11:00:00Z", end: "2026-10-03T13:00:00+01:00"}
	f, err := o.filter()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.PDIDs, []uint32{1, 2, 5, 9}) || f.Start.IsZero() {
		t.Fatalf("filter = %+v", f)
	}
	// 12:00+01:00 is 11:00Z: not after start.
	if _, err := (&uploadOptions{start: "2026-10-03T11:00:00Z", end: "2026-10-03T12:00:00+01:00"}).filter(); err == nil {
		t.Fatal("empty window: got nil error")
	}
	for _, bad := range []uploadOptions{
		{pdids: []string{"x"}},
		{pdids: []string{"4294967296"}},
		{percent: 101},
		{percent: -1},
		{start: "yesterday"},
	} {
		if _, err := bad.filter(); err == nil {
			t.Errorf("%+v: got nil error", bad)
		}
	}
}
