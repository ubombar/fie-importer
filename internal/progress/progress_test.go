package progress

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestBar(t *testing.T) {
	for _, tc := range []struct {
		frac float64
		want string
	}{
		{0, "▕    ▏"},
		{0.5, "▕██  ▏"},
		{0.56, "▕██▏ ▏"},
		{1, "▕████▏"},
		{1.7, "▕████▏"},
	} {
		got := bar(tc.frac, 4)
		if got != tc.want || utf8.RuneCountInString(got) != 6 {
			t.Errorf("bar(%v) = %q, want %q", tc.frac, got, tc.want)
		}
	}
}

func TestFormats(t *testing.T) {
	if got := human(20_486_494); got != "20.5M" {
		t.Errorf("human = %q", got)
	}
	if got := human(312_000); got != "312k" {
		t.Errorf("human = %q", got)
	}
	if got := clock(3725 * time.Second); got != "1:02:05" {
		t.Errorf("clock = %q", got)
	}
	if got := bytesHuman(9.4e6); got != "9.4 MB" {
		t.Errorf("bytesHuman = %q", got)
	}
}

func TestSetSentAndSink(t *testing.T) {
	d := &Display{width: 100}
	d.fileName.Store("f")
	d.phase.Store("")
	d.Start(1000, 2)
	d.SetSink("disk")
	d.SetSent(250, 4096)
	d.File(1, "fies2a-a.parquet", 600)
	d.SetRead(250)
	joined := strings.Join(d.frame(false), "\n")
	for _, want := range []string{" 25.0%", "250", "to disk", "file 1/2", "fies2a-a.parquet"} {
		if !strings.Contains(joined, want) {
			t.Errorf("frame %q lacks %q", joined, want)
		}
	}
	if strings.Contains(joined, "ClickHouse") {
		t.Errorf("frame %q names ClickHouse", joined)
	}
}
