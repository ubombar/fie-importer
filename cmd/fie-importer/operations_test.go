package main

import (
	"errors"
	"testing"
	"time"
)

func TestCommandLine(t *testing.T) {
	got := commandLine([]string{"/Users/u/go/bin/fie-importer", "upload-agents", "t", "--filter", "labels.env=research AND x", "--exclude", "it's", "--start=2026-10-03T11:00:00Z"})
	want := `fie-importer upload-agents t --filter 'labels.env=research AND x' --exclude 'it'\''s' --start=2026-10-03T11:00:00Z`
	if got != want {
		t.Errorf("commandLine = %s\nwant %s", got, want)
	}
	if got := shellQuote(""); got != "''" {
		t.Errorf("shellQuote(\"\") = %s", got)
	}
}

func TestNewOperation(t *testing.T) {
	ok := newOperation("compute-fdhs", "fdhs", []string{"fie-importer", "compute-fdhs", "fdhs"}, time.Now(), nil)
	if ok.Status != "ok" || ok.Error != nil || ok.Command != "fie-importer compute-fdhs fdhs" || ok.Version != version {
		t.Errorf("ok operation = %+v", ok)
	}
	failed := newOperation("compute-fdhs", "fdhs", nil, time.Now(), errors.New("boom"))
	if failed.Status != "failed" || failed.Error == nil || *failed.Error != "boom" {
		t.Errorf("failed operation = %+v", failed)
	}
}
