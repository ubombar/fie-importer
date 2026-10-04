package main

import "testing"

func TestCheckFDHTables(t *testing.T) {
	for _, c := range []struct {
		o  computeFDHsOptions
		ok bool
	}{
		{computeFDHsOptions{table: "fdhs", fiesTable: "fies", pdsTable: "pds"}, true},
		{computeFDHsOptions{table: "fies", fiesTable: "fies", pdsTable: "pds"}, false},
		{computeFDHsOptions{table: "pds", fiesTable: "fies", pdsTable: "pds"}, false},
		{computeFDHsOptions{table: "fdhs", fiesTable: "fies-1", pdsTable: "pds"}, false},
	} {
		if err := checkFDHTables(&c.o); (err == nil) != c.ok {
			t.Errorf("checkFDHTables(%+v) = %v, want ok %v", c.o, err, c.ok)
		}
	}
}
