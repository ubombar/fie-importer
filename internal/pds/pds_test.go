package pds

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
)

const sample = `{"probing_directive_id":5577364,"ip_version":6,"protocol":1,"agent_id":"a","destination_address":"68.236.223.99","near_ttl":3,"next_header":{"icmp_next_header":{"first_half_word":11150,"second_half_word":4238}}}

{"probing_directive_id":9,"ip_version":6,"protocol":58,"agent_id":"b","destination_address":"2001:db8::1","near_ttl":7,"next_header":{"icmpv6_next_header":{"first_half_word":7,"second_half_word":8}}}

{"protocol":17,"agent_id":"a","destination_address":"::ffff:192.0.2.9","near_ttl":255,"next_header":{"udp_next_header":{"source_port":24000,"destination_port":33434}}}
`

func readAll(t *testing.T, r io.Reader, firstID uint32) ([]Row, error) {
	t.Helper()
	reader, err := NewReader(r, firstID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	var rows []Row
	for {
		row, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
		rows = append(rows, row)
	}
}

func checkSample(t *testing.T, rows []Row, firstID uint32) {
	t.Helper()
	// IDs follow row order and blank lines are skipped; the file's own IDs
	// and ip_version are ignored. ICMP and ICMPv6 second half-words are
	// zeroed, UDP keeps both ports, IPv4 is stored mapped, TTL 255 passes.
	want := []Row{
		{PDID: firstID, AgentID: "a", IPVersion: 4, Protocol: ICMP, Destination: net.ParseIP("68.236.223.99"), NearTTL: 3, FirstHalfWord: 11150},
		{PDID: firstID + 1, AgentID: "b", IPVersion: 6, Protocol: ICMPv6, Destination: net.ParseIP("2001:db8::1"), NearTTL: 7, FirstHalfWord: 7},
		{PDID: firstID + 2, AgentID: "a", IPVersion: 4, Protocol: UDP, Destination: net.ParseIP("192.0.2.9"), NearTTL: 255, FirstHalfWord: 24000, SecondHalfWord: 33434},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows:\n got %+v\nwant %+v", rows, want)
	}
}

func TestReader_Plain(t *testing.T) {
	rows, err := readAll(t, strings.NewReader(sample), 0)
	if err != nil {
		t.Fatal(err)
	}
	checkSample(t, rows, 0)
}

func TestReader_GzipAndFirstID(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte(sample))
	_ = gz.Close()
	size := int64(buf.Len())
	reader, err := NewReader(&buf, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var rows []Row
	for {
		row, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	checkSample(t, rows, 1000)
	if reader.BytesRead() != size {
		t.Errorf("BytesRead = %d, want the compressed size %d", reader.BytesRead(), size)
	}
}

func TestReader_Rejects(t *testing.T) {
	for name, line := range map[string]string{
		"no agent":         `{"protocol":1,"agent_id":"","destination_address":"192.0.2.1","next_header":{"icmp_next_header":{}}}`,
		"bad address":      `{"protocol":1,"agent_id":"a","destination_address":"192.0.2","next_header":{"icmp_next_header":{}}}`,
		"no address":       `{"protocol":1,"agent_id":"a","next_header":{"icmp_next_header":{}}}`,
		"zone":             `{"protocol":58,"agent_id":"a","destination_address":"fe80::1%eth0","next_header":{"icmpv6_next_header":{}}}`,
		"tcp":              `{"protocol":6,"agent_id":"a","destination_address":"192.0.2.1","next_header":{"udp_next_header":{}}}`,
		"header mismatch":  `{"protocol":17,"agent_id":"a","destination_address":"192.0.2.1","next_header":{"icmp_next_header":{}}}`,
		"missing header":   `{"protocol":1,"agent_id":"a","destination_address":"192.0.2.1"}`,
		"not json":         `probing_directive_id,1`,
		"ttl out of range": `{"protocol":1,"agent_id":"a","destination_address":"192.0.2.1","near_ttl":256,"next_header":{"icmp_next_header":{}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := readAll(t, strings.NewReader(sample+line+"\n"), 0)
			if err == nil {
				t.Fatal("got nil error")
			}
			if len(rows) != 3 || !strings.Contains(err.Error(), "line 6 (PD 3)") {
				t.Fatalf("got %d rows and %v: want the 3 valid rows, then an error naming line 6 and PD 3", len(rows), err)
			}
		})
	}
}
