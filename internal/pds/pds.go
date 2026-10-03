// Package pds reads probing directive (PD) files and turns their lines into
// the rows of the ClickHouse PD table.
package pds

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
)

// Protocol numbers the orchestrator accepts.
const (
	ICMP   = 1
	UDP    = 17
	ICMPv6 = 58
)

// Row is one PD as the ClickHouse table stores it.
type Row struct {
	PDID    uint32
	AgentID string
	// IPVersion follows from the destination address, as in the orchestrator.
	IPVersion uint8
	Protocol  uint8
	// Destination is 16 bytes, IPv4 mapped.
	Destination net.IP
	NearTTL     uint8
	// FirstHalfWord and SecondHalfWord are what the agent probes with: the
	// UDP source and destination ports, or the ICMP/ICMPv6 first half-word
	// and zero, since the agent always probes with a zero second half-word.
	FirstHalfWord  uint16
	SecondHalfWord uint16
}

// line is one JSON line of a PD file, a retina-commons ProbingDirective. Its
// probing_directive_id and ip_version are ignored, as the orchestrator does.
type line struct {
	Protocol           int    `json:"protocol"`
	AgentID            string `json:"agent_id"`
	DestinationAddress string `json:"destination_address"`
	NearTTL            uint8  `json:"near_ttl"`
	NextHeader         struct {
		ICMP   *halfWords `json:"icmp_next_header"`
		ICMPv6 *halfWords `json:"icmpv6_next_header"`
		UDP    *struct {
			SourcePort      uint16 `json:"source_port"`
			DestinationPort uint16 `json:"destination_port"`
		} `json:"udp_next_header"`
	} `json:"next_header"`
}

type halfWords struct {
	First  uint16 `json:"first_half_word"`
	Second uint16 `json:"second_half_word"`
}

// Parse turns one JSON line into a row with the given ID. It accepts exactly
// what the orchestrator's insert API accepts, so that the IDs it assigns in
// file order match.
func Parse(data []byte, id uint32) (Row, error) {
	var l line
	if err := json.Unmarshal(data, &l); err != nil {
		return Row{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if l.AgentID == "" {
		return Row{}, errors.New("agent_id is empty")
	}
	addr, err := netip.ParseAddr(l.DestinationAddress)
	if err != nil || addr.Zone() != "" {
		return Row{}, fmt.Errorf("destination address %q is missing or invalid", l.DestinationAddress)
	}
	addr = addr.Unmap()
	row := Row{PDID: id, AgentID: l.AgentID, IPVersion: 6, Destination: net.IP(addr.AsSlice()).To16(), NearTTL: l.NearTTL}
	if addr.Is4() {
		row.IPVersion = 4
	}
	switch h := l.NextHeader; {
	case l.Protocol == ICMP && h.ICMP != nil:
		row.FirstHalfWord = h.ICMP.First
	case l.Protocol == ICMPv6 && h.ICMPv6 != nil:
		row.FirstHalfWord = h.ICMPv6.First
	case l.Protocol == UDP && h.UDP != nil:
		row.FirstHalfWord, row.SecondHalfWord = h.UDP.SourcePort, h.UDP.DestinationPort
	default:
		return Row{}, fmt.Errorf("protocol %d is unsupported or its next header is missing", l.Protocol)
	}
	row.Protocol = uint8(l.Protocol) //nolint:gosec // one of 1, 17, 58
	return row, nil
}

// Reader reads the PDs of a JSONL file, plain or gzip-compressed, giving each
// non-blank line the next ID.
type Reader struct {
	scanner *bufio.Scanner
	counter *countingReader
	closers []io.Closer
	nextID  uint32
	line    int
}

// NewReader reads PDs from r; IDs start at firstID. gzip is detected from the
// content.
func NewReader(r io.Reader, firstID uint32) (*Reader, error) {
	counter := &countingReader{r: r}
	buffered := bufio.NewReaderSize(counter, 1<<20)
	var source io.Reader = buffered
	var closers []io.Closer
	if magic, err := buffered.Peek(2); err == nil && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(buffered)
		if err != nil {
			return nil, fmt.Errorf("cannot read gzip stream: %w", err)
		}
		source, closers = gz, append(closers, gz)
	}
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	return &Reader{scanner: scanner, counter: counter, closers: closers, nextID: firstID}, nil
}

// Next returns the next PD. It returns io.EOF after the last one. An error
// names the line it happened on.
func (r *Reader) Next() (Row, error) {
	for r.scanner.Scan() {
		r.line++
		data := bytes.TrimSpace(r.scanner.Bytes())
		if len(data) == 0 {
			continue
		}
		row, err := Parse(data, r.nextID)
		if err != nil {
			return Row{}, fmt.Errorf("line %d (PD %d): %w", r.line, r.nextID, err)
		}
		if r.nextID == ^uint32(0) {
			return Row{}, fmt.Errorf("line %d: PD IDs exceed 32 bits", r.line)
		}
		r.nextID++
		return row, nil
	}
	if err := r.scanner.Err(); err != nil {
		return Row{}, fmt.Errorf("after line %d: %w", r.line, err)
	}
	return Row{}, io.EOF
}

// BytesRead is how many bytes of the input (compressed, for gzip) were read.
func (r *Reader) BytesRead() int64 { return r.counter.n.Load() }

// Close releases the gzip reader, if any. The input itself is not closed.
func (r *Reader) Close() error {
	var err error
	for _, c := range r.closers {
		err = errors.Join(err, c.Close())
	}
	return err
}
