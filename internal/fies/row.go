// Package fies reads fies2 capture files and turns their rows into the rows of
// the ClickHouse FIE table.
package fies

import (
	"net"
	"time"
)

// Row is one FIE as the ClickHouse table stores it. Every time is absolute.
// A nil pointer is NULL: SourceFormat tells whether the format lacks the field
// or the value is missing.
type Row struct {
	SourceFormat string
	PDID         uint32
	// CaptureTime is when the orchestrator received the FIE.
	CaptureTime time.Time
	// FIETransitS is the agent-to-orchestrator delay in seconds, as stored.
	FIETransitS *uint8
	// NearReplyAddr and FarReplyAddr are 16-byte addresses, IPv4 mapped.
	NearReplyAddr     *net.IP
	FarReplyAddr      *net.IP
	NearProbeSentTime *time.Time
	NearReplyRecvTime *time.Time
	FarProbeSentTime  *time.Time
	FarReplyRecvTime  *time.Time
}

// Raw2a is one row of a fies2a file. Nil pointers are NULLs.
type Raw2a struct {
	PDID          uint32
	CaptureSecond uint16
	NearReplyAddr []byte
	FarReplyAddr  []byte
	NearReplyAgeS *uint8
	FarReplyAgeS  *uint8
	FIETransitS   *uint8
}

// Convert2a turns a fies2a row into a table row. intervalStart is the start of
// the file's interval. fies2a has no probe sent times; a reply's received time
// is capture time - transit - reply age, NULL when any of them is NULL.
func Convert2a(raw *Raw2a, intervalStart time.Time) Row {
	capture := intervalStart.Add(time.Duration(raw.CaptureSecond) * time.Second).UTC()
	return Row{
		SourceFormat:      Format2a,
		PDID:              raw.PDID,
		CaptureTime:       capture,
		FIETransitS:       raw.FIETransitS,
		NearReplyAddr:     toIPv6(raw.NearReplyAddr),
		FarReplyAddr:      toIPv6(raw.FarReplyAddr),
		NearReplyRecvTime: recvTime(capture, raw.FIETransitS, raw.NearReplyAddr, raw.NearReplyAgeS),
		FarReplyRecvTime:  recvTime(capture, raw.FIETransitS, raw.FarReplyAddr, raw.FarReplyAgeS),
	}
}

func recvTime(capture time.Time, transit *uint8, addr []byte, age *uint8) *time.Time {
	if transit == nil || age == nil || len(addr) == 0 {
		return nil
	}
	t := capture.Add(-time.Duration(int(*transit)+int(*age)) * time.Second)
	return &t
}

func toIPv6(b []byte) *net.IP {
	if len(b) != net.IPv4len && len(b) != net.IPv6len {
		return nil
	}
	ip := net.IP(b).To16()
	return &ip
}
