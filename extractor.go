package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	api "github.com/dioptra-io/retina-commons/api/v1"
	"github.com/marcboeker/go-duckdb/v2"
	"golang.org/x/sync/errgroup"
)

// AgentTerm represents a time period an agent is connected and active.
// The BeginningTime should always be valid, EndTime can be zero, that means we
// do not know if the aget is disconnected.
type AgentTerm struct {
	BeginningTime time.Time
	EndTime       time.Time
	AgentID       string
	AgentIP       net.IP
	AgentPort     int
}

type CaptureExtractorConfig struct {
	EventsDir string `json:"events_dir"`
	FIEsDir   string `json:"fies_dir"`
}

type CaptureExtractor struct {
	cfg CaptureExtractorConfig

	agentTermsMap map[string][]*AgentTerm
	pdMap         map[uint64]*api.ProbingDirective
	fieHandles    []*fieHandle
	fieIndex      int
	loaded        bool
}

func NewCaptureExtractor(cfg CaptureExtractorConfig) (*CaptureExtractor, error) {
	if cfg.EventsDir == "" || cfg.FIEsDir == "" {
		return nil, fmt.Errorf("capture extractor needs event and fies directories to proceed")
	}
	return &CaptureExtractor{
		cfg:           cfg,
		agentTermsMap: make(map[string][]*AgentTerm),
		pdMap:         make(map[uint64]*api.ProbingDirective),
		fieHandles:    make([]*fieHandle, 0),
		fieIndex:      0,
		loaded:        false,
	}, nil
}

// Load methods scans and build the PD and AgentID map that is used for
// reconstructing the actual FIEs table.
func (x *CaptureExtractor) Load() (err error) {
	if x.loaded {
		return fmt.Errorf("already loaded")
	}

	defer func() {
		if err != nil {
			for _, h := range x.fieHandles {
				_ = h.Close()
			}

			x.agentTermsMap = make(map[string][]*AgentTerm)
			x.pdMap = make(map[uint64]*api.ProbingDirective)
			x.fieHandles = nil
			x.fieIndex = 0
		}
	}()

	if err := x.loadPDMap(); err != nil {
		return fmt.Errorf("cannot load the PDs: %w", err)
	}

	if err := x.loadAgentTermsMap(); err != nil {
		return fmt.Errorf("cannot load the agent ids: %w", err)
	}

	if err := x.loadFIEHandles(); err != nil {
		return fmt.Errorf("cannot load the FIE files: %w", err)
	}

	x.loaded = true
	return nil
}

func (x *CaptureExtractor) Close() error {
	var firstErr error

	for _, h := range x.fieHandles {
		if h == nil {
			continue
		}

		if err := h.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	x.fieHandles = nil
	x.fieIndex = 0
	x.loaded = false

	return firstErr
}

// Next reads from the appropriate FIE rotation file and returns the constructed
// FIE.
func (x *CaptureExtractor) Next() (*api.ForwardingInfoElement, error) {
	if !x.loaded {
		return nil, fmt.Errorf("cannot read before loading")
	}
	for {
		if x.fieIndex >= len(x.fieHandles) {
			return nil, io.EOF
		}
		fieHandle := x.fieHandles[x.fieIndex]
		fieRow, err := fieHandle.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if err := fieHandle.Close(); err != nil {
					return nil, fmt.Errorf("close FIE handle %q: %w", fieHandle.pathName, err)
				}
				x.fieIndex++
				continue
			}
			return nil, fmt.Errorf("read FIE from %q: %w", fieHandle.pathName, err)
		}
		fie, err := x.constructFIEFromFIERow(fieHandle, fieRow)
		if err != nil {
			return nil, fmt.Errorf("construct FIE from %q: %w", fieHandle.pathName, err)
		}

		return fie, nil
	}
}

//nolint:funlen,gocyclo
func (x *CaptureExtractor) constructFIEFromFIERow(h *fieHandle, row *fieRow) (*api.ForwardingInfoElement, error) {
	pd, ok := x.pdMap[uint64(row.probingDirectiveID)]
	if !ok {
		return nil, fmt.Errorf("probing directive %d not found", row.probingDirectiveID)
	}

	if row.protocol != pd.Protocol {
		return nil, fmt.Errorf("protocol mismatch for PD %d: table=%d pd=%d", row.probingDirectiveID, row.protocol, pd.Protocol)
	}

	if row.isIPv4 != (pd.IPVersion == api.IPv4) {
		return nil, fmt.Errorf("IP version mismatch for PD %d", row.probingDirectiveID)
	}

	// captureSecond is relative to the interval start encoded in
	// the DuckDB filename.
	captureTime := h.time.Add(time.Duration(row.captureSecond) * time.Second)

	// Extract the five 6-bit timestamp deltas.
	productionDelta := uint8(row.timeDeltas & 0x3f)
	nearSentDelta := uint8((row.timeDeltas >> 6) & 0x3f)
	nearRecvDelta := uint8((row.timeDeltas >> 12) & 0x3f)
	farSentDelta := uint8((row.timeDeltas >> 18) & 0x3f)
	farRecvDelta := uint8((row.timeDeltas >> 24) & 0x3f)

	decodeTimestamp := func(delta uint8) time.Time {
		switch delta {
		case 62, 63:
			// Unknown / missing / invalid / more than 62 seconds old timestamp.
			return time.Time{}
		default:
			return captureTime.Add(-time.Duration(delta) * time.Second)
		}
	}

	productionTime := decodeTimestamp(productionDelta)

	// Resolve the agent term that was active when this FIE was captured.
	//
	// pd.AgentID tells us which agent; the term tells us which IP that
	// agent had at this point in time.
	terms, ok := x.agentTermsMap[pd.AgentID]
	if !ok {
		return nil, fmt.Errorf("agent terms for agent %q not found", pd.AgentID)
	}

	var agentTerm *AgentTerm

	for _, term := range terms {
		if term == nil {
			continue
		}
		if captureTime.Before(term.BeginningTime) {
			continue
		}
		// Treat a zero EndTime as an open-ended term.
		if !term.EndTime.IsZero() && !captureTime.Before(term.EndTime) {
			continue
		}
		agentTerm = term
		break
	}

	if agentTerm == nil {
		return nil, fmt.Errorf("no agent term for agent %q at %s", pd.AgentID, captureTime)
	}

	fie := &api.ForwardingInfoElement{
		Agent: api.Agent{
			AgentID: pd.AgentID,
		},
		ProbingDirectiveID:  uint64(row.probingDirectiveID),
		IPVersion:           pd.IPVersion,
		Protocol:            pd.Protocol,
		SourceAddress:       agentTerm.AgentIP,
		DestinationAddress:  pd.DestinationAddress,
		ProductionTimestamp: productionTime,
	}

	// NearInfo existed if we retained some information about the near probe.
	//
	// Normally the reply address is enough to determine this, but checking
	// the timestamp deltas as well handles cases where the address is nil
	// but timing information exists.
	if len(row.nearReplyAddress) > 0 {
		fie.NearInfo = &api.Info{
			ProbeTTL:          pd.NearTTL,
			ReplyAddress:      net.IP(row.nearReplyAddress),
			SentTimestamp:     decodeTimestamp(nearSentDelta),
			ReceivedTimestamp: decodeTimestamp(nearRecvDelta),
		}
	}

	// The far probe is always NearTTL + 1.
	if len(row.farReplyAddress) > 0 {
		fie.FarInfo = &api.Info{
			ProbeTTL:          pd.NearTTL + 1,
			ReplyAddress:      net.IP(row.farReplyAddress),
			SentTimestamp:     decodeTimestamp(farSentDelta),
			ReceivedTimestamp: decodeTimestamp(farRecvDelta),
		}
	}

	return fie, nil
}

func (x *CaptureExtractor) loadPDMap() error {
	pds, err := loadPDs(x.cfg.EventsDir)
	if err != nil {
		return fmt.Errorf("cannot build inserted PDs from evnets: %w", err)
	}

	for _, pd := range pds {
		if _, ok := x.pdMap[pd.ProbingDirectiveID]; ok {
			return fmt.Errorf("PD with id %d is already inserted", pd.ProbingDirectiveID)
		}
		x.pdMap[pd.ProbingDirectiveID] = pd
	}

	return nil
}

func (x *CaptureExtractor) loadAgentTermsMap() error {
	terms, err := loadAgentTerms(x.cfg.EventsDir)
	if err != nil {
		return fmt.Errorf("cannot build agent terms from events: %w", err)
	}

	for _, term := range terms {
		x.agentTermsMap[term.AgentID] = append(x.agentTermsMap[term.AgentID], term)
	}

	return nil
}

func (x *CaptureExtractor) loadFIEHandles() error {
	fies, err := loadFIEFiles(x.cfg.FIEsDir)
	if err != nil {
		return err
	}

	if len(fies) == 0 {
		return fmt.Errorf("cannot find any FIE records in the given directory: %s", x.cfg.FIEsDir)
	}

	var opened []*fieHandle

	for _, path := range fies {
		h, err := NewFIEHandle(path)
		if err != nil {
			for _, openedHandle := range opened {
				_ = openedHandle.Close()
			}
			return err
		}

		opened = append(opened, h)
	}

	x.fieHandles = opened
	return nil
}

// This is the handler that is responsible from decoding the fies.
type fieHandle struct {
	pathName  string
	time      time.Time
	db        *sql.DB
	connector *duckdb.Connector

	tableIndex int
	rows       *sql.Rows
}

var fieFilenameRE = regexp.MustCompile(`^fies-(\d{8}T\d{6}Z)\.duckdb$`)

func NewFIEHandle(pathName string) (*fieHandle, error) {
	filename := filepath.Base(pathName)

	matches := fieFilenameRE.FindStringSubmatch(filename)
	if matches == nil {
		return nil, fmt.Errorf("invalid FIE filename %q", filename)
	}

	t, err := time.Parse("20060102T150405Z", matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid FIE timestamp in %q: %w", filename, err)
	}

	// Make sure the file exists and is not a directory.
	info, err := os.Stat(pathName)
	if err != nil {
		return nil, fmt.Errorf("stat FIE file %q: %w", pathName, err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("FIE path %q is a directory", pathName)
	}

	connector, err := duckdb.NewConnector(pathName, nil)
	if err != nil {
		return nil, fmt.Errorf("open DuckDB connector for %q: %w", pathName, err)
	}

	db := sql.OpenDB(connector)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		_ = connector.Close()
		return nil, fmt.Errorf("connect to FIE database %q: %w", pathName, err)
	}

	return &fieHandle{
		pathName:  pathName,
		time:      t.UTC(),
		db:        db,
		connector: connector,
	}, nil
}

func (f *fieHandle) Next() (*fieRow, error) {
	type fieTable struct {
		name     string
		protocol api.Protocol
		isIPv4   bool
	}
	var fieTables = []fieTable{
		{"fies_icmpv4", api.ICMP, true},
		{"fies_icmpv6", api.ICMPv6, false},
		{"fies_udpv4", api.UDP, true},
		{"fies_udpv6", api.UDP, false},
	}

	for {
		if f.tableIndex >= len(fieTables) {
			return nil, io.EOF
		}
		if f.rows == nil {
			table := fieTables[f.tableIndex]
			// #nosec G201
			query := fmt.Sprintf(`
				SELECT
					probing_directive_id,
					near_reply_address,
					far_reply_address,
					capture_second,
					time_deltas
				FROM %s
			`, table.name)
			rows, err := f.db.Query(query)
			if err != nil {
				return nil, fmt.Errorf("query table %q: %w", table.name, err)
			}
			f.rows = rows
		}
		if f.rows.Next() {
			table := fieTables[f.tableIndex]
			var row fieRow
			if err := f.rows.Scan(
				&row.probingDirectiveID,
				&row.nearReplyAddress,
				&row.farReplyAddress,
				&row.captureSecond,
				&row.timeDeltas,
			); err != nil {
				return nil, fmt.Errorf("scan row from table %q: %w", table.name, err)
			}
			row.protocol = table.protocol
			row.isIPv4 = table.isIPv4
			return &row, nil
		}
		if err := f.rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate table %q: %w", fieTables[f.tableIndex].name, err)
		}
		if err := f.rows.Close(); err != nil {
			return nil, fmt.Errorf("close rows for table %q: %w", fieTables[f.tableIndex].name, err)
		}
		f.rows = nil
		f.tableIndex++
	}
}

func (f *fieHandle) Close() error {
	var firstErr error

	if f.rows != nil {
		if err := f.rows.Close(); err != nil {
			firstErr = err
		}
		f.rows = nil
	}

	if f.db != nil {
		if err := f.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		f.db = nil
	}

	if f.connector != nil {
		if err := f.connector.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		f.connector = nil
	}

	return firstErr
}

type fieRow struct {
	probingDirectiveID uint32
	nearReplyAddress   []byte
	farReplyAddress    []byte
	captureSecond      uint16
	timeDeltas         uint32
	protocol           api.Protocol
	isIPv4             bool
}

func loadPDs(eventsDir string) ([]*api.ProbingDirective, error) {
	files, err := filepath.Glob(filepath.Join(eventsDir, "events-*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("glob event files: %w", err)
	}
	sort.Strings(files)

	cmd := exec.Command(
		"jq",
		"-c",
		`select(.type == "PDBulkInsertionEvent") | .pds[]`,
	)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("create jq stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create jq stdout pipe: %w", err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start jq: %w", err)
	}

	var directives []*api.ProbingDirective
	var g errgroup.Group

	g.Go(func() error {
		defer func() { _ = stdin.Close() }()
		for _, filename := range files {
			f, err := os.Open(filename) //nolint:gosec
			if err != nil {
				return fmt.Errorf("open %q: %w", filename, err)
			}
			_, copyErr := io.Copy(stdin, f)
			closeErr := f.Close()
			if copyErr != nil {
				return fmt.Errorf("copy %q: %w", filename, copyErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close %q: %w", filename, closeErr)
			}
		}
		return nil
	})

	g.Go(func() error {
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			pd := new(api.ProbingDirective)
			if err := json.Unmarshal(scanner.Bytes(), pd); err != nil {
				return fmt.Errorf("decode probing directive: %w", err)
			}
			directives = append(directives, pd)
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("read jq output: %w", err)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("jq failed: %w: %s", err, stderr.String())
	}

	return directives, nil
}

//nolint:gocyclo,funlen
func loadAgentTerms(eventsDir string) ([]*AgentTerm, error) {
	type agentEvent struct {
		Type          string    `json:"type"`
		Timestamp     time.Time `json:"timestamp"`
		AgentID       string    `json:"agent_id"`
		RemoteAddress string    `json:"remote_address"`
	}

	files, err := filepath.Glob(filepath.Join(eventsDir, "events-*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("glob event files: %w", err)
	}
	sort.Strings(files)

	cmd := exec.Command(
		"jq",
		"-c",
		`select(.type == "AgentConnectedEvent" or .type == "AgentDisconnectedEvent")`,
	)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("create jq stdin pipe: %w", err)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create jq stdout pipe: %w", err)
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start jq: %w", err)
	}

	var (
		terms  []*AgentTerm
		active = make(map[string]*AgentTerm)
	)

	var g errgroup.Group

	g.Go(func() error {
		defer func() { _ = stdin.Close() }()

		for _, filename := range files {
			f, err := os.Open(filename) //nolint:gosec
			if err != nil {
				return fmt.Errorf("open %q: %w", filename, err)
			}

			_, copyErr := io.Copy(stdin, f)
			closeErr := f.Close()

			if copyErr != nil {
				return fmt.Errorf("copy %q: %w", filename, copyErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close %q: %w", filename, closeErr)
			}
		}

		return nil
	})

	g.Go(func() error {
		scanner := bufio.NewScanner(stdout)

		for scanner.Scan() {
			var event agentEvent
			if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
				return fmt.Errorf("decode agent event: %w", err)
			}

			host, portStr, err := net.SplitHostPort(event.RemoteAddress)
			if err != nil {
				return fmt.Errorf(
					"parse remote address %q for agent %q: %w",
					event.RemoteAddress,
					event.AgentID,
					err,
				)
			}

			ip := net.ParseIP(host)
			if ip == nil {
				return fmt.Errorf(
					"invalid IP %q for agent %q",
					host,
					event.AgentID,
				)
			}

			port, err := strconv.Atoi(portStr)
			if err != nil {
				return fmt.Errorf(
					"parse port %q for agent %q: %w",
					portStr,
					event.AgentID,
					err,
				)
			}

			switch event.Type {
			case "AgentConnectedEvent":
				active[event.AgentID] = &AgentTerm{
					BeginningTime: event.Timestamp,
					AgentID:       event.AgentID,
					AgentIP:       ip,
					AgentPort:     port,
				}

			case "AgentDisconnectedEvent":
				term, ok := active[event.AgentID]
				if !ok {
					// We may have started reading the logs after the agent
					// originally connected.
					continue
				}

				term.EndTime = event.Timestamp
				terms = append(terms, term)
				delete(active, event.AgentID)
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("read jq output: %w", err)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("jq failed: %w: %s", err, stderr.String())
	}

	// Agents that are still connected when the event history ends.
	for _, term := range active {
		terms = append(terms, term)
	}

	return terms, nil
}

func loadFIEFiles(fieDir string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(fieDir, "fies-*.duckdb"))
	if err != nil {
		return nil, fmt.Errorf("glob FIE files: %w", err)
	}

	sort.Strings(files)

	return files, nil
}
