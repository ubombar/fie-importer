package importer

//
// import (
// 	"context"
//
// 	"fmt"
// 	"net"
// 	"strings"
// 	"time"
//
// 	"github.com/ClickHouse/clickhouse-go/v2"
// 	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
// 	api "github.com/dioptra-io/retina-commons/api/v1"
// )
//
// const (
// 	pdsDDL = `
// CREATE TABLE %s
// (
// 	probing_directive_id UInt64,
// 	agent_id             LowCardinality(String),
// 	ip_version           UInt8,
// 	protocol             UInt8,
// 	destination_address  IPv6,
// 	near_probe_ttl       UInt8
// )
// ENGINE = MergeTree
// ORDER BY probing_directive_id
// `
//
// 	agentTermsDDL = `
// CREATE TABLE %s
// (
// 	agent_id       LowCardinality(String),
// 	agent_ip       IPv6,
// 	agent_port     UInt16,
// 	beginning_time DateTime64(9, 'UTC'),
// 	end_time       DateTime64(9, 'UTC')
// )
// ENGINE = MergeTree
// ORDER BY (agent_id, beginning_time)
// `
//
// 	fiesDDL = `
// CREATE TABLE %s
// (
// 	-- Identifier columns.
// 	probing_directive_id UInt64,
// 	sequence_number      UInt64,
// 	agent_id             LowCardinality(String),
// 	ip_version           UInt8,
// 	protocol             UInt8,
//
// 	-- Addresses.
// 	source_address      Nullable(IPv6),
// 	destination_address IPv6,
// 	near_reply_address  Nullable(IPv6),
// 	far_reply_address   Nullable(IPv6),
//
// 	-- TTL values.
// 	near_probe_ttl Nullable(UInt8),
// 	far_probe_ttl  Nullable(UInt8)
// 		ALIAS if(isNull(near_probe_ttl), NULL, near_probe_ttl + 1),
//
// 	-- Timestamps.
// 	capture_timestamp DateTime,
// 	near_sent_age     UInt8,
// 	near_received_age UInt8,
// 	far_sent_age      UInt8,
// 	far_received_age  UInt8,
// 	production_age    UInt8,
//
// 	near_sent_timestamp Nullable(DateTime)
// 		ALIAS if(near_sent_age = 63, NULL, subtractSeconds(capture_timestamp, near_sent_age)),
// 	near_received_timestamp Nullable(DateTime)
// 		ALIAS if(near_received_age = 63, NULL, subtractSeconds(capture_timestamp, near_received_age)),
// 	far_sent_timestamp Nullable(DateTime)
// 		ALIAS if(far_sent_age = 63, NULL, subtractSeconds(capture_timestamp, far_sent_age)),
// 	far_received_timestamp Nullable(DateTime)
// 		ALIAS if(far_received_age = 63, NULL, subtractSeconds(capture_timestamp, far_received_age)),
// 	production_timestamp Nullable(DateTime)
// 		ALIAS if(production_age = 63, NULL, subtractSeconds(capture_timestamp, production_age))
// )
// ENGINE = MergeTree
// ORDER BY
// (
// 	capture_timestamp,
// 	sequence_number,
// 	agent_id,
// 	destination_address
// )
// SETTINGS index_granularity = 8192
// `
// )
//
// type FIEImporterStoreConfig struct {
// 	Address   string
// 	Database  string
// 	User      string
// 	Password  string
// 	Name      string
// 	BatchSize int
// }
//
// type FIEImporterStore struct {
// 	conn driver.Conn
//
// 	pdsTable        string
// 	agentTermsTable string
// 	fiesTable       string
//
// 	pdBatch        driver.Batch
// 	agentTermBatch driver.Batch
// 	fieBatch       driver.Batch
//
// 	pdPending        int
// 	agentTermPending int
// 	fiePending       int
//
// 	batchSize int
// }
//
// func NewFIEImporterStore(ctx context.Context, cfg *FIEImporterStoreConfig) (*FIEImporterStore, error) {
// 	conn, err := clickhouse.Open(&clickhouse.Options{
// 		Addr: []string{cfg.Address},
// 		Auth: clickhouse.Auth{
// 			Database: cfg.Database,
// 			Username: cfg.User,
// 			Password: cfg.Password,
// 		},
// 	})
// 	if err != nil {
// 		return nil, fmt.Errorf("open ClickHouse: %w", err)
// 	}
//
// 	if err := conn.Ping(ctx); err != nil {
// 		return nil, fmt.Errorf("ping ClickHouse: %w", err)
// 	}
//
// 	return &FIEImporterStore{
// 		conn:            conn,
// 		pdsTable:        quoteIdentifier(cfg.Name + "__pds"),
// 		agentTermsTable: quoteIdentifier(cfg.Name + "__agent_terms"),
// 		fiesTable:       quoteIdentifier(cfg.Name + "__fieslite"),
// 		batchSize:       cfg.BatchSize,
// 	}, nil
// }
//
// func (s *FIEImporterStore) CreateTables(ctx context.Context) error {
// 	if err := s.conn.Exec(ctx, fmt.Sprintf(pdsDDL, s.pdsTable)); err != nil {
// 		return fmt.Errorf("create PD table: %w", err)
// 	}
// 	if err := s.conn.Exec(ctx, fmt.Sprintf(agentTermsDDL, s.agentTermsTable)); err != nil {
// 		return fmt.Errorf("create agent terms table: %w", err)
// 	}
// 	if err := s.conn.Exec(ctx, fmt.Sprintf(fiesDDL, s.fiesTable)); err != nil {
// 		return fmt.Errorf("create FIE table: %w", err)
// 	}
// 	return nil
// }
//
// func (s *FIEImporterStore) InsertPD(ctx context.Context, pd *api.ProbingDirective) error {
// 	if s.pdBatch == nil {
// 		batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.pdsTable)
// 		if err != nil {
// 			return fmt.Errorf("prepare PD batch: %w", err)
// 		}
// 		s.pdBatch = batch
// 	}
//
// 	if err := s.pdBatch.Append(
// 		pd.ProbingDirectiveID,
// 		pd.AgentID,
// 		uint8(pd.IPVersion),
// 		uint8(pd.Protocol),
// 		toIPv6(pd.DestinationAddress),
// 		pd.NearTTL,
// 	); err != nil {
// 		return fmt.Errorf("append PD: %w", err)
// 	}
//
// 	s.pdPending++
// 	return s.flushIfNeeded()
// }
//
// func (s *FIEImporterStore) InsertAgentTerm(ctx context.Context, term *AgentTerm) error {
// 	if s.agentTermBatch == nil {
// 		batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.agentTermsTable)
// 		if err != nil {
// 			return fmt.Errorf("prepare agent term batch: %w", err)
// 		}
// 		s.agentTermBatch = batch
// 	}
//
// 	if err := s.agentTermBatch.Append(
// 		term.AgentID,
// 		toIPv6(term.AgentIP),
// 		uint16(term.AgentPort), //nolint:gosec
// 		term.BeginningTime,
// 		term.EndTime,
// 	); err != nil {
// 		return fmt.Errorf("append agent term: %w", err)
// 	}
//
// 	s.agentTermPending++
// 	return s.flushIfNeeded()
// }
//
// func (s *FIEImporterStore) InsertFIE(ctx context.Context, fie *ExtendedFIE) error {
// 	if s.fieBatch == nil {
// 		batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.fiesTable)
// 		if err != nil {
// 			return fmt.Errorf("prepare FIE batch: %w", err)
// 		}
// 		s.fieBatch = batch
// 	}
//
// 	var (
// 		nearProbeTTL     *uint8
// 		nearReplyAddress net.IP
// 		farReplyAddress  net.IP
// 		nearSentAge      = missingTimestampAge
// 		nearReceivedAge  = missingTimestampAge
// 		farSentAge       = missingTimestampAge
// 		farReceivedAge   = missingTimestampAge
// 	)
//
// 	if fie.NearInfo != nil {
// 		ttl := fie.NearInfo.ProbeTTL
// 		nearProbeTTL = &ttl
// 		nearReplyAddress = nullableIPv6(fie.NearInfo.ReplyAddress)
// 		nearSentAge = timestampAge(fie.CaptureTime, fie.NearInfo.SentTimestamp)
// 		nearReceivedAge = timestampAge(fie.CaptureTime, fie.NearInfo.ReceivedTimestamp)
// 	}
//
// 	if fie.FarInfo != nil {
// 		farReplyAddress = nullableIPv6(fie.FarInfo.ReplyAddress)
// 		farSentAge = timestampAge(fie.CaptureTime, fie.FarInfo.SentTimestamp)
// 		farReceivedAge = timestampAge(fie.CaptureTime, fie.FarInfo.ReceivedTimestamp)
// 	}
//
// 	if err := s.fieBatch.Append(
// 		fie.ProbingDirectiveID,
// 		fie.SequenceNumber,
// 		fie.Agent.AgentID,
// 		uint8(fie.IPVersion),
// 		uint8(fie.Protocol),
// 		nullableIPv6(fie.SourceAddress),
// 		toIPv6(fie.DestinationAddress),
// 		nearReplyAddress,
// 		farReplyAddress,
// 		nearProbeTTL,
// 		fie.CaptureTime,
// 		nearSentAge,
// 		nearReceivedAge,
// 		farSentAge,
// 		farReceivedAge,
// 		timestampAge(fie.CaptureTime, fie.ProductionTimestamp),
// 	); err != nil {
// 		return fmt.Errorf("append FIE: %w", err)
// 	}
//
// 	s.fiePending++
// 	return s.flushIfNeeded()
// }
//
// func (s *FIEImporterStore) Flush() error {
// 	if s.pdBatch != nil {
// 		if err := s.pdBatch.Send(); err != nil {
// 			return fmt.Errorf("send PD batch: %w", err)
// 		}
// 		s.pdBatch = nil
// 		s.pdPending = 0
// 	}
//
// 	if s.agentTermBatch != nil {
// 		if err := s.agentTermBatch.Send(); err != nil {
// 			return fmt.Errorf("send agent term batch: %w", err)
// 		}
// 		s.agentTermBatch = nil
// 		s.agentTermPending = 0
// 	}
//
// 	if s.fieBatch != nil {
// 		if err := s.fieBatch.Send(); err != nil {
// 			return fmt.Errorf("send FIE batch: %w", err)
// 		}
// 		s.fieBatch = nil
// 		s.fiePending = 0
// 	}
//
// 	return nil
// }
//
// func (s *FIEImporterStore) flushIfNeeded() error {
// 	if s.batchSize <= 0 {
// 		return nil
// 	}
//
// 	if s.pdPending >= s.batchSize ||
// 		s.agentTermPending >= s.batchSize ||
// 		s.fiePending >= s.batchSize {
// 		return s.Flush()
// 	}
//
// 	return nil
// }
//
// func (s *FIEImporterStore) Close() error {
// 	return s.conn.Close()
// }
//
// func timestampAge(captureTime, timestamp time.Time) uint8 {
// 	if timestamp.IsZero() {
// 		return missingTimestampAge
// 	}
//
// 	age := int64(captureTime.Sub(timestamp) / time.Second)
// 	if age < 0 || age > 62 {
// 		return missingTimestampAge
// 	}
//
// 	return uint8(age)
// }
//
// func nullableIPv6(ip net.IP) net.IP {
// 	if ip == nil || ip.IsUnspecified() {
// 		return nil
// 	}
// 	return ip.To16()
// }
//
// func toIPv6(ip net.IP) net.IP {
// 	ip = ip.To16()
// 	if ip == nil {
// 		return net.IPv6zero
// 	}
// 	return ip
// }
//
// func quoteIdentifier(name string) string {
// 	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
// }
