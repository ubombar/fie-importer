package main

import "fmt"

func main() {
	fmt.Println("test")
}

//
// import (
// 	"context"
// 	"errors"
// 	"flag"
// 	"fmt"
// 	"io"
// 	"net"
// 	"os"
// 	"time"
//
// 	"github.com/ClickHouse/clickhouse-go/v2"
// 	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
// 	api "github.com/dioptra-io/retina-commons/api/v1"
// )
//
// const fieBatchSize = 100_000
// const missingTimestampAge uint8 = 63
//
// type config struct {
// 	eventsDir          string
// 	fiesDir            string
// 	clickhouseAddress  string
// 	clickhouseDatabase string
// 	name               string
// 	clickhouseUser     string
// 	clickhousePassword string
// }
//
// func main() {
// 	if err := run(); err != nil {
// 		fmt.Fprintf(os.Stderr, "error: %v\n", err)
// 		os.Exit(1)
// 	}
// }
//
// func run() (retErr error) {
// 	cfg := &config{}
// 	flag.StringVar(&cfg.eventsDir, "events-dir", "", "directory containing events-*.jsonl")
// 	flag.StringVar(&cfg.fiesDir, "fies-dir", "", "directory containing FIE DuckDB captures")
// 	flag.StringVar(&cfg.clickhouseAddress, "clickhouse-address", "", "ClickHouse address, for example localhost:9000")
// 	flag.StringVar(&cfg.clickhouseDatabase, "clickhouse-database", "", "ClickHouse database")
// 	flag.StringVar(&cfg.name, "name", "", "experiment name")
// 	flag.Parse()
// 	cfg.clickhouseUser = os.Getenv("CH_USER")
// 	cfg.clickhousePassword = os.Getenv("CH_PASSWORD")
// 	switch {
// 	case cfg.eventsDir == "":
// 		return fmt.Errorf("--events-dir is required")
// 	case cfg.fiesDir == "":
// 		return fmt.Errorf("--fies-dir is required")
// 	case cfg.clickhouseAddress == "":
// 		return fmt.Errorf("--clickhouse-address is required")
// 	case cfg.clickhouseDatabase == "":
// 		return fmt.Errorf("--clickhouse-database is required")
// 	case cfg.name == "":
// 		return fmt.Errorf("--name is required")
// 	case cfg.clickhouseUser == "":
// 		return fmt.Errorf("CH_USER is required")
// 	}
//
// 	ctx := context.Background()
// 	extractor, err := NewCaptureExtractor(CaptureExtractorConfig{
// 		EventsDir: cfg.eventsDir,
// 		FIEsDir:   cfg.fiesDir,
// 	})
// 	if err != nil {
// 		return fmt.Errorf("create extractor: %w", err)
// 	}
//
// 	if err := extractor.Load(); err != nil {
// 		return fmt.Errorf("load extractor: %w", err)
// 	}
//
// 	defer func() {
// 		if err := extractor.Close(); err != nil && retErr == nil {
// 			retErr = fmt.Errorf("close extractor: %w", err)
// 		}
// 	}()
//
// 	conn, err := openClickHouse(ctx, cfg)
// 	if err != nil {
// 		return err
// 	}
// 	defer func() { _ = conn.Close() }()
//
// 	if err := createTables(ctx, conn, cfg); err != nil {
// 		return fmt.Errorf("create ClickHouse tables: %w", err)
// 	}
//
// 	pds, err := extractor.PDs()
// 	if err != nil {
// 		return fmt.Errorf("get PDs: %w", err)
// 	}
//
// 	if err := insertPDs(ctx, conn, cfg, pds); err != nil {
// 		return fmt.Errorf("insert PDs: %w", err)
// 	}
//
// 	agentTerms, err := extractor.AgentTerms()
// 	if err != nil {
// 		return fmt.Errorf("get agent terms: %w", err)
// 	}
//
// 	if err := insertAgentTerms(ctx, conn, cfg, agentTerms); err != nil {
// 		return fmt.Errorf("insert agent terms: %w", err)
// 	}
//
// 	fieCount, err := insertFIEs(ctx, conn, cfg, extractor)
// 	if err != nil {
// 		return fmt.Errorf("insert FIEs: %w", err)
// 	}
//
// 	fmt.Printf("Imported %d PDs\n", len(pds))
// 	fmt.Printf("Imported %d agent terms\n", len(agentTerms))
// 	fmt.Printf("Imported %d FIEs\n", fieCount)
// 	fmt.Printf("PD table: %s.%s__pds\n", cfg.clickhouseDatabase, cfg.name)
// 	fmt.Printf("Agent terms table: %s.%s__agent_terms\n", cfg.clickhouseDatabase, cfg.name)
// 	fmt.Printf("FIE table: %s.%s__fies\n", cfg.clickhouseDatabase, cfg.name)
//
// 	return nil
// }
//
//
// func printProgress(inserted uint64, started time.Time) {
// 	elapsed := time.Since(started)
// 	rate := float64(inserted) / elapsed.Seconds()
//
// 	fmt.Printf(
// 		"\rImported FIEs: %-12d | %8.0f FIE/s | elapsed: %s",
// 		inserted,
// 		rate,
// 		elapsed.Round(time.Second),
// 	)
// }
//
// func openClickHouse(ctx context.Context, cfg *config) (clickhouse.Conn, error) {
// 	conn, err := clickhouse.Open(&clickhouse.Options{
// 		Addr: []string{cfg.clickhouseAddress},
// 		Auth: clickhouse.Auth{
// 			Database: cfg.clickhouseDatabase,
// 			Username: cfg.clickhouseUser,
// 			Password: cfg.clickhousePassword,
// 		},
// 		Compression: &clickhouse.Compression{
// 			Method: clickhouse.CompressionLZ4,
// 		},
// 		DialTimeout: 30 * time.Second,
// 	})
// 	if err != nil {
// 		return nil, fmt.Errorf("open ClickHouse: %w", err)
// 	}
//
// 	if err := conn.Ping(ctx); err != nil {
// 		_ = conn.Close()
// 		return nil, fmt.Errorf("ping ClickHouse: %w", err)
// 	}
//
// 	return conn, nil
// }
//
// //nolint:funlen
// func createTables(ctx context.Context, conn clickhouse.Conn, cfg *config) error {
// 	pdTable := tableName(cfg, "pds")
// 	agentTermsTable := tableName(cfg, "agent_terms")
// 	fieTable := tableName(cfg, "fies")
//
// 	pdDDL := fmt.Sprintf(`
// CREATE TABLE %s
// (
//     probing_directive_id UInt64,
//     agent_id             String,
//     ip_version           UInt8,
//     protocol             UInt8,
//     destination_address  IPv6,
//     near_ttl             UInt8,
//     next_header_type     Enum8(
//         'icmp'   = 1,
//         'icmpv6' = 2,
//         'udp'    = 3
//     ),
//     first_half_word      UInt16,
//     second_half_word     UInt16,
//     event_timestamp      DateTime64(9, 'UTC')
// )
// ENGINE = MergeTree
// ORDER BY
// (
//     destination_address,
//     agent_id,
//     probing_directive_id
// )
// SETTINGS index_granularity = 8192
// `, pdTable)
//
// 	agentTermsDDL := fmt.Sprintf(`
// CREATE TABLE %s
// (
//     agent_id       String,
//     agent_ip       IPv6,
//     agent_port     UInt16,
//     beginning_time DateTime64(9, 'UTC'),
//     end_time       DateTime64(9, 'UTC')
// )
// ENGINE = MergeTree
// ORDER BY
// (
//     agent_id,
//     beginning_time
// )
// SETTINGS index_granularity = 8192
// `, agentTermsTable)
//
// 	fiesLiteDDL := fmt.Sprintf(`
// CREATE TABLE %s
// (
//     -- Identifier columns.
//     probing_directive_id UInt64,
//     sequence_number      UInt64,
//     agent_id             LowCardinality(String),
//     ip_version           UInt8,
//     protocol             UInt8,
//
//     -- Addresses.
//     source_address      Nullable(IPv6),
//     destination_address IPv6,
//     near_reply_address  Nullable(IPv6),
//     far_reply_address   Nullable(IPv6),
//
//     -- TTL values.
//     near_probe_ttl Nullable(UInt8),
//     far_probe_ttl  Nullable(UInt8) ALIAS if(isNull(near_probe_ttl), NULL, near_probe_ttl + 1),
//
//     -- Physical timestamps.
//     capture_timestamp  DateTime,
//     near_sent_age      UInt8,
//     near_received_age  UInt8,
//     far_sent_age       UInt8,
//     far_received_age   UInt8,
//     production_age     UInt8,
//
//     -- Reconstructed timestamps.
//     near_sent_timestamp Nullable(DateTime)
//         ALIAS if(
//             near_sent_age = 63,
//             NULL,
//             subtractSeconds(capture_timestamp, near_sent_age)
//         ),
//     near_received_timestamp Nullable(DateTime)
//         ALIAS if(
//             near_received_age = 63,
//             NULL,
//             subtractSeconds(capture_timestamp, near_received_age)
//         ),
//     far_sent_timestamp Nullable(DateTime)
//         ALIAS if(
//             far_sent_age = 63,
//             NULL,
//             subtractSeconds(capture_timestamp, far_sent_age)
//         ),
//     far_received_timestamp Nullable(DateTime)
//         ALIAS if(
//             far_received_age = 63,
//             NULL,
//             subtractSeconds(capture_timestamp, far_received_age)
//         ),
//     production_timestamp Nullable(DateTime)
//         ALIAS if(
//             production_age = 63,
//             NULL,
//             subtractSeconds(capture_timestamp, production_age)
//         )
// )
// ENGINE = MergeTree
// ORDER BY
// (
// 	capture_timestamp,
//     sequence_number,
//     agent_id,
//     destination_address
// )
// SETTINGS index_granularity = 8192
// `, fieTable)
//
// 	if err := conn.Exec(ctx, pdDDL); err != nil {
// 		return fmt.Errorf("create PD table: %w", err)
// 	}
//
// 	if err := conn.Exec(ctx, agentTermsDDL); err != nil {
// 		return fmt.Errorf("create agent terms table: %w", err)
// 	}
//
// 	if err := conn.Exec(ctx, fiesLiteDDL); err != nil {
// 		return fmt.Errorf("create FIE table: %w", err)
// 	}
//
// 	return nil
// }
//
// func insertPDs(ctx context.Context, conn clickhouse.Conn, cfg *config, pds []*api.ProbingDirective) error {
// 	if len(pds) == 0 {
// 		return nil
// 	}
//
// 	query := fmt.Sprintf(`
// INSERT INTO %s
// (
//     probing_directive_id,
//     agent_id,
//     ip_version,
//     protocol,
//     destination_address,
//     near_ttl,
//     next_header_type,
//     first_half_word,
//     second_half_word,
//     event_timestamp
// )
// `, tableName(cfg, "pds"))
//
// 	batch, err := conn.PrepareBatch(ctx, query)
// 	if err != nil {
// 		return fmt.Errorf("prepare PD batch: %w", err)
// 	}
// 	defer func() { _ = batch.Close() }()
//
// 	for _, pd := range pds {
// 		if pd == nil {
// 			continue
// 		}
//
// 		nextHeaderType, firstHalfWord, secondHalfWord, err := nextHeaderValues(pd)
// 		if err != nil {
// 			return err
// 		}
//
// 		if err := batch.Append(
// 			pd.ProbingDirectiveID,
// 			pd.AgentID,
// 			uint8(pd.IPVersion),
// 			uint8(pd.Protocol),
// 			toIPv6(pd.DestinationAddress),
// 			pd.NearTTL,
// 			nextHeaderType,
// 			firstHalfWord,
// 			secondHalfWord,
// 			zeroTimestamp,
// 		); err != nil {
// 			return fmt.Errorf("append PD %d: %w", pd.ProbingDirectiveID, err)
// 		}
// 	}
//
// 	if err := batch.Send(); err != nil {
// 		return fmt.Errorf("send PD batch: %w", err)
// 	}
//
// 	return nil
// }
//
// func nextHeaderValues(pd *api.ProbingDirective) (string, uint16, uint16, error) {
// 	switch pd.Protocol {
// 	case api.ICMP:
// 		if pd.NextHeader.ICMPNextHeader == nil {
// 			return "icmp", 0, 0, nil
// 		}
//
// 		return "icmp",
// 			pd.NextHeader.ICMPNextHeader.FirstHalfWord,
// 			pd.NextHeader.ICMPNextHeader.SecondHalfWord,
// 			nil
//
// 	case api.ICMPv6:
// 		if pd.NextHeader.ICMPv6NextHeader == nil {
// 			return "icmpv6", 0, 0, nil
// 		}
//
// 		return "icmpv6",
// 			pd.NextHeader.ICMPv6NextHeader.FirstHalfWord,
// 			pd.NextHeader.ICMPv6NextHeader.SecondHalfWord,
// 			nil
//
// 	case api.UDP:
// 		if pd.NextHeader.UDPNextHeader == nil {
// 			return "udp", 0, 0, nil
// 		}
//
// 		return "udp",
// 			pd.NextHeader.UDPNextHeader.SourcePort,
// 			pd.NextHeader.UDPNextHeader.DestinationPort,
// 			nil
//
// 	default:
// 		return "", 0, 0, fmt.Errorf("unsupported protocol %d for PD %d", pd.Protocol, pd.ProbingDirectiveID)
// 	}
// }
//
// func insertAgentTerms(ctx context.Context, conn clickhouse.Conn, cfg *config, terms []*AgentTerm) error {
// 	if len(terms) == 0 {
// 		return nil
// 	}
//
// 	query := fmt.Sprintf(`
// INSERT INTO %s
// (
//     agent_id,
//     agent_ip,
//     agent_port,
//     beginning_time,
//     end_time
// )
// `, tableName(cfg, "agent_terms"))
//
// 	batch, err := conn.PrepareBatch(ctx, query)
// 	if err != nil {
// 		return fmt.Errorf("prepare agent terms batch: %w", err)
// 	}
// 	defer func() { _ = batch.Close() }()
//
// 	for _, term := range terms {
// 		if term == nil {
// 			continue
// 		}
//
// 		if term.AgentPort < 0 || term.AgentPort > 65535 {
// 			return fmt.Errorf("agent %q port %d exceeds uint16", term.AgentID, term.AgentPort)
// 		}
//
// 		if err := batch.Append(
// 			term.AgentID,
// 			toIPv6(term.AgentIP),
// 			uint16(term.AgentPort),
// 			clickhouseTime(term.BeginningTime),
// 			clickhouseTime(term.EndTime),
// 		); err != nil {
// 			return fmt.Errorf("append agent term for %q: %w", term.AgentID, err)
// 		}
// 	}
//
// 	if err := batch.Send(); err != nil {
// 		return fmt.Errorf("send agent terms batch: %w", err)
// 	}
//
// 	return nil
// }
//
// func insertFIEs(ctx context.Context, conn clickhouse.Conn, cfg *config, extractor *CaptureExtractor) (uint64, error) {
// 	var inserted uint64
// 	started := time.Now()
//
// 	batch, err := newFIEBatch(ctx, conn, cfg)
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer func() { _ = batch.Close() }()
//
// 	batchCount := 0
// 	for {
// 		fie, err := extractor.Next()
// 		if err != nil {
// 			if errors.Is(err, io.EOF) {
// 				break
// 			}
// 			return inserted, err
// 		}
//
// 		sequenceNumber := extractor.EmittedCount() - 1
// 		if err := appendFIE(batch, fie); err != nil {
// 			return inserted, fmt.Errorf("append FIE %d: %w", sequenceNumber, err)
// 		}
//
// 		inserted++
// 		batchCount++
// 		if batchCount < fieBatchSize {
// 			continue
// 		}
//
// 		if err := batch.Send(); err != nil {
// 			return inserted, fmt.Errorf("send FIE batch: %w", err)
// 		}
// 		printProgress(inserted, started)
//
// 		batch, err = newFIEBatch(ctx, conn, cfg)
// 		if err != nil {
// 			return inserted, err
// 		}
// 		batchCount = 0
// 	}
//
// 	if batchCount > 0 {
// 		if err := batch.Send(); err != nil {
// 			return inserted, fmt.Errorf("send final FIE batch: %w", err)
// 		}
// 	}
//
// 	printProgress(inserted, started)
// 	fmt.Println()
// 	return inserted, nil
// }
//
// func newFIEBatch(ctx context.Context, conn clickhouse.Conn, cfg *config) (driver.Batch, error) {
// 	query := fmt.Sprintf(`
// INSERT INTO %s
// (
//     sequence_number,
//     agent_id,
//     probing_directive_id,
//     ip_version,
//     protocol,
//     source_address,
//     destination_address,
//     near_probe_ttl,
//     near_reply_address,
//     near_sent_timestamp,
//     near_received_timestamp,
//     far_probe_ttl,
//     far_reply_address,
//     far_sent_timestamp,
//     far_received_timestamp,
//     production_timestamp
// )
// `, tableName(cfg, "fies"))
//
// 	batch, err := conn.PrepareBatch(ctx, query)
// 	if err != nil {
// 		return nil, fmt.Errorf("prepare FIE batch: %w", err)
// 	}
//
// 	return batch, nil
// }
//
// func appendFIE(batch driver.Batch, fie *DuckDBFIE) error {
// 	var (
// 		nearProbeTTL     *uint8
// 		nearReplyAddress net.IP
// 		farReplyAddress  net.IP
// 		nearSentAge      uint8 = 63
// 		nearReceivedAge  uint8 = 63
// 		farSentAge       uint8 = 63
// 		farReceivedAge   uint8 = 63
// 	)
//
// 	if fie.NearInfo != nil {
// 		ttl := fie.NearInfo.ProbeTTL
// 		nearProbeTTL = &ttl
// 		nearReplyAddress = nullableIPv6(fie.NearInfo.ReplyAddress)
// 		nearSentAge = timestampAge(fie.CaptureTime, fie.NearInfo.SentTimestamp)
// 		nearReceivedAge = timestampAge(fie.CaptureTime, fie.NearInfo.ReceivedTimestamp)
// 	}
// 	if fie.FarInfo != nil {
// 		farReplyAddress = nullableIPv6(fie.FarInfo.ReplyAddress)
// 		farSentAge = timestampAge(fie.CaptureTime, fie.FarInfo.SentTimestamp)
// 		farReceivedAge = timestampAge(fie.CaptureTime, fie.FarInfo.ReceivedTimestamp)
// 	}
// 	productionAge := timestampAge(fie.CaptureTime, fie.ProductionTimestamp)
//
// 	return batch.Append(
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
// 		productionAge,
// 	)
// }
//
//
// func clickhouseTime(t time.Time) time.Time {
// 	if t.IsZero() {
// 		return zeroTimestamp
// 	}
//
// 	return t.UTC()
// }
