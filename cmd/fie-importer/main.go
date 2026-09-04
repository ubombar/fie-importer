package main

import (
	"fie-importer/internal/streams"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type CountingWriter struct {
	writer io.Writer
	count  atomic.Uint64
}

func (w *CountingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.count.Add(uint64(n)) //nolint:gosec
	return n, err
}

func (w *CountingWriter) Count() uint64 {
	return w.count.Load()
}

func main() {
	rootCmd := &cobra.Command{Use: "fie-importer"}
	rootCmd.AddCommand(newParquetCommand())

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func newParquetCommand() *cobra.Command { //nolint
	var (
		fiesDir   string
		output    string
		batchSize int
	)

	cmd := &cobra.Command{
		Use:   "parquet",
		Short: "Export compressed FIEs to Parquet",
		RunE: func(cmd *cobra.Command, args []string) error {
			compressedFIEStream, err := streams.NewCompressedFIEStream(fiesDir, "")
			if err != nil {
				return err
			}

			outputWriter := cmd.OutOrStdout()
			var outputFile *os.File

			if output != "" {
				outputFile, err = os.Create(output) //nolint:gosec
				if err != nil {
					return err
				}
				defer outputFile.Close() //nolint:errcheck
				outputWriter = outputFile
			}

			countingWriter := &CountingWriter{writer: outputWriter}

			ingester, err := streams.NewLiteFIEParquetIngester(countingWriter, batchSize)
			if err != nil {
				return err
			}
			defer ingester.Close() //nolint:errcheck

			total := uint64(compressedFIEStream.Len()) //nolint:gosec
			start := time.Now()
			statusWriter := cmd.ErrOrStderr()

			fmt.Fprintf(statusWriter, "parquet export started with %d FIEs total.\n", total) //nolint

			formatBytes := func(n uint64) string {
				const unit = 1024
				if n < unit {
					return fmt.Sprintf("%d B", n)
				}

				div, exp := uint64(unit), 0
				for n >= div*unit && exp < 5 {
					div *= unit
					exp++
				}

				return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
			}

			group, ctx := errgroup.WithContext(cmd.Context())
			done := make(chan struct{})

			group.Go(func() error {
				defer close(done)
				return ingester.Ingest(compressedFIEStream.Events())
			})

			group.Go(func() error {
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()

				for {
					select {
					case <-ticker.C:
						ingested := ingester.Count()
						elapsed := time.Since(start)
						size := countingWriter.Count()

						var percentage float64
						var eta time.Duration
						var projectedSize uint64

						if total > 0 {
							percentage = float64(ingested) / float64(total) * 100
						}

						if ingested > 0 && ingested < total {
							rate := float64(ingested) / elapsed.Seconds()
							eta = time.Duration(float64(total-ingested)/rate) * time.Second
							projectedSize = uint64(float64(size) * float64(total) / float64(ingested))
						}

						fmt.Fprintf(statusWriter, "\rtotal=%d ingested=%d since=%s ETA=%s completed=%.2f%% size=%s projected=%s\n", total, ingested, elapsed.Round(time.Second), eta.Round(time.Second), percentage, formatBytes(size), formatBytes(projectedSize)) //nolint

					case <-done:
						return nil

					case <-ctx.Done():
						return nil
					}
				}
			})

			if err := group.Wait(); err != nil {
				return err
			}

			elapsed := time.Since(start)
			fmt.Fprintf(statusWriter, "\rtotal=%d ingested=%d since=%s ETA=0s completed=100.00%% size=%s projected=%s\n", total, ingester.Count(), elapsed.Round(time.Second), formatBytes(countingWriter.Count()), formatBytes(countingWriter.Count())) //nolint
			fmt.Fprintf(statusWriter, "parquet export complete in %v\n", elapsed)                                                                                                                                                                        //nolint

			return nil
		},
	}

	cmd.Flags().StringVar(&fiesDir, "fies-dir", "", "directory containing compressed FIE files")
	cmd.Flags().StringVarP(&output, "output", "o", "", "output Parquet file, stdout if omitted")
	cmd.Flags().IntVar(&batchSize, "batch-size", 2_000_000, "Parquet ingestion batch size") // >= 1M to allow billions of row groups.

	_ = cmd.MarkFlagRequired("fies-dir")

	return cmd
}
