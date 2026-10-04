// Command fie-importer imports Retina captures into ClickHouse.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// version is the release of fie-importer.
const version = "2.4.1"

func main() {
	os.Exit(run())
}

func run() int {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	root := &cobra.Command{
		Use:           "fie-importer",
		Short:         "Import Retina captures into ClickHouse",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newUploadFIEsCommand())
	root.AddCommand(newUploadPDsCommand())
	root.AddCommand(newUploadAgentsCommand())
	root.AddCommand(newComputeFDHsCommand())
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	return 0
}
