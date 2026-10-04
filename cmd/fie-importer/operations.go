package main

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"fie-importer/internal/clickhouse"
)

// logged runs a command and records it in the operations table, whether it
// succeeds or fails. Dry runs are not recorded. Failing to record is a
// warning: the command's own result stands.
func logged(cmd *cobra.Command, c *clickhouse.Config, table string, dryRun bool, run func() error) error {
	started := time.Now()
	err := run()
	if dryRun {
		return err
	}
	op := newOperation(cmd.Name(), table, os.Args, started, err)
	if lerr := logOperation(c, op); lerr != nil {
		fmt.Fprintf(os.Stderr, "warning: the run was not recorded in %s: %v\n", clickhouse.OperationsTable, lerr)
	}
	return err
}

func newOperation(name, table string, args []string, started time.Time, err error) *clickhouse.Operation {
	op := &clickhouse.Operation{
		Time:      started.UTC(),
		Operation: name,
		Table:     table,
		Command:   commandLine(args),
		Status:    "ok",
		Duration:  time.Since(started),
		Version:   version,
	}
	if err != nil {
		msg := err.Error()
		op.Status, op.Error = "failed", &msg
	}
	if u, uerr := user.Current(); uerr == nil {
		op.OSUser = u.Username
	}
	op.Host, _ = os.Hostname()
	return op
}

// logOperation connects on its own, since the command's context may be
// canceled and its connection closed.
func logOperation(c *clickhouse.Config, op *clickhouse.Operation) error {
	c.Username, c.Password = os.Getenv("CH_USER"), os.Getenv("CH_PASSWORD")
	if c.Username == "" {
		return fmt.Errorf("CH_USER is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ch, err := clickhouse.Connect(ctx, c)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	return ch.LogOperation(ctx, op)
}

// commandLine rebuilds the command as it could be typed again: the program
// name without its directory, and arguments quoted for a POSIX shell where
// needed. Credentials never appear: they come only from the environment.
func commandLine(args []string) string {
	if len(args) == 0 {
		return ""
	}
	parts := make([]string, 0, len(args))
	parts = append(parts, filepath.Base(args[0]))
	for _, a := range args[1:] {
		parts = append(parts, shellQuote(a))
	}
	return strings.Join(parts, " ")
}

func shellQuote(s string) string {
	if s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_=./:,@+%") == "" {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
