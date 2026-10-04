package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"fie-importer/internal/agents"
	"fie-importer/internal/clickhouse"
	"fie-importer/internal/progress"
)

type uploadAgentsOptions struct {
	table       string
	filter      string
	exclude     []string
	project     string
	sshParallel int
	sshTimeout  time.Duration
	noSSH       bool
	appendRows  bool
	dropOnFail  bool
	dryRun      bool
	clickhouse  clickhouse.Config
}

func newUploadAgentsCommand() *cobra.Command {
	var o uploadAgentsOptions
	cmd := &cobra.Command{
		Use:   "upload-agents <table>",
		Short: "Upload a snapshot of the agent VMs into a ClickHouse agent table",
		Long: `Upload a snapshot of the agent VMs into a ClickHouse agent table.

The VMs are listed with gcloud: their zone, network, addresses and prefixes,
and, over gcloud compute ssh, the image of their retina-agent container. A VM
whose image cannot be looked up is kept, with the reason in version_error.

The table is created and must not exist, unless --append adds this snapshot
to an existing one. A table this run created is dropped if the upload fails,
unless --drop-on-fail=false.

ClickHouse credentials are read from CH_USER and CH_PASSWORD.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			o.table = args[0]
			return logged(cmd, &o.clickhouse, o.table, o.dryRun, func() error { return runUploadAgents(cmd.Context(), &o, agents.Gcloud) })
		},
	}
	f := cmd.Flags()
	f.StringVar(&o.filter, "filter", "labels.project=retina AND labels.env=research", "gcloud filter selecting the agent VMs")
	f.StringSliceVar(&o.exclude, "exclude", []string{"retina-server-research"}, "VM names to leave out")
	f.StringVar(&o.project, "project", "", "GCP project (default: gcloud's current project)")
	f.IntVar(&o.sshParallel, "ssh-parallel", 8, "ssh sessions at once for the image lookup")
	f.DurationVar(&o.sshTimeout, "ssh-timeout", 30*time.Second, "time allowed for each image lookup")
	f.BoolVar(&o.noSSH, "no-ssh", false, "skip the image lookup")
	f.BoolVar(&o.appendRows, "append", false, "add this snapshot to the table if it exists")
	f.BoolVar(&o.dropOnFail, "drop-on-fail", true, "drop the table if this run created it and the upload fails")
	f.BoolVar(&o.dryRun, "dry-run", false, "print the snapshot without touching ClickHouse")
	addClickHouseFlags(f, &o.clickhouse)
	return cmd
}

func runUploadAgents(ctx context.Context, o *uploadAgentsOptions, run agents.Runner) (err error) {
	if err := clickhouse.ValidateTableName(o.table); err != nil {
		return err
	}
	if o.sshParallel < 1 {
		return fmt.Errorf("--ssh-parallel must be at least 1: got %d", o.sshParallel)
	}
	disp := progress.New(os.Stderr)
	target := o.clickhouse.Database + "." + o.table
	switch {
	case o.dryRun:
		target += " (dry run)"
	case o.appendRows:
		target += " (append)"
	}
	disp.Header("upload-agents → "+target, "gcloud filter: "+o.filter)
	disp.Run()
	defer disp.Stop()

	snapshot, err := takeSnapshot(ctx, disp, o, run)
	if err != nil {
		return err
	}
	if o.dryRun {
		disp.Stop()
		printAgents(disp, snapshot)
		return nil
	}

	ch, created, err := openTable(ctx, disp, &o.clickhouse, o.table, (*clickhouse.Client).CreateAgentTable, o.appendRows)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	defer dropOnFailure(ch, o.table, created && o.dropOnFail, &err)

	disp.Phase(fmt.Sprintf("inserting %d agents", len(snapshot)))
	if err := ch.InsertAgents(ctx, o.table, snapshot); err != nil {
		return err
	}
	n, err := ch.CountSnapshot(ctx, o.table, snapshot[0].SnapshotTime)
	if err != nil {
		return err
	}
	if int(n) != len(snapshot) { //nolint:gosec // tens of rows
		return fmt.Errorf("ClickHouse has %d rows for this snapshot, %d were sent", n, len(snapshot))
	}
	disp.Phase("")
	disp.Stop()
	printAgents(disp, snapshot)
	verb := "uploaded"
	if !created {
		verb = "appended"
	}
	fmt.Fprintln(os.Stderr, disp.Success(fmt.Sprintf("%s %d agents into %s.%s, snapshot %s, verified in ClickHouse",
		verb, len(snapshot), o.clickhouse.Database, o.table, snapshot[0].SnapshotTime.Format(time.RFC3339))))
	return nil
}

// takeSnapshot lists the agents and looks up their images in parallel.
func takeSnapshot(ctx context.Context, disp *progress.Display, o *uploadAgentsOptions, run agents.Runner) ([]agents.Agent, error) {
	disp.Phase("listing VMs with gcloud")
	list, err := agents.List(ctx, run, o.project, o.filter, o.exclude, time.Now())
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, errors.New("no VM matches the filter")
	}
	if o.noSSH {
		return list, nil
	}
	var done atomic.Int64
	var mu sync.Mutex
	var pending []string
	for _, a := range list {
		pending = append(pending, a.AgentID)
	}
	status := func() {
		mu.Lock()
		defer mu.Unlock()
		waiting := ""
		if len(pending) > 0 && len(pending) <= 3 {
			waiting = " · waiting for " + strings.Join(pending, ", ")
		}
		disp.Phase(fmt.Sprintf("looking up agent images over ssh: %d/%d%s", done.Load(), len(list), waiting))
	}
	status()
	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(o.sshParallel)
	for i := range list {
		group.Go(func() error {
			agents.LookupImage(gctx, run, o.project, &list[i], o.sshTimeout)
			done.Add(1)
			mu.Lock()
			pending = removeString(pending, list[i].AgentID)
			mu.Unlock()
			status()
			return gctx.Err()
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return list, nil
}

func removeString(list []string, s string) []string {
	for i, v := range list {
		if v == s {
			return append(list[:i], list[i+1:]...)
		}
	}
	return list
}

// printAgents prints one line per agent, marking those whose unit is not
// active or whose container is not running, then counts images and problems.
func printAgents(disp *progress.Display, list []agents.Agent) {
	images := map[string]int{}
	failed, unhealthy := 0, 0
	for i := range list {
		a := &list[i]
		mark, detail := disp.Success(""), ""
		switch {
		case a.VersionError != nil && a.AgentServiceState == nil:
			mark, detail = disp.Failure(""), *a.VersionError
			failed++
		case !a.Healthy():
			mark = disp.Failure("")
			unhealthy++
		}
		if a.AgentImage != nil {
			images[*a.AgentImage]++
		}
		if detail == "" {
			detail = fmt.Sprintf("%-11s %-26s %s", service(a), deref(a.AgentContainerStatus, "no container"), deref(a.AgentImage, ""))
		}
		fmt.Fprintf(os.Stderr, "%s%-40s %-15s %s\n", mark, a.AgentID, str(a.ExternalIPv4), disp.Dim(detail))
	}
	parts := make([]string, 0, len(images)+2)
	for image, n := range images {
		parts = append(parts, fmt.Sprintf("%d × %s", n, image))
	}
	if unhealthy > 0 {
		parts = append(parts, fmt.Sprintf("%d not running (unit not active or container not running)", unhealthy))
	}
	if failed > 0 {
		parts = append(parts, fmt.Sprintf("%d lookups failed", failed))
	}
	if len(parts) > 0 {
		fmt.Fprintf(os.Stderr, "  %s\n", disp.Dim(strings.Join(parts, " · ")))
	}
}

// service formats the unit state and its restart count: "active ↻3".
func service(a *agents.Agent) string {
	s := deref(a.AgentServiceState, "?")
	if a.AgentServiceRestarts != nil && *a.AgentServiceRestarts > 0 {
		s += fmt.Sprintf(" ↻%d", *a.AgentServiceRestarts)
	}
	return s
}

func deref(s *string, fallback string) string {
	if s == nil {
		return fallback
	}
	return *s
}

func str[T fmt.Stringer](v *T) string {
	if v == nil {
		return "—"
	}
	return (*v).String()
}
