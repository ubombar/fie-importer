// Package agents lists the Retina agent VMs with gcloud and looks up the
// agent image each one runs.
package agents

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Agent is one agent VM as the ClickHouse table stores it. Nil pointers are
// NULL.
type Agent struct {
	SnapshotTime       time.Time
	AgentID            string
	Region             string
	Zone               string
	Network            string
	Subnetwork         string
	MachineType        string
	VMStatus           string
	VMCreatedTime      time.Time
	VMLastStartTime    *time.Time
	InternalIPv4       *net.IP
	InternalIPv4Prefix *string
	ExternalIPv4       *net.IP
	InternalIPv6       *net.IP
	ExternalIPv6       *net.IP
	ExternalIPv6Prefix *string
	SubnetIPv6Prefix   *string
	AgentImage         *string
	AgentImageDigest   *string
	// AgentContainerState is Docker's state of the retina-agent container:
	// running, exited, restarting…
	AgentContainerState *string
	// AgentContainerStatus is Docker's own wording, e.g. "Up 2 minutes".
	AgentContainerStatus      *string
	AgentContainerStartedTime *time.Time
	// AgentServiceState and AgentServiceRestarts come from the systemd unit
	// that runs the container: a unit that keeps restarting is crash-looping.
	AgentServiceState    *string
	AgentServiceRestarts *uint32
	VersionError         *string
}

// Healthy reports whether the agent's unit is active and its container is
// running.
func (a *Agent) Healthy() bool {
	return a.AgentServiceState != nil && *a.AgentServiceState == "active" &&
		a.AgentContainerState != nil && *a.AgentContainerState == "running"
}

// Runner runs a gcloud command and returns its standard output. Tests replace
// it.
type Runner func(ctx context.Context, args ...string) ([]byte, error)

// Gcloud runs the gcloud binary.
func Gcloud(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "gcloud", args...) //nolint:gosec // fixed binary; arguments are passed as is, never through a shell
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if i := strings.LastIndexByte(msg, '\n'); i >= 0 {
			msg = strings.TrimSpace(msg[i+1:])
		}
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("gcloud %s: %s", args[0]+" "+args[1]+" "+args[2], msg)
	}
	return out, nil
}

// instance is the part of `gcloud compute instances list --format=json` used.
type instance struct {
	Name               string `json:"name"`
	Zone               string `json:"zone"`
	MachineType        string `json:"machineType"`
	Status             string `json:"status"`
	CreationTimestamp  string `json:"creationTimestamp"`
	LastStartTimestamp string `json:"lastStartTimestamp"`
	NetworkInterfaces  []struct {
		Network       string `json:"network"`
		Subnetwork    string `json:"subnetwork"`
		NetworkIP     string `json:"networkIP"`
		IPv6Address   string `json:"ipv6Address"`
		AccessConfigs []struct {
			NatIP string `json:"natIP"`
		} `json:"accessConfigs"`
		IPv6AccessConfigs []struct {
			ExternalIPv6             string `json:"externalIpv6"`
			ExternalIPv6PrefixLength int    `json:"externalIpv6PrefixLength"`
		} `json:"ipv6AccessConfigs"`
	} `json:"networkInterfaces"`
}

// subnet is the part of `gcloud compute networks subnets list --format=json`
// used.
type subnet struct {
	SelfLink           string `json:"selfLink"`
	IPCidrRange        string `json:"ipCidrRange"`
	ExternalIPv6Prefix string `json:"externalIpv6Prefix"`
	InternalIPv6Prefix string `json:"internalIpv6Prefix"`
}

// List returns the instances matching filter, minus those named in exclude,
// sorted by name. project may be empty for gcloud's current project.
func List(ctx context.Context, run Runner, project, filter string, exclude []string, now time.Time) ([]Agent, error) {
	args := []string{"compute", "instances", "list", "--format=json"}
	if filter != "" {
		args = append(args, "--filter="+filter)
	}
	out, err := run(ctx, withProject(args, project)...)
	if err != nil {
		return nil, err
	}
	var instances []instance
	if err := json.Unmarshal(out, &instances); err != nil {
		return nil, fmt.Errorf("cannot parse the instance list: %w", err)
	}
	out, err = run(ctx, withProject([]string{"compute", "networks", "subnets", "list", "--format=json"}, project)...)
	if err != nil {
		return nil, err
	}
	var subnets []subnet
	if err := json.Unmarshal(out, &subnets); err != nil {
		return nil, fmt.Errorf("cannot parse the subnet list: %w", err)
	}
	bySelfLink := map[string]*subnet{}
	for i := range subnets {
		bySelfLink[subnets[i].SelfLink] = &subnets[i]
	}

	var agents []Agent
	for i := range instances {
		if slices.Contains(exclude, instances[i].Name) {
			continue
		}
		a, err := toAgent(&instances[i], bySelfLink, now)
		if err != nil {
			return nil, err
		}
		agents = append(agents, a)
	}
	slices.SortFunc(agents, func(a, b Agent) int { return strings.Compare(a.AgentID, b.AgentID) })
	return agents, nil
}

func withProject(args []string, project string) []string {
	if project != "" {
		args = append(args, "--project="+project)
	}
	return args
}

func toAgent(in *instance, subnets map[string]*subnet, now time.Time) (Agent, error) {
	created, err := time.Parse(time.RFC3339Nano, in.CreationTimestamp)
	if err != nil {
		return Agent{}, fmt.Errorf("%s: invalid creation time %q", in.Name, in.CreationTimestamp)
	}
	zone := path.Base(in.Zone)
	a := Agent{
		SnapshotTime:  now.UTC().Truncate(time.Microsecond), // the table's precision
		AgentID:       in.Name,
		Zone:          zone,
		Region:        zone[:max(strings.LastIndexByte(zone, '-'), 0)],
		MachineType:   path.Base(in.MachineType),
		VMStatus:      in.Status,
		VMCreatedTime: created.UTC(),
	}
	if t, err := time.Parse(time.RFC3339Nano, in.LastStartTimestamp); err == nil {
		t = t.UTC()
		a.VMLastStartTime = &t
	}
	if len(in.NetworkInterfaces) == 0 {
		return a, nil
	}
	nic := in.NetworkInterfaces[0]
	a.Network, a.Subnetwork = path.Base(nic.Network), path.Base(nic.Subnetwork)
	a.InternalIPv4 = ip(nic.NetworkIP)
	a.InternalIPv6 = ip(nic.IPv6Address)
	if len(nic.AccessConfigs) > 0 {
		a.ExternalIPv4 = ip(nic.AccessConfigs[0].NatIP)
	}
	if len(nic.IPv6AccessConfigs) > 0 {
		c := nic.IPv6AccessConfigs[0]
		a.ExternalIPv6 = ip(c.ExternalIPv6)
		if a.ExternalIPv6 != nil && c.ExternalIPv6PrefixLength > 0 {
			a.ExternalIPv6Prefix = cidr(c.ExternalIPv6, c.ExternalIPv6PrefixLength)
		}
	}
	if s := subnets[nic.Subnetwork]; s != nil {
		a.InternalIPv4Prefix = normalizeCIDR(s.IPCidrRange)
		if a.SubnetIPv6Prefix = normalizeCIDR(s.ExternalIPv6Prefix); a.SubnetIPv6Prefix == nil {
			a.SubnetIPv6Prefix = normalizeCIDR(s.InternalIPv6Prefix)
		}
	}
	return a, nil
}

func ip(s string) *net.IP {
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return nil
	}
	v := net.IP(addr.Unmap().AsSlice())
	return &v
}

// cidr returns addr/bits with the host bits cleared, in canonical form.
func cidr(addr string, bits int) *string {
	p, err := netip.ParsePrefix(fmt.Sprintf("%s/%d", addr, bits))
	if err != nil {
		return nil
	}
	s := p.Masked().String()
	return &s
}

// normalizeCIDR returns the prefix in canonical form, nil if empty or
// invalid. gcloud writes IPv6 prefixes uncompressed (2600:1900:0:0:…/64).
func normalizeCIDR(s string) *string {
	p, err := netip.ParsePrefix(s)
	if err != nil {
		return nil
	}
	v := p.Masked().String()
	return &v
}

// lookupCommand prints key=value lines about the agent: its systemd unit, and
// its container's image, digests, state, start time and Docker status. Each
// line is printed even when the container does not exist, which happens
// between two restarts of a crash-looping unit.
const lookupCommand = `echo "service=$(systemctl show retina-agent -p ActiveState --value)"; ` +
	`echo "restarts=$(systemctl show retina-agent -p NRestarts --value)"; ` +
	`img=$(sudo docker inspect --format '{{.Config.Image}}' retina-agent 2>/dev/null); echo "image=$img"; ` +
	`[ -n "$img" ] && echo "digests=$(sudo docker image inspect --format '{{join .RepoDigests ","}}' "$img" 2>/dev/null)"; ` +
	`echo "state=$(sudo docker inspect --format '{{.State.Status}}' retina-agent 2>/dev/null)"; ` +
	`echo "started=$(sudo docker inspect --format '{{.State.StartedAt}}' retina-agent 2>/dev/null)"; ` +
	`echo "status=$(sudo docker ps -a --filter 'name=^retina-agent$' --format '{{.Status}}')"`

// LookupImage fills the agent's image, container and service fields over
// ssh, and VersionError when the image cannot be read. A stopped VM is not
// contacted.
func LookupImage(ctx context.Context, run Runner, project string, a *Agent, timeout time.Duration) {
	fail := func(err error) {
		msg := err.Error()
		a.VersionError = &msg
	}
	if a.VMStatus != "RUNNING" {
		fail(fmt.Errorf("VM is %s", a.VMStatus))
		return
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out, err := run(ctx, withProject([]string{"compute", "ssh", a.AgentID, "--zone=" + a.Zone, "--quiet",
		"--command=" + lookupCommand}, project)...)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		fail(fmt.Errorf("ssh timed out after %s", timeout))
		return
	}
	if err != nil {
		fail(err)
		return
	}
	fields := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
			fields[k] = strings.TrimSpace(v)
		}
	}
	if _, ok := fields["service"]; !ok {
		fail(fmt.Errorf("unexpected ssh output %q", strings.TrimSpace(string(out))))
		return
	}
	a.AgentServiceState = nonEmpty(fields["service"])
	if n, err := strconv.ParseUint(fields["restarts"], 10, 32); err == nil {
		v := uint32(n)
		a.AgentServiceRestarts = &v
	}
	a.AgentContainerState = nonEmpty(fields["state"])
	a.AgentContainerStatus = nonEmpty(fields["status"])
	if t, err := time.Parse(time.RFC3339Nano, fields["started"]); err == nil && !t.IsZero() && t.Year() > 1 {
		t = t.UTC().Truncate(time.Microsecond)
		a.AgentContainerStartedTime = &t
	}
	a.AgentImage = nonEmpty(fields["image"])
	if a.AgentImage == nil {
		fail(errors.New("no retina-agent container"))
		return
	}
	for _, d := range strings.Split(fields["digests"], ",") {
		if _, digest, ok := strings.Cut(d, "@"); ok {
			a.AgentImageDigest = &digest
			break
		}
	}
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
