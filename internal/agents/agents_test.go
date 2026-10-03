package agents

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const instancesJSON = `[
 {"name": "retina-us-east1-research", "zone": "https://www.googleapis.com/compute/v1/projects/p/zones/us-east1-b",
  "machineType": "https://www.googleapis.com/compute/v1/projects/p/zones/us-east1-b/machineTypes/e2-small",
  "status": "RUNNING", "creationTimestamp": "2026-09-28T07:17:32.861-07:00", "lastStartTimestamp": "2026-10-03T03:32:11.719-07:00",
  "networkInterfaces": [{"network": "https://www.googleapis.com/compute/v1/projects/p/global/networks/retina",
   "subnetwork": "https://www.googleapis.com/compute/v1/projects/p/regions/us-east1/subnetworks/retina-us-east1",
   "networkIP": "10.4.0.13", "accessConfigs": [{"natIP": "203.0.113.45"}],
   "ipv6AccessConfigs": [{"externalIpv6": "2001:db8:4020:6760:0:a:0:0", "externalIpv6PrefixLength": 96}]}]},
 {"name": "retina-server-research", "zone": "zones/us-west4-b", "machineType": "e2-standard-2", "status": "RUNNING",
  "creationTimestamp": "2026-08-13T00:00:00Z", "networkInterfaces": []},
 {"name": "retina-africa-south1-research", "zone": "zones/africa-south1-a", "machineType": "e2-small", "status": "TERMINATED",
  "creationTimestamp": "2026-09-28T07:00:00Z", "networkInterfaces": [{"network": "global/networks/retina",
   "subnetwork": "regions/africa-south1/subnetworks/retina-africa-south1", "networkIP": "10.30.0.4"}]}
]`

const subnetsJSON = `[
 {"selfLink": "https://www.googleapis.com/compute/v1/projects/p/regions/us-east1/subnetworks/retina-us-east1",
  "ipCidrRange": "10.4.0.0/24", "externalIpv6Prefix": "2001:db8:4020:6760:0:0:0:0/64"}
]`

func fakeGcloud(ssh func(args []string) ([]byte, error)) Runner {
	return func(ctx context.Context, args ...string) ([]byte, error) {
		switch strings.Join(args[:3], " ") {
		case "compute instances list":
			return []byte(instancesJSON), nil
		case "compute networks subnets":
			return []byte(subnetsJSON), nil
		case "compute ssh " + args[2]:
			return ssh(args)
		}
		return nil, errors.New("unexpected command")
	}
}

func TestList(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	list, err := List(context.Background(), fakeGcloud(nil), "", "f", []string{"retina-server-research"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].AgentID != "retina-africa-south1-research" || list[1].AgentID != "retina-us-east1-research" {
		t.Fatalf("agents = %+v, want two, sorted, without the server", list)
	}
	a := list[1]
	checks := map[string][2]string{
		"region":        {a.Region, "us-east1"},
		"zone":          {a.Zone, "us-east1-b"},
		"network":       {a.Network, "retina"},
		"subnetwork":    {a.Subnetwork, "retina-us-east1"},
		"machine type":  {a.MachineType, "e2-small"},
		"internal IPv4": {str(a.InternalIPv4), "10.4.0.13"},
		"v4 prefix":     {*a.InternalIPv4Prefix, "10.4.0.0/24"},
		"external IPv4": {str(a.ExternalIPv4), "203.0.113.45"},
		"external IPv6": {str(a.ExternalIPv6), "2001:db8:4020:6760:0:a::"},
		"v6 prefix":     {*a.ExternalIPv6Prefix, "2001:db8:4020:6760:0:a::/96"},
		"subnet prefix": {*a.SubnetIPv6Prefix, "2001:db8:4020:6760::/64"},
		"created":       {a.VMCreatedTime.Format(time.RFC3339Nano), "2026-09-28T14:17:32.861Z"},
		"snapshot":      {a.SnapshotTime.Format(time.RFC3339), "2026-10-03T12:00:00Z"},
	}
	for name, c := range checks {
		if c[0] != c[1] {
			t.Errorf("%s = %q, want %q", name, c[0], c[1])
		}
	}
	if a.InternalIPv6 != nil {
		t.Errorf("internal IPv6 = %v, want NULL", a.InternalIPv6)
	}
	if b := list[0]; b.ExternalIPv4 != nil || b.InternalIPv4Prefix != nil || b.VMLastStartTime != nil {
		t.Errorf("stopped agent = %+v: want NULL external IPv4, prefix and start time", b)
	}
}

func str[T interface{ String() string }](v *T) string {
	if v == nil {
		return ""
	}
	return (*v).String()
}

const image = "ghcr.io/dioptra-io/retina-agent:research-v1.1.0"

// ssh fakes gcloud compute ssh printing out.
func ssh(out string) Runner {
	return fakeGcloud(func(args []string) ([]byte, error) {
		if args[3] != "--zone=us-east1-b" || !strings.Contains(args[5], "systemctl show retina-agent") {
			return nil, errors.New("bad ssh arguments")
		}
		return []byte(out), nil
	})
}

func TestLookupImage_Healthy(t *testing.T) {
	a := Agent{AgentID: "retina-us-east1-research", Zone: "us-east1-b", VMStatus: "RUNNING"}
	LookupImage(context.Background(), ssh("service=active\nrestarts=0\nimage="+image+
		"\ndigests=ghcr.io/dioptra-io/retina-agent@sha256:ea56\nstate=running\nstarted=2026-10-03T21:31:48.827982985Z\nstatus=Up 2 minutes\n"),
		"", &a, time.Second)
	if a.VersionError != nil || *a.AgentImage != image || *a.AgentImageDigest != "sha256:ea56" ||
		*a.AgentContainerState != "running" || *a.AgentContainerStatus != "Up 2 minutes" ||
		a.AgentContainerStartedTime.Format(time.RFC3339Nano) != "2026-10-03T21:31:48.827982Z" ||
		*a.AgentServiceState != "active" || *a.AgentServiceRestarts != 0 || !a.Healthy() {
		t.Fatalf("agent = %+v", a)
	}
}

func TestLookupImage_CrashLoop(t *testing.T) {
	// The exited container is still there between restarts.
	loop := Agent{AgentID: "x", Zone: "us-east1-b", VMStatus: "RUNNING"}
	LookupImage(context.Background(), ssh("service=activating\nrestarts=205\nimage="+image+
		"\ndigests=\nstate=exited\nstarted=2026-10-03T21:55:21Z\nstatus=Exited (2) 3 seconds ago\n"), "", &loop, time.Second)
	if loop.Healthy() || *loop.AgentServiceRestarts != 205 || *loop.AgentContainerStatus != "Exited (2) 3 seconds ago" || loop.AgentImageDigest != nil {
		t.Fatalf("crash-looping agent = %+v", loop)
	}

	// Between two restarts there is no container at all: the unit is still reported.
	gone := Agent{AgentID: "y", Zone: "us-east1-b", VMStatus: "RUNNING"}
	LookupImage(context.Background(), ssh("service=activating\nrestarts=235\nimage=\nstate=\nstarted=\nstatus=\n"), "", &gone, time.Second)
	if gone.Healthy() || gone.AgentImage != nil || *gone.VersionError != "no retina-agent container" ||
		*gone.AgentServiceRestarts != 235 || gone.AgentContainerStartedTime != nil {
		t.Fatalf("agent without container = %+v", gone)
	}
}

func TestLookupImage_Failures(t *testing.T) {
	stopped := Agent{AgentID: "z", VMStatus: "TERMINATED"}
	LookupImage(context.Background(), ssh(""), "", &stopped, time.Second)
	if stopped.AgentServiceState != nil || *stopped.VersionError != "VM is TERMINATED" {
		t.Fatalf("stopped agent = %+v", stopped)
	}

	timedOut := Agent{AgentID: "w", Zone: "z", VMStatus: "RUNNING"}
	LookupImage(context.Background(), func(c context.Context, args ...string) ([]byte, error) {
		<-c.Done()
		return nil, errors.New("killed")
	}, "", &timedOut, 50*time.Millisecond)
	if timedOut.VersionError == nil || !strings.Contains(*timedOut.VersionError, "timed out") {
		t.Fatalf("timed-out agent = %+v", timedOut)
	}

	garbled := Agent{AgentID: "v", Zone: "us-east1-b", VMStatus: "RUNNING"}
	LookupImage(context.Background(), ssh("Permission denied\n"), "", &garbled, time.Second)
	if garbled.VersionError == nil || garbled.AgentServiceState != nil {
		t.Fatalf("garbled output: agent = %+v", garbled)
	}
}
