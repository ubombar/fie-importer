package clickhouse

import (
	"fmt"
	"strings"
	"testing"
)

func TestFDHInsertSQL(t *testing.T) {
	q := FDHInsertSQL("fdhs", "fies", "pds")
	for _, want := range []string{
		"INSERT INTO `fdhs` (agent_id, ip_version, near_addr, destination_addr, capture_time, pd_id, near_ttl, far_addr)",
		"FROM `fies` AS f\nINNER JOIN `pds` AS p ON f.pd_id = p.pd_id",
		"WHERE f.near_reply_addr IS NOT NULL AND f.near_reply_addr NOT IN " + zeroAddrs,
		"if(f.far_reply_addr IN " + zeroAddrs + ", NULL, f.far_reply_addr)",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("insert lacks %q:\n%s", want, q)
		}
	}
}

func TestFDHTableDDL(t *testing.T) {
	ddl := fmt.Sprintf(FDHTableDDL, quote("fdhs"))
	for _, want := range []string{
		"ORDER BY (agent_id, ip_version, near_addr, destination_addr, capture_time, pd_id)",
		"PRIMARY KEY (agent_id, ip_version, near_addr, destination_addr, capture_time)",
		"far_addr         Nullable(IPv6)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL lacks %q", want)
		}
	}
}
