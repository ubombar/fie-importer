#!/usr/bin/env bash
# Download a RouteViews RIB for a given date, derive prefix -> origin ASN for
# IPv4 and IPv6, and load each family into its own ClickHouse table + IP_TRIE dictionary.
set -euo pipefail

: "${CH_USER:?CH_USER must be set}"
: "${CH_PASSWORD:?CH_PASSWORD must be set}"
: "${CH_DATABASE:?CH_DATABASE must be set}"

CH=(clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" --database "$CH_DATABASE")

log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2; }
usage() {
	log "usage: $0 -d|--date <YYYY-MM-DD> [-t|--time <HHMM>] [-c|--collector <name>] [-p|--prefix <table_prefix>] [-k|--keep <dir>]"
	log "  creates <prefix>_ipv4_<YYYYMMDD>, <prefix>_ipv6_<YYYYMMDD> and their _dict dictionaries"
	exit 1
}

date_arg=""
time_arg="0000"
collector="route-views2"
table_prefix="bgp_full"
keep_dir=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	-d | --date)
		date_arg="$2"
		shift 2
		;;
	-t | --time)
		time_arg="$2"
		shift 2
		;;
	-c | --collector)
		collector="$2"
		shift 2
		;;
	-p | --prefix)
		table_prefix="$2"
		shift 2
		;;
	-k | --keep)
		keep_dir="$2"
		shift 2
		;;
	*)
		log "Error: unknown argument: $1"
		usage
		;;
	esac
done
[[ -n "$date_arg" ]] || usage
[[ "$date_arg" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || {
	log "Error: date must be YYYY-MM-DD"
	exit 1
}
[[ "$time_arg" =~ ^[0-9]{4}$ ]] || {
	log "Error: time must be HHMM (RIBs exist every 2h: 0000, 0200, ...)"
	exit 1
}

for tool in curl bgpdump awk clickhouse-client; do
	command -v "$tool" >/dev/null || {
		log "Error: missing required tool: $tool (bgpdump: https://github.com/RIPE-NCC/bgpdump)"
		exit 1
	}
done

ymd="${date_arg//-/}"
ym="${date_arg:0:4}.${date_arg:5:2}"
table_v4="${table_prefix}_ipv4_${ymd}"
table_v6="${table_prefix}_ipv6_${ymd}"
url="https://archive.routeviews.org/${collector}/bgpdata/${ym}/RIBS/rib.${ymd}.${time_arg}.bz2"

work="$(mktemp -d "/tmp/bgpload.${ymd}.XXXXXX")"
cleanup() { [[ -n "$keep_dir" ]] || rm -rf "$work"; }
trap cleanup EXIT
[[ -n "$keep_dir" ]] && {
	mkdir -p "$keep_dir"
	work="$keep_dir"
}

log "Collector=$collector date=$date_arg time=$time_arg -> tables $table_v4 / $table_v6"

# Create both tables FIRST so an existing table fails the run before the expensive download.
for t in "$table_v4" "$table_v6"; do
	log "Creating table $CH_DATABASE.$t..."
	"${CH[@]}" --param_destination_database="$CH_DATABASE" --param_destination_table="$t" --multiquery <<-'SQL'
		CREATE TABLE {destination_database:Identifier}.{destination_table:Identifier}
		(
		    `cidr`       String,
		    `asn`        UInt32 -- 0 if NULL
		)
		ENGINE = MergeTree
		ORDER BY cidr;
	SQL
done

rib="${work}/rib.${ymd}.${time_arg}.bz2"
log "Downloading $url ..."
curl -fSL --retry 3 -o "$rib" "$url" || {
	log "Error: download failed; check that a RIB exists at $time_arg for this collector"
	exit 1
}
log "Downloaded $(du -h "$rib" | cut -f1)"

# Parse MRT -> "prefix,origin_asn". For each prefix, keep the origin seen by the
# most peers; skip AS_SET origins ("{...}") since they have no single origin.
log "Parsing RIB and deriving origin ASNs (this takes a few minutes)..."
bgpdump -m -v "$rib" | awk -F'|' '
	$6 != "" && $7 != "" {
		n = split($7, p, " ")
		o = p[n]
		if (o ~ /[{}]/) next
		c[$6 SUBSEP o]++
	}
	END {
		for (k in c) {
			split(k, a, SUBSEP)
			if (c[k] > best[a[1]]) { best[a[1]] = c[k]; org[a[1]] = a[2] }
		}
		for (pfx in org) print pfx "," org[pfx]
	}
' >"${work}/all.csv"

awk -F, '$1 ~ /:/' "${work}/all.csv" >"${work}/v6.csv"
awk -F, '$1 !~ /:/' "${work}/all.csv" >"${work}/v4.csv"
n4=$(wc -l <"${work}/v4.csv")
n6=$(wc -l <"${work}/v6.csv")
log "Derived $n4 IPv4 prefixes and $n6 IPv6 prefixes"
[[ "$n4" -gt 0 && "$n6" -gt 0 ]] || {
	log "Error: one family is empty; collector $collector may not carry it (try -c route-views6 for IPv6)"
	exit 1
}

load_dict() {
	local table="$1" csv="$2" n="$3"
	log "Inserting $n rows into $CH_DATABASE.$table ..."
	"${CH[@]}" --query="INSERT INTO ${CH_DATABASE}.${table} FORMAT CSV" <"$csv"
	log "Creating dictionary $CH_DATABASE.${table}_dict ..."
	"${CH[@]}" --param_destination_database="$CH_DATABASE" --param_destination_table="${table}_dict" --multiquery <<-SQL
		CREATE OR REPLACE DICTIONARY {destination_database:Identifier}.{destination_table:Identifier}
		(
		    \`cidr\` String,
		    \`asn\`  UInt32
		)
		PRIMARY KEY cidr
		SOURCE(CLICKHOUSE(
		    DATABASE '${CH_DATABASE}'
		    TABLE '${table}'
		    USER '${CH_USER}'
		    PASSWORD '${CH_PASSWORD}'
		))
		LAYOUT(IP_TRIE())
		LIFETIME(0);
	SQL
}

load_dict "$table_v4" "${work}/v4.csv" "$n4"
load_dict "$table_v6" "${work}/v6.csv" "$n6"

log "Done. Tables: ${table_v4}, ${table_v6}; dictionaries: ${table_v4}_dict, ${table_v6}_dict"
