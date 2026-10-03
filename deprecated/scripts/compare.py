#!/usr/bin/env python3

import json
import sys
from datetime import datetime, timedelta

S1 = "s1.jsonl"
S2 = "s2.jsonl"

PROBE_TIMESTAMP_PATHS = [
    ("near_info", "sent_timestamp"),
    ("near_info", "received_timestamp"),
    ("far_info", "sent_timestamp"),
    ("far_info", "received_timestamp"),
]


def normalize_timestamp(value):
    if not value:
        return value

    if "." in value:
        return value.split(".", 1)[0] + "Z"

    return value


def shift_probe_timestamp(value):
    if not value:
        return value

    value = normalize_timestamp(value)

    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    dt -= timedelta(hours=2)

    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize(obj, is_http=False):
    # HTTP capture does not contain source_address, so ignore it.
    obj.pop("source_address", None)

    # Production timestamp is compared normally at whole-second precision.
    if "production_timestamp" in obj:
        obj["production_timestamp"] = normalize_timestamp(
            obj["production_timestamp"]
        )

    # Probe timestamps are compared at whole-second precision.
    # The HTTP capture is consistently 2 hours ahead, so compensate for that.
    for parent, key in PROBE_TIMESTAMP_PATHS:
        info = obj.get(parent)
        if info is None or key not in info:
            continue

        if is_http:
            info[key] = shift_probe_timestamp(info[key])
        else:
            info[key] = normalize_timestamp(info[key])

    return obj


with open(S1, "r") as f1, open(S2, "r") as f2:
    line_no = 0

    while True:
        l1 = f1.readline()
        l2 = f2.readline()

        if not l1 and not l2:
            print(f"OK: all {line_no:,} FIEs match")
            sys.exit(0)

        line_no += 1

        if not l1:
            print(f"ERROR: {S1} ended after {line_no - 1:,} FIEs")
            sys.exit(1)

        if not l2:
            print(f"ERROR: {S2} ended after {line_no - 1:,} FIEs")
            sys.exit(1)

        try:
            j1 = normalize(json.loads(l1), is_http=False)
            j2 = normalize(json.loads(l2), is_http=True)
        except json.JSONDecodeError as err:
            print(f"ERROR: invalid JSON at line {line_no}: {err}")
            sys.exit(1)

        if j1 != j2:
            print(f"MISMATCH at line {line_no:,}")
            print()
            print("s1:")
            print(json.dumps(j1, indent=2, sort_keys=True))
            print()
            print("s2:")
            print(json.dumps(j2, indent=2, sort_keys=True))
            sys.exit(1)

        if line_no % 100000 == 0:
            print(f"checked {line_no:,} FIEs...", file=sys.stderr)
