#!/usr/bin/env python3
"""
Parse data/query_log.txt (insert and query progress lines with cumulative counts)
and produce a CSV: timestamp, records_inserted, records_queried (per-interval difference from previous row).
"""

import csv
import re
import sys
from pathlib import Path

# 2026-02-24 13:32:27 [INFO] Insert progress: 484 rows inserted
# 2026-02-24 13:32:27 [INFO] Query progress: 968 queries executed
INSERT_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+.*Insert progress:\s+(\d+)\s+rows inserted"
)
QUERY_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+.*Query progress:\s+(\d+)\s+queries executed"
)


def main():
    input_path = Path(__file__).parent.parent / "data" / "query_log.txt"
    output_path = Path(__file__).parent.parent / "data" / "query_log.csv"

    if len(sys.argv) >= 2:
        input_path = Path(sys.argv[1])
    if len(sys.argv) >= 3:
        output_path = Path(sys.argv[2])

    if not input_path.exists():
        print(f"Input not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    # Collect by timestamp: { "2026-02-24 13:32:27": { "insert": 484, "query": 968 }, ... }
    by_ts = {}
    with open(input_path) as f:
        for line in f:
            m = INSERT_RE.match(line)
            if m:
                ts, n = m.group(1), int(m.group(2))
                by_ts.setdefault(ts, {})["records_inserted"] = n
                continue
            m = QUERY_RE.match(line)
            if m:
                ts, n = m.group(1), int(m.group(2))
                by_ts.setdefault(ts, {})["records_queried"] = n
                continue

    # Build rows in chronological order with cumulative values
    cumul = []
    for ts in sorted(by_ts.keys()):
        d = by_ts[ts]
        ins = d.get("records_inserted")
        qry = d.get("records_queried")
        if ins is not None or qry is not None:
            cumul.append((ts, ins or 0, qry or 0))

    # Convert to per-interval differences (current - previous; first row uses cumulative as diff)
    rows = []
    for i, (ts, ins, qry) in enumerate(cumul):
        if i == 0:
            diff_ins, diff_qry = ins, qry
        else:
            diff_ins = ins - cumul[i - 1][1]
            diff_qry = qry - cumul[i - 1][2]
        rows.append({
            "timestamp": ts,
            "records_inserted": diff_ins,
            "records_queried": diff_qry,
        })

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "records_inserted", "records_queried"])
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
