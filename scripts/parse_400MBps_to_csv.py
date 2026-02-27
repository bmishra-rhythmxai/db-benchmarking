#!/usr/bin/env python3
"""
Parse cpu-mem-network.txt, disk.txt, and insertion-log.txt from a load-test folder
and produce:
  1. metrics_combined.csv: time, cpu (cgroup), total_vsz_bytes, sdb_read_iops, sdb_write_iops,
                          sdb_read_Bps, sdb_write_Bps
  2. insertion_interval.csv: time, records_inserted, latency_ms (interval data only)
"""

import re
import csv
import sys
from pathlib import Path


def parse_size_to_bytes(s: str) -> float:
    """Parse strings like '204.9M', '1.2G', '0.0B', '7.0M' to bytes."""
    s = s.strip()
    if not s or s == "0.0B" or s == "0.0K" or s == "0.0M" or s == "0.0G" or s == "0.0T":
        return 0.0
    m = re.match(r"^([\d.]+)\s*([KMGTP]?)B?$", s, re.IGNORECASE)
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = (m.group(2) or "").upper()
    mult = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
    return val * mult.get(unit, 1)


def parse_cpu_mem_network(path: Path) -> list[dict]:
    """Parse cpu-mem-network.txt. Return list of dicts with datetime, cpu, vsz_bytes."""
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not re.match(r"^\d{4}-\d{2}-\d{2}\s+┆", line):
            continue
        parts = [p.strip() for p in line.split("│")]
        if len(parts) < 3:
            continue
        # part[0]: "2026-02-27 ┆ 12:42:27"
        date_time = parts[0].replace("┆", " ").split()
        if len(date_time) < 2:
            continue
        dt = f"{date_time[0]} {date_time[1]}"
        # part[1]: "  8.000 ┆  0.004 ┆  0.002 ┆  0.002" -> cpu is 2nd number
        cpu_vals = [x.strip() for x in parts[1].split("┆")]
        cpu = float(cpu_vals[1]) if len(cpu_vals) > 1 else None
        # part[2]: "   16.0G ┆  204.9M ┆   24.5M" -> vsz is 2nd
        mem_vals = [x.strip() for x in parts[2].split("┆")]
        vsz_str = mem_vals[1] if len(mem_vals) > 1 else "0B"
        vsz_bytes = parse_size_to_bytes(vsz_str)
        rows.append({"datetime": dt, "cpu": cpu, "vsz_bytes": vsz_bytes})
    return rows


def parse_disk(path: Path) -> list[dict]:
    """Parse disk.txt for sdb. Return list of dicts with datetime, sdb_rIOPS, sdb_wIOPS, sdb_r_Bps, sdb_w_Bps."""
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not re.match(r"^\d{4}-\d{2}-\d{2}\s+┆", line):
            continue
        parts = [p.strip() for p in line.split("│")]
        if len(parts) < 3:
            continue
        date_time = parts[0].replace("┆", " ").split()
        if len(date_time) < 2:
            continue
        dt = f"{date_time[0]} {date_time[1]}"
        # part[2] is sdb: "rIOPS ┆ wIOPS ┆ r/s ┆ w/s ┆ ..."
        sdb_vals = [x.strip() for x in parts[2].split("┆")]
        if len(sdb_vals) < 4:
            continue
        try:
            r_iops = float(sdb_vals[0])
            w_iops = float(sdb_vals[1])
            r_bps = parse_size_to_bytes(sdb_vals[2])
            w_bps = parse_size_to_bytes(sdb_vals[3])
        except (ValueError, IndexError):
            continue
        rows.append({
            "datetime": dt,
            "sdb_read_iops": r_iops,
            "sdb_write_iops": w_iops,
            "sdb_read_Bps": r_bps,
            "sdb_write_Bps": w_bps,
        })
    return rows


def parse_insertion_log(path: Path) -> list[dict]:
    """Parse insertion-log.txt for 'this interval' lines only. Return time, records_inserted, latency_ms."""
    rows = []
    pat = re.compile(
        r"(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\s+Insert progress \(this interval\):\s+(\d+)\s+total.*?avg latency\s+([\d.]+)\s*ms",
        re.IGNORECASE,
    )
    for line in path.read_text().splitlines():
        m = pat.search(line)
        if not m:
            continue
        dt_str = m.group(1).replace("/", "-")
        records = int(m.group(2))
        latency_ms = float(m.group(3))
        rows.append({
            "datetime": dt_str,
            "records_inserted": records,
            "latency_ms": latency_ms,
        })
    return rows


def main():
    data_dir = Path(__file__).resolve().parent.parent / "data" / "400MBps"
    if len(sys.argv) > 1:
        data_dir = Path(sys.argv[1])

    cpu_path = data_dir / "cpu-mem-network.txt"
    disk_path = data_dir / "disk.txt"
    insert_path = data_dir / "insertion-log.txt"

    for p in (cpu_path, disk_path, insert_path):
        if not p.exists():
            print(f"Missing {p}", file=sys.stderr)
            sys.exit(1)

    cpu_rows = parse_cpu_mem_network(cpu_path)
    disk_rows = parse_disk(disk_path)
    insert_rows = parse_insertion_log(insert_path)

    # Build combined metrics by datetime (inner join on time)
    disk_by_dt = {r["datetime"]: r for r in disk_rows}
    combined = []
    for r in cpu_rows:
        dt = r["datetime"]
        d = disk_by_dt.get(dt)
        if d is None:
            continue
        combined.append({
            "time": dt,
            "cpu_cgroup": r["cpu"],
            "total_vsz_bytes": int(r["vsz_bytes"]),
            "sdb_read_iops": d["sdb_read_iops"],
            "sdb_write_iops": d["sdb_write_iops"],
            "sdb_read_throughput_Bps": int(d["sdb_read_Bps"]),
            "sdb_write_throughput_Bps": int(d["sdb_write_Bps"]),
        })

    out_combined = data_dir / "metrics_combined.csv"
    with open(out_combined, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "time",
                "cpu_cgroup",
                "total_vsz_bytes",
                "sdb_read_iops",
                "sdb_write_iops",
                "sdb_read_throughput_Bps",
                "sdb_write_throughput_Bps",
            ],
        )
        w.writeheader()
        w.writerows(combined)
    print(f"Wrote {len(combined)} rows to {out_combined}")

    out_insert = data_dir / "insertion_interval.csv"
    with open(out_insert, "w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["time", "records_inserted", "latency_ms"],
        )
        w.writeheader()
        w.writerows(
            [
                {"time": r["datetime"], "records_inserted": r["records_inserted"], "latency_ms": r["latency_ms"]}
                for r in insert_rows
            ]
        )
    print(f"Wrote {len(insert_rows)} rows to {out_insert}")


if __name__ == "__main__":
    main()
