#!/usr/bin/env python3
"""
Convert dstat columnar output (dstat.csv) to proper CSV with uniform size units.
- Parses pipe-and-colon delimited lines into one column per metric.
- Normalizes size values (B, k, M, G) to bytes (base 1024) for charting.
- Skips repeated header/separator lines.
"""

import csv
import re
import sys
from pathlib import Path

# Size suffix to multiplier (binary: 1024)
SIZE_UNITS = {
    "B": 1,
    "k": 1024,
    "K": 1024,
    "M": 1024**2,
    "G": 1024**3,
}

# Column names for the 6 groups (after splitting by |)
COLUMN_NAMES = [
    "time",
    "cpu_usr", "cpu_sys", "cpu_idl", "cpu_wai", "cpu_stl",
    "mem_used", "mem_free", "mem_buff", "mem_cach",
    "io_total_read", "io_total_writ", "io_sda_read", "io_sda_writ", "io_sdh_read", "io_sdh_writ",
    "dsk_total_read", "dsk_total_writ", "dsk_sda_read", "dsk_sda_writ", "dsk_sdh_read", "dsk_sdh_writ",
    "root_used", "root_free", "data_used", "data_free",
]

# Which columns are size columns (normalize to bytes)
SIZE_COLUMNS = {
    "mem_used", "mem_free", "mem_buff", "mem_cach",
    "dsk_total_read", "dsk_total_writ", "dsk_sda_read", "dsk_sda_writ", "dsk_sdh_read", "dsk_sdh_writ",
    "root_used", "root_free", "data_used", "data_free",
}


def parse_size(value: str) -> float:
    """Parse a value that may have a size suffix (B, k, M, G) into bytes. Returns float."""
    if not value or not value.strip():
        return 0.0
    s = value.strip()
    for suffix, mult in SIZE_UNITS.items():
        if s.endswith(suffix) and (len(suffix) == 1 or s[-len(suffix):] == suffix):
            try:
                num = float(s[:-len(suffix)].strip())
                return num * mult
            except ValueError:
                break
    # Plain number (e.g. CPU %, io ops)
    try:
        return float(s)
    except ValueError:
        return 0.0


def normalize_cell(col_name: str, raw: str) -> str:
    """Return CSV-safe string; normalize size columns to numeric bytes."""
    raw = raw.strip()
    if col_name in SIZE_COLUMNS:
        val = parse_size(raw)
        return str(int(val)) if val == int(val) else str(val)
    return raw


def split_row(line: str) -> list[str]:
    """Split a data line by | then expand : and spaces into flat values."""
    parts = line.split("|")
    if len(parts) != 6:
        return []
    out = []
    # time
    out.append(parts[0].strip())
    # cpu: 5 numbers
    out.extend(parts[1].split())
    # memory: 4 values
    out.extend(parts[2].split())
    # io: read writ : read writ : read writ
    for block in parts[3].split(":"):
        out.extend(block.split())
    # dsk: same
    for block in parts[4].split(":"):
        out.extend(block.split())
    # root/data: used free : used free
    for block in parts[5].split(":"):
        out.extend(block.split())
    return out


def is_data_line(line: str) -> bool:
    """True if line looks like a dstat data row (starts with DD-MM HH:MM:SS)."""
    return bool(re.match(r"^\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\|", line.strip()))


def main():
    input_path = Path(__file__).parent.parent / "dstat.csv"
    output_path = Path(__file__).parent.parent / "dstat_normalized.csv"

    if len(sys.argv) >= 2:
        input_path = Path(sys.argv[1])
    if len(sys.argv) >= 3:
        output_path = Path(sys.argv[2])

    if not input_path.exists():
        print(f"Input not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    rows = []
    with open(input_path, "r") as f:
        for line in f:
            if not is_data_line(line):
                continue
            cells = split_row(line.rstrip("\n"))
            if len(cells) != len(COLUMN_NAMES):
                continue
            row = {}
            for name, raw in zip(COLUMN_NAMES, cells):
                row[name] = normalize_cell(name, raw)
            rows.append(row)

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMN_NAMES)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_path}")
    print(f"Size columns are in bytes (base 1024). Columns: {', '.join(COLUMN_NAMES)}")


if __name__ == "__main__":
    main()
