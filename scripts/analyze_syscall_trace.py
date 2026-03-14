#!/usr/bin/env python3
"""
Analyze Xcode Instruments syscall trace for pwrite and semop.

Reads exported syscall XML (from xctrace export), filters for selected syscalls,
and writes one CSV per syscall with 8 columns and a header row:
  count, total_duration_ns, p90_duration_ns, avg_duration_ns, total_cpu_time_ns, p90_cpu_time_ns, avg_cpu_time_ns, callstack
First data row has callstack "All" (aggregates over all events); following rows are per distinct callstack.

Usage:
  python scripts/analyze_syscall_trace.py --syscall-xml syscall_table.xml -o syscall_stats.csv
  python scripts/analyze_syscall_trace.py --trace /path/to/postgres.trace -o out.csv
"""

import argparse
import csv
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from collections import defaultdict

def parse_syscall_name(elem):
    """Get syscall name from <syscall> element: prefer fmt, else text (e.g. BSC_pwrite -> pwrite)."""
    fmt = elem.get("fmt")
    if fmt:
        return fmt
    text = (elem.text or "").strip()
    if text.startswith("BSC_"):
        return text[4:]
    return text or None

def tag_local(elem):
    return elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag


def build_id_maps(root):
    """Walk tree in document order; build id -> value for syscall, duration, duration-on-core, frame, backtrace."""
    id_syscall = {}
    id_duration_ns = {}      # wall-clock duration
    id_cpu_ns = {}           # duration-on-core (CPU time)
    id_frame_name = {}
    id_backtrace_frames = {}

    def walk(elem):
        tag = tag_local(elem)
        eid = elem.get("id")
        if tag == "syscall" and eid is not None:
            id_syscall[eid] = parse_syscall_name(elem)
        elif tag == "duration" and eid is not None:
            try:
                id_duration_ns[eid] = int((elem.text or "").strip() or 0)
            except ValueError:
                id_duration_ns[eid] = 0
        elif tag == "duration-on-core" and eid is not None:
            try:
                id_cpu_ns[eid] = int((elem.text or "").strip() or 0)
            except ValueError:
                id_cpu_ns[eid] = 0
        elif tag == "frame" and eid is not None and elem.get("name"):
            id_frame_name[eid] = elem.get("name")
        elif tag == "backtrace" and eid is not None:
            frames = []
            for child in elem:
                if tag_local(child) == "frame":
                    name = child.get("name") or (id_frame_name.get(child.get("ref")) if child.get("ref") else None)
                    if name:
                        frames.append(name)
            id_backtrace_frames[eid] = tuple(frames)
        for child in elem:
            walk(child)
    walk(root)
    return id_syscall, id_duration_ns, id_cpu_ns, id_frame_name, id_backtrace_frames


def collect_rows(root, target_syscalls, id_syscall, id_duration_ns, id_cpu_ns, id_backtrace_frames):
    """Iterate all <row>, resolve syscall/duration/cpu/backtrace by ref/id, filter by target_syscalls."""
    target_set = set(target_syscalls)
    rows_out = []
    for row in root.iter():
        if tag_local(row) != "row":
            continue
        syscall_name = None
        duration_ns = 0
        cpu_ns = 0
        backtrace_ref = None
        backtrace_id = None
        for c in row:
            tag = tag_local(c)
            if tag == "syscall":
                ref, eid = c.get("ref"), c.get("id")
                if eid is not None:
                    syscall_name = parse_syscall_name(c)
                elif ref is not None:
                    syscall_name = id_syscall.get(ref)
            elif tag == "duration":
                ref, eid = c.get("ref"), c.get("id")
                if eid is not None:
                    try:
                        duration_ns = int((c.text or "").strip() or 0)
                    except ValueError:
                        pass
                elif ref is not None:
                    duration_ns = id_duration_ns.get(ref, 0)
            elif tag == "duration-on-core":
                ref, eid = c.get("ref"), c.get("id")
                if eid is not None:
                    try:
                        cpu_ns = int((c.text or "").strip() or 0)
                    except ValueError:
                        pass
                elif ref is not None:
                    cpu_ns = id_cpu_ns.get(ref, 0)
            elif tag == "backtrace":
                backtrace_ref = c.get("ref")
                backtrace_id = c.get("id")
        if syscall_name and syscall_name in target_set:
            frames = ()
            if backtrace_ref is not None:
                frames = id_backtrace_frames.get(backtrace_ref, ())
            elif backtrace_id is not None:
                frames = id_backtrace_frames.get(backtrace_id, ())
            rows_out.append((syscall_name, duration_ns, cpu_ns, frames))
    return rows_out

def main():
    ap = argparse.ArgumentParser(description="Analyze pwrite/semop syscalls from Instruments trace export")
    ap.add_argument("--syscall-xml", type=Path, help="Path to exported syscall table XML")
    ap.add_argument("--name-map", type=Path, help="Path to syscall-name-map XML (optional)")
    ap.add_argument("--trace", type=Path, help="Path to .trace bundle; will export syscall + name-map to current dir then analyze")
    ap.add_argument("--syscalls", nargs="+", default=["pwrite", "semop", "recvfrom"], help="Syscall names to filter (default: pwrite semop recvfrom)")
    ap.add_argument("--output", "-o", type=Path, default=Path("syscall_stats.csv"), help="Output CSV path (default: syscall_stats.csv; one file per syscall: stem_pwrite.csv, stem_semop.csv)")
    args = ap.parse_args()

    if args.trace is not None:
        trace = Path(args.trace)
        if not trace.exists():
            print(f"Trace path does not exist: {trace}", file=sys.stderr)
            sys.exit(1)
        cwd = Path.cwd()
        syscall_xml = cwd / "syscall_table.xml"
        name_map_xml = cwd / "syscall_name_map.xml"
        print("Exporting syscall table (this may take 1–2 minutes)...")
        subprocess.run([
            "xctrace", "export", "--input", str(trace),
            "--xpath", "/trace-toc/run[@number=\"1\"]/data/table[@schema=\"syscall\"]",
            "--output", str(syscall_xml)
        ], check=True)
        print("Exporting syscall-name-map...")
        subprocess.run([
            "xctrace", "export", "--input", str(trace),
            "--xpath", "/trace-toc/run[@number=\"1\"]/data/table[@schema=\"syscall-name-map\"]",
            "--output", str(name_map_xml)
        ], check=True)
        args.syscall_xml = syscall_xml
        args.name_map = name_map_xml

    if args.syscall_xml is None or not args.syscall_xml.exists():
        print("Missing or invalid --syscall-xml. Use --trace or provide --syscall-xml.", file=sys.stderr)
        sys.exit(1)

    target_syscalls = set(args.syscalls)
    print(f"Loading {args.syscall_xml}...")
    tree = ET.parse(args.syscall_xml)
    root = tree.getroot()
    id_syscall, id_duration_ns, id_cpu_ns, id_frame_name, id_backtrace_frames = build_id_maps(root)
    rows = collect_rows(root, target_syscalls, id_syscall, id_duration_ns, id_cpu_ns, id_backtrace_frames)
    print(f"Found {len(rows)} rows for syscalls: {sorted(target_syscalls)}")

    # Per-syscall: list of (duration_ns, cpu_ns, callstack_tuple)
    by_syscall = defaultdict(list)
    for name, duration_ns, cpu_ns, callstack in rows:
        by_syscall[name].append((duration_ns, cpu_ns, callstack))

    def p90_ns(vals):
        if not vals:
            return 0
        s = sorted(vals)
        idx = max(0, int(len(s) * 0.90) - 1)
        return s[idx]

    out_path = Path(args.output)
    if out_path.suffix != ".csv":
        out_path = out_path.with_suffix(out_path.suffix or ".csv")
    single = len(target_syscalls) == 1
    for syscall in sorted(target_syscalls):
        entries = by_syscall.get(syscall, [])
        if not entries:
            continue
        path = out_path if single else out_path.parent / f"{out_path.stem}_{syscall}.csv"

        # Group by distinct callstack
        by_stack = defaultdict(list)
        for dur, cpu, cs in entries:
            by_stack[cs].append((dur, cpu))

        csv_rows = [
            ("count", "total_duration_ns", "p90_duration_ns", "avg_duration_ns", "total_cpu_time_ns", "p90_cpu_time_ns", "avg_cpu_time_ns", "callstack"),
        ]
        # "All" row: aggregates over all events
        all_durs = [e[0] for e in entries]
        all_cpus = [e[1] for e in entries]
        n = len(entries)
        csv_rows.append((
            n,
            sum(all_durs), p90_ns(all_durs), sum(all_durs) // n if n else 0,
            sum(all_cpus), p90_ns(all_cpus), sum(all_cpus) // n if n else 0,
            "All",
        ))
        # One row per distinct callstack (same 6 metrics for that stack only)
        for cs in sorted(by_stack.keys(), key=lambda x: (-len(x), "\n".join(x))):
            durs = [d for d, _ in by_stack[cs]]
            cpus = [c for _, c in by_stack[cs]]
            nn = len(durs)
            # One frame per line inside the cell (CSV will quote the field)
            csv_rows.append((
                nn,
                sum(durs), p90_ns(durs), sum(durs) // nn if nn else 0,
                sum(cpus), p90_ns(cpus), sum(cpus) // nn if nn else 0,
                "\n".join(cs),
            ))

        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            for r in csv_rows:
                w.writerow(r)
        print(f"Wrote {path} ({len(csv_rows)} rows)")

if __name__ == "__main__":
    main()
