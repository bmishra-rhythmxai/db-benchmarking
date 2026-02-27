#!/usr/bin/env python3
"""
Print dstat-like system monitoring statistics from /proc, /sys and cgroups.
Columnar format with human-readable sizes. CPU in cores, memory tot/vsz/rss.
Runnable in containers; reads cgroup v1/v2 when available.
Requires Linux (/proc, /sys). Use --block and --net to filter devices.
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path

def human_bytes(n: int | float) -> str:
    if n < 0:
        n = 0
    for u in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024:
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}PiB"


def human_bytes_short(n: int | float) -> str:
    """Short units: B, K, M, G, T. Number part formatted as NUM_FMT for alignment."""
    if n < 0:
        n = 0
    for u in ("B", "K", "M", "G", "T"):
        if n < 1024:
            return f"{n:{NUM_FMT}}{u}"
        n /= 1024
    return f"{n:{NUM_FMT}}P"


def human_rate(n: float, unit: str = "B") -> str:
    if n < 0:
        n = 0
    for u in ("B", "KiB", "MiB", "GiB"):
        if n < 1024:
            return f"{n:.1f}{u}/s"
        n /= 1024
    return f"{n:.1f}GiB/s"


def human_rate_short(n: float) -> str:
    """Short units for rate: B, K, M, G (per second). Number part as NUM_FMT."""
    if n < 0:
        n = 0
    for u in ("B", "K", "M", "G"):
        if n < 1024:
            return f"{n:{NUM_FMT}}{u}"
        n /= 1024
    return f"{n:{NUM_FMT}}G"


# Box-drawing: major block separator, minor (sub-category) separator
SEP_MAJOR = " │ "
SEP_MINOR = " ┆ "
# Header rules: thick before/after header block, thin between main and sub header
LINE_THICK = "="
LINE_THIN = "-"

# Fixed numeric format: 3 digits before decimal, 1 after (e.g. "  0.1") for alignment
NUM_FMT = "6.1f"  # total width 6, 1 decimal
NUM_WIDTH = 6  # pad placeholders to this width


def read_one(path: Path, default: str = "") -> str:
    try:
        return path.read_text().strip()
    except (OSError, FileNotFoundError):
        return default


def read_int(path: Path, default: int = 0) -> int:
    try:
        return int(path.read_text().strip())
    except (OSError, FileNotFoundError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Cgroup detection (v1 and v2)
# ---------------------------------------------------------------------------

def find_cgroup_root() -> Path | None:
    """Unified cgroup v2 root. Returns None if only v1 is available."""
    root = Path("/sys/fs/cgroup")
    if (root / "cgroup.controllers").exists():
        return root
    return None


def get_cgroup_v1_controller_root(controller: str) -> Path | None:
    """Path to cgroup v1 controller root, e.g. /sys/fs/cgroup/memory."""
    p = Path("/sys/fs/cgroup") / controller
    return p if p.exists() else None


def _parse_proc_cgroup(content: str) -> tuple[str | None, str | None]:
    """Parse /proc/<pid>/cgroup content. Returns (v2_path, v1_memory_path).
    - v2: line '0::/path' -> path normalized (no leading/trailing slash).
    - v1: line with 'memory' in controllers -> path from that line."""
    v2_path: str | None = None
    v1_memory_path: str | None = None
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":", 2)  # hierarchy_id : controllers : path
        if len(parts) < 3:
            continue
        hierarchy_id, controllers, path = parts[0], parts[1], parts[2]
        path = path.strip("/")  # normalize for _join_cgroup
        if hierarchy_id == "0" and controllers == "":
            v2_path = path
        elif "memory" in controllers.split(","):
            v1_memory_path = path
    return (v2_path, v1_memory_path)


def get_cgroup_path_from_proc(pid: int = 1) -> tuple[Path | None, str | None]:
    """Read cgroup path from /proc/<pid>/cgroup (default PID 1 = container init).
    Returns (cgroup_root, rel_path): for v2 root is /sys/fs/cgroup and rel from '0::/path';
    for v1-only root is None and rel is the memory controller path. Use PID 1 so we see
    the real container cgroup in privileged containers where /sys/fs/cgroup is the host."""
    proc_cgroup = Path(f"/proc/{pid}/cgroup")
    if not proc_cgroup.exists():
        proc_cgroup = Path("/proc/self/cgroup")
    try:
        content = proc_cgroup.read_text()
    except OSError:
        return (None, None)
    v2_path, v1_memory_path = _parse_proc_cgroup(content)
    cgroup_root = find_cgroup_root()
    if cgroup_root is not None and v2_path is not None:
        return (cgroup_root, v2_path)
    if v1_memory_path is not None:
        return (None, v1_memory_path)
    return (None, None)


def get_cgroup_path_from_sys(cgroup_root: Path | None, mem_root: Path | None) -> str | None:
    """Fallback: assume cgroup at root when /proc/1/cgroup is not usable.
    In containers, /sys/fs/cgroup is sometimes the container's cgroup root, so path is ''."""
    if cgroup_root is not None:
        return ""  # v2: root of /sys/fs/cgroup is our cgroup
    if mem_root is not None:
        return ""  # v1: root of controller hierarchy
    return None


def _join_cgroup(root: Path, rel: str, *suffix: str) -> Path:
    p = root
    for part in (rel or "").strip("/").split("/"):
        if part:
            p = p / part
    for s in suffix:
        if s:
            p = p / s
    return p


# ---------------------------------------------------------------------------
# CPU (system-wide from /proc/stat; optional cgroup usage). Display in cores.
# ---------------------------------------------------------------------------

def get_n_cpus() -> int:
    """Number of CPUs (cores)."""
    n = os.cpu_count()
    if n is not None and n > 0:
        return n
    try:
        with open("/proc/stat") as f:
            count = sum(1 for line in f if line.startswith("cpu") and line[3:4].isdigit())
        return count if count > 0 else 1
    except OSError:
        return 1


def parse_proc_stat_cpu(line: str) -> tuple[float, float, float] | None:
    """Return (user_ratio, system_ratio, idle_ratio) in 0..1 from /proc/stat aggregate cpu line."""
    # cpu  user nice system idle iowait irq softirq steal ...
    parts = line.split()
    if len(parts) < 5 or parts[0] != "cpu":
        return None
    user = int(parts[1])
    nice = int(parts[2])
    system = int(parts[3])
    idle = int(parts[4])
    total = user + nice + system + idle
    if total == 0:
        return None
    user_ratio = (user + nice) / total
    system_ratio = system / total
    idle_ratio = idle / total
    return (user_ratio, system_ratio, idle_ratio)


def get_cpu_stats(n_cpus: int) -> tuple[float, float, float]:
    """Return (usr_cores, sys_cores, idl_cores) from /proc/stat."""
    try:
        with open("/proc/stat") as f:
            first = f.readline()
    except OSError:
        return (0.0, 0.0, float(n_cpus))
    parsed = parse_proc_stat_cpu(first)
    if parsed is None:
        return (0.0, 0.0, float(n_cpus))
    user_ratio, system_ratio, idle_ratio = parsed
    return (
        user_ratio * n_cpus,
        system_ratio * n_cpus,
        idle_ratio * n_cpus,
    )


USEC_PER_SEC = 1_000_000  # 1 second in microseconds


def get_cgroup_cpu_raw(cgroup_root: Path | None, rel: str) -> tuple[int, int] | None:
    """Return (user_usec, system_usec) from cgroup v2 cpu.stat. None if unavailable."""
    rel = rel or ""
    if cgroup_root is None:
        return None
    p = _join_cgroup(cgroup_root, rel, "cpu.stat")
    if not p.exists():
        return None
    try:
        data = p.read_text()
    except OSError:
        return None
    user_usec = sys_usec = None
    for line in data.splitlines():
        if line.startswith("user_usec "):
            user_usec = int(line.split()[1])
        elif line.startswith("system_usec "):
            sys_usec = int(line.split()[1])
    if user_usec is None or sys_usec is None:
        return None
    return (user_usec, sys_usec)


def get_cgroup_cpu_max_cores(cgroup_root: Path | None, rel: str) -> float | None:
    """Max CPU bandwidth in cores from cgroup v2 cpu.max (quota/period). None if unlimited or unavailable."""
    rel = rel or ""
    if cgroup_root is None:
        return None
    p = _join_cgroup(cgroup_root, rel, "cpu.max")
    if not p.exists():
        return None
    try:
        raw = p.read_text().strip().split()
        if len(raw) < 2 or raw[0] == "max":
            return None
        quota = int(raw[0])
        period = int(raw[1])
        if period <= 0:
            return None
        return quota / period  # cores (usec per period / period usec)
    except (OSError, ValueError):
        return None


def get_cgroup_cpu_usr_sys_cores_from_deltas(
    delta_user_usec: int, delta_system_usec: int
) -> tuple[float, float]:
    """(usr_cores, sys_cores) from usec deltas over 1 second (USEC_PER_SEC usec)."""
    usr_cores = delta_user_usec / USEC_PER_SEC
    sys_cores = delta_system_usec / USEC_PER_SEC
    return (usr_cores, sys_cores)


# ---------------------------------------------------------------------------
# Memory: total, free (host or cgroup); vsz, rss from meminfo (host) or memory.stat (cgroup)
# ---------------------------------------------------------------------------

def get_meminfo() -> tuple[int, int]:  # total, free (bytes)
    total = free = 0
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    total = int(line.split()[1]) * 1024
                elif line.startswith("MemFree:"):
                    free = int(line.split()[1]) * 1024
                if total and free:
                    break
    except OSError:
        pass
    return total, free


def get_cgroup_memory(cgroup_root: Path | None, rel: str) -> tuple[int, int | None] | None:
    """(current_bytes, max_bytes or None if max). Prefer cgroup v2, fallback to v1."""
    # Cgroup v2
    if cgroup_root is not None:
        cur_path = _join_cgroup(cgroup_root, rel, "memory.current")
        max_path = _join_cgroup(cgroup_root, rel, "memory.max")
        if cur_path.exists():
            try:
                cur = int(cur_path.read_text().strip())
            except (OSError, ValueError):
                return None
            try:
                raw = max_path.read_text().strip()
                if raw == "max":
                    return (cur, None)
                return (cur, int(raw))
            except (OSError, ValueError):
                return (cur, None)
    # Cgroup v1
    mem_root = get_cgroup_v1_controller_root("memory")
    if not mem_root:
        return None
    cur_path_v1 = _join_cgroup(mem_root, rel, "memory.usage_in_bytes")
    max_path_v1 = _join_cgroup(mem_root, rel, "memory.limit_in_bytes")
    try:
        cur = int(cur_path_v1.read_text().strip())
    except (OSError, ValueError):
        return None
    try:
        max_val = int(max_path_v1.read_text().strip())
        return (cur, max_val if max_val < 2**63 else None)  # v1 uses huge value for "unlimited"
    except (OSError, ValueError):
        return (cur, None)


def get_cgroup_memory_stat_rss(cgroup_root: Path | None, rel: str) -> int:
    """RSS from memory.stat (v2 anon or v1 rss)."""
    v, r = get_cgroup_memory_vsz_rss(cgroup_root, rel)
    return r


def _parse_memory_stat(data: str, use_rss: bool = False) -> tuple[int, int]:
    """Parse memory.stat text; return (anon_or_rss, inactive_file). use_rss=True for v1 (rss not anon)."""
    anon = inactive_file = 0
    for line in data.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        if parts[0] == "anon":
            anon = int(parts[1])
        elif use_rss and parts[0] == "rss":
            anon = int(parts[1])
        elif parts[0] == "inactive_file":
            inactive_file = int(parts[1])
    return (anon, inactive_file)


def get_cgroup_memory_vsz_rss(cgroup_root: Path | None, rel: str) -> tuple[int | None, int]:
    """(vsz, rss) from cgroup. vsz = memory.current - inactive_file; rss from anon (v2) or rss (v1)."""
    rel = rel or ""
    # v2: memory.current and memory.stat (anon, inactive_file)
    if cgroup_root is not None:
        cur_path = _join_cgroup(cgroup_root, rel, "memory.current")
        stat_path = _join_cgroup(cgroup_root, rel, "memory.stat")
        if cur_path.exists() and stat_path.exists():
            try:
                current = int(cur_path.read_text().strip())
                data = stat_path.read_text()
            except (OSError, ValueError):
                pass
            else:
                anon, inactive_file = _parse_memory_stat(data, use_rss=False)
                vsz = max(0, current - inactive_file)
                return (vsz, anon)
    # v1: memory.usage_in_bytes and memory.stat
    mem_root = get_cgroup_v1_controller_root("memory")
    if mem_root:
        cur_path = _join_cgroup(mem_root, rel, "memory.usage_in_bytes")
        stat_path = _join_cgroup(mem_root, rel, "memory.stat")
        if cur_path.exists() and stat_path.exists():
            try:
                current = int(cur_path.read_text().strip())
                data = stat_path.read_text()
            except (OSError, ValueError):
                pass
            else:
                anon, inactive_file = _parse_memory_stat(data, use_rss=True)
                vsz = max(0, current - inactive_file)
                return (vsz, anon)
    return (None, 0)


# ---------------------------------------------------------------------------
# Block: IOPS and throughput from /proc/diskstats; device sizes from /sys
# ---------------------------------------------------------------------------

def parse_diskstats(content: str) -> dict[str, tuple[int, int, int, int]]:
    """name -> (read_ios, read_sectors, write_ios, write_sectors)."""
    result: dict[str, tuple[int, int, int, int]] = {}
    for line in content.splitlines():
        parts = line.split()
        if len(parts) < 14:
            continue
        name = parts[2]
        # skip partition names if we only want whole disks (optional)
        read_ios = int(parts[3])
        read_sec = int(parts[5])
        write_ios = int(parts[7])
        write_sec = int(parts[9])
        result[name] = (read_ios, read_sec, write_ios, write_sec)
    return result


def _read_dev_t(path: Path) -> int | None:
    """Read major:minor from e.g. /sys/block/sda/dev and return os.makedev(major, minor)."""
    try:
        raw = path.read_text().strip()
        major, minor = map(int, raw.split(":", 1))
        return os.makedev(major, minor)
    except (OSError, ValueError, AttributeError):
        return None


def get_block_device_sizes(block_filter: list[str] | None) -> list[tuple[str, int, int | None, int | None]]:
    """(name, total_bytes, used_bytes|None, free_bytes|None). Used/free from mounts by device number."""
    result: list[tuple[str, int, int | None, int | None]] = []
    sys_block = Path("/sys/block")
    if not sys_block.exists():
        return result

    # Build map: device number (st_dev) -> (used, free) from each mount point
    dev_usage: dict[int, tuple[int, int]] = {}
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if len(parts) < 2:
                    continue
                mount_point = parts[1]
                try:
                    st_dev = os.stat(mount_point).st_dev
                    stat = os.statvfs(mount_point)
                    total = stat.f_blocks * stat.f_frsize
                    free = stat.f_bavail * stat.f_frsize
                    used = total - free
                    dev_usage[st_dev] = (used, free)
                except OSError:
                    pass
    except OSError:
        pass

    for dev_dir in sorted(sys_block.iterdir()):
        if dev_dir.name.startswith("loop"):
            continue
        name = dev_dir.name
        if block_filter is not None and name not in block_filter:
            continue
        size_path = dev_dir / "size"
        try:
            sectors = int(size_path.read_text().strip())
        except (OSError, ValueError):
            continue
        total_bytes = sectors * 512
        used_sum = free_sum = 0
        found = False

        # Whole-disk device number (e.g. /sys/block/sda/dev)
        dev_path = dev_dir / "dev"
        if dev_path.exists():
            dev_t = _read_dev_t(dev_path)
            if dev_t is not None and dev_t in dev_usage:
                u, f = dev_usage[dev_t]
                used_sum, free_sum, found = u, f, True

        # Partitions (e.g. /sys/block/sda/sda1/dev) – aggregate to this disk
        if not found:
            for part_path in dev_dir.iterdir():
                if part_path.name == name or not part_path.is_dir():
                    continue
                part_dev = part_path / "dev"
                if part_dev.exists():
                    dev_t = _read_dev_t(part_dev)
                    if dev_t is not None and dev_t in dev_usage:
                        u, f = dev_usage[dev_t]
                        used_sum += u
                        free_sum += f
                        found = True

        used = (used_sum, free_sum) if found else (None, None)
        result.append((name, total_bytes, used[0], used[1]))
    return result


# ---------------------------------------------------------------------------
# Network: /proc/net/dev
# ---------------------------------------------------------------------------

def parse_net_dev(content: str) -> dict[str, tuple[int, int]]:
    """iface -> (rx_bytes, tx_bytes)."""
    result: dict[str, tuple[int, int]] = {}
    lines = content.splitlines()
    for i, line in enumerate(lines):
        if i < 2 or ":" not in line:
            continue
        iface, rest = line.split(":", 1)
        iface = iface.strip()
        nums = rest.split()
        if len(nums) >= 16:
            result[iface] = (int(nums[0]), int(nums[8]))
    return result


# ---------------------------------------------------------------------------
# Metrics source summary (what is pulled from where)
# ---------------------------------------------------------------------------

def print_metrics_sources_summary(
    *,
    in_cgroup: bool,
    cgroup_root: Path | None,
    cgroup_rel: str | None,
    no_cpu: bool,
    no_mem: bool,
    no_disk: bool,
    no_net: bool,
    block_filter: list[str] | None,
    net_filter: list[str] | None,
) -> None:
    """Print a one-time summary of which metrics come from which sources."""
    lines = ["Metrics sources (what is pulled from where):"]
    if not no_cpu:
        if in_cgroup and cgroup_root is not None:
            cpu_stat = str(_join_cgroup(cgroup_root, cgroup_rel or "", "cpu.stat"))
            cpu_max = str(_join_cgroup(cgroup_root, cgroup_rel or "", "cpu.max"))
            lines.append(f"  CPU: tot = available ({cpu_max} quota/period or n_cpus); cpu = usr+sys; {cpu_stat} → usr/sys; else /proc/stat")
        else:
            lines.append("  CPU: tot = available (n_cpus); cpu = usr+sys; /proc/stat → usr/sys")
    if not no_mem:
        if in_cgroup and cgroup_root is not None:
            cur = str(_join_cgroup(cgroup_root, cgroup_rel or "", "memory.current"))
            mx = str(_join_cgroup(cgroup_root, cgroup_rel or "", "memory.max"))
            stat = str(_join_cgroup(cgroup_root, cgroup_rel or "", "memory.stat"))
            lines.append(f"  Memory: {cur}, {mx}, {stat} (anon, inactive_file) → tot/vsz/rss")
        elif in_cgroup:
            mem_root = get_cgroup_v1_controller_root("memory")
            if mem_root:
                cur = str(_join_cgroup(mem_root, cgroup_rel or "", "memory.usage_in_bytes"))
                lim = str(_join_cgroup(mem_root, cgroup_rel or "", "memory.limit_in_bytes"))
                stat = str(_join_cgroup(mem_root, cgroup_rel or "", "memory.stat"))
                lines.append(f"  Memory: {cur}, {lim}, {stat} → tot/vsz/rss")
            else:
                lines.append("  Memory: /proc/meminfo (MemTotal, MemFree) → tot/vsz/rss")
        else:
            lines.append("  Memory: /proc/meminfo (MemTotal, MemFree) → tot/vsz/rss")
    if not no_disk:
        lines.append("  Disk: /proc/diskstats (read_ios, read_sectors, write_ios, write_sectors) → r, w, rIO, wIO per device")
        lines.append("        /sys/block (size); /proc/mounts (used/free per device) → tot, used, free")
        if block_filter:
            lines.append(f"        (filter: {', '.join(block_filter)})")
    if not no_net:
        lines.append("  Network: /proc/net/dev (rx_bytes, tx_bytes) → in, out per interface")
        if net_filter:
            lines.append(f"           (filter: {', '.join(net_filter)})")
    for line in lines:
        print(line)
    print()


# ---------------------------------------------------------------------------
# Main output
# ---------------------------------------------------------------------------

def sample_disk_net() -> tuple[dict, dict, dict, dict]:
    try:
        d1 = Path("/proc/diskstats").read_text()
        n1 = Path("/proc/net/dev").read_text()
    except OSError:
        return {}, {}, {}, {}
    time.sleep(1)
    try:
        d2 = Path("/proc/diskstats").read_text()
        n2 = Path("/proc/net/dev").read_text()
    except OSError:
        return parse_diskstats(d1), parse_diskstats(d2), parse_net_dev(n1), parse_net_dev(n2)
    a = parse_diskstats(d1)
    b = parse_diskstats(d2)
    na = parse_net_dev(n1)
    nb = parse_net_dev(n2)
    return a, b, na, nb


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Print dstat-like stats from /proc, /sys and cgroups (columnar, human-readable).",
    )
    ap.add_argument("--block", type=str, default=None, help="Comma-separated block device filter (e.g. sda,sdb)")
    ap.add_argument("--net", type=str, default=None, help="Comma-separated network interface filter (e.g. eth0)")
    ap.add_argument("--interval", "-n", type=float, default=1, help="Refresh every N seconds (0 = single shot)")
    ap.add_argument("--repeat-header", type=int, default=24, help="Repeat header rows every N data lines (0 = never)")
    ap.add_argument("--no-cpu", action="store_true", help="Hide CPU section")
    ap.add_argument("--no-mem", action="store_true", help="Hide memory section")
    ap.add_argument("--no-disk", action="store_true", help="Hide disk (block device) section")
    ap.add_argument("--no-net", action="store_true", help="Hide network section")
    ap.add_argument("--debug", action="store_true", help="Print metrics sources summary before continuous output")
    args = ap.parse_args()
    block_filter: list[str] | None = [x.strip() for x in args.block.split(",")] if args.block else None
    net_filter: list[str] | None = [x.strip() for x in args.net.split(",")] if args.net else None

    # Prefer cgroup path from /proc/1/cgroup so we read the real container cgroup in privileged containers
    cgroup_root, cgroup_rel = get_cgroup_path_from_proc(1)
    if cgroup_rel is None:
        cgroup_root = find_cgroup_root()
        mem_root = get_cgroup_v1_controller_root("memory")
        cgroup_rel = get_cgroup_path_from_sys(cgroup_root, mem_root)
    in_cgroup = cgroup_rel is not None
    first_line = True
    saved_widths: list[int] = []
    data_row_count = 0

    if args.debug:
        print_metrics_sources_summary(
            in_cgroup=in_cgroup,
            cgroup_root=cgroup_root,
            cgroup_rel=cgroup_rel,
            no_cpu=args.no_cpu,
            no_mem=args.no_mem,
            no_disk=args.no_disk,
            no_net=args.no_net,
            block_filter=block_filter,
            net_filter=net_filter,
        )

    while True:
        # Cgroup CPU: sample before and after 1s so usr/sys cores from usec deltas
        before_cg = get_cgroup_cpu_raw(cgroup_root, cgroup_rel or "") if in_cgroup else None

        # Block IO and net (1s sample); also provides the 1s window for cgroup CPU deltas
        d1, d2, n1, n2 = sample_disk_net()

        n_cpus = get_n_cpus()
        # CPU: from cgroup deltas (cores) or from host /proc/stat (cores)
        cpu_src = None
        cg_max_cores: float | None = None
        if in_cgroup and before_cg:
            after_cg = get_cgroup_cpu_raw(cgroup_root, cgroup_rel or "")
            if after_cg:
                du = after_cg[0] - before_cg[0]
                ds = after_cg[1] - before_cg[1]
                usr_cores, sys_cores = get_cgroup_cpu_usr_sys_cores_from_deltas(du, ds)
                cg_max_cores = get_cgroup_cpu_max_cores(cgroup_root, cgroup_rel or "")
                cpu_src = "cgroup"
        if cpu_src is None:
            usr_cores, sys_cores, _idl_cores = get_cpu_stats(n_cpus)
            cpu_src = "host"

        CPU_CORES_FMT = "6.3f"  # 00.000 cores
        # Total available CPU: cgroup limit when set, else n_cpus
        cpu_tot_available = cg_max_cores if (cpu_src == "cgroup" and cg_max_cores is not None) else float(n_cpus)

        # Memory: use cgroup when in container, else host /proc/meminfo
        cg_cur = cg_max = None
        mem_total = mem_free = 0  # set for host or cgroup path
        if in_cgroup:
            cg_vsz, cg_rss = get_cgroup_memory_vsz_rss(cgroup_root, cgroup_rel or "")
            vsz = cg_vsz  # None when from cgroup (no vsz in memory.stat)
            rss = cg_rss
            cg = get_cgroup_memory(cgroup_root, cgroup_rel or "")
            if cg:
                cg_cur, cg_max = cg
        else:
            mem_total, mem_free = get_meminfo()
            vsz = mem_total
            rss = mem_free

        cg_unlimited = False
        if cg_cur is not None:
            # Container view: tot from cgroup (absolute)
            mem_total = cg_max if cg_max is not None else 0
            cg_unlimited = cg_max is None
        else:
            if in_cgroup:
                mem_total, _ = get_meminfo()
        mem_tot_str = f"{'max':>{NUM_WIDTH}}" if cg_unlimited else human_bytes_short(mem_total)
        disk_deltas: dict[str, tuple[int, int, int, int]] = {}
        if d1 and d2:
            for name in d2:
                if name not in d1:
                    continue
                if block_filter is not None and name not in block_filter:
                    continue
                a = d1[name]
                b = d2[name]
                disk_deltas[name] = (
                    b[0] - a[0], b[1] - a[1], b[2] - a[2], b[3] - a[3],
                )
        net_deltas: dict[str, tuple[int, int]] = {}
        if n1 and n2:
            for name in n2:
                if name not in n1 or name == "lo":
                    continue
                if net_filter is not None and name not in net_filter:
                    continue
                net_deltas[name] = (n2[name][0] - n1[name][0], n2[name][1] - n1[name][1])

        # Block device sizes
        block_sizes = get_block_device_sizes(block_filter)

        # Build table: (main_header, [(sub_header, value), ...]) with date/time first
        now = time.localtime()
        date_str = time.strftime("%Y-%m-%d", now)
        time_str = time.strftime("%H:%M:%S", now)
        pad = "  "  # between columns

        mem_src = "cgroup" if cg_cur is not None else "host"
        groups: list[tuple[str, list[tuple[str, str]]]] = [
            ("datetime", [("date", date_str), ("time", time_str)]),
        ]
        if not args.no_cpu:
            groups.append((f"cpu ({cpu_src})", [
                ("tot", f"{cpu_tot_available:{CPU_CORES_FMT}}"),
                ("cpu", f"{usr_cores + sys_cores:{CPU_CORES_FMT}}"),
                ("usr", f"{usr_cores:{CPU_CORES_FMT}}"),
                ("sys", f"{sys_cores:{CPU_CORES_FMT}}"),
            ]))
        if not args.no_mem:
            groups.append((f"mem ({mem_src})", [
                ("tot", mem_tot_str),
                ("vsz", human_bytes_short(vsz) if vsz is not None else f"{'-':>{NUM_WIDTH}}"),
                ("rss", human_bytes_short(rss)),
            ]))
        # Per-device disk IO + size: only devices mounted in container (have used/free from a mount)
        def _io_for_device(dev_name: str) -> tuple[int, int, int, int]:
            """Use whole-disk stats only (no partition sum). Kernel attributes the same I/O to both
            disk and partition, so summing disk+partitions would double-count vs dstat/iostat."""
            if dev_name not in disk_deltas:
                return (0, 0, 0, 0)
            return disk_deltas[dev_name]

        if not args.no_disk:
            for name, total, used, free in block_sizes:
                if used is None and free is None:
                    continue  # not mounted in container, skip
                r_ios, r_sec, w_ios, w_sec = _io_for_device(name)
                r_bps = r_sec * 512
                w_bps = w_sec * 512
                used_s = human_bytes_short(used) if used is not None else f"{'-':>{NUM_WIDTH}}"
                free_s = human_bytes_short(free) if free is not None else f"{'-':>{NUM_WIDTH}}"
                groups.append((f"{name} (host)", [
                    ("r", human_rate_short(r_bps)),
                    ("w", human_rate_short(w_bps)),
                    ("rIO", f"{r_ios:{NUM_FMT}}"),
                    ("wIO", f"{w_ios:{NUM_FMT}}"),
                    ("used", used_s),
                    ("free", free_s),
                    ("tot", human_bytes_short(total)),
                ]))
        if not args.no_net:
            for iface in sorted(net_deltas.keys()):
                rx, tx = net_deltas[iface]
                groups.append((f"{iface} (host)", [
                    ("in", human_rate_short(rx)),
                    ("out", human_rate_short(tx)),
                ]))

        # Flatten to columns: (main, sub, value)
        columns: list[tuple[str, str, str]] = []
        group_end_indices: set[int] = set()
        idx = 0
        for main, subs in groups:
            for sub, val in subs:
                columns.append((main, sub, val))
                idx += 1
            group_end_indices.add(idx - 1)

        def join_with_seps(cells: list[str], major_between_groups_only: bool = False) -> str:
            """Join cells with SEP_MAJOR between groups, SEP_MINOR within a group.
            If major_between_groups_only is True (main header row), use SEP_MAJOR between every cell."""
            out = []
            for i, c in enumerate(cells):
                out.append(c)
                if i < len(cells) - 1:
                    if major_between_groups_only:
                        out.append(SEP_MAJOR)
                    else:
                        out.append(SEP_MAJOR if i in group_end_indices else SEP_MINOR)
            return "".join(out)

        # Column widths from sub-header and value only (main header is centered over group)
        min_w = 5
        if first_line or not saved_widths:
            widths = [
                max(min_w, len(sub), len(val))
                for _main, sub, val in columns
            ]
            saved_widths = list(widths)
        else:
            widths = saved_widths
            while len(widths) < len(columns):
                _main, sub, val = columns[len(widths)]
                widths.append(max(min_w, len(sub), len(val)))
                saved_widths.append(widths[-1])

        def rjust(s: str, w: int) -> str:
            return " " * max(0, w - len(s)) + s

        def center_main(name: str, span: int, widths_slice: list[int]) -> str:
            # Total width = sum of column widths + (span-1) * len(SEP_MINOR)
            total_w = sum(widths_slice) + (span - 1) * len(SEP_MINOR)
            return name.center(max(total_w, len(name)))

        # Repeat header when scrolled (every --repeat-header data lines)
        repeat_header = args.repeat_header
        if repeat_header and data_row_count > 0 and data_row_count % repeat_header == 0:
            main_parts = []
            col_idx = 0
            for main, subs in groups:
                n = len(subs)
                w_slice = widths[col_idx : col_idx + n]
                main_parts.append(center_main(main, n, w_slice))
                col_idx += n
            sub_parts = [rjust(sub, widths[i]) for i, (_, sub, _) in enumerate(columns)]
            header_line = join_with_seps(main_parts, major_between_groups_only=True)
            w = len(header_line)
            print(LINE_THICK * w)
            print(header_line)
            print(LINE_THIN * w)
            print(join_with_seps(sub_parts))
            print(LINE_THICK * w)

        # Print main header row (group names over their columns) on first output
        if first_line:
            main_parts = []
            col_idx = 0
            for main, subs in groups:
                n = len(subs)
                w_slice = widths[col_idx : col_idx + n]
                main_parts.append(center_main(main, n, w_slice))
                col_idx += n
            sub_parts = [rjust(sub, widths[i]) for i, (_, sub, _) in enumerate(columns)]
            header_line = join_with_seps(main_parts, major_between_groups_only=True)
            w = len(header_line)
            print(LINE_THICK * w)
            print(header_line)
            print(LINE_THIN * w)
            print(join_with_seps(sub_parts))
            print(LINE_THICK * w)

        # Print data row
        val_parts = [rjust(val, widths[i]) for i, (_, _, val) in enumerate(columns)]
        print(join_with_seps(val_parts))

        data_row_count += 1
        if first_line:
            first_line = False

        if args.interval <= 0:
            break
        # sample_disk_net() already slept 1s; sleep the remainder so lines print at --interval
        time.sleep(max(0.0, args.interval - 1.0))


if __name__ == "__main__":
    main()
