# CPU and memory for a process from /proc/<pid>/stat and /proc/<pid>/statm.
# Enable with: dstat --custom-pid-cpu-mem
# Set PID via: DSTAT_PID=<pid> or DSTAT_CGROUP_PID=<pid>

class dstat_plugin(dstat):
    """
    CPU and memory for one process from /proc/<pid>/stat and /proc/<pid>/statm.

    PID from environment: DSTAT_PID or DSTAT_CGROUP_PID.
    CPU: user %, system % (over interval) from /proc/<pid>/stat (utime, stime).
    Memory: vsz (virtual) and rss (resident) from /proc/<pid>/statm; pages
    are multiplied by page size and displayed in B/K/M/G by dstat (scale 1024).
    """

    def __init__(self):
        self.name = 'custom-pid-cpu-mem'
        self.nick = ('sys-cpu', 'usr-cpu', 'vsz', 'rss')
        self.vars = ['cpu', 'mem']
        self.cols = 2
        self.types = ['p', 'b']  # cpu %, then bytes (dstat shows B/K/M/G for 'b')
        self.scales = [34, 1024]
        self.width = max(8, max(len(n) for n in self.nick))
        self._clk_tck = os.sysconf('SC_CLK_TCK') or 100
        self._page_size = os.sysconf('SC_PAGESIZE') or 4096

    def _pid(self):
        return os.environ.get('DSTAT_PID') or os.environ.get('DSTAT_CGROUP_PID') or ''

    def _read_stat(self, pid):
        """Parse /proc/<pid>/stat; return (utime, stime) in clock ticks or (None, None)."""
        try:
            with open('/proc/%s/stat' % pid) as f:
                line = f.read()
        except (IOError, OSError):
            return (None, None)
        r = line.rfind(')')
        if r < 0:
            return (None, None)
        rest = line[r + 2:].split()
        if len(rest) < 14:
            return (None, None)
        try:
            return (int(rest[12]), int(rest[13]))  # utime, stime (fields 14, 15)
        except (ValueError, IndexError):
            return (None, None)

    def _read_statm(self, pid):
        """Parse /proc/<pid>/statm; return (size_pages, resident_pages) or (0, 0).
        statm gives sizes in pages; multiply by page size for bytes (B/K/M/G).
        """
        try:
            with open('/proc/%s/statm' % pid) as f:
                parts = f.read().split()
        except (IOError, OSError):
            return (0, 0)
        if len(parts) < 2:
            return (0, 0)
        try:
            return (int(parts[0]), int(parts[1]))  # size (virtual), resident
        except (ValueError, IndexError):
            return (0, 0)

    def _pages_to_bytes(self, pages):
        """Convert statm page count to bytes for display in B/KiB/MiB/GiB."""
        return pages * self._page_size

    def extract(self):
        pid = self._pid()
        if not pid:
            self.val['cpu'] = [0, 0]
            self.val['mem'] = [0, 0]
            return

        utime, stime = self._read_stat(pid)
        if utime is None:
            utime, stime = 0, 0

        size_pages, resident_pages = self._read_statm(pid)
        vmem_bytes = self._pages_to_bytes(size_pages)
        resident_bytes = self._pages_to_bytes(resident_pages)

        self.set2['cpu'] = [utime, stime]
        deltas = [
            self.set2['cpu'][0] - self.set1['cpu'][0],
            self.set2['cpu'][1] - self.set1['cpu'][1],
        ]
        if deltas[0] >= 0 and deltas[1] >= 0 and elapsed > 0:
            cpu_pct = 100.0 / (elapsed * self._clk_tck)
            user_pct = cpu_pct * deltas[0]
            system_pct = cpu_pct * deltas[1]
        else:
            user_pct = system_pct = 0
        self.val['cpu'] = [system_pct, user_pct]
        self.val['mem'] = [vmem_bytes, resident_bytes]
        if step == op.delay:
            self.set1['cpu'] = list(self.set2['cpu'])

# vim:ts=4:sw=4:et
