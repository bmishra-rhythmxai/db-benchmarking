# Cgroup CPU and memory from current process cgroup; optional per-PID from /proc.
# Enable with: dstat --custom-cpu-mem
# Optional: DSTAT_PID=<pid> for additional per-process CPU and memory columns.
# When DSTAT_PID is not set, PID-related columns are omitted.

class dstat_plugin(dstat):
    """
    Overall CPU and memory from cgroup v2 (current process's cgroup):
      - Cgroup path is read from /proc/self/cgroup and resolved via cgroup2 mount.
      - CPU: system % and user % from cpu.stat (system_usec, user_usec deltas).
      - Memory: file (vsz) and anon (rss) in bytes from memory.stat.
    If DSTAT_PID is set, also shows that process's system %, user %, vsz, rss
    from /proc/<pid>/stat and /proc/<pid>/statm. When DSTAT_PID is not set,
    those PID columns are not shown.
    """

    def __init__(self):
        self.name = 'custom-cpu-mem'
        self._with_pid = bool(self._pid())
        if self._with_pid:
            self.nick = ['c-tot', 'c-sys', 'c-usr', 'c-vsz', 'c-rss', 'p-tot', 'p-sys', 'p-usr', 'p-vsz', 'p-rss']
            self.vars = ['cgrp_cpu', 'cgrp_sys', 'cgrp_usr', 'cgrp_vsz', 'cgrp_rss', 'pid_cpu', 'pid_sys', 'pid_usr', 'pid_vsz', 'pid_rss']
            self.types = ['p', 'p', 'p', 'b', 'b', 'p', 'p', 'p', 'b', 'b']
            self.scales = [34, 34, 34, 1024, 1024, 34, 34, 34, 1024, 1024]
        else:
            self.nick = ['c-tot', 'c-sys', 'c-usr', 'c-vsz', 'c-rss']
            self.vars = ['cgrp_cpu', 'cgrp_sys', 'cgrp_usr', 'cgrp_vsz', 'cgrp_rss']
            self.types = ['p', 'p', 'p', 'b', 'b']
            self.scales = [34, 34, 34, 1024, 1024]
        self.cols = 1
        self.width = 5
        self._clk_tck = os.sysconf('SC_CLK_TCK') or 100
        self._page_size = os.sysconf('SC_PAGESIZE') or 4096
        self._cgroup_root = self._discover_cgroup_root()
        self._pid_prev = (0, 0)  # (utime, stime) for delta when DSTAT_PID set

    def _discover_cgroup_root(self):
        """
        Resolve the cgroup v2 path for the current process: read /proc/self/cgroup
        for the cgroup path and /proc/self/mountinfo for the cgroup2 mount point.
        Returns the full path (mount_point + cgroup_path) or /sys/fs/cgroup as fallback.
        """
        mount_point = None
        try:
            with open('/proc/self/mountinfo') as f:
                for line in f:
                    parts = line.split()
                    if len(parts) < 10:
                        continue
                    # Optional fields end with '-'; after that: fs_type, source, options
                    try:
                        sep_idx = parts.index('-')
                    except ValueError:
                        continue
                    if sep_idx + 2 >= len(parts):
                        continue
                    if parts[sep_idx + 1] == 'cgroup2':
                        mount_point = parts[4]  # mount point is 5th field
                        break
        except (IOError, OSError):
            pass
        if not mount_point:
            return '/sys/fs/cgroup'

        cgroup_path = '/'
        try:
            with open('/proc/self/cgroup') as f:
                for line in f:
                    # Format: hierarchy_id:controller_list:cgroup_path
                    # cgroup v2 unified: "0::/path" (empty controller list)
                    if '::' in line:
                        raw = line.strip().split(':', 2)
                        if len(raw) >= 3:
                            cgroup_path = raw[2].strip()
                            if not cgroup_path:
                                cgroup_path = '/'
                            break
        except (IOError, OSError):
            pass

        if cgroup_path == '/':
            return mount_point.rstrip('/') or mount_point
        return os.path.normpath(mount_point.rstrip('/') + '/' + cgroup_path.lstrip('/'))

    def _pid(self):
        return os.environ.get('DSTAT_PID', '').strip() or ''

    def _read_cpu_stat(self):
        """Read user_usec and system_usec from cgroup cpu.stat; return (user_usec, system_usec) or (None, None)."""
        try:
            user_usec = system_usec = None
            with open(os.path.join(self._cgroup_root, 'cpu.stat')) as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 2:
                        if parts[0] == 'user_usec':
                            user_usec = int(parts[1])
                        elif parts[0] == 'system_usec':
                            system_usec = int(parts[1])
            if user_usec is not None and system_usec is not None:
                return (user_usec, system_usec)
        except (IOError, OSError, ValueError):
            pass
        return (None, None)

    def _read_memory_stat(self):
        """Read file (vsz) and anon (rss) in bytes from cgroup memory.stat; return (file, anon)."""
        try:
            file_bytes = anon_bytes = 0
            with open(os.path.join(self._cgroup_root, 'memory.stat')) as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 2:
                        if parts[0] == 'file':
                            file_bytes = int(parts[1])
                        elif parts[0] == 'anon':
                            anon_bytes = int(parts[1])
            return (file_bytes, anon_bytes)
        except (IOError, OSError, ValueError):
            pass
        return (0, 0)

    def _read_proc_stat(self, pid):
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
            return (int(rest[12]), int(rest[13]))
        except (ValueError, IndexError):
            return (None, None)

    def _read_proc_statm(self, pid):
        """Parse /proc/<pid>/statm; return (size_pages, resident_pages) or (0, 0)."""
        try:
            with open('/proc/%s/statm' % pid) as f:
                parts = f.read().split()
        except (IOError, OSError):
            return (0, 0)
        if len(parts) < 2:
            return (0, 0)
        try:
            return (int(parts[0]), int(parts[1]))
        except (ValueError, IndexError):
            return (0, 0)

    def extract(self):
        # Cgroup CPU: user_usec and system_usec deltas -> %
        user_usec, system_usec = self._read_cpu_stat()
        if user_usec is not None and system_usec is not None:
            self.set2['cgrp_sys'] = [system_usec]
            self.set2['cgrp_usr'] = [user_usec]
            delta_sys = self.set2['cgrp_sys'][0] - self.set1['cgrp_sys'][0]
            delta_usr = self.set2['cgrp_usr'][0] - self.set1['cgrp_usr'][0]
            if elapsed > 0:
                pct_scale = 100.0 / (elapsed * 1e6)
                cgrp_sys_pct = pct_scale * delta_sys if delta_sys >= 0 else 0
                cgrp_usr_pct = pct_scale * delta_usr if delta_usr >= 0 else 0
                self.val['cgrp_sys'] = [cgrp_sys_pct]
                self.val['cgrp_usr'] = [cgrp_usr_pct]
                self.val['cgrp_cpu'] = [cgrp_sys_pct + cgrp_usr_pct]
            else:
                self.val['cgrp_sys'] = [0]
                self.val['cgrp_usr'] = [0]
                self.val['cgrp_cpu'] = [0]
            if step == op.delay:
                self.set1['cgrp_sys'] = list(self.set2['cgrp_sys'])
                self.set1['cgrp_usr'] = list(self.set2['cgrp_usr'])
        else:
            self.val['cgrp_sys'] = [0]
            self.val['cgrp_usr'] = [0]
            self.val['cgrp_cpu'] = [0]

        # Cgroup memory: file (vsz) and anon (rss) from memory.stat
        file_bytes, anon_bytes = self._read_memory_stat()
        self.val['cgrp_vsz'] = [file_bytes]
        self.val['cgrp_rss'] = [anon_bytes]

        # Per-PID stats (only when DSTAT_PID is set; columns are omitted when not set)
        if self._with_pid:
            pid = self._pid()
            utime, stime = self._read_proc_stat(pid)
            if utime is None:
                utime, stime = 0, 0
            prev_utime, prev_stime = self._pid_prev
            deltas = (utime - prev_utime, stime - prev_stime)
            if deltas[0] >= 0 and deltas[1] >= 0 and elapsed > 0:
                cpu_pct = 100.0 / (elapsed * self._clk_tck)
                pid_usr_pct = cpu_pct * deltas[0]
                pid_sys_pct = cpu_pct * deltas[1]
                self.val['pid_usr'] = [pid_usr_pct]
                self.val['pid_sys'] = [pid_sys_pct]
                self.val['pid_cpu'] = [pid_usr_pct + pid_sys_pct]
            else:
                self.val['pid_sys'] = [0]
                self.val['pid_usr'] = [0]
                self.val['pid_cpu'] = [0]
            if step == op.delay:
                self._pid_prev = (utime, stime)

            size_pages, resident_pages = self._read_proc_statm(pid)
            self.val['pid_vsz'] = [size_pages * self._page_size]
            self.val['pid_rss'] = [resident_pages * self._page_size]

# vim:ts=4:sw=4:et
