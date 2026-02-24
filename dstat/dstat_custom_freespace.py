### Author: Dag Wieers <dag$wieers,com>

### FIXME: This module needs infrastructure to provide a list of mountpoints
### FIXME: Would be nice to have a total by default (half implemented)

class dstat_plugin(dstat):
    """
    Amount of used and free space per mountpoint.

    Environment variable: DSTAT_FREESPACE_MOUNTS
        Comma-separated path:display pairs (or plain paths). If set,
        only these mountpoints are shown. Display name is used in output
        so dstat truncation is readable. E.g.:
          DSTAT_FREESPACE_MOUNTS=/:root,/var/lib/postgresql/data:pgdata

    Environment variable: DSTAT_FREESPACE_DEBUG
        If set, print troubleshooting info to stderr (e.g. DSTAT_FREESPACE_DEBUG=1).
    """

    def _debug(self, msg):
        if os.environ.get('DSTAT_FREESPACE_DEBUG'):
            import sys
            print(msg, file=sys.stderr, flush=True)

    def __init__(self):
        self.nick = ('used', 'free')
        mtab_path = '/proc/mounts'
        self.open(mtab_path)
        self._debug('freespace_custom: opened %s' % mtab_path)
        self.cols = 2
        raw = os.environ.get('DSTAT_FREESPACE_MOUNTS', '').strip()
        self.mounts = []
        self.path_to_display = {}  # path or basename -> display name
        if raw:
            for part in raw.split(','):
                part = part.strip()
                if not part:
                    continue
                # path:display (path and display separated by colon)
                if ':' in part:
                    path_key, display = part.split(':', 1)
                else:
                    path_key, display = part, part
                path_key = path_key.strip()
                display = (display.strip() or path_key)
                if path_key:
                    self.mounts.append(path_key)
                    self.path_to_display[path_key] = display
        self._debug('freespace_custom: mounts filter=%s path_to_display=%s' % (self.mounts, self.path_to_display))

    def vars(self):
        mount_filter = set(self.mounts) if self.mounts else None
        ret = []
        lines = list(self.splitlines())
        self._debug('freespace_custom: vars() got %d lines from mtab' % len(lines))
        for l in lines:
            if len(l) < 6:
                self._debug('  skip (len<6): %s' % l)
                continue
            if l[2] in ('binfmt_misc', 'devpts', 'iso9660', 'none', 'proc', 'sysfs', 'usbfs', 'cgroup', 'tmpfs', 'devtmpfs', 'debugfs', 'mqueue', 'systemd-1', 'rootfs', 'autofs'):
                self._debug('  skip (fstype %s): %s' % (l[2], l[1]))
                continue
            ### FIXME: Excluding 'none' here may not be what people want (/dev/shm)
            if l[0] in ('devpts', 'none', 'proc', 'sunrpc', 'usbfs', 'securityfs', 'hugetlbfs', 'configfs', 'selinuxfs', 'pstore', 'nfsd'):
                self._debug('  skip (device %s): %s' % (l[0], l[1]))
                continue
            name = l[1]
            try:
                res = os.statvfs(name)
            except OSError as e:
                self._debug('  skip (statvfs %s): %s' % (e, name))
                continue
            if res[0] == 0:
                self._debug('  skip (zero blocks): %s' % name)
                continue ### Skip zero block filesystems
            if mount_filter is not None:
                basename = os.path.basename(name) or name
                if name not in mount_filter and basename not in mount_filter and ('/' + basename) not in mount_filter:
                    self._debug('  skip (filter): %s (basename=%s, filter=%s)' % (name, basename, mount_filter))
                    continue
            ret.append(name)
        self._debug('freespace_custom: vars() returning %d mounts: %s' % (len(ret), ret))
        return ret

    def name(self):
        out = []
        for path in self.vars:
            base = os.path.basename(path) or path
            display = (
                self.path_to_display.get(path)
                or self.path_to_display.get(base)
                or self.path_to_display.get('/' + base)
                or path
            )
            out.append(display)
        return out

    def extract(self):
        self.val['total'] = (0, 0)
        for name in self.vars:
            res = os.statvfs(name)
            self.val[name] = ( (float(res.f_blocks) - float(res.f_bavail)) * int(res.f_frsize), float(res.f_bavail) * float(res.f_frsize) )
            self.val['total'] = (self.val['total'][0] + self.val[name][0], self.val['total'][1] + self.val[name][1])

# vim:ts=4:sw=4:et
