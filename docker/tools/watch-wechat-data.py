#!/usr/bin/env python3
"""Emit coalescible hints when WeChat message database files change."""

import ctypes
import errno
import os
import select
import signal
import struct
import sys


IN_MODIFY = 0x00000002
IN_ATTRIB = 0x00000004
IN_CLOSE_WRITE = 0x00000008
IN_MOVED_FROM = 0x00000040
IN_MOVED_TO = 0x00000080
IN_CREATE = 0x00000100
IN_DELETE = 0x00000200
IN_DELETE_SELF = 0x00000400
IN_MOVE_SELF = 0x00000800
IN_Q_OVERFLOW = 0x00004000
IN_IGNORED = 0x00008000
IN_ISDIR = 0x40000000
WATCH_MASK = (IN_MODIFY | IN_ATTRIB | IN_CLOSE_WRITE | IN_MOVED_FROM | IN_MOVED_TO |
              IN_CREATE | IN_DELETE | IN_DELETE_SELF | IN_MOVE_SELF)
EVENT = struct.Struct("iIII")


def is_message_source(name: str) -> bool:
    return name.endswith(".db") or name.endswith(".db-wal")


class Watcher:
    def __init__(self, root):
        self.root = os.path.realpath(root)
        self.root_stat = os.stat(self.root, follow_symlinks=False)
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.libc.inotify_init1.argtypes = [ctypes.c_int]
        self.libc.inotify_init1.restype = ctypes.c_int
        self.libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        self.libc.inotify_add_watch.restype = ctypes.c_int
        self.libc.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
        self.libc.inotify_rm_watch.restype = ctypes.c_int
        self.fd = self.libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
        if self.fd < 0:
            raise OSError(ctypes.get_errno(), "inotify_init1")
        self.paths = {}
        try:
            self.add_tree(self.root)
        except OSError:
            os.close(self.fd)
            raise

    def add_dir(self, path):
        path = os.path.realpath(path)
        if path in self.paths.values():
            return
        wd = self.libc.inotify_add_watch(self.fd, os.fsencode(path), WATCH_MASK)
        if wd < 0:
            error = ctypes.get_errno() or errno.EIO
            raise OSError(error, "watch registration failed")
        self.paths[wd] = path

    def add_tree(self, path):
        self.add_dir(path)
        try:
            entries = os.scandir(path)
            with entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            self.add_tree(entry.path)
                    except FileNotFoundError:
                        continue
        except FileNotFoundError:
            return

    def rebuild_watches(self):
        for wd in list(self.paths):
            self.libc.inotify_rm_watch(self.fd, wd)
        self.paths.clear()
        while True:
            try:
                os.read(self.fd, 65536)
            except BlockingIOError:
                break
        self.add_tree(self.root)

    def handle_overflow(self, mask):
        if mask & IN_Q_OVERFLOW:
            self.rebuild_watches()
            return True
        return False

    def remove_tree(self, path):
        prefix = os.path.realpath(path)
        for wd, watched in list(self.paths.items()):
            if watched == prefix or watched.startswith(prefix + os.sep):
                self.libc.inotify_rm_watch(self.fd, wd)
                self.paths.pop(wd, None)

    def root_is_current(self):
        try:
            current = os.stat(self.root, follow_symlinks=False)
            return current.st_dev == self.root_stat.st_dev and current.st_ino == self.root_stat.st_ino
        except OSError:
            return False

    def run(self):
        while running:
            if not self.root_is_current():
                raise OSError(errno.ESTALE, "root changed")
            readable, _, _ = select.select([self.fd], [], [], 0.5)
            if not readable:
                continue
            data = os.read(self.fd, 65536)
            offset = 0
            changed = False
            overflow = False
            while offset + EVENT.size <= len(data):
                wd, mask, _cookie, length = EVENT.unpack_from(data, offset)
                offset += EVENT.size
                raw_name = data[offset:offset + length].split(b"\0", 1)[0]
                offset += length
                if mask & IN_Q_OVERFLOW:
                    overflow = True
                    changed = True
                    continue
                parent = self.paths.get(wd)
                if mask & IN_IGNORED:
                    self.paths.pop(wd, None)
                    continue
                if parent is None:
                    continue
                name = os.fsdecode(raw_name)
                target = os.path.join(parent, name) if name else parent
                if mask & IN_ISDIR:
                    if mask & (IN_CREATE | IN_MOVED_TO) and os.path.isdir(target) and not os.path.islink(target):
                        self.add_tree(target)
                    if mask & IN_MOVED_FROM:
                        self.remove_tree(target)
                    if mask & (IN_DELETE | IN_MOVED_FROM | IN_DELETE_SELF | IN_MOVE_SELF):
                        changed = True
                elif name and is_message_source(name):
                    changed = True
            if overflow:
                self.handle_overflow(IN_Q_OVERFLOW)
            if changed:
                sys.stdout.write("changed\n")
                sys.stdout.flush()


running = True


def stop(_signum, _frame):
    global running
    running = False


def main():
    global running
    if len(sys.argv) != 2:
        sys.stderr.write("watch-wechat-data: invalid data root\n")
        return 2
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    watcher = None
    try:
        if not os.path.isdir(sys.argv[1]) or os.path.islink(sys.argv[1]):
            raise OSError("invalid root")
        watcher = Watcher(sys.argv[1])
        watcher.run()
        return 0
    except (OSError, ValueError):
        sys.stderr.write("watch-wechat-data: invalid data root\n")
        return 2
    finally:
        if watcher is not None:
            os.close(watcher.fd)


if __name__ == "__main__":
    raise SystemExit(main())
