import os
import contextlib
import io
import pathlib
import select
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "docker/tools/watch-wechat-data.py"


def load_watcher_module():
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("watch_wechat_data", SCRIPT)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class WatchWechatDataTests(unittest.TestCase):
    def test_message_source_suffixes(self):
        self.assertTrue(SCRIPT.is_file(), "watcher implementation is missing")
        module = load_watcher_module()
        self.assertTrue(module.is_message_source("message_0.db"))
        self.assertTrue(module.is_message_source("message_0.db-wal"))
        self.assertFalse(module.is_message_source("message_0.db-shm"))
        self.assertFalse(module.is_message_source("snapshot.db.tmp"))

    def test_overflow_rebuilds_watch_descriptor_mapping(self):
        module = load_watcher_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "root"
            inside = root / "inside"
            outside = pathlib.Path(tmp) / "outside"
            inside.mkdir(parents=True)
            outside.mkdir()
            watcher = module.Watcher(str(root))
            try:
                inside.rename(outside / "moved")
                stale_wd = next(wd for wd, path in watcher.paths.items() if path == str(inside))
                watcher.paths[stale_wd] = str(inside)
                self.assertTrue(watcher.handle_overflow(module.IN_Q_OVERFLOW))
                self.assertNotIn(stale_wd, watcher.paths)
                self.assertEqual(set(watcher.paths.values()), {str(root)})
            finally:
                os.close(watcher.fd)

    def test_add_watch_failure_raises_without_private_path_message(self):
        module = load_watcher_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "private-root"
            root.mkdir()
            watcher = module.Watcher.__new__(module.Watcher)
            watcher.root = str(root)
            watcher.paths = {}
            watcher.fd = 1
            watcher.libc = mock.Mock()
            watcher.libc.inotify_add_watch.return_value = -1
            with mock.patch.object(module.ctypes, "get_errno", return_value=13):
                with self.assertRaises(OSError) as raised:
                    watcher.add_dir(root)
            self.assertNotIn(str(root), str(raised.exception))
            self.assert_fixed_main_error(module, root)

    def test_scandir_failure_raises_instead_of_silently_skipping_subtree(self):
        module = load_watcher_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "private-root"
            root.mkdir()
            watcher = module.Watcher.__new__(module.Watcher)
            watcher.paths = {}
            watcher.fd = 1
            watcher.libc = mock.Mock()
            watcher.libc.inotify_add_watch.return_value = 7
            with mock.patch.object(module.os, "scandir", side_effect=PermissionError(13, "denied")):
                with self.assertRaises(OSError) as raised:
                    watcher.add_tree(root)
            self.assertNotIn(str(root), str(raised.exception))
            self.assert_fixed_main_error(module, root)

    def test_initial_scan_failure_closes_inotify_fd(self):
        module = load_watcher_module()
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "root"
            root.mkdir()
            with mock.patch.object(module.Watcher, "add_tree", side_effect=PermissionError(13, "denied")), \
                 mock.patch.object(module.os, "close", wraps=os.close) as close_fd:
                with self.assertRaises(PermissionError):
                    module.Watcher(str(root))
            self.assertTrue(close_fd.called, "failed initialization leaked the inotify fd")

    def assert_fixed_main_error(self, module, root):
        stderr = io.StringIO()
        with mock.patch.object(module.sys, "argv", [str(SCRIPT), str(root)]), \
             mock.patch.object(module, "Watcher", side_effect=OSError("private path hidden")), \
             contextlib.redirect_stderr(stderr):
            self.assertEqual(module.main(), 2)
        self.assertEqual(stderr.getvalue(), "watch-wechat-data: invalid data root\n")
        self.assertNotIn(str(root), stderr.getvalue())

    def test_missing_root_has_fixed_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), str(pathlib.Path(tmp) / "missing")],
                capture_output=True,
                text=True,
                timeout=3,
            )
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proc.stdout, "")
        self.assertEqual(proc.stderr, "watch-wechat-data: invalid data root\n")

    def test_root_replacement_exits_instead_of_watching_old_inode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "data"
            replacement = pathlib.Path(tmp) / "replacement"
            root.mkdir()
            replacement.mkdir()
            proc = subprocess.Popen(
                [sys.executable, str(SCRIPT), str(root)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline and proc.poll() is None:
                    fd_dir = pathlib.Path(f"/proc/{proc.pid}/fd")
                    if fd_dir.exists() and any(
                        os.readlink(fd).startswith("anon_inode:inotify")
                        for fd in fd_dir.iterdir()
                    ):
                        break
                    select.select([], [], [], 0.02)
                else:
                    self.fail("watcher did not establish inotify fd")
                root.rename(pathlib.Path(tmp) / "old-data")
                replacement.rename(root)
                self.assertEqual(proc.wait(timeout=3), 2)
                self.assertEqual(proc.stdout.read(), "")
                self.assertEqual(proc.stderr.read(), "watch-wechat-data: invalid data root\n")
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=2)
                proc.stdout.close()
                proc.stderr.close()

    def test_moved_directory_is_unwatched_outside_and_rewatched_when_moved_in(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = pathlib.Path(tmp)
            root = base / "data"
            root.mkdir()
            moved = root / "shard"
            moved.mkdir()
            db = moved / "part.db"
            db.write_bytes(b"synthetic")
            outside = base / "outside"
            outside.mkdir()
            proc = subprocess.Popen(
                [sys.executable, str(SCRIPT), str(root)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            try:
                fd_dir = pathlib.Path(f"/proc/{proc.pid}/fd")
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline and not any(
                    os.readlink(fd).startswith("anon_inode:inotify") for fd in fd_dir.iterdir()
                ):
                    select.select([], [], [], 0.02)
                moved.rename(outside / "shard")
                self.assertTrue(select.select([proc.stdout], [], [], 2)[0])
                self.assertEqual(proc.stdout.readline(), "changed\n")
                (outside / "shard" / "part.db").write_bytes(b"outside mutation")
                self.assertFalse(select.select([proc.stdout], [], [], 0.2)[0])

                (outside / "shard").rename(root / "shard")
                deadline = time.monotonic() + 2
                ready = []
                while time.monotonic() < deadline and not ready:
                    (root / "shard" / "part.db").write_bytes(b"inside mutation")
                    ready = select.select([proc.stdout], [], [], 0.1)[0]
                self.assertTrue(ready, "moved-in existing directory was not watched")
                self.assertEqual(proc.stdout.readline(), "changed\n")
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=3)
                proc.stdout.close()
                proc.stderr.close()

    def test_shm_events_are_ignored_but_wal_events_are_emitted(self):
        module = load_watcher_module()
        with tempfile.TemporaryDirectory() as tmp:
            # Construct synchronously so watches are installed before mutation.
            watcher = module.Watcher(tmp)
            real_select = select.select

            def until_idle(readers, writers, errors, timeout):
                ready = real_select(readers, writers, errors, 0.1)
                if not ready[0]:
                    module.running = False
                return ready

            try:
                # Feed real inotify events into the real run loop. Separate
                # batches prevent earlier DB notifications contaminating the
                # negative SHM assertion.
                for name, expected in [("part.db-shm", ""), ("part.db-wal", "changed\n")]:
                    pathlib.Path(tmp, name).write_bytes(b"synthetic")
                    module.running = True
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output), mock.patch.object(
                        module.select, "select", side_effect=until_idle
                    ):
                        watcher.run()
                    self.assertEqual(output.getvalue(), expected)
            finally:
                os.close(watcher.fd)

    def test_file_and_new_subdirectory_changes_and_sigterm(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "data"
            root.mkdir()
            proc = subprocess.Popen(
                [sys.executable, str(SCRIPT), str(root)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            def read_change(timeout):
                if not select.select([proc.stdout], [], [], timeout)[0]:
                    return False
                data = os.read(proc.stdout.fileno(), 65536)
                self.assertTrue(data, "watcher exited before emitting a hint")
                # One file operation can yield several inotify read batches.
                # Drain that operation before starting the next one. Use raw
                # reads: TextIOWrapper can hide ready lines from select().
                deadline = time.monotonic() + 2
                while select.select([proc.stdout], [], [], 0.05)[0]:
                    self.assertLess(time.monotonic(), deadline)
                    chunk = os.read(proc.stdout.fileno(), 65536)
                    self.assertTrue(chunk, "watcher exited while draining hints")
                    data += chunk
                self.assertTrue(data.endswith(b"\n"))
                self.assertEqual(set(data.splitlines()), {b"changed"})
                return True

            try:
                # Repeat mutations during bounded startup-race window; output is event-driven.
                ready = False
                deadline = time.monotonic() + 4
                while time.monotonic() < deadline and not ready:
                    (root / "message.db-wal").write_bytes(b"synthetic")
                    ready = read_change(0.1)
                self.assertTrue(ready)

                shard = root / "shard"
                shard.mkdir()
                deadline = time.monotonic() + 2
                ready = False
                while time.monotonic() < deadline and not ready:
                    (shard / "part.db").write_bytes(b"synthetic")
                    ready = read_change(0.1)
                self.assertTrue(ready, "new subdirectory was not watched")

                # Rename, WAL creation and deletion are sources. SHM exclusion
                # is tested separately, without pending DB notifications.
                wal = shard / "part.db-wal"
                wal.write_bytes(b"synthetic")
                self.assertTrue(read_change(2))
                wal.unlink()
                self.assertTrue(read_change(2))
                (shard / "part.db").rename(shard / "renamed.db")
                self.assertTrue(read_change(2))

                proc.terminate()
                self.assertEqual(proc.wait(timeout=3), 0)
                self.assertEqual(proc.stderr.read(), b"")
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=2)
                proc.stdout.close()
                proc.stderr.close()


if __name__ == "__main__":
    unittest.main()
