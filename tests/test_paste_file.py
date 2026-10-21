"""Offline helper checks; clipboard and keyboard executables are replaced."""
import os
import pathlib
import pwd
import subprocess
import tempfile
import unittest
from urllib.parse import quote, unquote

HELPER = pathlib.Path(__file__).resolve().parents[1] / "docker/tools/paste-file"


class PasteFilePathTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.capture = self.root / "clipboard"
        self.keys = self.root / "keys"
        self.env = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}",
                        CAPTURE=str(self.capture), KEYS=str(self.keys))
        self.executable("xclip", 'cat > "$CAPTURE"')
        self.executable("xdotool", 'printf "%s\\n" "$*" >> "$KEYS"')

    def executable(self, name, body):
        p = self.bin / name
        p.write_text("#!/bin/sh\n" + body + "\n")
        p.chmod(0o755)

    def source(self, name):
        p = self.root / name
        p.write_bytes(b"alias-owner-check\n")
        p.chmod(0o600)
        return p

    def run_helper(self, source):
        return subprocess.run(["bash", str(HELPER), str(source)], env=self.env,
                              capture_output=True, text=True, timeout=5)

    def test_chinese_path_is_preserved_without_copying(self):
        source = self.source("中文路径验收.md")
        before = source.stat()
        self.executable("cp", "exit 97")
        result = self.run_helper(source)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.capture.read_bytes(),
            ("file://" + quote(str(source), safe="/")).encode("ascii"),
        )
        after = source.stat()
        self.assertEqual((before.st_ino, before.st_uid, before.st_gid, before.st_mode),
                         (after.st_ino, after.st_uid, after.st_gid, after.st_mode))
        self.assertEqual(self.keys.read_text(), "key ctrl+v\n")

    def test_ascii_source_is_not_copied(self):
        source = self.source("control.md")
        result = self.run_helper(source)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.capture.read_text(), f"file://{source}")
        self.assertEqual(source.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.keys.read_text(), "key ctrl+v\n")

    def test_file_uri_percent_encodes_path_and_round_trips(self):
        source = self.source("# percent% question? space 中文.md")
        result = self.run_helper(source)
        self.assertEqual(result.returncode, 0, result.stderr)

        captured = self.capture.read_bytes()
        self.assertTrue(captured.startswith(b"file://"))
        encoded_path = captured[len(b"file://"):].decode("ascii")
        self.assertIn("%23", encoded_path)
        self.assertIn("%25", encoded_path)
        self.assertIn("%3F", encoded_path)
        self.assertIn("%20", encoded_path)
        self.assertIn("%E4%B8%AD%E6%96%87", encoded_path)
        self.assertEqual(unquote(encoded_path), str(source))
        self.assertEqual(
            captured.decode("ascii"),
            "file://" + quote(str(source), safe="/"),
        )

    def test_missing_file_stops_before_clipboard_and_keys(self):
        result = self.run_helper(self.root / "missing.md")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.capture.exists())
        self.assertFalse(self.keys.exists())

    def test_launch_exports_utf8_through_real_su(self):
        if os.geteuid() != 0:
            self.skipTest("real su check needs root in an isolated container")
        try:
            pwd.getpwnam("wechat")
        except KeyError:
            self.skipTest("isolated WeChat image supplies the session user")
        self.executable("su", '''exec /usr/bin/su -s /bin/sh -c 'printf "%s\\n" "$LANG" "$LC_ALL" "$LC_CTYPE"; locale charmap' wechat''')
        env = dict(self.env, DISPLAY=":99", DBUS_SESSION_BUS_ADDRESS="")
        for key in ("LANG", "LC_ALL", "LC_CTYPE"):
            env.pop(key, None)
        result = subprocess.run(["bash", str(HELPER.with_name("launch-wechat"))],
                                env=env, capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["C.UTF-8"] * 3 + ["UTF-8"])
