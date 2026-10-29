import os
import hashlib
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).parents[1]
VERIFY = ROOT / "docker/verify-wechat-package.sh"
DOWNLOAD = ROOT / "scripts/download-wechat.sh"
ARCHIVE = os.environ.get("WECHAT_TEST_DEB_PATH")
TEMP_ROOT = ROOT / "tmp"


class WechatPackageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        TEMP_ROOT.mkdir(exist_ok=True)

    def run_command(self, command, *, env=None):
        return subprocess.run(command, capture_output=True, text=True, env=env)

    def test_missing_package_fails(self):
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as tmp:
            result = self.run_command([str(VERIFY), "4.1.13.23", "amd64", str(pathlib.Path(tmp) / "missing.deb")])
        self.assertNotEqual(result.returncode, 0)

    def test_unknown_architecture_fails(self):
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as tmp:
            result = self.run_command([str(VERIFY), "4.1.13.23", "arm64", str(pathlib.Path(tmp) / "wechat.deb")])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unsupported", result.stderr.lower())

    def test_wrong_hash_fails(self):
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as tmp:
            package = pathlib.Path(tmp) / "wechat.deb"
            package.write_bytes(b"not the pinned package")
            result = self.run_command([str(VERIFY), "4.1.13.23", "amd64", str(package)])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("sha256", result.stderr.lower())

    def test_valid_local_archive_is_copied_to_docker_context(self):
        if not ARCHIVE:
            self.skipTest("approved local archive is not available")
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as tmp:
            output = pathlib.Path(tmp) / "docker/wechat.deb"
            env = os.environ.copy()
            env.update(WECHAT_ARCH="amd64", WECHAT_DEB_PATH=str(ARCHIVE), WECHAT_DEB_OUTPUT=str(output))
            result = self.run_command(["bash", str(DOWNLOAD)], env=env)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(hashlib.sha256(output.read_bytes()).digest(),
                             hashlib.sha256(pathlib.Path(ARCHIVE).read_bytes()).digest())

    def test_absent_package_does_not_download_latest(self):
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as tmp:
            output = pathlib.Path(tmp) / "docker/wechat.deb"
            env = os.environ.copy()
            env = {k: v for k, v in env.items() if not k.startswith("WECHAT_DEB_")}
            env.update(WECHAT_ARCH="amd64", WECHAT_DEB_OUTPUT=str(output))
            result = self.run_command(["bash", str(DOWNLOAD)], env=env)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(output.exists())

    def test_existing_invalid_package_is_not_overwritten(self):
        with tempfile.TemporaryDirectory(dir=TEMP_ROOT) as tmp:
            output = pathlib.Path(tmp) / "wechat.deb"
            output.write_bytes(b"existing package")
            env = os.environ.copy()
            env.update(WECHAT_ARCH="amd64", WECHAT_DEB_OUTPUT=str(output))
            result = self.run_command(["bash", str(DOWNLOAD)], env=env)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(output.read_bytes(), b"existing package")


if __name__ == "__main__":
    unittest.main()
