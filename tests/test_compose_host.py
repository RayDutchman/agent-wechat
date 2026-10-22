import os
import json
import shutil
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class HostComposeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("docker"):
            raise unittest.SkipTest("Docker Compose CLI is unavailable")

    def test_compose_is_host_networked_and_keeps_required_settings(self):
        env = os.environ.copy()
        for name in ("WECHAT_SCREEN_WIDTH", "WECHAT_SCREEN_HEIGHT", "WECHAT_SCREEN_DEPTH",
                     "WECHAT_SCREEN_DPI", "WECHAT_LOCALE", "TZ", "ENABLE_VNC"):
            env.pop(name, None)
        env.update({
            "AGENT_WECHAT_IMAGE": "sha256:" + "a" * 64,
            "AGENT_WECHAT_TOKEN_FILE": str(ROOT / "tmp" / "compose-host-token"),
            "AGENT_WECHAT_DATA_VOLUME": "test-agent-data",
            "AGENT_WECHAT_HOME_VOLUME": "test-agent-home",
            "AGENT_WECHAT_HOSTNAME": "test-agent",
        })
        result = subprocess.run(
            ["docker", "compose", "-f", "docker-compose.yml", "config", "--format", "json"],
            cwd=ROOT, env=env, check=True, capture_output=True, text=True,
        )
        config = json.loads(result.stdout)
        service = config["services"]["agent-wechat"]
        self.assertEqual(service["image"], env["AGENT_WECHAT_IMAGE"])
        self.assertEqual(service["pull_policy"], "never")
        self.assertEqual(service["hostname"], "test-agent")
        self.assertEqual(service["network_mode"], "host")
        self.assertNotIn("ports", service)
        self.assertEqual(set(service["cap_add"]), {"SYS_PTRACE", "NET_ADMIN"})
        self.assertIn("seccomp=unconfined", service["security_opt"])
        self.assertEqual(service["shm_size"], "67108864")
        mounts = {mount["target"]: mount for mount in service["volumes"]}
        self.assertEqual(mounts["/data"]["source"], "agent-wechat-data")
        self.assertEqual(mounts["/home/wechat"]["source"], "agent-wechat-home")
        self.assertEqual(mounts["/data/auth-token"]["source"], env["AGENT_WECHAT_TOKEN_FILE"])
        self.assertTrue(mounts["/data/auth-token"]["read_only"])
        # Compose versions may omit false-valued fields from normalized JSON.
        # A missing CreateHostPath is false in the normalized bind model;
        # short-syntax binds instead normalize to explicit true.
        self.assertFalse(mounts["/data/auth-token"]["bind"].get("create_host_path", False))
        self.assertEqual(service["environment"]["AGENT_HOST"], "[::]")
        self.assertEqual(service["environment"]["AGENT_PORT"], "6174")
        self.assertEqual(service["environment"]["AGENT_DB_PATH"], "/data/agent.db")
        self.assertEqual(service["environment"]["DISPLAY"], ":99")
        self.assertEqual(service["environment"]["WECHAT_SCREEN_WIDTH"], "1280")
        self.assertEqual(service["environment"]["WECHAT_SCREEN_HEIGHT"], "800")
        self.assertEqual(service["environment"]["WECHAT_SCREEN_DEPTH"], "24")
        self.assertEqual(service["environment"]["WECHAT_SCREEN_DPI"], "100")
        self.assertEqual(service["environment"]["WECHAT_LOCALE"], "C.UTF-8")
        self.assertEqual(service["environment"]["TZ"], "UTC")
        self.assertEqual(service["environment"]["ENABLE_VNC"], "1")
        self.assertEqual(service["restart"], "unless-stopped")
        self.assertTrue(config["volumes"]["agent-wechat-data"]["external"])
        self.assertTrue(config["volumes"]["agent-wechat-home"]["external"])
        self.assertEqual(config["volumes"]["agent-wechat-data"]["name"], "test-agent-data")
        self.assertEqual(config["volumes"]["agent-wechat-home"]["name"], "test-agent-home")


if __name__ == "__main__":
    unittest.main()
