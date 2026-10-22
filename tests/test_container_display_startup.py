"""Regression guards for the single-session container display configuration."""
import pathlib
import shlex
import subprocess
import tempfile
import unittest


ENTRYPOINT = pathlib.Path(__file__).resolve().parents[1] / "docker" / "entrypoint.sh"


class ContainerDisplayStartupTests(unittest.TestCase):
    def test_shell_syntax(self):
        subprocess.run(["bash", "-n", str(ENTRYPOINT)], check=True)

    def command_for(self, program):
        lines = ENTRYPOINT.read_text().splitlines()
        matches = [shlex.split(line) for line in lines
                   if line.strip().startswith("su ") and f"exec {program} " in line]
        self.assertEqual(len(matches), 1)
        command = matches[0]
        self.assertEqual(command[0], "su")
        self.assertEqual(command[-2:], ["wechat", "&"])
        return shlex.split(command[command.index("-c") + 1])

    def test_x_server_runs_as_gui_user_without_tcp_listener(self):
        command = self.command_for("Xvfb")
        self.assertIn("-nolisten", command)
        self.assertEqual(command[command.index("-nolisten") + 1], "tcp")
        self.assertIn("-dpi", command)
        self.assertIn("${WECHAT_SCREEN_WIDTH}x${WECHAT_SCREEN_HEIGHT}x${WECHAT_SCREEN_DEPTH}", command)

    def test_environment_defaults_are_shared_and_safe(self):
        source = ENTRYPOINT.read_text()
        self.assertIn('WECHAT_SCREEN_WIDTH=${WECHAT_SCREEN_WIDTH:-1280}', source)
        self.assertIn('WECHAT_SCREEN_HEIGHT=${WECHAT_SCREEN_HEIGHT:-800}', source)
        self.assertIn('WECHAT_SCREEN_DEPTH=${WECHAT_SCREEN_DEPTH:-24}', source)
        self.assertIn('WECHAT_SCREEN_DPI=${WECHAT_SCREEN_DPI:-100}', source)
        self.assertIn('WECHAT_LOCALE=${WECHAT_LOCALE:-C.UTF-8}', source)
        self.assertIn('TZ=${TZ:-UTC}', source)
        self.assertIn('WECHAT_SCREEN_DEPTH" in 8|16|24', source)
        self.assertNotIn('in 8|16|24|32', source)
        self.assertIn('4294967295', source)
        self.assertIn('export LANG="$WECHAT_LOCALE" LC_ALL="$WECHAT_LOCALE" LC_CTYPE="$WECHAT_LOCALE"', source)
        self.assertIn('TZ=${TZ:-UTC}', source)

    def test_vnc_runs_as_gui_user_and_remains_read_only(self):
        command = self.command_for("x11vnc")
        self.assertIn("-viewonly", command)
        self.assertEqual(command[command.index("-listen") + 1], "127.0.0.1")
        self.assertIn("-listenv6", command)
        self.assertEqual(command[command.index("-listenv6") + 1], "::1")

    def test_session_manager_uses_display_environment_and_loopback_vnc(self):
        manager = ENTRYPOINT.parents[1] / "packages" / "agent-server-rust" / "src" / "sessions" / "manager.rs"
        source = manager.read_text()
        self.assertIn('"-dpi"', source)
        self.assertIn('"-nolisten", "tcp"', source)
        self.assertIn('"-listenv6", "::1"', source)
        self.assertIn('"WECHAT_SCREEN_WIDTH"', source)
        self.assertIn('Invalid {name}: {v}', source)
        self.assertIn('None => Ok(24)', source)
        self.assertIn('matches!(v, "8" | "16" | "24")', source)
        self.assertIn('Some(v) => Err(format!("Invalid WECHAT_SCREEN_DEPTH: {v}"))', source)
        self.assertIn('validate_start_config()', source)
        self.assertIn('let config = validate_start_config()?;', source)
        self.assertIn('let _ = std::fs::remove_file', source)

    def test_locale_checks_do_not_strip_punctuation_and_tz_rejects_dot_segments(self):
        for path in (ENTRYPOINT, ENTRYPOINT.parent / "tools" / "launch-wechat"):
            source = path.read_text()
            self.assertNotIn("tr -cd '[:alnum:]\\n'", source)
            self.assertIn("WECHAT_LOCALE\" == C.UTF-8", source)
            self.assertIn('..', source)
        manager = ENTRYPOINT.parents[1] / "packages" / "agent-server-rust" / "src" / "sessions" / "manager.rs"
        self.assertIn('"C.UTF-8" => Some("C.utf8")', manager.read_text())
        self.assertIn('"C.UTF-8" => Some("C.utf8"), "C.utf8" => Some("C.UTF-8")', manager.read_text())

    def test_proxy_credentials_use_private_nonsticky_directory(self):
        source = ENTRYPOINT.read_text()
        self.assertNotIn("/tmp/redsocks.conf", source)
        self.assertIn("install -d -m 700 -o redsocks -g redsocks /run/agent-wechat-proxy", source)
        self.assertIn("umask 077; cat > /run/agent-wechat-proxy/redsocks.conf", source)

    def test_launch_wechat_passes_overrides_to_gui_process_and_rejects_shell_syntax(self):
        script = ENTRYPOINT.parent / "tools" / "launch-wechat"
        with tempfile.TemporaryDirectory() as tmp:
            capture = pathlib.Path(tmp) / "su-args"
            fake_su = pathlib.Path(tmp) / "su"
            fake_su.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$CAPTURE"\n')
            fake_su.chmod(0o755)
            env = {**__import__("os").environ, "PATH": f"{tmp}:{__import__('os').environ['PATH']}", "CAPTURE": str(capture),
                   "WECHAT_LOCALE": "C.UTF-8", "TZ": "Europe/Paris", "DISPLAY": ":99"}
            subprocess.run([str(script)], env=env, check=True)
            args = capture.read_text()
            self.assertIn("LANG='C.utf8' LC_ALL='C.utf8' LC_CTYPE='C.utf8' TZ='Europe/Paris'", args)
            env["WECHAT_LOCALE"] = "$(touch /tmp/injected)"
            rejected = subprocess.run([str(script)], env=env, capture_output=True)
            self.assertNotEqual(rejected.returncode, 0)
            env["WECHAT_LOCALE"] = "C.UTF--8"
            rejected = subprocess.run([str(script)], env=env, capture_output=True)
            self.assertNotEqual(rejected.returncode, 0)


if __name__ == "__main__":
    unittest.main()
