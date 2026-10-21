import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const cliPath = fileURLToPath(new URL("../dist/cli.js", import.meta.url));

test("wx events watch --json emits only API event JSONL and exits on SIGINT", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), "agent-wechat-events-"));
  const preloadPath = path.join(tempDir, "fake-websocket.mjs");
  await writeFile(preloadPath, `
    globalThis.fetch = async () => { throw new Error("unexpected HTTP request"); };
    globalThis.WebSocket = class extends EventTarget {
      constructor(url) {
        super();
        this.url = String(url);
        queueMicrotask(() => this.dispatchEvent(new Event("open")));
        setTimeout(() => {
          this.dispatchEvent(new MessageEvent("message", { data: '{"type":"data_changed"}' }));
          setTimeout(() => process.kill(process.pid, "SIGINT"), 20);
        }, 20);
      }
      close() {}
    };
  `);

  try {
    const child = spawn(process.execPath, [
      "--import", preloadPath, cliPath, "events", "watch", "--json",
    ], {
      cwd: path.dirname(cliPath),
      env: {
        ...process.env,
        AGENT_WECHAT_URL: "https://events.test",
        AGENT_WECHAT_TOKEN: "test-token",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk) => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", (chunk) => { stderr += chunk; });
    const exit = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        child.kill("SIGKILL");
        reject(new Error("CLI did not exit after SIGINT"));
      }, 3000);
      child.once("error", reject);
      child.once("exit", (code, signal) => {
        clearTimeout(timeout);
        resolve({ code, signal });
      });
    });
    assert.deepEqual(exit, { code: 0, signal: null });
    assert.deepEqual(stdout.trim().split("\n").map((line) => JSON.parse(line)), [
      { type: "data_changed" },
    ]);
    assert.match(stderr, /connected/);
    assert.doesNotMatch(stderr, /test-token|data_changed/);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
});
