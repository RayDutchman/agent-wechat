import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const cliPath = process.env.WX_CLI_PATH ?? fileURLToPath(new URL("../dist/cli.js", import.meta.url));

test("configured wx lifecycle selects only the agent-wechat Compose service", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), "wx-container-lifecycle-"));
  const home = path.join(dir, "home");
  const bin = path.join(dir, "bin");
  const callsFile = path.join(dir, "docker-args.jsonl");
  const configDir = path.join(home, ".config", "agent-wechat");
  const configPath = path.join(configDir, "container.json");
  const compose = path.join(dir, "compose file.yml");
  const envFile = path.join(dir, "env file");
  mkdirSync(bin, { recursive: true });
  mkdirSync(configDir, { recursive: true });
  writeFileSync(compose, "services: {}\n");
  writeFileSync(envFile, "TAG=test\n");
  writeFileSync(configPath, JSON.stringify({ project: "agent-wechat-environment", files: [compose], env_file: envFile }));
  const docker = path.join(bin, "docker");
  writeFileSync(docker, `#!/usr/bin/env node
const args = process.argv.slice(2);
require('fs').appendFileSync(process.env.DOCKER_CALLS, JSON.stringify(args) + "\\n");
if (process.env.DOCKER_FAIL === '1') process.exit(1);
if (args[0] === 'compose' && args.includes('ps') && args.includes('agent-wechat') && process.env.DOCKER_WECHAT_RUNNING === '1') process.stdout.write('wechat-container\\n');
if (args[0] === 'compose' && args.includes('ps') && !args.includes('agent-wechat') && process.env.DOCKER_OLLAMA_RUNNING === '1') process.stdout.write('ollama-container\\n');
if (args[0] === 'ps' && (args.includes('-q') || args.includes('-aq')) && args.includes('name=^agent-wechat$')) process.stdout.write('wrong-fixed-name-container\\n');
`);
  chmodSync(docker, 0o755);

  let healthStatus = 200;
  const server = createServer((_request, response) => { response.writeHead(healthStatus); response.end("ok"); });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${server.address().port}`;
  const run = (args, extraEnv = {}) => new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [cliPath, ...args], {
      env: { ...process.env, HOME: home, PATH: `${bin}:${process.env.PATH}`, DOCKER_CALLS: callsFile, AGENT_WECHAT_URL: url, DOCKER_WECHAT_RUNNING: "1", DOCKER_OLLAMA_RUNNING: "1", ...extraEnv },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk) => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    child.once("exit", (code) => resolve({ code, stdout, stderr }));
  });
  try {
    const up = await run(["up"]);
    assert.equal(up.code, 0, up.stderr);
    const down = await run(["down"]);
    assert.equal(down.code, 0, down.stderr);
    const statusUp = await run(["status"]);
    assert.match(statusUp.stdout, /Container: up/);
    const logs = await run(["logs"]);
    assert.equal(logs.code, 0, logs.stderr);
    assert.deepEqual(readFileSync(callsFile, "utf8").trim().split("\n").map(JSON.parse), [
      ["compose", "--project-name", "agent-wechat-environment", "--env-file", envFile, "-f", compose, "up", "-d"],
      ["compose", "--project-name", "agent-wechat-environment", "--env-file", envFile, "-f", compose, "ps", "--status", "running", "--quiet", "agent-wechat"],
      ["compose", "--project-name", "agent-wechat-environment", "--env-file", envFile, "-f", compose, "stop"],
      ["compose", "--project-name", "agent-wechat-environment", "--env-file", envFile, "-f", compose, "ps", "--status", "running", "--quiet", "agent-wechat"],
      ["compose", "--project-name", "agent-wechat-environment", "--env-file", envFile, "-f", compose, "logs", "-f", "agent-wechat"],
    ]);
    const beforeDownState = readFileSync(callsFile, "utf8").trim().split("\n").length;
    const env = { ...process.env, HOME: home, PATH: `${bin}:${process.env.PATH}`, DOCKER_CALLS: callsFile, AGENT_WECHAT_URL: url, DOCKER_WECHAT_RUNNING: "0", DOCKER_OLLAMA_RUNNING: "1" };
    const composeOnlyOllama = await new Promise((resolve, reject) => {
      const child = spawn(process.execPath, [cliPath, "status"], { env, stdio: ["ignore", "pipe", "pipe"] });
      let stdout = ""; let stderr = "";
      child.stdout.setEncoding("utf8").on("data", (chunk) => { stdout += chunk; });
      child.stderr.setEncoding("utf8").on("data", (chunk) => { stderr += chunk; });
      child.once("error", reject); child.once("exit", (code) => resolve({ code, stdout, stderr }));
    });
    assert.match(composeOnlyOllama.stdout, /Container: down/);
    assert.equal(readFileSync(callsFile, "utf8").trim().split("\n").length, beforeDownState + 1);
    const dockerUnavailable = await run(["status"], { DOCKER_FAIL: "1" });
    assert.match(dockerUnavailable.stdout, /Container: unknown \(Docker unavailable\)/);
    writeFileSync(docker, `#!/usr/bin/env node\nconst args = process.argv.slice(2); require('fs').appendFileSync(process.env.DOCKER_CALLS, JSON.stringify(args) + "\\n");\n`);
    const exited = await run(["up"]);
    assert.notEqual(exited.code, 0);
    assert.match(exited.stderr, /process exited before the server became ready/);

    const callsBeforeInvalidConfig = readFileSync(callsFile, "utf8").trim().split("\n").length;
    writeFileSync(configPath, "{invalid json");
    const invalid = await run(["up"]);
    assert.notEqual(invalid.code, 0);
    assert.match(invalid.stderr, /Invalid container config/);
    assert.equal(readFileSync(callsFile, "utf8").trim().split("\n").length, callsBeforeInvalidConfig);
    const invalidStatus = await run(["status"]);
    assert.notEqual(invalidStatus.code, 0);
    assert.match(invalidStatus.stderr, /Invalid container config/);
    assert.equal(readFileSync(callsFile, "utf8").trim().split("\n").length, callsBeforeInvalidConfig);
    writeFileSync(configPath, JSON.stringify({ project: "agent-wechat-environment", files: [compose], env_file: envFile }));

    const proxy = await run(["up", "--proxy", "http://proxy.invalid:8080"]);
    assert.notEqual(proxy.code, 0);
    assert.match(proxy.stderr, /--proxy is not supported with container\.json/);
    assert.equal(readFileSync(callsFile, "utf8").trim().split("\n").length, callsBeforeInvalidConfig); // Invalid config and proxy rejection must not invoke Docker.

    writeFileSync(docker, `#!/usr/bin/env node\nconst args = process.argv.slice(2); require('fs').appendFileSync(process.env.DOCKER_CALLS, JSON.stringify(args) + "\\n"); if (args.includes('ps')) process.stdout.write('test-container\\n');\n`);
    healthStatus = 503;
    const timeout = await run(["up"]);
    assert.notEqual(timeout.code, 0);
    assert.match(timeout.stderr, /Server did not become ready in time/);

    unlinkSync(configPath);
    const standaloneStatus = await run(["status"]);
    assert.match(standaloneStatus.stdout, /Container: up/);
    assert.match(readFileSync(callsFile, "utf8").split("\n").slice(-2).join("\n"), /name=\^agent-wechat\$/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    rmSync(dir, { recursive: true, force: true });
  }
});
