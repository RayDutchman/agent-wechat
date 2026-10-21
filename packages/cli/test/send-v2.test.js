import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import test from "node:test";
import { fileURLToPath } from "node:url";

const packageDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const cli = path.join(packageDir, "dist", "cli.js");

function runCli(args, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [cli, ...args], { env, cwd: packageDir });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk) => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    child.once("close", (code) => resolve({ code, stdout, stderr }));
  });
}

test("built CLI emits one V2 POST with all chosen inputs and reports ignored priority fields", async (t) => {
  const temp = await mkdtemp(path.join(os.tmpdir(), "wx-cli-v2-"));
  t.after(() => rm(temp, { recursive: true, force: true }));
  const imagePath = path.join(temp, "photo.png");
  const filePath = path.join(temp, "note.txt");
  await writeFile(imagePath, Buffer.from("image bytes"));
  await writeFile(filePath, Buffer.from("file bytes"));

  const captured = [];
  const server = createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    captured.push({ method: req.method, url: req.url, body: JSON.parse(Buffer.concat(chunks).toString()) });
    const body = captured.at(-1).body;
    const selected = body.file ? "file" : body.image ? "image" : "text";
    const ignored_fields = ["text", "image", "file"].filter((field) => body[field] !== undefined && field !== selected);
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({
      success: true, account_username: "account-a", chat_username: "recipient",
      selected_payload: selected, ignored_fields, commit_attempted: true, ui_confirmed: true,
      error_code: null, failure_stage: null,
      ...(selected === "file" ? { payload_sha256: body.file.sha256, payload_size: body.file.size } : {}),
    }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const env = { ...process.env, AGENT_WECHAT_URL: `http://127.0.0.1:${server.address().port}`, AGENT_WECHAT_TOKEN: "test-only" };
  const imageArg = ["--image", imagePath];
  const fileArg = ["--file", filePath];
  const combos = [
    [["--text", "hello"], "text", []],
    [imageArg, "image", []],
    [fileArg, "file", []],
    [["--text", "hello", ...imageArg], "image", ["text"]],
    [["--text", "hello", ...fileArg], "file", ["text"]],
    [[...imageArg, ...fileArg], "file", ["image"]],
    [["--text", "hello", ...imageArg, ...fileArg], "file", ["text", "image"]],
  ];

  for (const [args, selected, ignored] of combos) {
    const before = captured.length;
    const result = await runCli(["messages", "send", "recipient", ...args, "--json", "--expected-account-username", "account-a"], env);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(captured.length, before + 1, "each CLI invocation must issue exactly one request");
    const request = captured.at(-1);
    assert.equal(request.method, "POST");
    assert.equal(request.url, "/api/v2/messages/send");
    assert.equal(request.body.chat_username, "recipient");
    assert.equal(request.body.expected_account_username, "account-a");
    assert.equal(JSON.parse(result.stdout).selected_payload, selected);
    assert.deepEqual(JSON.parse(result.stdout).ignored_fields, ignored);
    assert.deepEqual(result.stderr.trim().split("\n").filter(Boolean), ignored.map((field) => `Ignored send field: ${field}`));
  }
});

test("built CLI preserves a complete non-2xx preflight envelope in JSON and keeps network failures unknown", async (t) => {
  const preflight = {
    success: false, account_username: null, chat_username: "recipient",
    selected_payload: "text", ignored_fields: [], commit_attempted: false, ui_confirmed: false,
    error_code: "account_mismatch", failure_stage: "preflight",
  };
  const calls = [];
  let responseMode = { status: 409, body: preflight };
  const server = createServer(async (req, res) => {
    for await (const _chunk of req) { /* consume request */ }
    calls.push(req.url);
    if (responseMode === "network") {
      req.socket.destroy();
    } else {
      res.writeHead(responseMode.status, { "content-type": "application/json" });
      res.end(JSON.stringify(responseMode.body));
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const env = { ...process.env, AGENT_WECHAT_URL: `http://127.0.0.1:${server.address().port}`, AGENT_WECHAT_TOKEN: "test-only" };

  const known = await runCli(["messages", "send", "recipient", "--text", "hello", "--json"], env);
  assert.equal(known.code, 1);
  assert.deepEqual(JSON.parse(known.stdout), preflight);
  assert.match(known.stderr, /account_mismatch/);
  assert.equal(calls.length, 1);

  const mismatches = [
    { status: 401, body: preflight },
    { status: 500, body: preflight },
    { status: 500, body: { ...preflight, error_code: "execution_failed", failure_stage: "prepare" } },
  ];
  for (const response of mismatches) {
    responseMode = response;
    const unknownEnvelope = await runCli(["messages", "send", "recipient", "--text", "hello", "--json"], env);
    assert.equal(unknownEnvelope.code, 1);
    assert.equal(unknownEnvelope.stdout, "", `HTTP ${response.status} must not become known send JSON`);
    assert.match(unknownEnvelope.stderr, /Fatal error/);
  }

  const storageFailure = { ...preflight, error_code: "temp_storage_failed" };
  responseMode = { status: 500, body: storageFailure };
  const knownStorageFailure = await runCli(["messages", "send", "recipient", "--text", "hello", "--json"], env);
  assert.equal(knownStorageFailure.code, 1);
  assert.deepEqual(JSON.parse(knownStorageFailure.stdout), storageFailure);
  assert.match(knownStorageFailure.stderr, /temp_storage_failed/);

  responseMode = "network";
  const unknown = await runCli(["messages", "send", "recipient", "--text", "hello", "--json"], env);
  assert.equal(unknown.code, 1);
  assert.equal(unknown.stdout, "", "network failure must not be turned into a fabricated result");
  assert.match(unknown.stderr, /Fatal error/);
  assert.equal(calls.length, 6, "one POST per invocation; no retry");
});

test("built CLI does not emit JSON for a file preflight envelope missing required metadata", async (t) => {
  const temp = await mkdtemp(path.join(os.tmpdir(), "wx-cli-file-envelope-"));
  t.after(() => rm(temp, { recursive: true, force: true }));
  const filePath = path.join(temp, "upload.bin");
  await writeFile(filePath, Buffer.from("payload"));
  const invalidEnvelope = {
    success: false, account_username: null, chat_username: "recipient",
    selected_payload: "file", ignored_fields: [], commit_attempted: false, ui_confirmed: false,
    error_code: "payload_mismatch", failure_stage: "preflight",
  };
  let requests = 0;
  const server = createServer(async (req, res) => {
    for await (const _chunk of req) { /* consume request */ }
    requests++;
    res.writeHead(400, { "content-type": "application/json" });
    res.end(JSON.stringify(invalidEnvelope));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const env = { ...process.env, AGENT_WECHAT_URL: `http://127.0.0.1:${server.address().port}`, AGENT_WECHAT_TOKEN: "test-only" };
  const result = await runCli(["messages", "send", "recipient", "--file", filePath, "--json"], env);
  assert.equal(result.code, 1);
  assert.equal(result.stdout, "");
  assert.match(result.stderr, /Fatal error/);
  assert.equal(requests, 1, "the invalid envelope does not trigger a resend");
});

test("built CLI resolves local media through every page and releases samples on success and failure", async (t) => {
  const temp = await mkdtemp(path.join(os.tmpdir(), "wx-cli-media-v2-"));
  t.after(() => rm(temp, { recursive: true, force: true }));
  const calls = [];
  let mode = "success";
  const server = createServer(async (req, res) => {
    calls.push({ method: req.method, url: req.url });
    res.setHeader("content-type", "application/json");
    if (req.method === "DELETE") return res.end(JSON.stringify({ released: true }));
    if (req.url.startsWith("/api/v2/chats/room/messages")) {
      const url = new URL(req.url, "http://mock");
      if (mode === "paging-failure" && url.searchParams.has("cursor")) {
        res.statusCode = 503;
        return res.end(JSON.stringify({ error: "temporary" }));
      }
      const page = url.searchParams.has("cursor")
        ? { items: [{ local_id: "other", server_id: "42" }], next_cursor: null, release_token: "sample-1" }
        : mode === "ambiguous"
          ? { items: [{ local_id: "7", server_id: "42" }, { local_id: "7", server_id: "43" }], next_cursor: null, release_token: "sample-1" }
          : { items: [{ local_id: "7", server_id: mode === "missing-server-id" ? null : "42" }], next_cursor: "page-2", release_token: "sample-1" };
      return res.end(JSON.stringify(page));
    }
    if (req.url.startsWith("/api/v2/messages/42/media")) {
      return res.end(JSON.stringify({ type: "image", data: Buffer.from("pixels").toString("base64"), format: "image/png", filename: "image.png" }));
    }
    res.statusCode = 404;
    res.end(JSON.stringify({ error: "not_found" }));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const env = { ...process.env, AGENT_WECHAT_URL: `http://127.0.0.1:${server.address().port}`, AGENT_WECHAT_TOKEN: "test-only" };
  const output = path.join(temp, "result.png");

  mode = "success";
  calls.length = 0;
  let result = await runCli(["messages", "media", "room", "7", "--output", output], env);
  assert.equal(result.code, 0, result.stderr);
  assert.equal((await readFile(output)).toString(), "pixels");
  assert.equal(calls.filter((call) => call.method === "DELETE").length, 1);
  assert.ok(calls.some((call) => call.method === "GET" && call.url.startsWith("/api/v2/messages/42/media?")));

  for (const failingMode of ["ambiguous", "missing-server-id", "paging-failure"]) {
    mode = failingMode;
    calls.length = 0;
    result = await runCli(["messages", "media", "room", "7", "--output", output], env);
    assert.notEqual(result.code, 0, failingMode);
    assert.equal(calls.filter((call) => call.method === "DELETE").length, 1, `${failingMode} releases its frozen sample`);
    assert.ok(!calls.some((call) => call.url.includes("/api/v2/messages/42/media")), `${failingMode} never guesses/fetches media`);
  }
});

test("built CLI voice create, status, and cancel use the independent voice-job routes", async (t) => {
  const temp = await mkdtemp(path.join(os.tmpdir(), "wx-cli-voice-"));
  t.after(() => rm(temp, { recursive: true, force: true }));
  const audioPath = path.join(temp, "clip.wav");
  await writeFile(audioPath, Buffer.from("RIFF synthetic audio"));
  const calls = [];
  const job = { jobId: "job-1", chatId: "recipient", status: "queued", sourceDurationSeconds: 1, chunks: [] };
  const server = createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    calls.push({ method: req.method, url: req.url, headers: req.headers, body });
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(job));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const env = { ...process.env, AGENT_WECHAT_URL: `http://127.0.0.1:${server.address().port}`, AGENT_WECHAT_TOKEN: "test-only" };

  let result = await runCli(["messages", "send", "recipient", "--voice", audioPath, "--detach"], env);
  assert.equal(result.code, 0, result.stderr);
  assert.equal(calls[0].method, "POST");
  assert.equal(calls[0].url, "/api/messages/voice");
  assert.match(calls[0].headers["idempotency-key"], /^[0-9a-f]{32}$/);
  assert.match(calls[0].headers["content-type"], /^multipart\/form-data; boundary=/);
  assert.match(calls[0].body.toString(), /name="chatId"\r\n\r\nrecipient/);
  assert.match(calls[0].body.toString(), /name="audio"; filename="voice-audio"/);
  assert.match(result.stdout, /Voice job: job-1/);

  result = await runCli(["messages", "voice", "status", "job-1"], env);
  assert.equal(result.code, 0, result.stderr);
  assert.deepEqual(JSON.parse(result.stdout), job);
  assert.equal(calls[1].method, "GET");
  assert.equal(calls[1].url, "/api/messages/voice/job-1");

  result = await runCli(["messages", "voice", "cancel", "job-1"], env);
  assert.equal(result.code, 0, result.stderr);
  assert.match(result.stdout, /Cancellation requested for job-1/);
  assert.equal(calls[2].method, "POST");
  assert.equal(calls[2].url, "/api/messages/voice/job-1/cancel");
});
