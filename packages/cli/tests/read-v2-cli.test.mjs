import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const cliPath = fileURLToPath(new URL("../dist/cli.js", import.meta.url));

test("wx chat output preserves null unread JSON and displays it as an em dash", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  const chat = {
    username: "room@chatroom", display_name: "Room", remark: "", nick_name: "Room", unread_count: null,
    is_group: true, last_message_preview: "hello", last_msg_sender: "_$_CUSTOM_USERNAME_PREFIX_$_1234567890123456789:gh_fixture",
    last_sender_display_name: "A; B", last_activity_at: null, last_msg_local_id: null,
  };
  const server = createServer((request, response) => {
    response.setHeader("content-type", "application/json");
    response.end(request.url.includes("/chats?") ? JSON.stringify({ items: [chat], next_cursor: null, release_token: null }) : JSON.stringify(chat));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    const json = await runCli(`http://127.0.0.1:${address.port}`, ["chats", "get", chat.username, "--json"]);
    assert.equal(json.code, 0, json.stderr);
    assert.deepEqual(JSON.parse(json.stdout), chat);
    const readable = await runCli(`http://127.0.0.1:${address.port}`, ["chats", "get", chat.username]);
    assert.equal(readable.code, 0, readable.stderr);
    assert.match(readable.stdout, /Unread:\s+—/);
    assert.match(readable.stdout, /A; B: hello/);
    assert.doesNotMatch(readable.stdout, /undefined/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

function runCli(url, args, onChild = () => {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [cliPath, ...args], {
      env: { ...process.env, AGENT_WECHAT_URL: url, AGENT_WECHAT_TOKEN: "test-token" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    onChild(child);
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8").on("data", (chunk) => { stdout += chunk; });
    child.stderr.setEncoding("utf8").on("data", (chunk) => { stderr += chunk; });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}

test("wx contacts list --all emits one V2 envelope per page and finally releases", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  const calls = [];
  const server = createServer((request, response) => {
    const url = new URL(request.url, "http://localhost");
    calls.push([request.method, url.pathname, url.search]);
    response.setHeader("content-type", "application/json");
    if (request.method === "DELETE") response.end(JSON.stringify({ released: true }));
    else if (url.searchParams.has("cursor")) response.end(JSON.stringify({ items: [{ username: "b" }], next_cursor: null, release_token: "token-1" }));
    else response.end(JSON.stringify({ items: [{ username: "a" }], next_cursor: "cursor-1", release_token: "token-1" }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    const result = await runCli(`http://127.0.0.1:${address.port}`, ["contacts", "list", "--limit", "1", "--all", "--json"]);
    assert.equal(result.code, 0, result.stderr);
    assert.deepEqual(result.stdout.trim().split("\n").map((line) => JSON.parse(line).items[0].username), ["a", "b"]);
    assert.deepEqual(calls.map(([method, path]) => [method, path]), [
      ["GET", "/api/v2/contacts"], ["GET", "/api/v2/contacts"], ["DELETE", "/api/v2/read-snapshots/token-1"],
    ]);
    assert.match(calls[0][2], /limit=1/);
    assert.match(calls[1][2], /cursor=cursor-1/);
    assert.doesNotMatch(calls[1][2], /limit=/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("wx --all releases a retained page after a continuation error", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  const calls = [];
  const server = createServer((request, response) => {
    const url = new URL(request.url, "http://localhost");
    calls.push([request.method, url.pathname, url.search]);
    response.setHeader("content-type", "application/json");
    if (request.method === "DELETE") response.end(JSON.stringify({ released: true }));
    else if (url.searchParams.has("cursor")) { response.statusCode = 410; response.end(JSON.stringify({ error: "expired" })); }
    else response.end(JSON.stringify({ items: [], next_cursor: "cursor-2", release_token: "token-2" }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    const result = await runCli(`http://127.0.0.1:${address.port}`, ["chats", "list", "--limit", "1", "--all", "--json"]);
    assert.notEqual(result.code, 0);
    assert.equal(calls.at(-1)[0], "DELETE");
    assert.equal(calls.at(-1)[1], "/api/v2/read-snapshots/token-2");
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("wx --all releases a retained sample when SIGINT arrives during a page request", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  const calls = [];
  let child;
  const server = createServer((request, response) => {
    const url = new URL(request.url, "http://localhost");
    calls.push([request.method, url.pathname]);
    response.setHeader("content-type", "application/json");
    if (request.method === "DELETE") response.end(JSON.stringify({ released: true }));
    else {
      child.kill("SIGINT");
      setTimeout(() => response.end(JSON.stringify({ items: [], next_cursor: "cursor-3", release_token: "token-3" })), 25);
    }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    const result = await runCli(`http://127.0.0.1:${address.port}`, ["contacts", "list", "--limit", "1", "--all", "--json"], (process) => { child = process; });
    assert.notEqual(result.code, 0);
    assert.deepEqual(calls, [["GET", "/api/v2/contacts"], ["DELETE", "/api/v2/read-snapshots/token-3"]]);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("wx contacts list forwards expected account username unchanged", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  let requestUrl;
  const server = createServer((request, response) => {
    requestUrl = new URL(request.url, "http://localhost");
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({ items: [], next_cursor: null, release_token: null }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    const result = await runCli(`http://127.0.0.1:${address.port}`, ["contacts", "list", "--expected-account-username", "foo", "--json"]);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(requestUrl.searchParams.get("expected_account_username"), "foo");
    assert.equal(requestUrl.searchParams.has("expected_account_dir"), false);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test("wx messages list --all forwards configured type filters once and releases the frozen sample", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), "wx-message-filter-"));
  const configPath = path.join(dir, "filters.json");
  writeFileSync(configPath, JSON.stringify({ include: [{ type: 49, sub_type: 6 }], exclude: [] }));
  const calls = [];
  const server = createServer((request, response) => {
    const url = new URL(request.url, "http://localhost");
    calls.push([request.method, url.pathname, url.searchParams]);
    response.setHeader("content-type", "application/json");
    if (request.method === "DELETE") response.end(JSON.stringify({ released: true }));
    else if (url.searchParams.has("cursor")) response.end(JSON.stringify({ items: [], next_cursor: null, release_token: "msg-token" }));
    else response.end(JSON.stringify({ items: [], next_cursor: "msg-cursor", release_token: "msg-token" }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    const result = await runCli(`http://127.0.0.1:${address.port}`, ["messages", "list", "room@chatroom", "--limit", "1", "--order", "asc", "--config", configPath, "--expected-account-username", "me", "--all", "--json"]);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(calls[0][1], "/api/v2/chats/room%40chatroom/messages");
    assert.equal(calls[0][2].get("include"), JSON.stringify([{ type: 49, sub_type: 6 }]));
    assert.equal(calls[0][2].get("exclude"), "[]");
    assert.equal(calls[0][2].get("order"), "asc");
    assert.equal(calls[0][2].get("expected_account_username"), "me");
    assert.deepEqual([...calls[1][2].keys()], ["cursor"]);
    assert.equal(calls.at(-1)[0], "DELETE");
    assert.equal(calls.at(-1)[1], "/api/v2/read-snapshots/msg-token");
  } finally {
    await new Promise((resolve) => server.close(resolve));
    rmSync(dir, { recursive: true, force: true });
  }
});

test("wx messages media uses server IDs directly and resolves local IDs through V2 frozen pages", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), "wx-native-media-"));
  const requests = [];
  const payload = Buffer.from("synthetic media bytes");
  const server = createServer((request, response) => {
    const url = new URL(request.url, "http://localhost");
    requests.push(url);
    response.setHeader("content-type", "application/json");
    if (url.pathname === "/api/v2/chats/room%40chatroom/messages") {
      response.end(JSON.stringify({
        items: [{ local_id: "7", server_id: "9168265261376983777" }],
        next_cursor: null, release_token: "local-id-sample",
      }));
    } else if (request.method === "DELETE") {
      response.end(JSON.stringify({ released: true }));
    } else {
      response.end(JSON.stringify({ type: "file", data: payload.toString("base64"), format: "txt", filename: "sample.txt", quality: null }));
    }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    const nativeOutput = path.join(dir, "native.bin");
    const native = await runCli(`http://127.0.0.1:${address.port}`, ["messages", "media", "room@chatroom", "--server-id", "9168265261376983777", "--full", "--output", nativeOutput]);
    assert.equal(native.code, 0, native.stderr);
    assert.equal(requests[0].pathname, "/api/v2/messages/9168265261376983777/media");
    assert.equal(requests[0].searchParams.get("chat_username"), "room@chatroom");
    assert.equal(requests[0].searchParams.get("quality"), "full");
    assert.deepEqual(readFileSync(nativeOutput), payload);

    const localOutput = path.join(dir, "local.bin");
    const local = await runCli(`http://127.0.0.1:${address.port}`, ["messages", "media", "room@chatroom", "7", "--output", localOutput]);
    assert.equal(local.code, 0, local.stderr);
    assert.deepEqual(requests.slice(1).map((url) => url.pathname), [
      "/api/v2/chats/room%40chatroom/messages",
      "/api/v2/read-snapshots/local-id-sample",
      "/api/v2/messages/9168265261376983777/media",
    ]);
    assert.deepEqual(readFileSync(localOutput), payload);
  } finally {
    await new Promise((resolve) => server.close(resolve));
    rmSync(dir, { recursive: true, force: true });
  }
});

test("wx messages list renders reference authors and forwarded-entry details without rewriting JSON", { skip: !existsSync(cliPath) && "Build CLI before running this integration test" }, async () => {
  const message = {
    server_id: "9168265261376983777", local_id: "9", raw_type: "25769803825", type: 49, sub_type: 6,
    xml_sub_type: 19, chat_username: "room@chatroom", sender_username: "outer-forwarder",
    sender_display_name: "Outer", content: "existing title", timestamp: "2026-01-01T00:00:00Z",
    is_self: null, is_mentioned: null, raw_content: "3c6d7367", raw_content_compression: true,
    reference: { server_id: "1234567890123456789", chat_username: "room@chatroom", sender_username: "quoted-author", display_name: "777", content: "quoted text" },
    forwarded_record: { parse_status: "complete", items: [{
      datatype: 987, dataid: "9007199254740993", datadesc: "forwarded text", sourcename: "Inner author",
      sourceheadurl: null, sourcetime: "1700000000123456789", src_msg_create_time: "1700000000.125",
      from_new_msg_id: "1234567890123456789", dataitem_source_hash_username: "hash-only",
    }] },
  };
  const missingTime = { ...message, server_id: "10", content: "no source time", timestamp: null, reference: null, forwarded_record: null };
  const server = createServer((_request, response) => {
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({ items: [missingTime, message], next_cursor: null, release_token: null }));
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    const json = await runCli(`http://127.0.0.1:${address.port}`, ["messages", "list", "room@chatroom", "--json"]);
    assert.equal(json.code, 0, json.stderr);
    assert.deepEqual(JSON.parse(json.stdout).items, [missingTime, message]);

    const readable = await runCli(`http://127.0.0.1:${address.port}`, ["messages", "list", "room@chatroom"]);
    assert.equal(readable.code, 0, readable.stderr);
    assert.match(readable.stdout, /引用.*777.*quoted-author.*quoted text/);
    assert.match(readable.stdout, /合并转发.*complete/);
    assert.match(readable.stdout, /987.*forwarded text.*Inner author.*hash-only/);
    assert.match(readable.stdout, /1700000000123456789.*1234567890123456789/);
    assert.match(readable.stdout, /—/);
    assert.doesNotMatch(readable.stdout, /1970/);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});
