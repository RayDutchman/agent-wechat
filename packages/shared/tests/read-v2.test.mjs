import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import { WeChatClient } from "../dist/client.js";
import { messageTypeDictionary } from "../src/message-types.generated.ts";
import { chatV2Schema, messageTypeDictionarySchema, messageV2Schema } from "../src/schemas/index.ts";

const originalFetch = globalThis.fetch;
afterEach(() => { globalThis.fetch = originalFetch; });

test("V2 read methods preserve snake_case envelopes and never fall back to V1", async () => {
  const calls = [];
  const responses = [
    { items: [{ username: "wxid_a" }], next_cursor: "opaque", release_token: "release" },
    { items: [{ username: "wxid_b" }], next_cursor: null, release_token: "release" },
    { username: "room@chatroom", display_name: "Room" },
    { items: [], next_cursor: null, release_token: null },
    { items: [{ username: "member", group_nickname: "Room name" }], next_cursor: null, release_token: null },
    { released: true },
  ];
  globalThis.fetch = async (url, init) => {
    calls.push([String(url), init?.method ?? "GET"]);
    return new Response(JSON.stringify(responses.shift()), { status: 200, headers: { "content-type": "application/json" } });
  };
  const client = new WeChatClient({ baseUrl: "http://example.test", token: "t" });
  const first = await client.listContactsV2({ q: "wxid a", kind: "group", limit: 2, expected_account_username: "wxid_self" });
  const second = await client.listContactsV2({ cursor: first.next_cursor });
  const chat = await client.getChatV2("room@chatroom");
  const chats = await client.listChatsV2({ q: "Room", limit: 5 });
  const members = await client.listGroupMembersV2("room@chatroom", { cursor: "member-cursor" });
  const released = await client.releaseReadSnapshotV2(first.release_token);
  assert.equal(first.next_cursor, "opaque");
  assert.equal(second.next_cursor, null);
  assert.equal(chat.display_name, "Room");
  assert.equal(chats.next_cursor, null);
  assert.equal(members.items[0].group_nickname, "Room name");
  assert.deepEqual(released, { released: true });
  assert.deepEqual(calls.map(([url, method]) => [new URL(url).pathname, new URL(url).search, method]), [
    ["/api/v2/contacts", "?q=wxid%20a&kind=group&limit=2&expected_account_username=wxid_self", "GET"],
    ["/api/v2/contacts", "?cursor=opaque", "GET"],
    ["/api/v2/chats/room%40chatroom", "", "GET"],
    ["/api/v2/chats", "?q=Room&limit=5", "GET"],
    ["/api/v2/chats/room%40chatroom/members", "?cursor=member-cursor", "GET"],
    ["/api/v2/read-snapshots/release", "", "DELETE"],
  ]);
});

test("V2 methods surface server errors without invoking legacy endpoints", async () => {
  const calls = [];
  globalThis.fetch = async (url) => {
    calls.push(String(url));
    return new Response('{"error":"invalid_parameter"}', { status: 400, statusText: "Bad Request" });
  };
  const client = new WeChatClient({ baseUrl: "http://example.test" });
  await assert.rejects(client.listContactsV2({ kind: "not-a-kind" }), /400 Bad Request/);
  assert.deepEqual(calls.map((url) => new URL(url).pathname), ["/api/v2/contacts"]);
});

test("message V2 client preserves the string identity and snake_case page contract", async () => {
  let request;
  globalThis.fetch = async (url) => {
    request = String(url);
    return new Response(JSON.stringify({
      items: [{ server_id: "9168265261376983777", local_id: "9", raw_type: "25769803825", type: 49, sub_type: 6 }],
      next_cursor: null, release_token: null,
    }), { status: 200, headers: { "content-type": "application/json" } });
  };
  const client = new WeChatClient({ baseUrl: "http://example.test" });
  const page = await client.listMessagesV2("room@chatroom", {
    cursor: "opaque", order: "desc", include: [{ type: 49, sub_type: 6 }], exclude: [],
  });
  assert.equal(page.items[0].server_id, "9168265261376983777");
  assert.equal(new URL(request).pathname, "/api/v2/chats/room%40chatroom/messages");
  assert.equal(new URL(request).search, "?cursor=opaque&order=desc&include=%5B%7B%22type%22%3A49%2C%22sub_type%22%3A6%7D%5D&exclude=%5B%5D");
});

test("native message client routes keep server IDs textual and raw voice bytes unparsed", async () => {
  const calls = [];
  globalThis.fetch = async (url) => {
    const text = String(url);
    calls.push(text);
    if (new URL(text).pathname.endsWith("/voice")) return new Response(new Uint8Array([0x02, 0x23]), { status: 200, headers: { "content-type": "audio/silk" } });
    if (new URL(text).pathname.endsWith("/media")) return new Response(JSON.stringify({ type: "pending", format: "", filename: "" }), { status: 200 });
    return new Response(JSON.stringify({ server_id: "9168265261376983777" }), { status: 200 });
  };
  const client = new WeChatClient({ baseUrl: "http://example.test" });
  const message = await client.getMessageV2("9168265261376983777", { chat_username: "room@chatroom" });
  const media = await client.getMessageMediaV2("9168265261376983777", { quality: "best" });
  const voice = await client.getMessageVoiceV2("9168265261376983777");
  assert.equal(message.server_id, "9168265261376983777");
  assert.equal(media.type, "pending");
  assert.deepEqual([...voice], [0x02, 0x23]);
  assert.deepEqual(calls.map((url) => new URL(url).pathname), [
    "/api/v2/messages/9168265261376983777",
    "/api/v2/messages/9168265261376983777/media",
    "/api/v2/messages/9168265261376983777/voice",
  ]);
});

test("sendMessageV2 validates requests and complete responses against the exported schemas", async () => {
  let fetchCalls = 0;
  globalThis.fetch = async () => {
    fetchCalls++;
    return new Response(JSON.stringify({
      success: true, account_username: "account-a", chat_username: "recipient",
      selected_payload: "text", ignored_fields: [], commit_attempted: true, ui_confirmed: true,
      error_code: null, failure_stage: null,
    }), { status: 200, headers: { "content-type": "application/json" } });
  };
  const client = new WeChatClient({ baseUrl: "http://example.test" });
  await assert.rejects(client.sendMessageV2({ chat_username: "recipient" }), (error) => error.name === "ZodError");
  await assert.rejects(client.sendMessageV2({ chat_username: "recipient", image: { data: "aW1hZ2U=", mime_type: "image/bmp" } }), (error) => error.name === "ZodError");
  await assert.rejects(client.sendMessageV2({ chat_username: "recipient", file: { filename: "../unsafe", data: "ZmlsZQ==", sha256: "0".repeat(64), size: 4 } }), (error) => error.name === "ZodError");
  await assert.rejects(client.sendMessageV2({ conversation_username: "recipient", text: "hello" }), (error) => error.name === "ZodError");
  assert.equal(fetchCalls, 0, "invalid request must be rejected before HTTP");
  const result = await client.sendMessageV2({ chat_username: "recipient", text: "hello" });
  assert.equal(result.ui_confirmed, true);
  assert.equal(fetchCalls, 1);

  globalThis.fetch = async () => new Response(JSON.stringify({
    success: true, account_username: "account-a", chat_username: "recipient",
    selected_payload: "text", ignored_fields: [], commit_attempted: false, ui_confirmed: true,
    error_code: null, failure_stage: null,
  }), { status: 200, headers: { "content-type": "application/json" } });
  await assert.rejects(client.sendMessageV2({ chat_username: "recipient", text: "hello" }));
});

test("sendMessageV2 exposes a schema-valid non-2xx preflight envelope without changing generic errors", async () => {
  const preflight = {
    success: false, account_username: null, chat_username: "recipient",
    selected_payload: "text", ignored_fields: [], commit_attempted: false, ui_confirmed: false,
    error_code: "account_mismatch", failure_stage: "preflight",
  };
  globalThis.fetch = async () => new Response(JSON.stringify(preflight), {
    status: 409, headers: { "content-type": "application/json" },
  });
  const client = new WeChatClient({ baseUrl: "http://example.test" });
  await assert.rejects(client.sendMessageV2({ chat_username: "recipient", text: "hello" }), (error) => {
    assert.equal(error.name, "SendMessageV2Error");
    assert.deepEqual(error.result, preflight);
    assert.equal(error.status, 409);
    return true;
  });

  globalThis.fetch = async () => new Response(JSON.stringify({ error: "unauthorized" }), {
    status: 401, headers: { "content-type": "application/json" },
  });
  await assert.rejects(client.sendMessageV2({ chat_username: "recipient", text: "hello" }), (error) => {
    assert.equal(error.name, "Error");
    assert.equal("result" in error, false);
    return true;
  });

  for (const body of ["not-json", JSON.stringify({ error: "payload_too_large", code: "payload_too_large" })]) {
    globalThis.fetch = async () => new Response(body, { status: 413, headers: { "content-type": "application/json" } });
    await assert.rejects(client.sendMessageV2({ chat_username: "recipient", text: "hello" }), (error) => {
      assert.equal(error.name, "Error");
      assert.equal("result" in error, false);
      return true;
    });
  }

  globalThis.fetch = async () => { throw new TypeError("network unavailable"); };
  await assert.rejects(client.sendMessageV2({ chat_username: "recipient", text: "hello" }), /network unavailable/);
});

test("sendMessageV2 only types non-2xx envelopes allowed by the §11.3 status/code/preflight mapping", async () => {
  const base = {
    success: false, account_username: null, chat_username: "recipient",
    selected_payload: "text", ignored_fields: [], commit_attempted: false, ui_confirmed: false,
    error_code: "account_mismatch", failure_stage: "preflight",
  };
  const cases = [
    { status: 401, body: base, typed: false },
    { status: 500, body: base, typed: false },
    { status: 500, body: { ...base, error_code: "execution_failed", failure_stage: "prepare" }, typed: false },
    { status: 500, body: { ...base, error_code: "temp_storage_failed" }, typed: true },
    { status: 409, body: base, typed: true },
  ];
  const client = new WeChatClient({ baseUrl: "http://example.test" });
  for (const item of cases) {
    globalThis.fetch = async () => new Response(JSON.stringify(item.body), {
      status: item.status, headers: { "content-type": "application/json" },
    });
    await assert.rejects(client.sendMessageV2({ chat_username: "recipient", text: "hello" }), (error) => {
      assert.equal(error instanceof Error && error.name === "SendMessageV2Error", item.typed,
        `HTTP ${item.status} ${item.body.error_code}/${item.body.failure_stage}`);
      if (item.typed) assert.deepEqual(error.result, item.body);
      else assert.equal("result" in error, false);
      return true;
    });
  }
});

test("send result schema requires paired valid file metadata but preserves facts verified before later preflight failures", async () => {
  const fileResult = {
    success: false, account_username: null, chat_username: "recipient",
    selected_payload: "file", ignored_fields: [], commit_attempted: false, ui_confirmed: false,
    error_code: "payload_mismatch", failure_stage: "preflight",
  };
  const verified = {
    ...fileResult,
    error_code: "account_mismatch",
    payload_sha256: "a".repeat(64), payload_size: 23,
  };
  const invalid = [
    fileResult,
    { ...fileResult, payload_sha256: "a".repeat(64) },
    { ...fileResult, payload_size: 23 },
    { ...fileResult, payload_sha256: "not-a-sha", payload_size: 23 },
    { ...fileResult, payload_sha256: null, payload_size: 23 },
    { ...fileResult, selected_payload: "text", payload_sha256: null, payload_size: null },
  ];
  const client = new WeChatClient({ baseUrl: "http://example.test" });
  for (const body of invalid) {
    globalThis.fetch = async () => new Response(JSON.stringify(body), { status: 400, headers: { "content-type": "application/json" } });
    await assert.rejects(client.sendMessageV2({ chat_username: "recipient", file: {
      filename: "file.bin", data: "ZmlsZQ==", sha256: "a".repeat(64), size: 4,
    } }), (error) => {
      assert.equal(error.name, "Error");
      assert.equal("result" in error, false);
      return true;
    });
  }

  for (const body of [
    { ...fileResult, payload_sha256: null, payload_size: null },
    verified,
    { ...verified, error_code: "temp_storage_failed" },
  ]) {
    const status = body.error_code === "account_mismatch" ? 409
      : body.error_code === "temp_storage_failed" ? 500 : 400;
    globalThis.fetch = async () => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
    await assert.rejects(client.sendMessageV2({ chat_username: "recipient", file: {
      filename: "file.bin", data: "ZmlsZQ==", sha256: "a".repeat(64), size: 4,
    } }), (error) => {
      assert.equal(error.name, "SendMessageV2Error");
      assert.deepEqual(error.result, body);
      return true;
    });
  }
});

test("generated type dictionary validates and keeps the three numeric namespaces separate", () => {
  assert.equal(messageTypeDictionarySchema.parse(messageTypeDictionary).appmsg[3].code, 6);
  assert.equal(messageTypeDictionary.main.find((entry) => entry.code === 49).key, "app_message");
  assert.equal(messageTypeDictionary.appmsg.find((entry) => entry.code === 6).key, "file");
  assert.equal(messageTypeDictionary.forwarded_inner.find((entry) => entry.code === 1).basis, "local_observed");
});

test("MessageV2 schema preserves reference and forwarded-record identity fields", () => {
  const message = messageV2Schema.parse({
    server_id: "9168265261376983777", local_id: "9", raw_type: "25769803825", type: 49, sub_type: 6,
    xml_sub_type: 19, chat_username: "room@chatroom", sender_username: "outer-forwarder",
    sender_display_name: "Outer", content: "existing title", timestamp: "2026-01-01T00:00:00Z",
    is_self: null, is_mentioned: null, raw_content: "3c6d7367", raw_content_compression: false,
    reference: { server_id: "1234567890123456789", chat_username: "room@chatroom", sender_username: "quoted-author", display_name: "777", content: "quoted" },
    forwarded_record: { parse_status: "complete", items: [{
      datatype: 987, dataid: "9007199254740993", datadesc: "inner text", sourcename: "Inner author",
      sourceheadurl: null, sourcetime: "1700000000123456789", src_msg_create_time: "1700000000.125",
      from_new_msg_id: "1234567890123456789", dataitem_source_hash_username: "hash-only",
    }] },
  });
  assert.equal(message.xml_sub_type, 19);
  assert.equal(message.reference.sender_username, "quoted-author");
  assert.throws(() => messageV2Schema.parse({ ...message, reference: { ...message.reference, chat_username: undefined, conversation_username: "room@chatroom" } }));
  assert.equal(message.reference.display_name, "777");
  assert.equal(message.forwarded_record.items[0].dataid, "9007199254740993");
  assert.equal(message.forwarded_record.items[0].dataitem_source_hash_username, "hash-only");
  assert.equal("username" in message.forwarded_record.items[0], false);
});

test("V2 schemas preserve nullable source fields and reject legacy sender names", () => {
  const chat = chatV2Schema.parse({
    username: "chat", display_name: "Chat", remark: "", nick_name: "", unread_count: null,
    is_group: false, last_message_preview: null, last_msg_sender: "room@chatroom;wxid_a",
    last_sender_display_name: "A; B", last_activity_at: null, last_msg_local_id: null,
  });
  assert.equal(chat.last_msg_sender, "room@chatroom;wxid_a");
  assert.equal("last_message_sender_username" in chat, false);
  assert.equal("last_message_sender_display_name" in chat, false);
  assert.equal(chatV2Schema.parse({ ...chat, unread_count: 0 }).unread_count, 0);
});

test("MessageV2 schema preserves valid timestamps and accepts null", () => {
  const base = {
    server_id: null, local_id: null, raw_type: "1", type: 1, sub_type: 0,
    chat_username: "chat", sender_username: null, sender_display_name: null,
    content: "x", is_self: null, is_mentioned: null, raw_content: null,
    raw_content_compression: false, xml_sub_type: null, reference: null, forwarded_record: null,
  };
  const valid = "2026-01-01T00:00:00Z";
  assert.equal(messageV2Schema.parse({ ...base, timestamp: valid }).timestamp, valid);
  assert.equal(messageV2Schema.parse({ ...base, timestamp: null }).timestamp, null);
});

test("MessageV2 schema requires and preserves the source compression boolean", () => {
  const base = {
    server_id: "9168265261376983777", local_id: "9", raw_type: "25769803825", type: 49, sub_type: 6,
    xml_sub_type: 19, chat_username: "room@chatroom", sender_username: null, sender_display_name: null,
    content: "title", timestamp: "now", is_self: null, is_mentioned: null, raw_content: "3c6d7367",
    reference: null, forwarded_record: null,
  };
  assert.equal(messageV2Schema.parse({ ...base, raw_content_compression: false }).raw_content_compression, false);
  assert.equal(messageV2Schema.parse({ ...base, raw_content_compression: true }).raw_content_compression, true);
  assert.throws(() => messageV2Schema.parse(base));
  assert.throws(() => messageV2Schema.parse({ ...base, raw_content_compression: null }));
  assert.throws(() => messageV2Schema.parse({ ...base, raw_content_compression: "true" }));
});
