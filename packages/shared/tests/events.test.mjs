import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import { WeChatClient } from "../dist/client.js";

class FakeWebSocket {
  static instances = [];
  static active = 0;
  static maximumActive = 0;
  static autoOpen = true;

  constructor(url) {
    this.url = String(url);
    this.readyState = 0;
    this.listeners = new Map();
    FakeWebSocket.instances.push(this);
    if (FakeWebSocket.autoOpen) queueMicrotask(() => this.open());
  }

  addEventListener(type, callback) {
    const callbacks = this.listeners.get(type) ?? new Set();
    callbacks.add(callback);
    this.listeners.set(type, callbacks);
  }

  removeEventListener(type, callback) {
    this.listeners.get(type)?.delete(callback);
  }

  open() {
    if (this.readyState !== 0) return;
    this.readyState = 1;
    FakeWebSocket.active++;
    FakeWebSocket.maximumActive = Math.max(FakeWebSocket.maximumActive, FakeWebSocket.active);
    this.emit("open", {});
  }

  message(data) {
    this.emit("message", { data });
  }

  drop() {
    if (this.readyState === 3) return;
    if (this.readyState === 1) FakeWebSocket.active--;
    this.readyState = 3;
    this.emit("close", {});
  }

  close() {
    this.drop();
  }

  emit(type, event) {
    for (const callback of this.listeners.get(type) ?? []) callback(event);
  }
}

const originalWebSocket = globalThis.WebSocket;
const originalFetch = globalThis.fetch;
let fetchCalls = 0;
function installFakeWebSocket() {
  FakeWebSocket.instances = [];
  FakeWebSocket.active = 0;
  FakeWebSocket.maximumActive = 0;
  FakeWebSocket.autoOpen = true;
  fetchCalls = 0;
  globalThis.fetch = async () => {
    fetchCalls++;
    throw new Error("subscribeChanges must not make HTTP reads or sends");
  };
  globalThis.WebSocket = FakeWebSocket;
}

afterEach(() => {
  globalThis.WebSocket = originalWebSocket;
  globalThis.fetch = originalFetch;
});

function waitFor(predicate, timeoutMs = 1000) {
  const started = Date.now();
  return new Promise((resolve, reject) => {
    const check = () => {
      if (predicate()) return resolve();
      if (Date.now() - started > timeoutMs) return reject(new Error("timed out waiting for fake WebSocket"));
      setTimeout(check, 1);
    };
    check();
  });
}

test("subscribeChanges uses authenticated wss URL and yields only the exact data_changed contract", async () => {
  installFakeWebSocket();
  const client = new WeChatClient({ baseUrl: "https://example.test", token: "secret token" });
  const controller = new AbortController();
  const states = [];
  const iterator = client.subscribeChanges({ signal: controller.signal, onStatus: (state) => states.push(state) });
  const next = iterator.next();
  await waitFor(() => FakeWebSocket.instances.length === 1 && FakeWebSocket.instances[0].readyState === 1);
  const socket = FakeWebSocket.instances[0];
  const url = new URL(socket.url);
  assert.equal(url.protocol, "wss:");
  assert.equal(url.pathname, "/api/ws/events");
  assert.equal(url.searchParams.get("token"), "secret token");
  socket.message("not json");
  socket.message("{\"type\":\"data_changed\",\"path\":\"must-not-pass\"}");
  socket.message("{\"type\":\"data_changed\"}");
  assert.deepEqual((await next).value, { type: "data_changed" });
  assert.equal(fetchCalls, 0);
  assert.deepEqual(states.slice(0, 2), ["connecting", "connected"]);
  controller.abort();
  assert.equal((await iterator.next()).done, true);
  assert.equal(socket.readyState, 3);
});

test("an already-aborted subscription opens no socket", async () => {
  installFakeWebSocket();
  const controller = new AbortController();
  controller.abort();
  const iterator = new WeChatClient({ baseUrl: "http://example.test" }).subscribeChanges({ signal: controller.signal });
  assert.equal((await iterator.next()).done, true);
  assert.equal(FakeWebSocket.instances.length, 0);
});

test("http base URLs map to ws and include the existing token auth", async () => {
  installFakeWebSocket();
  const controller = new AbortController();
  const iterator = new WeChatClient({ baseUrl: "http://example.test", token: "token" }).subscribeChanges({ signal: controller.signal });
  const pending = iterator.next();
  await waitFor(() => FakeWebSocket.instances.length === 1 && FakeWebSocket.instances[0].readyState === 1);
  const url = new URL(FakeWebSocket.instances[0].url);
  assert.equal(url.protocol, "ws:");
  assert.equal(url.pathname, "/api/ws/events");
  assert.equal(url.searchParams.get("token"), "token");
  controller.abort();
  assert.equal((await pending).done, true);
});

test("cancellation while connecting closes the pending socket", async () => {
  installFakeWebSocket();
  FakeWebSocket.autoOpen = false;
  const controller = new AbortController();
  const iterator = new WeChatClient({ baseUrl: "http://example.test" }).subscribeChanges({ signal: controller.signal });
  const pending = iterator.next();
  await waitFor(() => FakeWebSocket.instances.length === 1);
  controller.abort();
  assert.equal((await pending).done, true);
  assert.equal(FakeWebSocket.instances[0].readyState, 3);
});

test("disconnect reconnects serially and cancellation interrupts reconnect backoff", async () => {
  installFakeWebSocket();
  const controller = new AbortController();
  const states = [];
  const iterator = new WeChatClient({ baseUrl: "http://example.test" }).subscribeChanges({
    signal: controller.signal,
    onStatus: (state) => states.push(state),
  });
  const pending = iterator.next();
  await waitFor(() => FakeWebSocket.instances.length === 1 && FakeWebSocket.instances[0].readyState === 1);
  FakeWebSocket.instances[0].drop();
  await waitFor(() => states.includes("reconnecting"));
  controller.abort();
  assert.equal((await pending).done, true);
  assert.equal(FakeWebSocket.instances.length, 1);
  assert.equal(FakeWebSocket.maximumActive, 1);
});

test("reconnection never overlaps the previous socket", async () => {
  installFakeWebSocket();
  const controller = new AbortController();
  const iterator = new WeChatClient({ baseUrl: "http://example.test" }).subscribeChanges({ signal: controller.signal });
  const pending = iterator.next();
  await waitFor(() => FakeWebSocket.instances.length === 1 && FakeWebSocket.instances[0].readyState === 1);
  FakeWebSocket.instances[0].drop();
  await waitFor(() => FakeWebSocket.instances.length === 2, 2000);
  assert.equal(FakeWebSocket.maximumActive, 1);
  controller.abort();
  assert.equal((await pending).done, true);
});
