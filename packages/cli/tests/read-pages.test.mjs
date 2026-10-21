import assert from "node:assert/strict";
import test from "node:test";
import { readAllPages } from "../src/lib/read-pages.ts";

test("readAllPages serializes cursor reads, treats only null as terminal, and releases in finally", async () => {
  const calls = [];
  const pages = [
    { items: [1], next_cursor: "c2", release_token: "r1" },
    { items: [], next_cursor: "c3", release_token: "r1" },
    { items: [3], next_cursor: null, release_token: "r1" },
  ];
  await readAllPages(
    async (cursor) => { calls.push(["page", cursor]); return pages.shift(); },
    async (token) => { calls.push(["release", token]); return { released: true }; },
    async (page) => { calls.push(["emit", page.items[0] ?? "empty"]); },
  );
  assert.deepEqual(calls, [
    ["page", undefined], ["emit", 1], ["page", "c2"], ["emit", "empty"],
    ["page", "c3"], ["emit", 3], ["release", "r1"],
  ]);
});

test("readAllPages releases a retained sample when a later request fails", async () => {
  const released = [];
  await assert.rejects(readAllPages(
    async (cursor) => cursor ? Promise.reject(new Error("page failed")) : ({ items: [], next_cursor: "c2", release_token: "r2" }),
    async (token) => { released.push(token); return { released: true }; },
    async () => {},
  ), /page failed/);
  assert.deepEqual(released, ["r2"]);
});

test("readAllPages stops duplicate cursors and still releases", async () => {
  let released = false;
  await assert.rejects(readAllPages(
    async () => ({ items: [], next_cursor: "same", release_token: "r3" }),
    async () => { released = true; return { released: true }; },
    async () => {},
  ), /repeated cursor/);
  assert.equal(released, true);
});

test("readAllPages releases the current snapshot when interrupted between pages", async () => {
  const released = [];
  await assert.rejects(readAllPages(
    async () => ({ items: [], next_cursor: "next", release_token: "r4" }),
    async (token) => { released.push(token); return { released: true }; },
    async () => {},
    () => true,
  ), /interrupted/);
  assert.deepEqual(released, ["r4"]);
});
