import test from "node:test";
import assert from "node:assert/strict";
import { createButchrd } from "../src/index.js";

const config = id => ({ contractVersion: "1.0", kind: "snapshot", subscriptionId: id, streamEpoch: 1, sequence: 1, cursor: "c1", subject: { kind: "config", key: "daemon" }, data: { pollMs: 1000 } });

test("persists deployment identity while each start gets a new instance identity", () => {
  const values = new Map(); const store = { get: k => values.get(k), set: (k, v) => values.set(k, v) };
  const first = createButchrd({ store }); const second = createButchrd({ store });
  assert.equal(first.deploymentId, second.deploymentId); assert.notEqual(first.instanceId, second.instanceId);
});

test("standalone operation has no Morbr dependency and only uses capable local config", () => {
  assert.deepEqual(createButchrd({ local: { config: { unsafe: true } } }).effectiveConfig(), {});
  assert.deepEqual(createButchrd({ local: { capabilities: true, config: { pollMs: 2000 } } }).effectiveConfig(), { pollMs: 2000 });
});

test("connected config, cursor dedupe, replay-gap reset and terminal revocation are safe", () => {
  const daemon = createButchrd(); const sub = daemon.openSubscription({ subject: { kind: "config", key: "daemon" }, watchMode: "resource" });
  assert.deepEqual(daemon.receive(config(sub.id)), { accepted: true });
  assert.equal(daemon.connected, true); assert.deepEqual(daemon.receive(config(sub.id)), { accepted: false, duplicate: true });
  assert.deepEqual(daemon.reconnect(sub.id, { replayAvailable: false }), { subscriptionId: sub.id, streamEpoch: 2, resetRequired: true });
  assert.deepEqual(daemon.receive({ contractVersion: "1.0", code: "FORBIDDEN" }), { accepted: false, terminal: true });
  assert.equal(daemon.connected, false); assert.deepEqual(daemon.receive(config(sub.id)), { accepted: false, terminal: true });
});

test("reports lifecycle activity to fake workers and replaces changed streams", () => {
  const events = []; const daemon = createButchrd({ workers: { reportActivity: e => events.push(e) } });
  const first = daemon.openSubscription({ subject: { kind: "query", key: "a" }, watchMode: "query-membership" });
  const second = daemon.replaceSubscription(first.id, { subject: { kind: "query", key: "b" }, watchMode: "query-content" });
  assert.notEqual(first.id, second.id); assert.equal(events.filter(e => e.kind === "subscription-opened").length, 2);
});

test("uses Morbin-shaped terminal errors, closes transports, and executes fake work", async () => {
  let closed = 0; const activity = []; const daemon = createButchrd({ workers: { execute: async work => work.id, reportActivity: e => activity.push(e) } });
  const sub = daemon.openSubscription({ subject: { kind: "resource", key: "a" }, watchMode: "resource", transport: { close: () => { closed += 1; } } });
  assert.deepEqual(daemon.receive({ contractVersion: "1.0", kind: "error", error: { code: "FORBIDDEN" } }), { accepted: false, terminal: true });
  assert.equal(closed, 1); assert.equal(await daemon.runWork({ id: "work-1" }), "work-1");
  assert.deepEqual(activity.map(e => e.kind), ["subscription-opened", "connection-terminal", "work-started", "work-finished"]);
});

test("resets sequence on reconnect, requires replacement snapshots after gaps, and ignores equal config versions", () => {
  const daemon = createButchrd(); const sub = daemon.openSubscription({ subject: { kind: "config", key: "daemon" }, watchMode: "resource" });
  assert.equal(daemon.receive({ ...config(sub.id), sequence: 42, data: { version: 1, pollMs: 1000 } }).accepted, true);
  daemon.reconnect(sub.id);
  assert.equal(daemon.receive({ ...config(sub.id), kind: "change", streamEpoch: 2, sequence: 1, cursor: "c2", data: { version: 2, pollMs: 2000 } }).accepted, true);
  assert.deepEqual(daemon.effectiveConfig(), { version: 2, pollMs: 2000 });
  assert.deepEqual(daemon.receive({ ...config(sub.id), kind: "change", streamEpoch: 2, sequence: 2, cursor: "c3", data: { version: 2, pollMs: 9999 } }), { accepted: false, stale: true });
  daemon.reconnect(sub.id, { replayAvailable: false });
  assert.deepEqual(daemon.receive({ ...config(sub.id), kind: "change", streamEpoch: 3, sequence: 1, cursor: "c4", data: { version: 3 } }), { accepted: false, reason: "snapshot-required" });
});
