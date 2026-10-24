import test from "node:test";
import assert from "node:assert/strict";
import { createButchrd, createFileStore } from "../src/index.js";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";

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

test("ordinary reconnect preserves unready state and reset requires a replacement snapshot", () => {
  const daemon = createButchrd(); const sub = daemon.openSubscription({ subject: { kind: "config", key: "daemon" }, watchMode: "resource" });
  daemon.reconnect(sub.id);
  assert.deepEqual(daemon.receive({ ...config(sub.id), kind: "change", streamEpoch: 2, sequence: 1, cursor: "n1", data: { version: 1 } }), { accepted: false, reason: "snapshot-required" });
  assert.equal(daemon.receive({ ...config(sub.id), streamEpoch: 2, sequence: 2, cursor: "n2", data: { version: 1 } }).accepted, true);
  daemon.reconnect(sub.id);
  assert.deepEqual(daemon.receive({ contractVersion: "1.0", kind: "reset", subscriptionId: sub.id, streamEpoch: 3, sequence: 1, cursor: null, subject: { kind: "config", key: "daemon" }, data: null }), { accepted: true, reset: true });
  assert.deepEqual(daemon.receive({ ...config(sub.id), kind: "change", streamEpoch: 3, sequence: 2, cursor: "n3", data: { version: 2 } }), { accepted: false, reason: "snapshot-required" });
});

test("a delayed reset cannot undo its replacement snapshot", () => {
  const daemon = createButchrd(); const sub = daemon.openSubscription({ subject: { kind: "config", key: "daemon" }, watchMode: "resource" });
  assert.equal(daemon.receive({ ...config(sub.id), data: { version: 1 } }).accepted, true); daemon.reconnect(sub.id, { replayAvailable: false });
  const reset = { contractVersion: "1.0", kind: "reset", subscriptionId: sub.id, streamEpoch: 2, sequence: 1, cursor: "r1", subject: { kind: "config", key: "daemon" }, data: null };
  assert.equal(daemon.receive(reset).accepted, true); assert.equal(daemon.receive({ ...config(sub.id), streamEpoch: 2, sequence: 2, cursor: "s2", data: { version: 2 } }).accepted, true);
  assert.equal(daemon.receive(reset).accepted, false); assert.equal(daemon.receive({ ...config(sub.id), kind: "change", streamEpoch: 2, sequence: 3, cursor: "c3", data: { version: 3 } }).accepted, true);
});

test("runnable controller reconciles adopted work, preserves held/resting exclusions, and stops cleanly", async () => {
  const calls = []; const daemon = createButchrd({ workers: { execute: async work => calls.push(`run:${work.id}`), stop: async id => calls.push(`stop:${id}`) } });
  assert.deepEqual(await daemon.reconcile({ desired: [{ id: "adopted" }, { id: "held", held: true }], running: ["old", "resting"], resting: ["resting"] }), { spawned: ["adopted"], stopped: ["old"] });
  assert.deepEqual(calls, ["run:adopted", "stop:old"]);
  const service = await daemon.start(); service.stop(); assert.deepEqual(daemon.activity.slice(-2).map(e => e.kind), ["daemon-started", "daemon-stopped"]);
});

test("file store survives process-style restart with a new instance identity", () => {
  const dir = mkdtempSync(join(tmpdir(), "butchrd-")); const path = join(dir, "state.json");
  const first = createButchrd({ store: createFileStore(path) }); const second = createButchrd({ store: createFileStore(path) });
  assert.equal(first.deploymentId, second.deploymentId); assert.notEqual(first.instanceId, second.instanceId); rmSync(dir, { recursive: true });
});

test("a fake Morbin resource watch drives desired worker reconciliation", async () => {
  let listener; const ran = []; const monitoring = { watchResource: (_p, _r, next) => { listener = next; return { close() {} }; } };
  const daemon = createButchrd({ morbin: monitoring, workers: { execute: async work => ran.push(work.id) } });
  await daemon.start({ watches: [{ provider: "fake", resource: "desired" }] });
  listener({ contractVersion: "1.0", kind: "snapshot", subscriptionId: daemon.openSubscription({ subject: { kind: "resource", key: "desired" }, watchMode: "resource" }).id, streamEpoch: 1, sequence: 1, cursor: "d1", subject: { kind: "resource", key: "desired" }, data: { desired: [{ id: "from-provider" }] } });
  await new Promise(resolve => setImmediate(resolve)); assert.deepEqual(ran, ["from-provider"]);
});

const delay = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
async function eventually(assertion, timeout = 2_000) {
  const deadline = Date.now() + timeout;
  let lastError;
  while (Date.now() < deadline) {
    try { return assertion(); } catch (error) { lastError = error; await delay(20); }
  }
  throw lastError;
}

function startCli(args) {
  const child = spawn(process.execPath, ["src/cli.js", ...args], { cwd: process.cwd(), stdio: ["ignore", "pipe", "pipe"] });
  let stdout = ""; let stderr = "";
  child.stdout.on("data", chunk => { stdout += chunk; });
  child.stderr.on("data", chunk => { stderr += chunk; });
  return { child, stdout: () => stdout, stderr: () => stderr };
}

function waitForExit(child, timeout = 2_000) {
  return Promise.race([
    new Promise(resolve => child.once("exit", (code, signal) => resolve({ code, signal }))),
    delay(timeout).then(() => { throw new Error("CLI did not exit within bounded interval"); }),
  ]);
}

test("CLI keeps a configured fake Morbin transport alive and SIGTERM closes it", async t => {
  const dir = mkdtempSync(join(tmpdir(), "butchrd-cli-"));
  const state = join(dir, "state.json"); const eventFile = join(dir, "event.json");
  const activityFile = join(dir, "activity.jsonl"); const configPath = join(dir, "config.json");
  writeFileSync(eventFile, JSON.stringify({ resource: "desired", value: { desired: [] }, cursor: "initial" }));
  writeFileSync(configPath, JSON.stringify({
    fakeProvider: { resources: { desired: { desired: [] } }, eventFile },
    watches: [{ provider: "fake", resource: "desired" }], activityFile,
  }));
  const first = startCli(["--state", state, "--config", configPath]);
  t.after(() => { if (first.child.exitCode === null) first.child.kill("SIGKILL"); });
  await eventually(() => assert.match(first.stdout(), /deploymentId/));
  assert.equal(first.child.exitCode, null, "the configured provider transport keeps the CLI alive");
  await eventually(() => assert.match(readFileSync(activityFile, "utf8"), /envelope-received/));
  writeFileSync(eventFile, JSON.stringify({ resource: "desired", value: { desired: [] }, cursor: "process-change", source: "push" }));
  await eventually(() => assert.ok((readFileSync(activityFile, "utf8").match(/envelope-received/g) ?? []).length >= 2));
  first.child.kill("SIGTERM");
  assert.deepEqual(await waitForExit(first.child), { code: 0, signal: null });
  assert.equal(first.stderr(), "");
  const firstIdentity = JSON.parse(first.stdout().trim());
  const second = startCli(["--state", state, "--config", configPath]);
  t.after(() => { if (second.child.exitCode === null) second.child.kill("SIGKILL"); });
  await eventually(() => assert.match(second.stdout(), /deploymentId/));
  second.child.kill("SIGTERM");
  assert.deepEqual(await waitForExit(second.child), { code: 0, signal: null });
  const secondIdentity = JSON.parse(second.stdout().trim());
  assert.equal(firstIdentity.deploymentId, secondIdentity.deploymentId);
  assert.notEqual(firstIdentity.instanceId, secondIdentity.instanceId);
  assert.equal(second.stderr(), "");
  rmSync(dir, { recursive: true });
});
