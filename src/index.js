import { randomUUID } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { createMorbin } from "@brooswit-factory/morbin";

const TERMINAL_CODES = new Set(["UNAUTHORIZED", "FORBIDDEN"]);

/**
 * Small headless ButchrD core.  Adapters supply persistence, workers and an
 * optional Morbr-compatible transport; no central credential is accepted or
 * retained by this module.
 */
export function createButchrd({ store = memoryStore(), workers = {}, local = {}, morbin } = {}) {
  const deploymentId = store.get("deploymentId") ?? randomUUID();
  store.set("deploymentId", deploymentId);
  const instanceId = randomUUID();
  const subscriptions = new Map();
  const seenCursors = new Set();
  const activity = [];
  const activeWork = new Map();
  const activeHandles = new Set();
  const closedHandles = new WeakSet();
  let lifecycleStopped = false;
  let reconciliation = Promise.resolve();
  let connectedConfig = null;
  let connectedConfigVersion = -1;
  let revoked = false;
  // The shared library owns provider monitoring/freshness; this daemon never polls a provider itself.
  const monitoring = morbin ?? createMorbin({ providers: local.providers ?? {} });

  function report(kind, data = {}) {
    const event = { kind, data, deploymentId, instanceId };
    activity.push(event);
    workers.reportActivity?.(event);
    return event;
  }

  function shutdown() {
    if (lifecycleStopped) return false;
    lifecycleStopped = true;
    for (const handle of activeHandles) closeHandle(handle);
    activeHandles.clear();
    for (const id of activeWork.keys()) void workers.stop?.(id);
    return true;
  }

  function closeHandle(handle) {
    if (!handle?.close || closedHandles.has(handle)) return;
    closedHandles.add(handle);
    handle.close();
  }

  function openSubscription({ id = randomUUID(), subject, watchMode, transport } = {}) {
    if (!subject || !watchMode) throw new TypeError("subject and watchMode are required");
    const subscription = { id, subject, watchMode, transport, epoch: 1, sequence: 0, state: null, closed: false };
    subscriptions.set(id, subscription);
    report("subscription-opened", { id, subject, watchMode, epoch: 1 });
    return { ...subscription };
  }

  function replaceSubscription(id, next) {
    const prior = subscriptions.get(id);
    if (!prior) throw new Error(`unknown subscription ${id}`);
    prior.transport?.close?.();
    subscriptions.delete(id);
    // Changed subject/mode/auth is a new logical subscription, as required.
    return openSubscription({ ...next, transport: next.transport });
  }

  function receive(envelope) {
    const terminalCode = envelope.error?.code ?? envelope.code;
    if (TERMINAL_CODES.has(terminalCode)) {
      revoked = true;
      shutdown();
      for (const subscription of subscriptions.values()) closeHandle(subscription.transport);
      subscriptions.clear();
      connectedConfig = null;
      connectedConfigVersion = -1;
      report("connection-terminal", { code: terminalCode });
      return { accepted: false, terminal: true };
    }
    if (envelope.contractVersion?.split(".")[0] !== "1") throw new Error("incompatible contract version");
    if (revoked) return { accepted: false, terminal: true };
    const subscription = subscriptions.get(envelope.subscriptionId);
    if (!subscription) return { accepted: false, reason: "unknown-subscription" };
    if (envelope.kind === "reset") {
      if (envelope.cursor && seenCursors.has(`${envelope.subscriptionId}:${envelope.cursor}`)) return { accepted: false, duplicate: true };
      if (envelope.streamEpoch < subscription.epoch || (envelope.streamEpoch === subscription.epoch && envelope.sequence <= subscription.sequence)) return { accepted: false, stale: true };
      if (envelope.cursor) seenCursors.add(`${envelope.subscriptionId}:${envelope.cursor}`);
      subscription.epoch = envelope.streamEpoch;
      subscription.sequence = envelope.sequence;
      subscription.ready = false;
      subscription.state = null;
      report("envelope-received", { subscriptionId: envelope.subscriptionId, kind: envelope.kind });
      return { accepted: true, reset: true };
    }
    if (envelope.cursor && seenCursors.has(`${envelope.subscriptionId}:${envelope.cursor}`)) return { accepted: false, duplicate: true };
    if (envelope.streamEpoch < subscription.epoch || (envelope.streamEpoch === subscription.epoch && envelope.sequence <= subscription.sequence)) return { accepted: false, stale: true };
    if (!subscription.ready && envelope.kind === "change") return { accepted: false, reason: "snapshot-required" };
    if (envelope.cursor) seenCursors.add(`${envelope.subscriptionId}:${envelope.cursor}`);
    subscription.epoch = envelope.streamEpoch;
    subscription.sequence = envelope.sequence;
    if (envelope.kind === "snapshot") { subscription.state = envelope.data; subscription.ready = true; }
    if (envelope.kind === "change" && envelope.data?.operation === "remove") subscription.state = null;
    if (envelope.subject?.kind === "config") {
      const version = envelope.data?.version ?? envelope.data?.configVersion ?? 0;
      if (version <= connectedConfigVersion) return { accepted: false, stale: true };
      if (envelope.kind === "snapshot" || envelope.kind === "change") { connectedConfig = envelope.data; connectedConfigVersion = version; }
    }
    report("envelope-received", { subscriptionId: envelope.subscriptionId, kind: envelope.kind });
    return { accepted: true };
  }

  function reconnect(id, { replayAvailable = true, authorized = true } = {}) {
    const sub = subscriptions.get(id);
    if (!sub) throw new Error(`unknown subscription ${id}`);
    if (!authorized) return receive({ contractVersion: "1.0", code: "UNAUTHORIZED" });
    sub.epoch += 1;
    sub.sequence = 0;
    if (!replayAvailable) sub.ready = false;
    if (!replayAvailable) report("reset-required", { subscriptionId: id, epoch: sub.epoch });
    return { subscriptionId: id, streamEpoch: sub.epoch, resetRequired: !replayAvailable };
  }

  function effectiveConfig() {
    // Local config is allowed only from independently configured capability.
    return { ...connectedConfig, ...(local.capabilities ? local.config : {}) };
  }

  async function runWork(work) {
    if (typeof workers.execute !== "function") throw new TypeError("workers.execute is required to run work");
    activeWork.set(work.id, work);
    report("work-started", { id: work.id });
    try { const result = await workers.execute(work); report("work-finished", { id: work.id }); return result; }
    catch (error) { report("work-failed", { id: work.id }); throw error; }
    finally { activeWork.delete(work.id); }
  }

  async function start({ mode = "standalone", watches = [], connectedSubscriptions = [] } = {}) {
    if (mode === "connected" && !local.transport) throw new Error("connected mode requires a configured transport");
    if (mode === "connected" && typeof local.transport.subscribe !== "function") throw new TypeError("connected transport must implement subscribe");
    lifecycleStopped = false;
    const applyWatch = event => {
      const result = receive(event);
      if (result.accepted && Array.isArray(event.data?.desired)) {
        void reconcile({ desired: event.data.desired, running: event.data.running ?? [], resting: event.data.resting ?? [] })
          .catch(error => report("reconcile-failed", { message: error.message }));
      }
      return result;
    };
    for (const subscription of connectedSubscriptions) {
      if (!subscriptions.has(subscription.id)) openSubscription(subscription);
    }
    if (mode === "connected") {
      const transport = await local.transport.subscribe({ deploymentId, instanceId, onEnvelope: applyWatch });
      if (!transport?.close) throw new TypeError("connected transport subscription must return a closeable handle");
      // A synchronous authorization failure can arrive from subscribe() before
      // its handle is returned. Close that late handle rather than reviving a
      // terminal transport after shutdown.
      if (lifecycleStopped) closeHandle(transport);
      else activeHandles.add(transport);
    }
    for (const { provider, resource, query, mode: watchMode = "resource" } of watches) {
      if (lifecycleStopped) break;
      const handle = resource !== undefined
        ? monitoring.watchResource(provider, resource, applyWatch)
        : monitoring.watchQuery(provider, query, watchMode === "query-membership" ? "membership" : "content", applyWatch);
      // Morbin owns the logical ID for a real watch. Register it before its
      // asynchronous initial snapshot so the envelope is accepted by us.
      if (handle.id && !subscriptions.has(handle.id)) {
        openSubscription({
          id: handle.id,
          subject: resource !== undefined ? { kind: "resource", key: resource } : { kind: "query", key: query },
          watchMode: resource !== undefined ? "resource" : watchMode,
          transport: handle,
        });
      }
      if (lifecycleStopped) closeHandle(handle);
      else activeHandles.add(handle);
    }
    report("daemon-started", { mode });
    let stopped = false;
    return {
      mode,
      stop: () => {
        if (stopped || lifecycleStopped) return;
        stopped = true;
        if (shutdown()) report("daemon-stopped", { mode });
      },
    };
  }

  function reconcile(input = {}) {
    const job = reconciliation.then(() => reconcileNow(input));
    // Keep the serial queue usable after a worker/reconciliation failure; the
    // caller still receives the rejected job and watch callers report it.
    reconciliation = job.catch(() => {});
    return job;
  }

  async function reconcileNow({ desired = [], running = [], resting = [] } = {}) {
    if (lifecycleStopped) return { spawned: [], stopped: [] };
    const want = new Map(desired.filter(work => !work.held).map(work => [work.id, work]));
    const have = new Set(running); const asleep = new Set(resting);
    const spawned = [];
    for (const [id, work] of want) {
      if (lifecycleStopped) break;
      if (!have.has(id) && !activeWork.has(id)) { await runWork(work); spawned.push(id); }
    }
    const stopped = [];
    for (const id of have) {
      if (lifecycleStopped) break;
      if (!want.has(id) && !asleep.has(id)) { await workers.stop?.(id); stopped.push(id); }
    }
    return { spawned, stopped };
  }

  return {
    daemon: "butchrd", deploymentId, instanceId, openSubscription, replaceSubscription,
    receive, reconnect, effectiveConfig, runWork, start, reconcile, monitoring, reportActivity: report,
    get activity() { return [...activity]; },
    get connected() { return !revoked && connectedConfig !== null; },
  };
}

function memoryStore() {
  const values = new Map();
  return { get: key => values.get(key), set: (key, value) => values.set(key, value) };
}

/** A tiny JSON store for deployment identity and last accepted local config. */
export function createFileStore(path) {
  const values = existsSync(path) ? JSON.parse(readFileSync(path, "utf8")) : {};
  return {
    get: key => values[key],
    set: (key, value) => { values[key] = value; writeFileSync(path, JSON.stringify(values)); },
  };
}
