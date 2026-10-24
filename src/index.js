import { randomUUID } from "node:crypto";
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
      for (const subscription of subscriptions.values()) subscription.transport?.close?.();
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
    report("work-started", { id: work.id });
    try { const result = await workers.execute(work); report("work-finished", { id: work.id }); return result; }
    catch (error) { report("work-failed", { id: work.id }); throw error; }
  }

  return {
    daemon: "butchrd", deploymentId, instanceId, openSubscription, replaceSubscription,
    receive, reconnect, effectiveConfig, runWork, monitoring, reportActivity: report,
    get activity() { return [...activity]; },
    get connected() { return !revoked && connectedConfig !== null; },
  };
}

function memoryStore() {
  const values = new Map();
  return { get: key => values.get(key), set: (key, value) => values.set(key, value) };
}
