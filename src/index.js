import { randomUUID } from "node:crypto";

const TERMINAL_CODES = new Set(["UNAUTHORIZED", "FORBIDDEN"]);

/**
 * Small headless ButchrD core.  Adapters supply persistence, workers and an
 * optional Morbr-compatible transport; no central credential is accepted or
 * retained by this module.
 */
export function createButchrd({ store = memoryStore(), workers = {}, local = {} } = {}) {
  const deploymentId = store.get("deploymentId") ?? randomUUID();
  store.set("deploymentId", deploymentId);
  const instanceId = randomUUID();
  const subscriptions = new Map();
  const seenCursors = new Set();
  const activity = [];
  let connectedConfig = null;
  let revoked = false;

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
    if (TERMINAL_CODES.has(envelope.code)) {
      revoked = true;
      subscriptions.clear();
      connectedConfig = null;
      report("connection-terminal", { code: envelope.code });
      return { accepted: false, terminal: true };
    }
    if (envelope.contractVersion?.split(".")[0] !== "1") throw new Error("incompatible contract version");
    if (revoked) return { accepted: false, terminal: true };
    const subscription = subscriptions.get(envelope.subscriptionId);
    if (!subscription) return { accepted: false, reason: "unknown-subscription" };
    if (envelope.cursor && seenCursors.has(`${envelope.subscriptionId}:${envelope.cursor}`)) return { accepted: false, duplicate: true };
    if (envelope.cursor) seenCursors.add(`${envelope.subscriptionId}:${envelope.cursor}`);
    subscription.epoch = envelope.streamEpoch;
    subscription.sequence = envelope.sequence;
    if (envelope.kind === "snapshot") subscription.state = envelope.data;
    if (envelope.kind === "change" && envelope.data?.operation === "remove") subscription.state = null;
    if (envelope.subject?.kind === "config" && envelope.kind === "snapshot") connectedConfig = envelope.data;
    report("envelope-received", { subscriptionId: envelope.subscriptionId, kind: envelope.kind });
    return { accepted: true };
  }

  function reconnect(id, { replayAvailable = true, authorized = true } = {}) {
    const sub = subscriptions.get(id);
    if (!sub) throw new Error(`unknown subscription ${id}`);
    if (!authorized) return receive({ contractVersion: "1.0", code: "UNAUTHORIZED" });
    sub.epoch += 1;
    if (!replayAvailable) report("reset-required", { subscriptionId: id, epoch: sub.epoch });
    return { subscriptionId: id, streamEpoch: sub.epoch, resetRequired: !replayAvailable };
  }

  function effectiveConfig() {
    // Local config is allowed only from independently configured capability.
    return { ...connectedConfig, ...(local.capabilities ? local.config : {}) };
  }

  return {
    daemon: "butchrd", deploymentId, instanceId, openSubscription, replaceSubscription,
    receive, reconnect, effectiveConfig, reportActivity: report,
    get activity() { return [...activity]; },
    get connected() { return !revoked && connectedConfig !== null; },
  };
}

function memoryStore() {
  const values = new Map();
  return { get: key => values.get(key), set: (key, value) => values.set(key, value) };
}
