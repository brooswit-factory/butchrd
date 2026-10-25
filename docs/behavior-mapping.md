# ButchrD behavior mapping

ButchrD retains the existing daemon-shaped lifecycle: establish identity, accept
work/subscription configuration, execute through adapters, and report activity.
It deliberately does not copy the legacy daemon or perform Jira pickup itself.

| Existing lifecycle intent | ButchrD boundary |
| --- | --- |
| Stable installation identity | `deploymentId` is persisted through the supplied store. |
| Per-process lifecycle | `instanceId` is generated at every start. |
| Receive assigned state/config | Versioned config snapshots enter through `receive`. |
| Do work through provider/worker adapters | `workers.reportActivity` receives lifecycle activity; no provider credentials enter this API. |
| Reconnect safely | Logical subscription survives transient reconnects, epoch advances, and cursor replay is deduplicated. |
| Authorization loss | `UNAUTHORIZED`/`FORBIDDEN` clears connected config and subscriptions without retaining protected state. |

Standalone mode needs no Morbr transport. Connected mode accepts the shared
v1 envelope at an adapter boundary. A replay gap requests reset followed by an
adapter-delivered replacement snapshot; it never turns into application data.
Fallback config is permitted only when `local.capabilities` is independently
configured, and this core never accepts, stores, or forwards central secrets.
