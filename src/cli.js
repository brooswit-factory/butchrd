#!/usr/bin/env node
import { createButchrd, createFileStore } from "./index.js";

const statePath = process.argv[2] ?? ".butchrd-state.json";
const daemon = createButchrd({ store: createFileStore(statePath) });
const service = await daemon.start();
console.log(JSON.stringify({ deploymentId: daemon.deploymentId, instanceId: daemon.instanceId }));
for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => { service.stop(); process.exit(0); });
