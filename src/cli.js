#!/usr/bin/env node
import { appendFileSync, readFileSync, watch, writeSync } from "node:fs";
import { createButchrd, createFileStore } from "./index.js";

function options(argv) {
  const positional = [];
  let statePath;
  let configPath;
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] === "--state") statePath = argv[++index];
    else if (argv[index] === "--config") configPath = argv[++index];
    else positional.push(argv[index]);
  }
  return { statePath: statePath ?? positional[0] ?? ".butchrd-state.json", configPath };
}

function configuredFakeProvider(config = {}) {
  const values = new Map(Object.entries(config.resources ?? {}));
  return {
    async fetch(resource) {
      if (!values.has(resource)) throw new Error(`fake provider has no resource: ${resource}`);
      return structuredClone(values.get(resource));
    },
    async subscribe({ onEvent }) {
      if (!config.eventFile) return () => {};
      const relay = () => {
        try { onEvent(JSON.parse(readFileSync(config.eventFile, "utf8"))); }
        catch (error) { process.stderr.write(`fake-provider event ignored: ${error.message}\n`); }
      };
      // This is a configured provider transport, not a CLI keepalive: closing
      // it is what lets a signalled daemon leave the event loop naturally.
      const transport = watch(config.eventFile, { persistent: true }, relay);
      return () => transport.close();
    },
  };
}

const { statePath, configPath } = options(process.argv.slice(2));
const config = configPath ? JSON.parse(readFileSync(configPath, "utf8")) : {};
const workers = config.activityFile
  ? { reportActivity: event => appendFileSync(config.activityFile, `${JSON.stringify(event)}\n`) }
  : {};
const providers = config.fakeProvider ? { fake: configuredFakeProvider(config.fakeProvider) } : {};
const daemon = createButchrd({ store: createFileStore(statePath), workers, local: { providers } });
const service = await daemon.start({ mode: config.mode ?? "standalone", watches: config.watches ?? [] });
for (const signal of ["SIGINT", "SIGTERM"]) process.once(signal, () => service.stop());
writeSync(1, `${JSON.stringify({ deploymentId: daemon.deploymentId, instanceId: daemon.instanceId })}\n`);
