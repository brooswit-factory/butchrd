import test from "node:test";
import assert from "node:assert/strict";
import { createButchrd } from "../src/index.js";

test("creates the ButchrD daemon marker", () => {
  assert.deepEqual(createButchrd(), { daemon: "butchrd" });
});
