import { chmodSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { assert, it } from "@effect/vitest";
import { Effect, Result } from "effect";

import { probeCodexDiscovery } from "./codexAppServer.ts";

function writeFakeCodexBinary(script: string) {
  const tempDir = mkdtempSync(path.join(os.tmpdir(), "t3-codex-discovery-test-"));
  const scriptPath = path.join(tempDir, "fake-codex.mjs");
  writeFileSync(scriptPath, script, "utf8");

  if (process.platform === "win32") {
    const binaryPath = path.join(tempDir, "codex.cmd");
    writeFileSync(binaryPath, `@echo off\r\n"${process.execPath}" "${scriptPath}" %*\r\n`, "utf8");
    return {
      binaryPath,
      tempDir,
    } as const;
  }

  const binaryPath = path.join(tempDir, "codex");
  writeFileSync(binaryPath, `#!/bin/sh\nexec "${process.execPath}" "${scriptPath}" "$@"\n`, "utf8");
  chmodSync(binaryPath, 0o755);
  return {
    binaryPath,
    tempDir,
  } as const;
}

it.effect("probeCodexDiscovery reports account/read errors as typed failures", () =>
  Effect.gen(function* () {
    const fakeCodex = writeFakeCodexBinary(`
      import readline from "node:readline";

      const input = readline.createInterface({ input: process.stdin });
      for await (const line of input) {
        const message = JSON.parse(line);
        if (message.id === 1) {
          console.log(JSON.stringify({ id: 1, result: {} }));
        } else if (message.id === 2) {
          console.log(JSON.stringify({ id: 2, result: { skills: [] } }));
        } else if (message.id === 3) {
          console.log(JSON.stringify({ id: 3, error: { message: "Invalid request" } }));
        }
      }
    `);

    const result = yield* probeCodexDiscovery({
      binaryPath: fakeCodex.binaryPath,
      cwd: fakeCodex.tempDir,
    }).pipe(
      Effect.timeoutOption("2 seconds"),
      Effect.result,
      Effect.ensuring(
        Effect.sync(() => rmSync(fakeCodex.tempDir, { recursive: true, force: true })),
      ),
    );

    assert.equal(Result.isFailure(result), true);
    if (Result.isFailure(result)) {
      assert.match(result.failure.message, /account\/read failed: Invalid request/);
    }
  }),
);
