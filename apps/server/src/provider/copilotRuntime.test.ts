import assert from "node:assert/strict";

import { describe, it } from "vitest";

import {
  buildCopilotClientOptions,
  capabilitiesFromCopilotModel,
  toCopilotReasoningEffort,
} from "./copilotRuntime.ts";

describe("buildCopilotClientOptions", () => {
  it("strips inherited COPILOT_CLI_PATH so the SDK uses the bundled CLI by default", () => {
    const options = buildCopilotClientOptions({
      settings: {
        enabled: true,
        binaryPath: "",
        serverUrl: "",
        customModels: [],
      },
      cwd: "/tmp/project",
      env: {
        PATH: "/usr/bin",
        COPILOT_CLI_PATH: "/opt/homebrew/bin/copilot",
        GITHUB_TOKEN: "github-token",
      },
      logLevel: "error",
    });

    assert.equal(options.cliPath, undefined);
    assert.equal(options.cwd, "/tmp/project");
    assert.equal(options.logLevel, "error");
    assert.equal(options.env?.COPILOT_CLI_PATH, undefined);
    assert.equal(options.env?.GITHUB_TOKEN, "github-token");
  });

  it("prefers the configured binary path over any inherited CLI path override", () => {
    const options = buildCopilotClientOptions({
      settings: {
        enabled: true,
        binaryPath: "/custom/copilot",
        serverUrl: "",
        customModels: [],
      },
      env: {
        COPILOT_CLI_PATH: "/opt/homebrew/bin/copilot",
      },
    });

    assert.equal(options.cliPath, "/custom/copilot");
    assert.equal(options.env?.COPILOT_CLI_PATH, undefined);
  });

  it("normalizes string reasoning effort values to the SDK union", () => {
    assert.equal(toCopilotReasoningEffort("high"), "high");
    assert.equal(toCopilotReasoningEffort("  "), undefined);
  });

  it("maps Copilot reasoning efforts onto option descriptors", () => {
    const capabilities = capabilitiesFromCopilotModel({
      supportedReasoningEfforts: ["low", "medium", "high"],
      defaultReasoningEffort: "medium",
    });

    assert.deepEqual(capabilities, {
      optionDescriptors: [
        {
          id: "reasoningEffort",
          label: "Reasoning",
          type: "select",
          currentValue: "medium",
          options: [
            { id: "low", label: "Low" },
            { id: "medium", label: "Medium", isDefault: true },
            { id: "high", label: "High" },
          ],
        },
      ],
    });
  });
});
