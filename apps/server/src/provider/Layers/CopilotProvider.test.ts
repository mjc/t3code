import assert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { beforeEach, vi } from "vitest";

import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { CopilotProvider } from "../Services/CopilotProvider.ts";
import { CopilotProviderLive } from "./CopilotProvider.ts";

const runtimeMock = vi.hoisted(() => {
  const state = {
    listModelsError: null as Error | null,
  };

  return {
    state,
    reset() {
      state.listModelsError = null;
    },
  };
});

vi.mock("../copilotRuntime.ts", async () => {
  const actual =
    await vi.importActual<typeof import("../copilotRuntime.ts")>("../copilotRuntime.ts");

  return {
    ...actual,
    createCopilotClient: vi.fn(() => ({
      start: vi.fn(async () => undefined),
      stop: vi.fn(async () => undefined),
      getStatus: vi.fn(async () => ({
        version: "1.0.32",
        protocolVersion: 3,
      })),
      getAuthStatus: vi.fn(async () => ({
        isAuthenticated: true,
        authType: "gh-cli",
        host: "https://github.com",
        statusMessage: "zortos293 (via gh)",
        login: "zortos293",
      })),
      listModels: vi.fn(async () => {
        if (runtimeMock.state.listModelsError) {
          throw runtimeMock.state.listModelsError;
        }
        return [];
      }),
    })),
  };
});

beforeEach(() => {
  runtimeMock.reset();
});

const CopilotProviderTestLayer = CopilotProviderLive.pipe(
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(NodeServices.layer),
);

it.layer(CopilotProviderTestLayer)("CopilotProviderLive", (it) => {
  it.effect("surfaces underlying SDK errors instead of leaking Effect.tryPromise text", () =>
    Effect.gen(function* () {
      runtimeMock.state.listModelsError = new Error("401 Unauthorized");

      const provider = yield* CopilotProvider;
      const snapshot = yield* provider.refresh;

      assert.equal(snapshot.status, "error");
      assert.equal(snapshot.installed, true);
      assert.equal(snapshot.message, "401 Unauthorized");
    }),
  );
});
