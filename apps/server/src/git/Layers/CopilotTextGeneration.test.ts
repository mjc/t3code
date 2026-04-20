import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { beforeEach, expect, vi } from "vitest";

import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { TextGeneration } from "../Services/TextGeneration.ts";
import { CopilotTextGenerationLive } from "./CopilotTextGeneration.ts";

const runtimeMock = vi.hoisted(() => {
  const state = {
    createdClients: [] as Array<{
      readonly input: { readonly cwd?: string };
      readonly client: {
        readonly start: ReturnType<typeof vi.fn>;
        readonly stop: ReturnType<typeof vi.fn>;
        readonly createSession: ReturnType<typeof vi.fn>;
      };
    }>,
    sessions: [] as Array<{
      readonly disconnect: ReturnType<typeof vi.fn>;
      readonly sendAndWait: ReturnType<typeof vi.fn>;
    }>,
  };

  return {
    state,
    reset() {
      state.createdClients = [];
      state.sessions = [];
    },
  };
});

vi.mock("../../provider/copilotRuntime.ts", async () => {
  const actual = await vi.importActual<typeof import("../../provider/copilotRuntime.ts")>(
    "../../provider/copilotRuntime.ts",
  );

  return {
    ...actual,
    createCopilotClient: vi.fn((input: { readonly cwd?: string }) => {
      const start = vi.fn(async () => undefined);
      const stop = vi.fn(async () => undefined);
      const createSession = vi.fn(async () => {
        const sendAndWait = vi.fn(async () => ({
          data: {
            content: JSON.stringify({
              subject: "Add change",
              body: "",
            }),
          },
        }));
        const disconnect = vi.fn(async () => undefined);
        runtimeMock.state.sessions.push({ disconnect, sendAndWait });
        return {
          sendAndWait,
          disconnect,
        };
      });

      const client = {
        start,
        stop,
        createSession,
      };
      runtimeMock.state.createdClients.push({ input, client });
      return client;
    }),
  };
});

beforeEach(() => {
  runtimeMock.reset();
});

const CopilotTextGenerationTestLayer = CopilotTextGenerationLive.pipe(
  Layer.provideMerge(
    ServerSettingsService.layerTest({
      providers: {
        copilot: {
          enabled: true,
        },
      },
    }),
  ),
  Layer.provideMerge(
    ServerConfig.layerTest(process.cwd(), {
      prefix: "t3code-copilot-text-generation-test-",
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);

it.layer(CopilotTextGenerationTestLayer)("CopilotTextGenerationLive", (it) => {
  it.effect("reuses a started Copilot client across git text generation requests", () =>
    Effect.gen(function* () {
      const textGeneration = yield* TextGeneration;
      const modelSelection = {
        provider: "copilot" as const,
        model: "gpt-4.1",
      };

      const first = yield* textGeneration.generateCommitMessage({
        cwd: process.cwd(),
        branch: "feature/copilot-text-generation",
        stagedSummary: "M README.md",
        stagedPatch: "diff --git a/README.md b/README.md",
        modelSelection,
      });

      const second = yield* textGeneration.generateCommitMessage({
        cwd: process.cwd(),
        branch: "feature/copilot-text-generation",
        stagedSummary: "M README.md",
        stagedPatch: "diff --git a/README.md b/README.md",
        modelSelection,
      });

      expect(first.subject).toBe("Add change");
      expect(second.subject).toBe("Add change");

      expect(runtimeMock.state.createdClients).toHaveLength(1);
      expect(runtimeMock.state.sessions).toHaveLength(2);

      const sharedClient = runtimeMock.state.createdClients[0]?.client;
      expect(sharedClient?.start).toHaveBeenCalledTimes(1);
      expect(sharedClient?.createSession).toHaveBeenCalledTimes(2);
      expect(sharedClient?.stop).not.toHaveBeenCalled();

      expect(runtimeMock.state.sessions[0]?.sendAndWait).toHaveBeenCalledTimes(1);
      expect(runtimeMock.state.sessions[0]?.disconnect).toHaveBeenCalledTimes(1);
      expect(runtimeMock.state.sessions[1]?.sendAndWait).toHaveBeenCalledTimes(1);
      expect(runtimeMock.state.sessions[1]?.disconnect).toHaveBeenCalledTimes(1);
    }),
  );
});
