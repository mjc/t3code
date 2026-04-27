import assert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import type {
  CopilotClient,
  CopilotSession,
  PermissionRequest,
  SessionConfig,
  SessionEvent,
} from "@github/copilot-sdk";
import { it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { beforeEach, vi } from "vitest";

import { ThreadId } from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { CopilotAdapter } from "../Services/CopilotAdapter.ts";
import { makeCopilotAdapterLive } from "./CopilotAdapter.ts";

const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const waitForSdkEventQueue = () =>
  Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 10)));

const runtimeMock = vi.hoisted(() => {
  const makeSession = () => ({
    sessionId: "copilot-sdk-session-1",
    rpc: {
      mode: {
        set: vi.fn(async () => undefined),
      },
      plan: {
        read: vi.fn(async () => ({ content: "" })),
      },
    },
    disconnect: vi.fn(async () => undefined),
    setModel: vi.fn(async () => undefined),
    send: vi.fn(async () => undefined),
    abort: vi.fn(async () => undefined),
  });

  const state = {
    startCalls: 0,
    stopCalls: 0,
    createSessionConfigs: [] as SessionConfig[],
    createSessionImpl: null as ((config: SessionConfig) => Promise<CopilotSession>) | null,
    lastSession: makeSession(),
  };

  return {
    state,
    reset() {
      state.startCalls = 0;
      state.stopCalls = 0;
      state.createSessionConfigs.length = 0;
      state.lastSession = makeSession();
      state.createSessionImpl = async () => state.lastSession as unknown as CopilotSession;
    },
  };
});

vi.mock("../copilotRuntime.ts", async () => {
  const actual =
    await vi.importActual<typeof import("../copilotRuntime.ts")>("../copilotRuntime.ts");

  return {
    ...actual,
    createCopilotClient: vi.fn(
      () =>
        ({
          start: vi.fn(async () => {
            runtimeMock.state.startCalls += 1;
          }),
          stop: vi.fn(async () => {
            runtimeMock.state.stopCalls += 1;
          }),
          createSession: vi.fn(async (config: SessionConfig) => {
            runtimeMock.state.createSessionConfigs.push(config);
            return (runtimeMock.state.createSessionImpl ?? (async () => undefined as never))(
              config,
            );
          }),
          resumeSession: vi.fn(async () => {
            throw new Error("resumeSession is not used in CopilotAdapter tests");
          }),
        }) as unknown as CopilotClient,
    ),
  };
});

beforeEach(() => {
  runtimeMock.reset();
});

const CopilotAdapterTestLayer = makeCopilotAdapterLive().pipe(
  Layer.provideMerge(
    ServerConfig.layerTest(process.cwd(), {
      prefix: "t3code-copilot-adapter-test-",
    }),
  ),
  Layer.provideMerge(
    ServerSettingsService.layerTest({ providers: { copilot: { enabled: true } } }),
  ),
  Layer.provideMerge(NodeServices.layer),
);

it.layer(CopilotAdapterTestLayer)("CopilotAdapterLive", (it) => {
  it.effect(
    "denies bootstrap permission requests before the session context exists in approval-required mode",
    () =>
      Effect.gen(function* () {
        runtimeMock.state.createSessionImpl = async (config) => {
          assert.ok(config.onPermissionRequest);
          const result = await config.onPermissionRequest({ kind: "shell" } as PermissionRequest, {
            sessionId: runtimeMock.state.lastSession.sessionId,
          });
          assert.deepStrictEqual(result, { kind: "denied-interactively-by-user" });
          return runtimeMock.state.lastSession as unknown as CopilotSession;
        };

        const adapter = yield* CopilotAdapter;
        const threadId = asThreadId("copilot-bootstrap-permission-denied");

        const session = yield* adapter.startSession({
          provider: "copilot",
          threadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });

        assert.equal(session.provider, "copilot");
        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect(
    "approves bootstrap permission requests before the session context exists in full-access mode",
    () =>
      Effect.gen(function* () {
        runtimeMock.state.createSessionImpl = async (config) => {
          assert.ok(config.onPermissionRequest);
          const result = await config.onPermissionRequest({ kind: "shell" } as PermissionRequest, {
            sessionId: runtimeMock.state.lastSession.sessionId,
          });
          assert.deepStrictEqual(result, { kind: "approved" });
          return runtimeMock.state.lastSession as unknown as CopilotSession;
        };

        const adapter = yield* CopilotAdapter;
        const threadId = asThreadId("copilot-bootstrap-permission-approved");

        const session = yield* adapter.startSession({
          provider: "copilot",
          threadId,
          cwd: process.cwd(),
          runtimeMode: "full-access",
        });

        assert.equal(session.provider, "copilot");
        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect(
    "returns an empty bootstrap user input response before the session context exists",
    () =>
      Effect.gen(function* () {
        runtimeMock.state.createSessionImpl = async (config) => {
          assert.ok(config.onUserInputRequest);
          const response = await config.onUserInputRequest(
            {
              question: "How should Copilot continue?",
              choices: ["Continue"],
              allowFreeform: true,
            },
            { sessionId: runtimeMock.state.lastSession.sessionId },
          );
          assert.deepStrictEqual(response, {
            answer: "",
            wasFreeform: true,
          });
          return runtimeMock.state.lastSession as unknown as CopilotSession;
        };

        const adapter = yield* CopilotAdapter;
        const threadId = asThreadId("copilot-bootstrap-user-input");

        const session = yield* adapter.startSession({
          provider: "copilot",
          threadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });

        assert.equal(session.provider, "copilot");
        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect("switches the injected Copilot developer instructions with plan mode", () =>
    Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const threadId = asThreadId("copilot-plan-mode-instructions");

      yield* adapter.startSession({
        provider: "copilot",
        threadId,
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });

      const config = runtimeMock.state.createSessionConfigs.at(-1);
      const systemMessage = config?.systemMessage;
      assert.ok(systemMessage);
      assert.equal(systemMessage.mode, "customize");
      const lastInstructions = systemMessage.sections?.last_instructions;
      assert.ok(lastInstructions);
      if (typeof lastInstructions.action !== "function") {
        assert.fail("expected a transform callback for the Copilot last_instructions section");
      }

      const renderInstructions = lastInstructions.action;
      const defaultInstructions = yield* Effect.promise(() =>
        Promise.resolve(renderInstructions("SDK base instructions")),
      );
      assert.match(defaultInstructions, /Collaboration Mode: Default/);

      yield* adapter.sendTurn({
        threadId,
        input: "plan this change",
        attachments: [],
        interactionMode: "plan",
      });

      const planInstructions = yield* Effect.promise(() =>
        Promise.resolve(renderInstructions("SDK base instructions")),
      );
      assert.match(planInstructions, /Plan Mode \(Conversational\)/);
      assert.match(planInstructions, /<proposed_plan>/);
      assert.deepStrictEqual(runtimeMock.state.lastSession.rpc.mode.set.mock.calls.at(-1), [
        { mode: "plan" },
      ]);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect(
    "renders Copilot Task_complete tool output as assistant text when no assistant message arrives",
    () =>
      Effect.gen(function* () {
        const adapter = yield* CopilotAdapter;
        const threadId = asThreadId("copilot-task-complete-assistant-fallback");

        yield* adapter.startSession({
          provider: "copilot",
          threadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });

        const turn = yield* adapter.sendTurn({
          threadId,
          input: "make an architecture diagram",
          attachments: [],
        });

        const config = runtimeMock.state.createSessionConfigs.at(-1);
        assert.ok(config?.onEvent);
        const emit = (event: SessionEvent) => config.onEvent?.(event);
        const resultText =
          "Task completed: **Architecture diagram prepared**\n\n```mermaid\nflowchart TD\n  Client --> Server\n```";

        emit({
          id: "evt-copilot-turn-start",
          timestamp: new Date().toISOString(),
          parentId: null,
          type: "assistant.turn_start",
          data: {
            turnId: "sdk-turn-1",
          },
        } as SessionEvent);
        emit({
          id: "evt-copilot-task-start",
          timestamp: new Date().toISOString(),
          parentId: null,
          type: "tool.execution_start",
          data: {
            toolCallId: "tool-task-complete",
            toolName: "Task_complete",
            arguments: {},
          },
        } as SessionEvent);
        emit({
          id: "evt-copilot-task-complete",
          timestamp: new Date().toISOString(),
          parentId: null,
          type: "tool.execution_complete",
          data: {
            toolCallId: "tool-task-complete",
            success: true,
            result: {
              content: resultText,
            },
          },
        } as SessionEvent);
        emit({
          id: "evt-copilot-idle",
          timestamp: new Date().toISOString(),
          parentId: null,
          type: "session.idle",
          data: {
            aborted: false,
          },
        } as SessionEvent);

        let thread = yield* adapter.readThread(threadId);
        for (
          let attempt = 0;
          attempt < 20 &&
          !thread.turns.some((entry) =>
            entry.items.some(
              (item) =>
                typeof item === "object" &&
                item !== null &&
                "type" in item &&
                item.type === "assistant_message",
            ),
          );
          attempt += 1
        ) {
          yield* waitForSdkEventQueue();
          thread = yield* adapter.readThread(threadId);
        }

        const turnSnapshot = thread.turns.find((entry) => entry.id === turn.turnId);
        assert.ok(turnSnapshot);
        const assistantItem = turnSnapshot.items.find(
          (item) =>
            typeof item === "object" &&
            item !== null &&
            "type" in item &&
            item.type === "assistant_message",
        );
        assert.deepStrictEqual(assistantItem, {
          type: "assistant_message",
          messageId: `copilot-task-completion-${String(turn.turnId)}`,
          content: resultText,
        });

        yield* adapter.stopSession(threadId);
      }),
  );

  it.effect("keeps Copilot turns queued until the SDK reports assistant.turn_start", () =>
    Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const threadId = asThreadId("copilot-turn-start-driven-running-state");

      yield* adapter.startSession({
        provider: "copilot",
        threadId,
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "hello",
        attachments: [],
      });

      let session = (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId);
      assert.ok(session);
      assert.equal(session.status, "ready");
      assert.equal(session.activeTurnId, undefined);

      const config = runtimeMock.state.createSessionConfigs.at(-1);
      assert.ok(config?.onEvent);
      config.onEvent?.({
        id: "evt-copilot-turn-start-running-state",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "assistant.turn_start",
        data: {
          turnId: "sdk-turn-running-state",
        },
      } as SessionEvent);

      yield* waitForSdkEventQueue();

      session = (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId);
      assert.ok(session);
      assert.equal(session.status, "running");
      assert.equal(session.activeTurnId, turn.turnId);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("maps queued Copilot turns to SDK turn_start events in order", () =>
    Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const threadId = asThreadId("copilot-queued-turn-order");

      yield* adapter.startSession({
        provider: "copilot",
        threadId,
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });

      const firstTurn = yield* adapter.sendTurn({
        threadId,
        input: "first",
        attachments: [],
      });
      const secondTurn = yield* adapter.sendTurn({
        threadId,
        input: "second",
        attachments: [],
      });

      const config = runtimeMock.state.createSessionConfigs.at(-1);
      assert.ok(config?.onEvent);
      const emit = (event: SessionEvent) => config.onEvent?.(event);

      emit({
        id: "evt-copilot-turn-start-1",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "assistant.turn_start",
        data: {
          turnId: "sdk-turn-1",
        },
      } as SessionEvent);
      emit({
        id: "evt-copilot-message-1",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "assistant.message",
        data: {
          messageId: "message-1",
          content: "first result",
        },
      } as SessionEvent);
      emit({
        id: "evt-copilot-turn-end-1",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "assistant.turn_end",
        data: {
          turnId: "sdk-turn-1",
        },
      } as SessionEvent);
      emit({
        id: "evt-copilot-turn-start-2",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "assistant.turn_start",
        data: {
          turnId: "sdk-turn-2",
        },
      } as SessionEvent);
      emit({
        id: "evt-copilot-message-2",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "assistant.message",
        data: {
          messageId: "message-2",
          content: "second result",
        },
      } as SessionEvent);
      emit({
        id: "evt-copilot-turn-end-2",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "assistant.turn_end",
        data: {
          turnId: "sdk-turn-2",
        },
      } as SessionEvent);

      for (let attempt = 0; attempt < 20; attempt += 1) {
        yield* waitForSdkEventQueue();
      }

      const thread = yield* adapter.readThread(threadId);
      const firstSnapshot = thread.turns.find((entry) => entry.id === firstTurn.turnId);
      const secondSnapshot = thread.turns.find((entry) => entry.id === secondTurn.turnId);
      assert.ok(firstSnapshot);
      assert.ok(secondSnapshot);
      assert.ok(
        firstSnapshot.items.some(
          (item) =>
            typeof item === "object" &&
            item !== null &&
            "messageId" in item &&
            item.messageId === "message-1",
        ),
      );
      assert.ok(
        secondSnapshot.items.some(
          (item) =>
            typeof item === "object" &&
            item !== null &&
            "messageId" in item &&
            item.messageId === "message-2",
        ),
      );

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("does not clear the running turn until the SDK abort event arrives", () =>
    Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const threadId = asThreadId("copilot-interrupt-waits-for-sdk-abort");

      yield* adapter.startSession({
        provider: "copilot",
        threadId,
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "interrupt me",
        attachments: [],
      });

      const config = runtimeMock.state.createSessionConfigs.at(-1);
      assert.ok(config?.onEvent);
      config.onEvent?.({
        id: "evt-copilot-turn-start-interrupt",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "assistant.turn_start",
        data: {
          turnId: "sdk-turn-interrupt",
        },
      } as SessionEvent);

      yield* waitForSdkEventQueue();
      yield* adapter.interruptTurn(threadId, turn.turnId);

      let session = (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId);
      assert.ok(session);
      assert.equal(session.status, "running");
      assert.equal(session.activeTurnId, turn.turnId);
      assert.equal(runtimeMock.state.lastSession.abort.mock.calls.length, 1);

      config.onEvent?.({
        id: "evt-copilot-abort-interrupt",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "abort",
        data: {
          reason: "Interrupted by user.",
        },
      } as SessionEvent);

      yield* waitForSdkEventQueue();

      session = (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId);
      assert.ok(session);
      assert.equal(session.status, "ready");
      assert.equal(session.activeTurnId, undefined);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("aborts an active Copilot turn before stopping the session", () =>
    Effect.gen(function* () {
      const adapter = yield* CopilotAdapter;
      const threadId = asThreadId("copilot-stop-session-aborts-active-turn");

      yield* adapter.startSession({
        provider: "copilot",
        threadId,
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });

      yield* adapter.sendTurn({
        threadId,
        input: "stop me",
        attachments: [],
      });

      const config = runtimeMock.state.createSessionConfigs.at(-1);
      assert.ok(config?.onEvent);
      config.onEvent?.({
        id: "evt-copilot-turn-start-stop",
        timestamp: new Date().toISOString(),
        parentId: null,
        type: "assistant.turn_start",
        data: {
          turnId: "sdk-turn-stop",
        },
      } as SessionEvent);

      yield* waitForSdkEventQueue();
      yield* adapter.stopSession(threadId);

      assert.equal(runtimeMock.state.lastSession.abort.mock.calls.length, 1);
      assert.equal(runtimeMock.state.lastSession.disconnect.mock.calls.length, 1);
    }),
  );

  it.effect("treats bare aborted send failures as cancelled without leaving a session error", () =>
    Effect.gen(function* () {
      runtimeMock.state.lastSession.send.mockRejectedValueOnce(new Error("aborted"));

      const adapter = yield* CopilotAdapter;
      const threadId = asThreadId("copilot-send-aborted-without-session-error");

      yield* adapter.startSession({
        provider: "copilot",
        threadId,
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });

      const turn = yield* adapter.sendTurn({
        threadId,
        input: "hello",
        attachments: [],
      });

      assert.ok(turn.turnId);

      const session = (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId);
      assert.ok(session);
      assert.equal(session.status, "ready");
      assert.equal(session.activeTurnId, undefined);
      assert.equal(session.lastError, undefined);

      yield* adapter.stopSession(threadId);
    }),
  );
});
