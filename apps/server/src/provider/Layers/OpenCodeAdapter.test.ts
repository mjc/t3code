import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { Effect, Fiber, Layer, Option, Stream } from "effect";
import { beforeEach } from "vitest";

import { ApprovalRequestId, ThreadId } from "@t3tools/contracts";
import { ServerConfig } from "../../config.ts";
import { ProviderSessionRuntimeRepositoryLive } from "../../persistence/Layers/ProviderSessionRuntime.ts";
import { makeSqlitePersistenceLive } from "../../persistence/Layers/Sqlite.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { OpenCodeAdapter } from "../Services/OpenCodeAdapter.ts";
import {
  OpenCodeRuntime,
  OpenCodeRuntimeError,
  type OpenCodeRuntimeShape,
} from "../opencodeRuntime.ts";
import {
  appendOpenCodeAssistantTextDelta,
  makeOpenCodeAdapterLive,
  mergeOpenCodeAssistantText,
} from "./OpenCodeAdapter.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";

const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const asApprovalRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.make(value);

type MessageEntry = {
  info: {
    id: string;
    role: "user" | "assistant";
  };
  parts: Array<unknown>;
};

const CLOSED_EVENT_STREAM = Symbol("closed-opencode-event-stream");

const runtimeMock = {
  state: {
    startCalls: [] as string[],
    sessionCreateUrls: [] as string[],
    authHeaders: [] as Array<string | null>,
    abortCalls: [] as string[],
    closeCalls: [] as string[],
    questionReplyCalls: [] as Array<{ requestID: string; answers: string[][] }>,
    revertCalls: [] as Array<{ sessionID: string; messageID?: string }>,
    promptAsyncError: null as Error | null,
    closeError: null as Error | null,
    messages: [] as MessageEntry[],
    subscribedEvents: [] as unknown[],
    subscribedEventResolvers: [] as Array<(value: unknown | typeof CLOSED_EVENT_STREAM) => void>,
  },
  reset() {
    this.state.startCalls.length = 0;
    this.state.sessionCreateUrls.length = 0;
    this.state.authHeaders.length = 0;
    this.state.abortCalls.length = 0;
    this.state.closeCalls.length = 0;
    this.state.questionReplyCalls.length = 0;
    this.state.revertCalls.length = 0;
    this.state.promptAsyncError = null;
    this.state.closeError = null;
    this.state.messages = [];
    this.state.subscribedEvents = [];
    for (const resolve of this.state.subscribedEventResolvers.splice(0)) {
      resolve(CLOSED_EVENT_STREAM);
    }
  },
  pushSubscribedEvent(event: unknown) {
    const resolve = this.state.subscribedEventResolvers.shift();
    if (resolve) {
      resolve(event);
      return;
    }
    this.state.subscribedEvents.push(event);
  },
};

const OpenCodeRuntimeTestDouble: OpenCodeRuntimeShape = {
  startOpenCodeServerProcess: ({ binaryPath }) =>
    Effect.gen(function* () {
      runtimeMock.state.startCalls.push(binaryPath);
      const url = "http://127.0.0.1:4301";
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          runtimeMock.state.closeCalls.push(url);
          if (runtimeMock.state.closeError) {
            throw runtimeMock.state.closeError;
          }
        }),
      );
      return {
        url,
        exitCode: Effect.never,
      };
    }),
  connectToOpenCodeServer: ({ serverUrl }) =>
    Effect.gen(function* () {
      const url = serverUrl ?? "http://127.0.0.1:4301";
      // Unconditionally register a scope finalizer for test observability —
      // preserves the `closeCalls` / `closeError` probes that the existing
      // suites rely on. Production code never attaches a finalizer to an
      // external server (it simply returns `Effect.succeed(...)`).
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          runtimeMock.state.closeCalls.push(url);
          if (runtimeMock.state.closeError) {
            throw runtimeMock.state.closeError;
          }
        }),
      );
      return {
        url,
        exitCode: null,
        external: Boolean(serverUrl),
      };
    }),
  runOpenCodeCommand: () => Effect.succeed({ stdout: "", stderr: "", code: 0 }),
  createOpenCodeSdkClient: ({ baseUrl, serverPassword }) =>
    ({
      session: {
        create: async () => {
          runtimeMock.state.sessionCreateUrls.push(baseUrl);
          runtimeMock.state.authHeaders.push(
            serverPassword ? `Basic ${btoa(`opencode:${serverPassword}`)}` : null,
          );
          return { data: { id: `${baseUrl}/session` } };
        },
        abort: async ({ sessionID }: { sessionID: string }) => {
          runtimeMock.state.abortCalls.push(sessionID);
        },
        promptAsync: async () => {
          if (runtimeMock.state.promptAsyncError) {
            throw runtimeMock.state.promptAsyncError;
          }
        },
        messages: async () => ({ data: runtimeMock.state.messages }),
        revert: async ({ sessionID, messageID }: { sessionID: string; messageID?: string }) => {
          runtimeMock.state.revertCalls.push({
            sessionID,
            ...(messageID ? { messageID } : {}),
          });
          if (!messageID) {
            runtimeMock.state.messages = [];
            return;
          }

          const targetIndex = runtimeMock.state.messages.findIndex(
            (entry) => entry.info.id === messageID,
          );
          runtimeMock.state.messages =
            targetIndex >= 0
              ? runtimeMock.state.messages.slice(0, targetIndex + 1)
              : runtimeMock.state.messages;
        },
      },
      question: {
        reply: async ({ requestID, answers }: { requestID: string; answers: string[][] }) => {
          runtimeMock.state.questionReplyCalls.push({ requestID, answers });
        },
      },
      permission: {
        reply: async () => undefined,
      },
      event: {
        subscribe: async (_params?: unknown, options?: { signal?: AbortSignal }) => ({
          stream: (async function* () {
            const signal = options?.signal;
            while (!signal?.aborted) {
              const queued = runtimeMock.state.subscribedEvents.shift();
              if (queued !== undefined) {
                yield queued;
                continue;
              }
              const next = await new Promise<unknown | typeof CLOSED_EVENT_STREAM>((resolve) => {
                const onAbort = () => resolve(CLOSED_EVENT_STREAM);
                const wake = (value: unknown | typeof CLOSED_EVENT_STREAM) => {
                  clearTimeout(timeout);
                  signal?.removeEventListener("abort", onAbort);
                  resolve(value);
                };
                const timeout = setTimeout(() => {
                  const index = runtimeMock.state.subscribedEventResolvers.indexOf(wake);
                  if (index >= 0) {
                    runtimeMock.state.subscribedEventResolvers.splice(index, 1);
                  }
                  wake(CLOSED_EVENT_STREAM);
                }, 500);
                signal?.addEventListener("abort", onAbort, { once: true });
                runtimeMock.state.subscribedEventResolvers.push(wake);
              });
              if (next === CLOSED_EVENT_STREAM) {
                return;
              }
              yield next;
            }
          })(),
        }),
      },
    }) as unknown as ReturnType<OpenCodeRuntimeShape["createOpenCodeSdkClient"]>,
  loadOpenCodeInventory: () =>
    Effect.fail(
      new OpenCodeRuntimeError({
        operation: "loadOpenCodeInventory",
        detail: "OpenCodeRuntimeTestDouble.loadOpenCodeInventory not used in this test",
        cause: null,
      }),
    ),
};

const providerSessionDirectoryTestLayer = Layer.succeed(ProviderSessionDirectory, {
  upsert: () => Effect.void,
  getProvider: () =>
    Effect.die(new Error("ProviderSessionDirectory.getProvider is not used in test")),
  getBinding: () => Effect.succeed(Option.none()),
  listThreadIds: () => Effect.succeed([]),
  listBindings: () => Effect.succeed([]),
});

const OpenCodeAdapterTestLayer = makeOpenCodeAdapterLive().pipe(
  Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
  Layer.provideMerge(
    ServerSettingsService.layerTest({
      providers: {
        opencode: {
          binaryPath: "fake-opencode",
          serverUrl: "http://127.0.0.1:9999",
          serverPassword: "secret-password",
        },
      },
    }),
  ),
  Layer.provideMerge(providerSessionDirectoryTestLayer),
  Layer.provideMerge(NodeServices.layer),
);

beforeEach(() => {
  runtimeMock.reset();
});

const sleep = (ms: number) =>
  Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, ms)));

const waitFor = (predicate: () => boolean, timeoutMs = 1_000) =>
  Effect.promise(
    () =>
      new Promise<void>((resolve, reject) => {
        const startedAt = Date.now();
        const poll = () => {
          if (predicate()) {
            resolve();
            return;
          }
          if (Date.now() - startedAt >= timeoutMs) {
            reject(new Error("Timed out waiting for expectation."));
            return;
          }
          setTimeout(poll, 10);
        };
        poll();
      }),
  );

const makeTestDbPath = (name: string) => {
  const directory = fs.mkdtempSync(path.join(process.cwd(), `${name}-`));
  return {
    directory,
    dbPath: path.join(directory, "orchestration.sqlite"),
  };
};

const makePersistentOpenCodeAdapterLayer = (dbPath: string) => {
  const persistenceLayer = makeSqlitePersistenceLive(dbPath);
  const runtimeRepositoryLayer = ProviderSessionRuntimeRepositoryLive.pipe(
    Layer.provide(persistenceLayer),
  );
  const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
  return makeOpenCodeAdapterLive().pipe(
    Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
    Layer.provideMerge(
      ServerSettingsService.layerTest({
        providers: {
          opencode: {
            binaryPath: "fake-opencode",
            serverUrl: "http://127.0.0.1:9999",
            serverPassword: "secret-password",
          },
        },
      }),
    ),
    Layer.provideMerge(directoryLayer),
    Layer.provideMerge(NodeServices.layer),
  );
};

it.layer(OpenCodeAdapterTestLayer)("OpenCodeAdapterLive", (it) => {
  it.effect("reuses a configured OpenCode server URL instead of spawning a local server", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;

      const session = yield* adapter.startSession({
        provider: "opencode",
        threadId: asThreadId("thread-opencode"),
        runtimeMode: "full-access",
      });

      assert.equal(session.provider, "opencode");
      assert.equal(session.threadId, "thread-opencode");
      assert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "http://127.0.0.1:9999/session",
      });
      assert.deepEqual(runtimeMock.state.startCalls, []);
      assert.deepEqual(runtimeMock.state.sessionCreateUrls, ["http://127.0.0.1:9999"]);
      assert.deepEqual(runtimeMock.state.authHeaders, [
        `Basic ${btoa("opencode:secret-password")}`,
      ]);
    }),
  );

  it.effect("resumes an existing OpenCode session from the persisted resume cursor", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;

      const session = yield* adapter.startSession({
        provider: "opencode",
        threadId: asThreadId("thread-opencode-resume"),
        runtimeMode: "full-access",
        resumeCursor: {
          schemaVersion: 1,
          sessionId: "http://127.0.0.1:9999/restored-session",
        },
      });

      assert.equal(session.provider, "opencode");
      assert.equal(session.threadId, "thread-opencode-resume");
      assert.deepEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "http://127.0.0.1:9999/restored-session",
      });
      assert.equal(session.status, "ready");
      assert.equal(session.activeTurnId, undefined);
      assert.equal(session.lastError, undefined);
      assert.deepEqual(runtimeMock.state.sessionCreateUrls, []);
      assert.deepEqual(runtimeMock.state.authHeaders, []);
      yield* adapter.stopSession(asThreadId("thread-opencode-resume"));
    }),
  );

  it.effect("stops a configured-server session without trying to own server lifecycle", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      yield* adapter.startSession({
        provider: "opencode",
        threadId: asThreadId("thread-opencode"),
        runtimeMode: "full-access",
      });

      yield* adapter.stopSession(asThreadId("thread-opencode"));

      assert.deepEqual(runtimeMock.state.startCalls, []);
      assert.deepEqual(
        runtimeMock.state.abortCalls.includes("http://127.0.0.1:9999/session"),
        true,
      );
    }),
  );

  it.effect("clears session state even when cleanup finalizers throw", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      yield* adapter.startSession({
        provider: "opencode",
        threadId: asThreadId("thread-stop-all-a"),
        runtimeMode: "full-access",
      });
      yield* adapter.startSession({
        provider: "opencode",
        threadId: asThreadId("thread-stop-all-b"),
        runtimeMode: "full-access",
      });

      runtimeMock.state.closeError = new Error("close failed");
      // `stopAll` relies on `stopOpenCodeContext`, which is typed as
      // never-failing. A throwing finalizer surfaces as a defect — `Effect.exit`
      // captures it so the assertions can still run. The key invariant we're
      // validating is "the sessions map and close-call probes reflect cleanup
      // attempts regardless of finalizer outcome".
      yield* Effect.exit(adapter.stopAll());
      const sessions = yield* adapter.listSessions();

      assert.equal(runtimeMock.state.closeCalls.length >= 2, true);
      assert.deepEqual(runtimeMock.state.closeCalls.slice(0, 2), [
        "http://127.0.0.1:9999",
        "http://127.0.0.1:9999",
      ]);
      assert.deepEqual(sessions, []);
    }),
  );

  it.effect("rolls back session state when sendTurn fails before OpenCode accepts the prompt", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      yield* adapter.startSession({
        provider: "opencode",
        threadId: asThreadId("thread-send-turn-failure"),
        runtimeMode: "full-access",
      });

      runtimeMock.state.promptAsyncError = new Error("prompt failed");
      const error = yield* adapter
        .sendTurn({
          threadId: asThreadId("thread-send-turn-failure"),
          input: "Fix it",
          modelSelection: {
            provider: "opencode",
            model: "openai/gpt-5",
          },
        })
        .pipe(Effect.flip);
      const sessions = yield* adapter.listSessions();

      assert.equal(error._tag, "ProviderAdapterRequestError");
      if (error._tag !== "ProviderAdapterRequestError") {
        throw new Error("Unexpected error type");
      }
      assert.equal(error.detail, "prompt failed");
      assert.equal(
        error.message,
        "Provider adapter request failed (opencode) for session.promptAsync: prompt failed",
      );
      assert.equal(sessions.length, 1);
      assert.equal(sessions[0]?.status, "ready");
      assert.equal(sessions[0]?.activeTurnId, undefined);
      assert.equal(sessions[0]?.lastError, "prompt failed");
    }),
  );

  it.effect("emits a ready session state on resume and ignores stale idle cleanup", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-resume-ready-state");
      runtimeMock.state.subscribedEvents = [
        {
          type: "session.status",
          properties: {
            sessionID: "http://127.0.0.1:9999/restored-session",
            status: {
              type: "idle",
            },
          },
        },
      ];
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 4)).pipe(
        Effect.forkChild,
      );

      yield* adapter.startSession({
        provider: "opencode",
        threadId,
        runtimeMode: "full-access",
        resumeCursor: {
          schemaVersion: 1,
          sessionId: "http://127.0.0.1:9999/restored-session",
        },
      });

      const events = yield* Fiber.join(eventsFiber);

      const session = (yield* adapter.listSessions()).find((entry) => entry.threadId === threadId);
      assert.ok(session);
      assert.equal(session.status, "ready");
      assert.equal(session.activeTurnId, undefined);
      assert.equal(session.lastError, undefined);
      const eventTypes = Array.from(events).map((event) => event.type);
      assert.equal(eventTypes.includes("session.started"), true);
      assert.equal(eventTypes.includes("thread.started"), true);
      assert.equal(
        eventTypes.filter((eventType) => eventType === "session.state.changed").length >= 1,
        true,
      );
      assert.equal(eventTypes.includes("turn.completed"), false);

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("restores pending requests and queued turn ids after restart", () =>
    Effect.gen(function* () {
      const { directory, dbPath } = makeTestDbPath("t3-opencode-restart");
      const firstAdapterLayer = makePersistentOpenCodeAdapterLayer(dbPath);
      const secondAdapterLayer = makePersistentOpenCodeAdapterLayer(dbPath);
      const threadId = asThreadId("thread-opencode-restart-state");

      const { session, firstTurn, secondTurn } = yield* Effect.gen(function* () {
        const adapter = yield* OpenCodeAdapter;
        const session = yield* adapter.startSession({
          provider: "opencode",
          threadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });
        const sessionId = (session.resumeCursor as { sessionId: string }).sessionId;
        const firstTurn = yield* adapter.sendTurn({
          threadId,
          input: "first",
          attachments: [],
          modelSelection: {
            provider: "opencode",
            model: "openai/gpt-5",
          },
        });
        const secondTurn = yield* adapter.sendTurn({
          threadId,
          input: "second",
          attachments: [],
          modelSelection: {
            provider: "opencode",
            model: "openai/gpt-5",
          },
        });

        runtimeMock.pushSubscribedEvent({
          type: "session.status",
          properties: {
            sessionID: sessionId,
            status: { type: "busy" },
          },
        });
        runtimeMock.pushSubscribedEvent({
          type: "permission.asked",
          properties: {
            sessionID: sessionId,
            id: "permission-request-restart",
            permission: "bash",
            patterns: ["echo hello"],
            metadata: {},
          },
        });
        runtimeMock.pushSubscribedEvent({
          type: "question.asked",
          properties: {
            sessionID: sessionId,
            id: "question-request-restart",
            questions: [
              {
                header: "Sandbox mode",
                question: "Choose sandbox mode",
                options: ["workspace-write", "danger-full-access"],
              },
            ],
          },
        });
        yield* sleep(50);
        return { session, firstTurn, secondTurn } as const;
      }).pipe(Effect.provide(firstAdapterLayer));

      const events = yield* Effect.gen(function* () {
        const adapter = yield* OpenCodeAdapter;
        const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 13)).pipe(
          Effect.forkChild,
        );

        yield* adapter.startSession({
          provider: "opencode",
          threadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
          resumeCursor: session.resumeCursor,
        });
        yield* adapter.respondToRequest(
          threadId,
          asApprovalRequestId("permission-request-restart"),
          "accept",
        );
        yield* adapter.respondToUserInput(
          threadId,
          asApprovalRequestId("question-request-restart"),
          {
            "question-0-sandbox-mode": "workspace-write",
          },
        );

        const sessionId = (session.resumeCursor as { sessionId: string }).sessionId;
        runtimeMock.pushSubscribedEvent({
          type: "session.status",
          properties: {
            sessionID: sessionId,
            status: { type: "busy" },
          },
        });
        runtimeMock.pushSubscribedEvent({
          type: "session.status",
          properties: {
            sessionID: sessionId,
            status: { type: "idle" },
          },
        });
        runtimeMock.pushSubscribedEvent({
          type: "session.status",
          properties: {
            sessionID: sessionId,
            status: { type: "busy" },
          },
        });
        runtimeMock.pushSubscribedEvent({
          type: "session.status",
          properties: {
            sessionID: sessionId,
            status: { type: "idle" },
          },
        });
        return yield* Fiber.join(eventsFiber);
      }).pipe(Effect.provide(secondAdapterLayer));

      assert.deepEqual(runtimeMock.state.questionReplyCalls, [
        {
          requestID: "question-request-restart",
          answers: [["workspace-write"]],
        },
      ]);
      const turnEvents = Array.from(events).filter(
        (event) => event.type === "turn.started" || event.type === "turn.completed",
      );
      assert.deepEqual(
        turnEvents.map((event) => ({
          type: event.type,
          turnId: event.turnId ? `${event.turnId}` : undefined,
        })),
        [
          { type: "turn.started", turnId: `${firstTurn.turnId}` },
          { type: "turn.completed", turnId: `${firstTurn.turnId}` },
          { type: "turn.started", turnId: `${secondTurn.turnId}` },
          { type: "turn.completed", turnId: `${secondTurn.turnId}` },
        ],
      );

      fs.rmSync(directory, { recursive: true, force: true });
    }),
  );

  it.effect("clears pending approval and input state when a session stops", () =>
    Effect.gen(function* () {
      const { directory, dbPath } = makeTestDbPath("t3-opencode-stop-clears-pending");
      const firstAdapterLayer = makePersistentOpenCodeAdapterLayer(dbPath);
      const secondAdapterLayer = makePersistentOpenCodeAdapterLayer(dbPath);
      const threadId = asThreadId("thread-opencode-stop-clears-pending");

      const session = yield* Effect.gen(function* () {
        const adapter = yield* OpenCodeAdapter;
        const session = yield* adapter.startSession({
          provider: "opencode",
          threadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });
        const sessionId = (session.resumeCursor as { sessionId: string }).sessionId;
        runtimeMock.pushSubscribedEvent({
          type: "permission.asked",
          properties: {
            sessionID: sessionId,
            id: "permission-request-stop",
            permission: "bash",
            patterns: ["echo hello"],
            metadata: {},
          },
        });
        runtimeMock.pushSubscribedEvent({
          type: "question.asked",
          properties: {
            sessionID: sessionId,
            id: "question-request-stop",
            questions: [
              {
                header: "Sandbox mode",
                question: "Choose sandbox mode",
                options: ["workspace-write", "danger-full-access"],
              },
            ],
          },
        });
        yield* sleep(50);
        yield* adapter.stopSession(threadId);
        return session;
      }).pipe(Effect.provide(firstAdapterLayer));

      const [permissionReply, userInputReply] = yield* Effect.gen(function* () {
        const adapter = yield* OpenCodeAdapter;
        yield* adapter.startSession({
          provider: "opencode",
          threadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
          resumeCursor: session.resumeCursor,
        });
        const permissionReply = yield* Effect.exit(
          adapter.respondToRequest(
            threadId,
            asApprovalRequestId("permission-request-stop"),
            "accept",
          ),
        );
        const userInputReply = yield* Effect.exit(
          adapter.respondToUserInput(threadId, asApprovalRequestId("question-request-stop"), {
            "question-0-sandbox-mode": "workspace-write",
          }),
        );
        return [permissionReply, userInputReply] as const;
      }).pipe(Effect.provide(secondAdapterLayer));

      assert.equal(permissionReply._tag, "Failure");
      assert.equal(userInputReply._tag, "Failure");
      fs.rmSync(directory, { recursive: true, force: true });
    }),
  );

  it.effect("clears stale abort-like lastError state after an OpenCode abort", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-opencode-abort-like-error");
      const eventsFiber = yield* Stream.runCollect(Stream.take(adapter.streamEvents, 7)).pipe(
        Effect.forkChild,
      );

      const session = yield* adapter.startSession({
        provider: "opencode",
        threadId,
        runtimeMode: "approval-required",
      });
      const sessionId = (session.resumeCursor as { sessionId: string }).sessionId;
      const turn = yield* adapter.sendTurn({
        threadId,
        input: "abort me",
        attachments: [],
        modelSelection: {
          provider: "opencode",
          model: "openai/gpt-5",
        },
      });

      runtimeMock.pushSubscribedEvent({
        type: "session.status",
        properties: {
          sessionID: sessionId,
          status: { type: "busy" },
        },
      });
      runtimeMock.pushSubscribedEvent({
        type: "session.error",
        properties: {
          sessionID: sessionId,
          error: {
            data: {
              message: "Request was aborted by user.",
            },
          },
        },
      });

      const events = yield* Fiber.join(eventsFiber);
      const resumedSession = (yield* adapter.listSessions()).find(
        (entry) => entry.threadId === threadId,
      );
      assert.ok(resumedSession);
      assert.equal(resumedSession.status, "ready");
      assert.equal(resumedSession.activeTurnId, undefined);
      assert.equal(resumedSession.lastError, undefined);
      assert.deepEqual(
        Array.from(events)
          .filter((event) => event.type === "turn.completed" || event.type === "runtime.error")
          .map((event) => ({
            type: event.type,
            turnId: event.turnId ? `${event.turnId}` : undefined,
          })),
        [{ type: "turn.completed", turnId: `${turn.turnId}` }],
      );

      yield* adapter.stopSession(threadId);
    }),
  );

  it.effect("reverts the full thread when rollback removes every assistant turn", () =>
    Effect.gen(function* () {
      const adapter = yield* OpenCodeAdapter;
      const threadId = asThreadId("thread-rollback-all");
      yield* adapter.startSession({
        provider: "opencode",
        threadId,
        runtimeMode: "full-access",
      });

      runtimeMock.state.messages = [
        {
          info: { id: "assistant-1", role: "assistant" },
          parts: [],
        },
        {
          info: { id: "assistant-2", role: "assistant" },
          parts: [],
        },
      ];

      const snapshot = yield* adapter.rollbackThread(threadId, 2);

      assert.deepEqual(runtimeMock.state.revertCalls, [
        { sessionID: "http://127.0.0.1:9999/session" },
      ]);
      assert.deepEqual(snapshot.turns, []);
    }),
  );

  it.effect("deduplicates overlapping assistant text deltas after part updates", () =>
    Effect.sync(() => {
      const firstUpdate = mergeOpenCodeAssistantText(undefined, "Hello");
      const overlapDelta = appendOpenCodeAssistantTextDelta(firstUpdate.latestText, "lo world");
      const secondUpdate = mergeOpenCodeAssistantText(overlapDelta.nextText, "Hello world!");

      assert.deepEqual(
        [firstUpdate.deltaToEmit, overlapDelta.deltaToEmit, secondUpdate.deltaToEmit],
        ["Hello", " world", "!"],
      );
      assert.equal(secondUpdate.latestText, "Hello world!");
    }),
  );

  it.effect("keeps the event pump alive when native event logging fails", () =>
    Effect.gen(function* () {
      runtimeMock.state.subscribedEvents = [
        {
          type: "message.updated",
          properties: {
            sessionID: "http://127.0.0.1:9999/session",
            info: {
              id: "msg-native-log-failure",
              role: "assistant",
            },
          },
        },
      ];

      const nativeEventLogger = {
        filePath: "memory://opencode-native-events",
        write: () => Effect.die(new Error("native log write failed")),
        close: () => Effect.void,
      };

      const adapterLayer = makeOpenCodeAdapterLive({
        nativeEventLogger,
      }).pipe(
        Layer.provideMerge(Layer.succeed(OpenCodeRuntime, OpenCodeRuntimeTestDouble)),
        Layer.provideMerge(ServerConfig.layerTest(process.cwd(), process.cwd())),
        Layer.provideMerge(
          ServerSettingsService.layerTest({
            providers: {
              opencode: {
                binaryPath: "fake-opencode",
                serverUrl: "http://127.0.0.1:9999",
                serverPassword: "secret-password",
              },
            },
          }),
        ),
        Layer.provideMerge(providerSessionDirectoryTestLayer),
        Layer.provideMerge(NodeServices.layer),
      );

      // Capture closeCalls *inside* the provided layer scope: the adapter's
      // layer finalizer now tears down any live sessions when the layer
      // closes (which is exactly what we want for leak prevention), so
      // inspecting closeCalls after `Effect.provide` completes would observe
      // the teardown — not the behavior under test. We care that the event
      // pump kept the session alive while logging was failing.
      const { sessions, closeCallsDuringRun } = yield* Effect.gen(function* () {
        const adapter = yield* OpenCodeAdapter;
        yield* adapter.startSession({
          provider: "opencode",
          threadId: asThreadId("thread-native-log-failure"),
          runtimeMode: "full-access",
        });
        yield* sleep(10);
        return {
          sessions: yield* adapter.listSessions(),
          closeCallsDuringRun: [...runtimeMock.state.closeCalls],
        };
      }).pipe(Effect.provide(adapterLayer));

      assert.equal(sessions.length, 1);
      assert.equal(sessions[0]?.threadId, "thread-native-log-failure");
      assert.deepEqual(closeCallsDuringRun, []);
    }),
  );
});
