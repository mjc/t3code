import * as NodeServices from "@effect/platform-node/NodeServices";
import { DEFAULT_MODEL_BY_PROVIDER, ProjectId, ThreadId, TurnId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Option, Ref, Stream } from "effect";

import { ServerConfig } from "./config.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "./orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./orchestration/Services/ProjectionSnapshotQuery.ts";
import { AnalyticsService } from "./telemetry/Services/AnalyticsService.ts";
import {
  getAutoBootstrapDefaultModelSelection,
  launchStartupHeartbeat,
  makeCommandGate,
  reconcileStaleProjectedRunningThreads,
  resolveAutoBootstrapWelcomeTargets,
  resolveWelcomeBase,
  ServerRuntimeStartupError,
} from "./serverRuntimeStartup.ts";
import { ProviderService } from "./provider/Services/ProviderService.ts";

it("uses the canonical Codex default for auto-bootstrapped model selection", () => {
  assert.deepStrictEqual(getAutoBootstrapDefaultModelSelection(), {
    provider: "codex",
    model: DEFAULT_MODEL_BY_PROVIDER.codex,
  });
});

it.effect("enqueueCommand waits for readiness and then drains queued work", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const executionCount = yield* Ref.make(0);
      const commandGate = yield* makeCommandGate;

      const queuedCommandFiber = yield* commandGate
        .enqueueCommand(Ref.updateAndGet(executionCount, (count) => count + 1))
        .pipe(Effect.forkScoped);

      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(executionCount), 0);

      yield* commandGate.signalCommandReady;

      const result = yield* Fiber.join(queuedCommandFiber);
      assert.equal(result, 1);
      assert.equal(yield* Ref.get(executionCount), 1);
    }),
  ),
);

it.effect("enqueueCommand fails queued work when readiness fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const commandGate = yield* makeCommandGate;
      const failure = yield* Deferred.make<void, never>();

      const queuedCommandFiber = yield* commandGate
        .enqueueCommand(Deferred.await(failure).pipe(Effect.as("should-not-run")))
        .pipe(Effect.forkScoped);

      yield* commandGate.failCommandReady(
        new ServerRuntimeStartupError({
          message: "startup failed",
        }),
      );

      const error = yield* Effect.flip(Fiber.join(queuedCommandFiber));
      assert.equal(error.message, "startup failed");
    }),
  ),
);

it.effect("launchStartupHeartbeat does not block the caller while counts are loading", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const releaseCounts = yield* Deferred.make<void, never>();

      yield* launchStartupHeartbeat.pipe(
        Effect.provideService(ProjectionSnapshotQuery, {
          getCommandReadModel: () => Effect.die("unused"),
          getSnapshot: () => Effect.die("unused"),
          getShellSnapshot: () => Effect.die("unused"),
          getSnapshotSequence: () => Effect.die("unused"),
          getCounts: () =>
            Deferred.await(releaseCounts).pipe(
              Effect.as({
                projectCount: 2,
                threadCount: 3,
              }),
            ),
          getActiveProjectByWorkspaceRoot: () => Effect.succeed(Option.none()),
          getProjectShellById: () => Effect.succeed(Option.none()),
          getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.none()),
          getThreadCheckpointContext: () => Effect.succeed(Option.none()),
          getThreadShellById: () => Effect.succeed(Option.none()),
          getThreadDetailById: () => Effect.succeed(Option.none()),
        }),
        Effect.provideService(AnalyticsService, {
          record: () => Effect.void,
          flush: Effect.void,
        }),
      );
    }),
  ),
);

it.effect("resolveWelcomeBase derives cwd and project name from server config", () =>
  Effect.gen(function* () {
    const welcome = yield* resolveWelcomeBase.pipe(
      Effect.provideService(ServerConfig, {
        cwd: "/tmp/startup-project",
      } as never),
    );

    assert.deepStrictEqual(welcome, {
      cwd: "/tmp/startup-project",
      projectName: "startup-project",
    });
  }),
);

it.effect("resolveAutoBootstrapWelcomeTargets returns existing project and thread ids", () => {
  const bootstrapProjectId = ProjectId.make("project-startup-bootstrap");
  const bootstrapThreadId = ThreadId.make("thread-startup-bootstrap");

  return Effect.gen(function* () {
    const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);
    const targets = yield* resolveAutoBootstrapWelcomeTargets.pipe(
      Effect.provideService(ServerConfig, {
        cwd: "/tmp/startup-project",
        autoBootstrapProjectFromCwd: true,
      } as never),
      Effect.provideService(ProjectionSnapshotQuery, {
        getCommandReadModel: () => Effect.die("unused"),
        getSnapshot: () => Effect.die("unused"),
        getShellSnapshot: () => Effect.die("unused"),
        getSnapshotSequence: () => Effect.die("unused"),
        getCounts: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () =>
          Effect.succeed(
            Option.some({
              id: bootstrapProjectId,
              title: "Startup Project",
              workspaceRoot: "/tmp/startup-project",
              defaultModelSelection: getAutoBootstrapDefaultModelSelection(),
              scripts: [],
              createdAt: "2026-01-01T00:00:00.000Z",
              updatedAt: "2026-01-01T00:00:00.000Z",
              deletedAt: null,
            }),
          ),
        getProjectShellById: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.some(bootstrapThreadId)),
        getThreadCheckpointContext: () => Effect.succeed(Option.none()),
        getThreadShellById: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
      }),
      Effect.provideService(OrchestrationEngineService, {
        getReadModel: () => Effect.die("unused"),
        readEvents: () => Stream.empty,
        dispatch: (command) =>
          Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
            Effect.as({ sequence: 1 }),
          ),
        streamDomainEvents: Stream.empty,
      } satisfies OrchestrationEngineShape),
      Effect.provide(NodeServices.layer),
    );

    assert.deepStrictEqual(targets, {
      bootstrapProjectId,
      bootstrapThreadId,
    });
    assert.deepStrictEqual(yield* Ref.get(dispatchCalls), []);
  });
});

it.effect("resolveAutoBootstrapWelcomeTargets creates a project and thread when missing", () =>
  Effect.gen(function* () {
    const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);
    const targets = yield* resolveAutoBootstrapWelcomeTargets.pipe(
      Effect.provideService(ServerConfig, {
        cwd: "/tmp/startup-project",
        autoBootstrapProjectFromCwd: true,
      } as never),
      Effect.provideService(ProjectionSnapshotQuery, {
        getCommandReadModel: () => Effect.die("unused"),
        getSnapshot: () => Effect.die("unused"),
        getShellSnapshot: () => Effect.die("unused"),
        getSnapshotSequence: () => Effect.die("unused"),
        getCounts: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () => Effect.succeed(Option.none()),
        getProjectShellById: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.none()),
        getThreadCheckpointContext: () => Effect.succeed(Option.none()),
        getThreadShellById: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
      }),
      Effect.provideService(OrchestrationEngineService, {
        getReadModel: () => Effect.die("unused"),
        readEvents: () => Stream.empty,
        dispatch: (command) =>
          Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
            Effect.as({ sequence: 1 }),
          ),
        streamDomainEvents: Stream.empty,
      } satisfies OrchestrationEngineShape),
      Effect.provide(NodeServices.layer),
    );

    assert.equal(typeof targets.bootstrapProjectId, "string");
    assert.equal(typeof targets.bootstrapThreadId, "string");
    assert.deepStrictEqual(yield* Ref.get(dispatchCalls), ["project.create", "thread.create"]);
  }),
);

it.effect("reconciles stale projected running sessions when no live provider session exists", () =>
  Effect.gen(function* () {
    const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);
    const projectedSequence = yield* Ref.make(0);

    yield* reconcileStaleProjectedRunningThreads.pipe(
      Effect.provideService(ProjectionSnapshotQuery, {
        getCommandReadModel: () => Effect.die("unused"),
        getSnapshot: () => Effect.die("unused"),
        getShellSnapshot: () =>
          Effect.succeed({
            snapshotSequence: 1,
            updatedAt: "2026-04-29T00:00:00.000Z",
            projects: [],
            threads: [
              {
                id: ThreadId.make("thread-stale-running"),
                projectId: ProjectId.make("project-1"),
                title: "Stale Thread",
                modelSelection: getAutoBootstrapDefaultModelSelection(),
                runtimeMode: "full-access",
                interactionMode: "default",
                branch: null,
                worktreePath: null,
                createdAt: "2026-04-29T00:00:00.000Z",
                updatedAt: "2026-04-29T00:00:00.000Z",
                archivedAt: null,
                session: {
                  threadId: ThreadId.make("thread-stale-running"),
                  status: "running",
                  providerName: "codex",
                  runtimeMode: "full-access",
                  activeTurnId: "turn-stale-running",
                  lastError: null,
                  updatedAt: "2026-04-29T00:00:00.000Z",
                },
                latestTurn: {
                  turnId: "turn-stale-running",
                  state: "running",
                  requestedAt: "2026-04-29T00:00:00.000Z",
                  startedAt: "2026-04-29T00:00:01.000Z",
                  completedAt: null,
                  assistantMessageId: null,
                },
                latestUserMessageAt: null,
                hasPendingApprovals: false,
                hasPendingUserInput: false,
                hasActionableProposedPlan: false,
              },
            ],
          } as never),
        getSnapshotSequence: () =>
          Ref.get(projectedSequence).pipe(Effect.map((snapshotSequence) => ({ snapshotSequence }))),
        getCounts: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
        getProjectShellById: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
        getThreadCheckpointContext: () => Effect.succeed(Option.none()),
        getThreadShellById: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
      }),
      Effect.provideService(ProviderService, {
        startSession: () => Effect.die("unused"),
        sendTurn: () => Effect.die("unused"),
        interruptTurn: () => Effect.die("unused"),
        respondToRequest: () => Effect.die("unused"),
        respondToUserInput: () => Effect.die("unused"),
        stopSession: () => Effect.die("unused"),
        listSessions: () => Effect.succeed([]),
        getCapabilities: () => Effect.die("unused"),
        rollbackConversation: () => Effect.die("unused"),
        streamEvents: Stream.empty,
      }),
      Effect.provideService(OrchestrationEngineService, {
        getReadModel: () => Effect.die("unused"),
        readEvents: () => Stream.empty,
        dispatch: (command) =>
          Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
            Effect.andThen(Ref.updateAndGet(projectedSequence, (sequence) => sequence + 1)),
            Effect.map((sequence) => ({ sequence })),
          ),
        streamDomainEvents: Stream.empty,
      } satisfies OrchestrationEngineShape),
    );

    assert.deepStrictEqual(yield* Ref.get(dispatchCalls), [
      "thread.turn.reconcile",
      "thread.session.set",
    ]);
  }),
);

it.effect(
  "does not reconcile projected running sessions that already have a live provider session",
  () =>
    Effect.gen(function* () {
      const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);

      yield* reconcileStaleProjectedRunningThreads.pipe(
        Effect.provideService(ProjectionSnapshotQuery, {
          getCommandReadModel: () => Effect.die("unused"),
          getSnapshot: () => Effect.die("unused"),
          getShellSnapshot: () =>
            Effect.succeed({
              snapshotSequence: 1,
              updatedAt: "2026-04-29T00:00:00.000Z",
              projects: [],
              threads: [
                {
                  id: ThreadId.make("thread-live-running"),
                  projectId: ProjectId.make("project-1"),
                  title: "Live Thread",
                  modelSelection: getAutoBootstrapDefaultModelSelection(),
                  runtimeMode: "full-access",
                  interactionMode: "default",
                  branch: null,
                  worktreePath: null,
                  createdAt: "2026-04-29T00:00:00.000Z",
                  updatedAt: "2026-04-29T00:00:00.000Z",
                  archivedAt: null,
                  session: {
                    threadId: ThreadId.make("thread-live-running"),
                    status: "running",
                    providerName: "codex",
                    runtimeMode: "full-access",
                    activeTurnId: "turn-live-running",
                    lastError: null,
                    updatedAt: "2026-04-29T00:00:00.000Z",
                  },
                  latestTurn: {
                    turnId: "turn-live-running",
                    state: "running",
                    requestedAt: "2026-04-29T00:00:00.000Z",
                    startedAt: "2026-04-29T00:00:01.000Z",
                    completedAt: null,
                    assistantMessageId: null,
                  },
                  latestUserMessageAt: null,
                  hasPendingApprovals: false,
                  hasPendingUserInput: false,
                  hasActionableProposedPlan: false,
                },
              ],
            } as never),
          getSnapshotSequence: () => Effect.succeed({ snapshotSequence: 0 }),
          getCounts: () => Effect.die("unused"),
          getActiveProjectByWorkspaceRoot: () => Effect.die("unused"),
          getProjectShellById: () => Effect.die("unused"),
          getFirstActiveThreadIdByProjectId: () => Effect.die("unused"),
          getThreadCheckpointContext: () => Effect.succeed(Option.none()),
          getThreadShellById: () => Effect.die("unused"),
          getThreadDetailById: () => Effect.die("unused"),
        }),
        Effect.provideService(ProviderService, {
          startSession: () => Effect.die("unused"),
          sendTurn: () => Effect.die("unused"),
          interruptTurn: () => Effect.die("unused"),
          respondToRequest: () => Effect.die("unused"),
          respondToUserInput: () => Effect.die("unused"),
          stopSession: () => Effect.die("unused"),
          listSessions: () =>
            Effect.succeed([
              {
                provider: "codex",
                threadId: ThreadId.make("thread-live-running"),
                status: "running",
                runtimeMode: "full-access",
                activeTurnId: TurnId.make("turn-live-running"),
                createdAt: "2026-04-29T00:00:00.000Z",
                updatedAt: "2026-04-29T00:00:00.000Z",
              },
            ]),
          getCapabilities: () => Effect.die("unused"),
          rollbackConversation: () => Effect.die("unused"),
          streamEvents: Stream.empty,
        }),
        Effect.provideService(OrchestrationEngineService, {
          getReadModel: () => Effect.die("unused"),
          readEvents: () => Stream.empty,
          dispatch: (command) =>
            Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
              Effect.as({ sequence: 1 }),
            ),
          streamDomainEvents: Stream.empty,
        } satisfies OrchestrationEngineShape),
      );

      assert.deepStrictEqual(yield* Ref.get(dispatchCalls), []);
    }),
);
