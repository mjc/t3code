import { CommandId, ThreadId, TurnId, type OrchestrationThreadShell } from "@t3tools/contracts";
import { Duration, Effect, Option } from "effect";

import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";

const RECONCILIATION_POLL_INTERVAL = Duration.millis(20);
const RECONCILIATION_MAX_POLLS = 100;

function getStaleRunningTurnId(thread: OrchestrationThreadShell): TurnId | null {
  return (
    thread.session?.activeTurnId ??
    (thread.latestTurn?.state === "running" ? thread.latestTurn.turnId : null)
  );
}

function hasStaleRunningSession(thread: OrchestrationThreadShell): boolean {
  return thread.session?.status === "running" || thread.session?.status === "starting";
}

function isThreadStale(
  thread: OrchestrationThreadShell,
  liveThreadIds: ReadonlySet<ThreadId>,
): boolean {
  return (
    !liveThreadIds.has(thread.id) &&
    (getStaleRunningTurnId(thread) !== null || hasStaleRunningSession(thread))
  );
}

const awaitProjectedSequence = (sequence: number) =>
  Effect.gen(function* () {
    const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;

    for (let attempt = 0; attempt < RECONCILIATION_MAX_POLLS; attempt += 1) {
      const { snapshotSequence } = yield* projectionSnapshotQuery.getSnapshotSequence();
      if (snapshotSequence >= sequence) {
        return;
      }
      yield* Effect.sleep(RECONCILIATION_POLL_INTERVAL);
    }

    yield* Effect.logWarning("stale.projected.running-thread.reconciliation.projection-timeout", {
      targetSequence: sequence,
      maxPolls: RECONCILIATION_MAX_POLLS,
    });
  });

const reconcileThreads = (threads: ReadonlyArray<OrchestrationThreadShell>) =>
  Effect.gen(function* () {
    const orchestrationEngine = yield* OrchestrationEngineService;
    const providerService = yield* ProviderService;
    const liveSessions = yield* providerService.listSessions();
    const liveThreadIds = new Set(liveSessions.map((session) => session.threadId));
    let maxSequence = 0;

    for (const thread of threads) {
      if (!isThreadStale(thread, liveThreadIds)) {
        continue;
      }

      const now = new Date().toISOString();
      const staleRunningTurnId = getStaleRunningTurnId(thread);

      if (staleRunningTurnId !== null) {
        const result = yield* orchestrationEngine.dispatch({
          type: "thread.turn.reconcile",
          commandId: CommandId.make(`server:thread-turn-reconcile:${crypto.randomUUID()}`),
          threadId: thread.id,
          turnId: staleRunningTurnId,
          createdAt: now,
        });
        maxSequence = Math.max(maxSequence, result.sequence);
      }

      if (thread.session && hasStaleRunningSession(thread)) {
        const result = yield* orchestrationEngine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make(`server:thread-session-set:${crypto.randomUUID()}`),
          threadId: thread.id,
          session: {
            ...thread.session,
            status: staleRunningTurnId !== null ? "interrupted" : "stopped",
            activeTurnId: null,
            updatedAt: now,
          },
          createdAt: now,
        });
        maxSequence = Math.max(maxSequence, result.sequence);
      }
    }

    if (maxSequence > 0) {
      yield* awaitProjectedSequence(maxSequence);
    }
  });

export const reconcileStaleProjectedRunningThreads = Effect.gen(function* () {
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const snapshot = yield* projectionSnapshotQuery.getShellSnapshot();

  yield* reconcileThreads(snapshot.threads);
});

export const reconcileStaleProjectedRunningThread = Effect.fn(
  "reconcileStaleProjectedRunningThread",
)(function* (threadId: ThreadId) {
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const thread = yield* projectionSnapshotQuery.getThreadShellById(threadId);

  if (Option.isNone(thread)) {
    return;
  }

  yield* reconcileThreads([thread.value]);
});
