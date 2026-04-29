import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ThreadId,
  type OrchestrationEvent,
  type OrchestrationThread,
} from "@t3tools/contracts";
import { Deferred, Effect, Fiber, Option, Queue, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { buildThreadSubscriptionStream } from "./ws.ts";

function makeEvent(input: {
  sequence: number;
  type: OrchestrationEvent["type"];
  occurredAt: string;
  aggregateKind: OrchestrationEvent["aggregateKind"];
  aggregateId: string;
  commandId: string | null;
  payload: unknown;
}): OrchestrationEvent {
  return {
    sequence: input.sequence,
    eventId: EventId.make(`event-${input.sequence}`),
    type: input.type,
    aggregateKind: input.aggregateKind,
    aggregateId:
      input.aggregateKind === "project"
        ? ProjectId.make(input.aggregateId)
        : ThreadId.make(input.aggregateId),
    occurredAt: input.occurredAt,
    commandId: input.commandId === null ? null : CommandId.make(input.commandId),
    causationEventId: null,
    correlationId: null,
    metadata: {},
    payload: input.payload as never,
  } as OrchestrationEvent;
}

function createThreadSnapshot(
  threadId: ThreadId,
  includeFirstMessage: boolean,
): OrchestrationThread {
  const createdAt = "2026-02-23T10:00:00.000Z";
  return {
    id: threadId,
    projectId: ProjectId.make("project-1"),
    title: "demo",
    modelSelection: {
      provider: "copilot",
      model: "gpt-4.1",
    },
    runtimeMode: "approval-required",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    createdAt,
    updatedAt: includeFirstMessage ? "2026-02-23T10:00:01.000Z" : createdAt,
    archivedAt: null,
    deletedAt: null,
    messages: includeFirstMessage
      ? [
          {
            id: MessageId.make("user-msg-1"),
            role: "user",
            text: "First edit",
            turnId: null,
            streaming: false,
            createdAt: "2026-02-23T10:00:01.000Z",
            updatedAt: "2026-02-23T10:00:01.000Z",
          },
        ]
      : [],
    proposedPlans: [],
    activities: [],
    checkpoints: [],
    session: {
      threadId,
      status: "ready",
      providerName: "copilot",
      runtimeMode: "approval-required",
      activeTurnId: null,
      lastError: null,
      updatedAt: includeFirstMessage ? "2026-02-23T10:00:01.000Z" : createdAt,
    },
  };
}

describe("buildThreadSubscriptionStream", () => {
  it("replays thread detail events that arrive while the snapshot is loading", async () => {
    const threadId = ThreadId.make("thread-race");
    const snapshot = createThreadSnapshot(threadId, false);
    const liveEvent = makeEvent({
      sequence: 2,
      type: "thread.message-sent",
      aggregateKind: "thread",
      aggregateId: threadId,
      occurredAt: "2026-02-23T10:00:01.000Z",
      commandId: "cmd-user-1",
      payload: {
        threadId,
        messageId: "user-msg-1",
        role: "user",
        text: "First edit",
        turnId: null,
        streaming: false,
        createdAt: "2026-02-23T10:00:01.000Z",
        updatedAt: "2026-02-23T10:00:01.000Z",
      },
    });

    const items = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const snapshotStarted = yield* Deferred.make<void>();
          const releaseSnapshot = yield* Deferred.make<void>();
          const queue = yield* Queue.unbounded<OrchestrationEvent>();
          const streamFiber = yield* buildThreadSubscriptionStream({
            threadId,
            getThreadDetail: Deferred.succeed(snapshotStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseSnapshot)),
              Effect.as(Option.some(snapshot)),
            ),
            getSnapshotSequence: Effect.succeed(1),
            streamDomainEvents: Stream.fromQueue(queue),
          }).pipe(Effect.forkScoped);

          yield* Deferred.await(snapshotStarted);
          yield* Queue.offer(queue, liveEvent);
          yield* Deferred.succeed(releaseSnapshot, undefined);

          const stream = yield* Fiber.join(streamFiber);
          return Array.from(yield* Stream.runCollect(Stream.take(stream, 2)));
        }),
      ),
    );

    expect(items).toHaveLength(2);
    expect(items[0]).toMatchObject({
      kind: "snapshot",
      snapshot: {
        snapshotSequence: 1,
        thread: {
          id: threadId,
        },
      },
    });
    expect(items[1]).toMatchObject({
      kind: "event",
      event: {
        sequence: 2,
        type: "thread.message-sent",
      },
    });
  });

  it("drops queued live events already covered by the loaded snapshot sequence", async () => {
    const threadId = ThreadId.make("thread-filtered");
    const snapshot = createThreadSnapshot(threadId, true);
    const coveredEvent = makeEvent({
      sequence: 2,
      type: "thread.message-sent",
      aggregateKind: "thread",
      aggregateId: threadId,
      occurredAt: "2026-02-23T10:00:01.000Z",
      commandId: "cmd-user-1",
      payload: {
        threadId,
        messageId: "user-msg-1",
        role: "user",
        text: "First edit",
        turnId: null,
        streaming: false,
        createdAt: "2026-02-23T10:00:01.000Z",
        updatedAt: "2026-02-23T10:00:01.000Z",
      },
    });

    const items = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const snapshotStarted = yield* Deferred.make<void>();
          const releaseSnapshot = yield* Deferred.make<void>();
          const queue = yield* Queue.unbounded<OrchestrationEvent>();
          const streamFiber = yield* buildThreadSubscriptionStream({
            threadId,
            getThreadDetail: Deferred.succeed(snapshotStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseSnapshot)),
              Effect.as(Option.some(snapshot)),
            ),
            getSnapshotSequence: Effect.succeed(2),
            streamDomainEvents: Stream.fromQueue(queue),
          }).pipe(Effect.forkScoped);

          yield* Deferred.await(snapshotStarted);
          yield* Queue.offer(queue, coveredEvent);
          yield* Deferred.succeed(releaseSnapshot, undefined);

          const stream = yield* Fiber.join(streamFiber);
          return Array.from(yield* Stream.runCollect(Stream.take(stream, 1)));
        }),
      ),
    );

    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({
      kind: "snapshot",
      snapshot: {
        snapshotSequence: 2,
        thread: {
          id: threadId,
          messages: [{ id: "user-msg-1" }],
        },
      },
    });
  });

  it("streams thread.turn-reconciled as a thread detail event", async () => {
    const threadId = ThreadId.make("thread-reconciled");
    const snapshot = createThreadSnapshot(threadId, false);
    const reconciledEvent = makeEvent({
      sequence: 2,
      type: "thread.turn-reconciled",
      aggregateKind: "thread",
      aggregateId: threadId,
      occurredAt: "2026-02-23T10:00:02.000Z",
      commandId: "cmd-reconcile-1",
      payload: {
        threadId,
        turnId: "turn-1",
        createdAt: "2026-02-23T10:00:02.000Z",
      },
    });

    const items = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const queue = yield* Queue.unbounded<OrchestrationEvent>();
          yield* Queue.offer(queue, reconciledEvent);

          const stream = yield* buildThreadSubscriptionStream({
            threadId,
            getThreadDetail: Effect.succeed(Option.some(snapshot)),
            getSnapshotSequence: Effect.succeed(1),
            streamDomainEvents: Stream.fromQueue(queue),
          });

          return Array.from(yield* Stream.runCollect(Stream.take(stream, 2)));
        }),
      ),
    );

    expect(items).toHaveLength(2);
    expect(items[1]).toMatchObject({
      kind: "event",
      event: {
        sequence: 2,
        type: "thread.turn-reconciled",
      },
    });
  });
});
