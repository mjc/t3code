import { randomUUID } from "node:crypto";
import * as nodePath from "node:path";

import type {
  CopilotClient,
  CopilotSession,
  MessageOptions,
  PermissionRequest,
  PermissionRequestResult,
  SessionConfig,
  SessionEvent,
} from "@github/copilot-sdk";
import {
  EventId,
  type ModelSelection,
  type ProviderRuntimeEvent,
  type ProviderRuntimeTurnStatus,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  RuntimeRequestId,
  type ThreadTokenUsageSnapshot,
  ThreadId,
  TurnId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import { Effect, Layer, PubSub, Schema, Stream } from "effect";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { CopilotAdapter, type CopilotAdapterShape } from "../Services/CopilotAdapter.ts";
import { createCopilotClient } from "../copilotRuntime.ts";
import { type EventNdjsonLogger, makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = "copilot" as const;
const COPILOT_RESUME_SCHEMA_VERSION = 1 as const;

type CopilotMode = "interactive" | "plan" | "autopilot";
type CopilotReasoningEffort = NonNullable<SessionConfig["reasoningEffort"]>;
type CopilotUserInputRequest = Parameters<NonNullable<SessionConfig["onUserInputRequest"]>>[0];
type CopilotUserInputResponse = Awaited<
  ReturnType<NonNullable<SessionConfig["onUserInputRequest"]>>
>;
type SessionPermissionRequestedEvent = Extract<SessionEvent, { type: "permission.requested" }>;
type SessionUserInputRequestedEvent = Extract<SessionEvent, { type: "user_input.requested" }>;
type SessionPermissionRequest = SessionPermissionRequestedEvent["data"]["permissionRequest"];

interface CopilotAdapterLiveOptions {
  readonly nativeEventLogPath?: string;
  readonly nativeEventLogger?: EventNdjsonLogger;
}

const getCopilotReasoningEffort = (
  modelSelection: ModelSelection | undefined,
): CopilotReasoningEffort | undefined => {
  const reasoningEffort = getModelSelectionStringOptionValue(modelSelection, "reasoningEffort");
  return reasoningEffort ? (reasoningEffort as CopilotReasoningEffort) : undefined;
};

interface PromiseKit<T> {
  readonly promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
}

interface CopilotTurnSnapshot {
  readonly id: TurnId;
  readonly items: Array<unknown>;
}

interface PendingPermissionHandler {
  readonly signature: string;
  readonly promiseKit: PromiseKit<PermissionRequestResult>;
}

interface PendingUserInputHandler {
  readonly signature: string;
  readonly promiseKit: PromiseKit<CopilotUserInputResponse>;
}

interface PendingPermissionBinding {
  readonly requestId: string;
  readonly requestType:
    | "command_execution_approval"
    | "file_read_approval"
    | "file_change_approval";
  readonly promiseKit: PromiseKit<PermissionRequestResult>;
}

interface PendingUserInputBinding {
  readonly requestId: string;
  readonly question: string;
  readonly choices: ReadonlyArray<string>;
  readonly allowFreeform: boolean;
  readonly promiseKit: PromiseKit<CopilotUserInputResponse>;
}

interface ToolMeta {
  readonly toolName: string;
  readonly itemType:
    | "command_execution"
    | "file_change"
    | "mcp_tool_call"
    | "dynamic_tool_call"
    | "collab_agent_tool_call"
    | "web_search"
    | "image_view";
}

interface CopilotSessionContext {
  readonly threadId: ThreadId;
  readonly client: CopilotClient;
  readonly sdkSession: CopilotSession;
  session: ProviderSession;
  readonly cwd: string;
  readonly turns: Array<CopilotTurnSnapshot>;
  readonly queuedTurnIds: Array<TurnId>;
  readonly sdkTurnIdsToTurnIds: Map<string, TurnId>;
  readonly completedTurnIds: Set<TurnId>;
  readonly turnUsageByTurnId: Map<TurnId, ThreadTokenUsageSnapshot>;
  readonly pendingPermissionHandlersBySignature: Map<string, Array<PendingPermissionHandler>>;
  readonly pendingPermissionEventsBySignature: Map<
    string,
    Array<SessionPermissionRequestedEvent["data"]>
  >;
  readonly pendingPermissionBindings: Map<string, PendingPermissionBinding>;
  readonly pendingUserInputHandlersBySignature: Map<string, Array<PendingUserInputHandler>>;
  readonly pendingUserInputEventsBySignature: Map<
    string,
    Array<SessionUserInputRequestedEvent["data"]>
  >;
  readonly pendingUserInputBindings: Map<string, PendingUserInputBinding>;
  readonly toolMetaById: Map<string, ToolMeta>;
  readonly turnIdByProviderItemId: Map<string, TurnId>;
  readonly emittedTextByItemId: Map<string, string>;
  readonly startedItemIds: Set<string>;
  activeTurnId: TurnId | undefined;
  activeSdkTurnId: string | undefined;
  eventChain: Promise<void>;
  stopped: boolean;
}

function nowIso(): string {
  return new Date().toISOString();
}

function createPromiseKit<T>(): PromiseKit<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function trimToUndefined(value: string | null | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

function parseCopilotResumeCursor(raw: unknown): { sessionId: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== COPILOT_RESUME_SCHEMA_VERSION) return undefined;
  if (typeof raw.sessionId !== "string") return undefined;
  const sessionId = raw.sessionId.trim();
  return sessionId.length > 0 ? { sessionId } : undefined;
}

function toCopilotResumeCursor(sessionId: string): { schemaVersion: 1; sessionId: string } {
  return {
    schemaVersion: COPILOT_RESUME_SCHEMA_VERSION,
    sessionId,
  };
}

function createBaseEvent(input: {
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly createdAt?: string | undefined;
  readonly raw?: SessionEvent | undefined;
}) {
  return {
    eventId: EventId.make(randomUUID()),
    provider: PROVIDER,
    threadId: input.threadId,
    createdAt: input.createdAt ?? nowIso(),
    ...(input.turnId ? { turnId: input.turnId } : {}),
    ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
    ...(input.requestId ? { requestId: RuntimeRequestId.make(input.requestId) } : {}),
    ...(input.raw
      ? {
          raw: {
            source: "copilot.sdk.event" as const,
            method: input.raw.type,
            payload: input.raw,
          },
        }
      : {}),
  };
}

function ensureTurnSnapshot(context: CopilotSessionContext, turnId: TurnId): CopilotTurnSnapshot {
  const existing = context.turns.find((turn) => turn.id === turnId);
  if (existing) {
    return existing;
  }
  const created: CopilotTurnSnapshot = { id: turnId, items: [] };
  context.turns.push(created);
  return created;
}

function appendTurnItem(
  context: CopilotSessionContext,
  turnId: TurnId | undefined,
  item: unknown,
): void {
  if (!turnId) {
    return;
  }
  ensureTurnSnapshot(context, turnId).items.push(item);
}

function requestError(
  method: string,
  detail: string,
  cause?: unknown,
): ProviderAdapterRequestError {
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail,
    ...(cause !== undefined ? { cause } : {}),
  });
}

function processError(
  threadId: ThreadId,
  detail: string,
  cause?: unknown,
): ProviderAdapterProcessError {
  return new ProviderAdapterProcessError({
    provider: PROVIDER,
    threadId,
    detail,
    ...(cause !== undefined ? { cause } : {}),
  });
}

function validationError(operation: string, issue: string): ProviderAdapterValidationError {
  return new ProviderAdapterValidationError({
    provider: PROVIDER,
    operation,
    issue,
  });
}

function sessionClosedError(threadId: ThreadId): ProviderAdapterSessionClosedError {
  return new ProviderAdapterSessionClosedError({
    provider: PROVIDER,
    threadId,
  });
}

function sessionNotFoundError(threadId: ThreadId): ProviderAdapterSessionNotFoundError {
  return new ProviderAdapterSessionNotFoundError({
    provider: PROVIDER,
    threadId,
  });
}

function requireSessionContext(
  sessions: ReadonlyMap<ThreadId, CopilotSessionContext>,
  threadId: ThreadId,
): CopilotSessionContext {
  const context = sessions.get(threadId);
  if (!context) {
    throw sessionNotFoundError(threadId);
  }
  if (context.stopped) {
    throw sessionClosedError(threadId);
  }
  return context;
}

function requireSessionContextEffect(
  sessions: ReadonlyMap<ThreadId, CopilotSessionContext>,
  threadId: ThreadId,
): Effect.Effect<
  CopilotSessionContext,
  ProviderAdapterSessionNotFoundError | ProviderAdapterSessionClosedError
> {
  return Effect.try({
    try: () => requireSessionContext(sessions, threadId),
    catch: (cause) =>
      Schema.is(ProviderAdapterSessionNotFoundError)(cause) ||
      Schema.is(ProviderAdapterSessionClosedError)(cause)
        ? cause
        : sessionNotFoundError(threadId),
  });
}

function requestedCopilotMode(input: {
  readonly runtimeMode: ProviderSession["runtimeMode"];
  readonly interactionMode?: ProviderSendTurnInput["interactionMode"] | undefined;
}): CopilotMode {
  if (input.interactionMode === "plan") {
    return "plan";
  }
  return input.runtimeMode === "approval-required" ? "interactive" : "autopilot";
}

function mapPermissionRequestType(
  request: SessionPermissionRequest,
): "command_execution_approval" | "file_read_approval" | "file_change_approval" {
  switch (request.kind) {
    case "read":
      return "file_read_approval";
    case "write":
      return "file_change_approval";
    default:
      return "command_execution_approval";
  }
}

function permissionDetail(request: SessionPermissionRequest): string | undefined {
  switch (request.kind) {
    case "shell":
      return trimToUndefined(request.fullCommandText) ?? trimToUndefined(request.intention);
    case "write":
      return trimToUndefined(request.fileName) ?? trimToUndefined(request.intention);
    case "read":
      return trimToUndefined(request.path) ?? trimToUndefined(request.intention);
    case "mcp":
      return trimToUndefined(request.toolTitle) ?? `${request.serverName}:${request.toolName}`;
    case "url":
      return trimToUndefined(request.url) ?? trimToUndefined(request.intention);
    case "memory":
      return trimToUndefined(request.subject);
    case "custom-tool":
      return trimToUndefined(request.toolName) ?? trimToUndefined(request.toolDescription);
    case "hook":
      return trimToUndefined(request.hookMessage) ?? trimToUndefined(request.toolName);
    default:
      return undefined;
  }
}

function permissionSignature(request: {
  readonly kind: string;
  readonly toolCallId?: string;
  readonly [key: string]: unknown;
}): string {
  switch (request.kind) {
    case "shell":
      return JSON.stringify([
        request.kind,
        request.toolCallId ?? null,
        request.fullCommandText ?? null,
        request.intention ?? null,
      ]);
    case "write":
      return JSON.stringify([
        request.kind,
        request.toolCallId ?? null,
        request.fileName ?? null,
        request.diff ?? null,
      ]);
    case "read":
      return JSON.stringify([request.kind, request.toolCallId ?? null, request.path ?? null]);
    case "mcp":
      return JSON.stringify([
        request.kind,
        request.toolCallId ?? null,
        request.serverName ?? null,
        request.toolName ?? null,
        request.args ?? null,
      ]);
    case "url":
      return JSON.stringify([request.kind, request.toolCallId ?? null, request.url ?? null]);
    case "memory":
      return JSON.stringify([
        request.kind,
        request.toolCallId ?? null,
        request.subject ?? null,
        request.fact ?? null,
      ]);
    case "custom-tool":
      return JSON.stringify([
        request.kind,
        request.toolCallId ?? null,
        request.toolName ?? null,
        request.args ?? null,
      ]);
    case "hook":
      return JSON.stringify([
        request.kind,
        request.toolCallId ?? null,
        request.toolName ?? null,
        request.toolArgs ?? null,
        request.hookMessage ?? null,
      ]);
    default:
      return JSON.stringify(request);
  }
}

function userInputSignature(input: {
  readonly question: string;
  readonly choices?: ReadonlyArray<string>;
  readonly allowFreeform?: boolean;
}): string {
  return JSON.stringify([input.question, input.choices ?? [], input.allowFreeform ?? true]);
}

function updateProviderSession(
  context: CopilotSessionContext,
  patch: Partial<ProviderSession>,
): void {
  context.session = {
    ...context.session,
    ...patch,
    updatedAt: nowIso(),
  };
}

function commonPrefixLength(left: string, right: string): number {
  let index = 0;
  while (index < left.length && index < right.length && left[index] === right[index]) {
    index += 1;
  }
  return index;
}

function deltaFromBufferedText(previous: string | undefined, next: string): string {
  return next.slice(commonPrefixLength(previous ?? "", next));
}

function toolItemType(toolName: string, mcpServerName?: string): ToolMeta["itemType"] {
  const normalized = toolName.toLowerCase();
  if (mcpServerName) {
    return "mcp_tool_call";
  }
  if (
    normalized.includes("bash") ||
    normalized.includes("shell") ||
    normalized.includes("exec") ||
    normalized.includes("command")
  ) {
    return "command_execution";
  }
  if (
    normalized.includes("write") ||
    normalized.includes("edit") ||
    normalized.includes("patch") ||
    normalized.includes("replace")
  ) {
    return "file_change";
  }
  if (normalized.includes("search") || normalized.includes("fetch") || normalized.includes("web")) {
    return "web_search";
  }
  if (normalized.includes("image") || normalized.includes("screenshot")) {
    return "image_view";
  }
  if (
    normalized.includes("subagent") ||
    normalized.includes("agent") ||
    normalized.includes("delegate") ||
    normalized.includes("task")
  ) {
    return "collab_agent_tool_call";
  }
  return "dynamic_tool_call";
}

function toolStreamKind(
  itemType: ToolMeta["itemType"] | undefined,
): "command_output" | "file_change_output" | "unknown" {
  if (itemType === "command_execution") {
    return "command_output";
  }
  if (itemType === "file_change") {
    return "file_change_output";
  }
  return "unknown";
}

function usageSnapshotFromAssistantUsage(
  event: Extract<SessionEvent, { type: "assistant.usage" }>,
): ThreadTokenUsageSnapshot {
  const inputTokens = event.data.inputTokens ?? 0;
  const cachedInputTokens = event.data.cacheReadTokens ?? 0;
  const outputTokens = event.data.outputTokens ?? 0;
  const usedTokens = inputTokens + cachedInputTokens + outputTokens;
  return {
    usedTokens,
    lastUsedTokens: usedTokens,
    ...(inputTokens > 0 ? { inputTokens, lastInputTokens: inputTokens } : {}),
    ...(cachedInputTokens > 0
      ? { cachedInputTokens, lastCachedInputTokens: cachedInputTokens }
      : {}),
    ...(outputTokens > 0 ? { outputTokens, lastOutputTokens: outputTokens } : {}),
    ...(typeof event.data.duration === "number" && Number.isFinite(event.data.duration)
      ? { durationMs: Math.max(0, Math.round(event.data.duration)) }
      : {}),
  };
}

function usageSnapshotFromUsageInfo(
  event: Extract<SessionEvent, { type: "session.usage_info" }>,
): ThreadTokenUsageSnapshot {
  const currentTokens = Math.max(0, Math.round(event.data.currentTokens));
  return {
    usedTokens: currentTokens,
    lastUsedTokens: currentTokens,
    ...(event.data.tokenLimit > 0 ? { maxTokens: Math.round(event.data.tokenLimit) } : {}),
    ...(event.data.conversationTokens !== undefined
      ? {
          inputTokens: event.data.conversationTokens,
          lastInputTokens: event.data.conversationTokens,
        }
      : {}),
  };
}

function firstAnswerValue(answers: ProviderUserInputAnswers): string | undefined {
  for (const value of Object.values(answers)) {
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
    if (typeof value === "number" || typeof value === "boolean") {
      return String(value);
    }
    if (Array.isArray(value)) {
      const first = value.find((entry) => typeof entry === "string" && entry.trim().length > 0);
      if (typeof first === "string") {
        return first.trim();
      }
    }
  }
  return undefined;
}

function answerFromUserInput(
  binding: PendingUserInputBinding,
  answers: ProviderUserInputAnswers,
): CopilotUserInputResponse {
  const preferredAnswer =
    firstAnswerValue(answers) ??
    (binding.choices.length > 0 ? binding.choices[0] : undefined) ??
    "";
  const normalizedChoices = new Set(binding.choices.map((choice) => choice.trim()));
  const wasFreeform =
    normalizedChoices.size === 0 ? true : !normalizedChoices.has(preferredAnswer.trim());
  return {
    answer: preferredAnswer,
    wasFreeform,
  };
}

function settlePendingPermissionHandlers(context: CopilotSessionContext): void {
  for (const handlers of context.pendingPermissionHandlersBySignature.values()) {
    for (const handler of handlers) {
      handler.promiseKit.resolve({ kind: "denied-interactively-by-user" });
    }
  }
  context.pendingPermissionHandlersBySignature.clear();
  context.pendingPermissionEventsBySignature.clear();

  for (const binding of context.pendingPermissionBindings.values()) {
    binding.promiseKit.resolve({ kind: "denied-interactively-by-user" });
  }
  context.pendingPermissionBindings.clear();
}

function settlePendingUserInputs(context: CopilotSessionContext): void {
  for (const handlers of context.pendingUserInputHandlersBySignature.values()) {
    for (const handler of handlers) {
      handler.promiseKit.resolve({
        answer: "",
        wasFreeform: true,
      });
    }
  }
  context.pendingUserInputHandlersBySignature.clear();
  context.pendingUserInputEventsBySignature.clear();

  for (const binding of context.pendingUserInputBindings.values()) {
    binding.promiseKit.resolve({
      answer: "",
      wasFreeform: true,
    });
  }
  context.pendingUserInputBindings.clear();
}

function latestTurnId(context: CopilotSessionContext): TurnId | undefined {
  return context.turns.at(-1)?.id;
}

function resolveTurnIdForSdkTurn(context: CopilotSessionContext, sdkTurnId: string): TurnId {
  const existing = context.sdkTurnIdsToTurnIds.get(sdkTurnId);
  if (existing) {
    return existing;
  }
  const nextTurnId =
    context.queuedTurnIds.shift() ??
    context.activeTurnId ??
    latestTurnId(context) ??
    TurnId.make(`copilot-turn-${randomUUID()}`);
  context.sdkTurnIdsToTurnIds.set(sdkTurnId, nextTurnId);
  ensureTurnSnapshot(context, nextTurnId);
  context.activeSdkTurnId = sdkTurnId;
  context.activeTurnId = nextTurnId;
  updateProviderSession(context, {
    status: "running",
    activeTurnId: nextTurnId,
  });
  return nextTurnId;
}

function resolveTurnIdForEvent(
  context: CopilotSessionContext,
  input?: {
    readonly providerItemId?: string | undefined;
    readonly sdkTurnId?: string | undefined;
    readonly parentProviderItemId?: string | undefined;
  },
): TurnId | undefined {
  const parentTurnId =
    input?.parentProviderItemId && context.turnIdByProviderItemId.get(input.parentProviderItemId);
  if (parentTurnId) {
    return parentTurnId;
  }
  const providerItemTurnId =
    input?.providerItemId && context.turnIdByProviderItemId.get(input.providerItemId);
  if (providerItemTurnId) {
    return providerItemTurnId;
  }
  if (input?.sdkTurnId) {
    return resolveTurnIdForSdkTurn(context, input.sdkTurnId);
  }
  if (context.activeSdkTurnId) {
    return context.sdkTurnIdsToTurnIds.get(context.activeSdkTurnId) ?? context.activeTurnId;
  }
  return context.activeTurnId ?? latestTurnId(context);
}

export function makeCopilotAdapterLive(options?: CopilotAdapterLiveOptions) {
  return Layer.effect(
    CopilotAdapter,
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig;
      const serverSettingsService = yield* ServerSettingsService;
      const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
      const nativeEventLogger =
        options?.nativeEventLogger ??
        (options?.nativeEventLogPath !== undefined
          ? yield* makeEventNdjsonLogger(options.nativeEventLogPath, {
              stream: "native",
            })
          : undefined);
      const managedNativeEventLogger =
        options?.nativeEventLogger === undefined ? nativeEventLogger : undefined;
      const sessions = new Map<ThreadId, CopilotSessionContext>();
      const runtimeContext = yield* Effect.context();
      const runWithContext = Effect.runPromiseWith(runtimeContext);

      const emit = (event: ProviderRuntimeEvent) =>
        PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);
      const emitAsync = (event: ProviderRuntimeEvent) => runWithContext(emit(event));
      const writeNativeAsync = (threadId: ThreadId, event: SessionEvent) =>
        nativeEventLogger
          ? runWithContext(
              nativeEventLogger.write({ source: "copilot.sdk.event", payload: event }, threadId),
            )
          : Promise.resolve();

      const enqueueSdkEvent = (context: CopilotSessionContext, event: SessionEvent) => {
        context.eventChain = context.eventChain
          .then(async () => {
            await writeNativeAsync(context.threadId, event);
            await handleSdkEvent(context, event);
          })
          .catch(async (error) => {
            const message =
              error instanceof Error && error.message.trim().length > 0
                ? error.message.trim()
                : "Copilot event handling failed.";
            updateProviderSession(context, {
              status: "error",
              lastError: message,
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
              }),
              type: "runtime.error",
              payload: {
                message,
                class: "provider_error",
                detail: {
                  error,
                  sourceEventType: event.type,
                },
              },
            });
          });
      };

      const emitTurnCompleted = async (
        context: CopilotSessionContext,
        turnId: TurnId,
        status: ProviderRuntimeTurnStatus,
        input?: {
          readonly stopReason?: string | null | undefined;
          readonly errorMessage?: string | undefined;
          readonly raw?: SessionEvent | undefined;
        },
      ) => {
        if (context.completedTurnIds.has(turnId)) {
          return;
        }
        context.completedTurnIds.add(turnId);
        if (context.activeTurnId === turnId) {
          context.activeTurnId = undefined;
        }
        updateProviderSession(context, {
          status: status === "failed" ? "error" : context.stopped ? "closed" : "ready",
          ...(status === "failed" && input?.errorMessage ? { lastError: input.errorMessage } : {}),
          activeTurnId: undefined,
        });
        await emitAsync({
          ...createBaseEvent({
            threadId: context.threadId,
            turnId,
            raw: input?.raw,
          }),
          type: "turn.completed",
          payload: {
            state: status,
            ...(input?.stopReason !== undefined ? { stopReason: input.stopReason } : {}),
            ...(context.turnUsageByTurnId.has(turnId)
              ? { usage: context.turnUsageByTurnId.get(turnId) }
              : {}),
            ...(input?.errorMessage ? { errorMessage: input.errorMessage } : {}),
          },
        });
      };

      const emitTextDelta = async (input: {
        readonly context: CopilotSessionContext;
        readonly turnId: TurnId;
        readonly itemId: string;
        readonly itemType: "assistant_message" | "reasoning";
        readonly streamKind: "assistant_text" | "reasoning_text";
        readonly nextText: string;
        readonly raw?: SessionEvent | undefined;
      }) => {
        if (!input.context.startedItemIds.has(input.itemId)) {
          input.context.startedItemIds.add(input.itemId);
          await emitAsync({
            ...createBaseEvent({
              threadId: input.context.threadId,
              turnId: input.turnId,
              itemId: input.itemId,
              raw: input.raw,
            }),
            type: "item.started",
            payload: {
              itemType: input.itemType,
              status: "inProgress",
            },
          });
        }

        const previousText = input.context.emittedTextByItemId.get(input.itemId);
        const delta = deltaFromBufferedText(previousText, input.nextText);
        input.context.emittedTextByItemId.set(input.itemId, input.nextText);
        if (delta.length === 0) {
          return;
        }
        await emitAsync({
          ...createBaseEvent({
            threadId: input.context.threadId,
            turnId: input.turnId,
            itemId: input.itemId,
            raw: input.raw,
          }),
          type: "content.delta",
          payload: {
            streamKind: input.streamKind,
            delta,
          },
        });
      };

      const emitPermissionRequestOpened = async (
        context: CopilotSessionContext,
        pending: PendingPermissionBinding,
        data: SessionPermissionRequestedEvent["data"],
      ) => {
        await emitAsync({
          ...createBaseEvent({
            threadId: context.threadId,
            requestId: pending.requestId,
            raw: {
              ...({
                id: pending.requestId,
                timestamp: nowIso(),
                parentId: null,
                ephemeral: true,
                type: "permission.requested",
                data,
              } satisfies SessionPermissionRequestedEvent),
            },
          }),
          type: "request.opened",
          payload: {
            requestType: pending.requestType,
            ...(permissionDetail(data.permissionRequest)
              ? { detail: permissionDetail(data.permissionRequest) }
              : {}),
            args: data.permissionRequest,
          },
        });
      };

      const emitUserInputRequested = async (
        context: CopilotSessionContext,
        requestId: string,
        request: PendingUserInputBinding,
        raw?: SessionEvent,
      ) => {
        const options = request.choices.map((choice) => ({
          label: choice,
          description: choice,
        }));
        const questions: ReadonlyArray<UserInputQuestion> = [
          {
            id: "answer",
            header: "Input",
            question: request.question.trim(),
            options,
            ...(options.length > 1 ? { multiSelect: false } : {}),
          },
        ];
        await emitAsync({
          ...createBaseEvent({
            threadId: context.threadId,
            requestId,
            raw,
          }),
          type: "user-input.requested",
          payload: {
            questions,
          },
        });
      };

      const bindPermissionRequests = async (
        context: CopilotSessionContext,
        signature: string,
      ): Promise<void> => {
        const pendingHandlers = context.pendingPermissionHandlersBySignature.get(signature);
        const pendingEvents = context.pendingPermissionEventsBySignature.get(signature);
        if (!pendingHandlers?.length || !pendingEvents?.length) {
          return;
        }

        while (pendingHandlers.length > 0 && pendingEvents.length > 0) {
          const handler = pendingHandlers.shift()!;
          const eventData = pendingEvents.shift()!;
          const requestId = eventData.requestId.trim();
          context.pendingPermissionBindings.set(requestId, {
            requestId,
            requestType: mapPermissionRequestType(eventData.permissionRequest),
            promiseKit: handler.promiseKit,
          });
          if (
            context.session.runtimeMode === "approval-required" &&
            eventData.resolvedByHook !== true
          ) {
            await emitPermissionRequestOpened(
              context,
              context.pendingPermissionBindings.get(requestId)!,
              eventData,
            );
          }
        }

        if (pendingHandlers.length === 0) {
          context.pendingPermissionHandlersBySignature.delete(signature);
        }
        if (pendingEvents.length === 0) {
          context.pendingPermissionEventsBySignature.delete(signature);
        }
      };

      const bindUserInputRequests = async (
        context: CopilotSessionContext,
        signature: string,
      ): Promise<void> => {
        const pendingHandlers = context.pendingUserInputHandlersBySignature.get(signature);
        const pendingEvents = context.pendingUserInputEventsBySignature.get(signature);
        if (!pendingHandlers?.length || !pendingEvents?.length) {
          return;
        }

        while (pendingHandlers.length > 0 && pendingEvents.length > 0) {
          const handler = pendingHandlers.shift()!;
          const eventData = pendingEvents.shift()!;
          const requestId = eventData.requestId.trim();
          const binding: PendingUserInputBinding = {
            requestId,
            question: eventData.question.trim(),
            choices: eventData.choices?.map((choice) => choice.trim()).filter(Boolean) ?? [],
            allowFreeform: eventData.allowFreeform ?? true,
            promiseKit: handler.promiseKit,
          };
          context.pendingUserInputBindings.set(requestId, binding);
          await emitUserInputRequested(context, requestId, binding, {
            id: requestId,
            timestamp: nowIso(),
            parentId: null,
            type: "user_input.requested",
            ephemeral: true,
            data: eventData,
          });
        }

        if (pendingHandlers.length === 0) {
          context.pendingUserInputHandlersBySignature.delete(signature);
        }
        if (pendingEvents.length === 0) {
          context.pendingUserInputEventsBySignature.delete(signature);
        }
      };

      const onPermissionRequest = async (
        context: CopilotSessionContext,
        request: PermissionRequest,
      ): Promise<PermissionRequestResult> => {
        if (context.session.runtimeMode !== "approval-required") {
          return { kind: "approved" };
        }
        if (context.stopped) {
          return { kind: "denied-interactively-by-user" };
        }

        const signature = permissionSignature(request as PermissionRequest & { kind: string });
        const promiseKit = createPromiseKit<PermissionRequestResult>();
        const queue = context.pendingPermissionHandlersBySignature.get(signature) ?? [];
        queue.push({
          signature,
          promiseKit,
        });
        context.pendingPermissionHandlersBySignature.set(signature, queue);
        await bindPermissionRequests(context, signature);
        return promiseKit.promise;
      };

      const onUserInputRequest = async (
        context: CopilotSessionContext,
        request: CopilotUserInputRequest,
      ): Promise<CopilotUserInputResponse> => {
        if (context.stopped) {
          return {
            answer: "",
            wasFreeform: true,
          };
        }

        const signature = userInputSignature(request);
        const promiseKit = createPromiseKit<CopilotUserInputResponse>();
        const queue = context.pendingUserInputHandlersBySignature.get(signature) ?? [];
        queue.push({
          signature,
          promiseKit,
        });
        context.pendingUserInputHandlersBySignature.set(signature, queue);
        await bindUserInputRequests(context, signature);
        return promiseKit.promise;
      };

      const syncSessionMode = async (
        context: CopilotSessionContext,
        mode: CopilotMode,
      ): Promise<void> => {
        await context.sdkSession.rpc.mode.set({ mode });
        await emitAsync({
          ...createBaseEvent({
            threadId: context.threadId,
          }),
          type: "session.configured",
          payload: {
            config: {
              mode,
            },
          },
        });
      };

      const emitPlanSnapshot = async (
        context: CopilotSessionContext,
        raw: SessionEvent,
        fallbackPlan?: string | undefined,
      ): Promise<void> => {
        const turnId = context.activeTurnId ?? latestTurnId(context);
        if (!turnId) {
          return;
        }
        const plan = fallbackPlan
          ? fallbackPlan.trim()
          : ((await context.sdkSession.rpc.plan.read()).content ?? "").trim();
        if (plan.length === 0) {
          return;
        }
        await emitAsync({
          ...createBaseEvent({
            threadId: context.threadId,
            turnId,
            raw,
          }),
          type: "turn.proposed.completed",
          payload: {
            planMarkdown: plan,
          },
        });
      };

      const handleSdkEvent = async (
        context: CopilotSessionContext,
        event: SessionEvent,
      ): Promise<void> => {
        switch (event.type) {
          case "session.start": {
            updateProviderSession(context, {
              status: "ready",
              model: trimToUndefined(event.data.selectedModel) ?? context.session.model,
              ...(event.data.context?.cwd ? { cwd: event.data.context.cwd } : {}),
              resumeCursor: toCopilotResumeCursor(event.data.sessionId),
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.started",
              payload: {
                message: "Copilot session started.",
                resume: toCopilotResumeCursor(event.data.sessionId),
              },
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.configured",
              payload: {
                config: {
                  model: event.data.selectedModel ?? null,
                  reasoningEffort: event.data.reasoningEffort ?? null,
                  cwd: event.data.context?.cwd ?? context.cwd,
                },
              },
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.state.changed",
              payload: {
                state: "ready",
                reason: "Copilot session ready",
              },
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "thread.started",
              payload: {
                providerThreadId: event.data.sessionId,
              },
            });
            return;
          }
          case "session.resume": {
            updateProviderSession(context, {
              status: "ready",
              model: trimToUndefined(event.data.selectedModel) ?? context.session.model,
              ...(event.data.context?.cwd ? { cwd: event.data.context.cwd } : {}),
              resumeCursor: toCopilotResumeCursor(context.sdkSession.sessionId),
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.started",
              payload: {
                message: "Copilot session resumed.",
                resume: toCopilotResumeCursor(context.sdkSession.sessionId),
              },
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.configured",
              payload: {
                config: {
                  model: event.data.selectedModel ?? null,
                  reasoningEffort: event.data.reasoningEffort ?? null,
                  cwd: event.data.context?.cwd ?? context.cwd,
                },
              },
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.state.changed",
              payload: {
                state: "ready",
                reason: "Copilot session resumed",
              },
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "thread.started",
              payload: {
                providerThreadId: context.sdkSession.sessionId,
              },
            });
            return;
          }
          case "session.error": {
            const message = trimToUndefined(event.data.message) ?? "Copilot session failed.";
            updateProviderSession(context, {
              status: "error",
              lastError: message,
              activeTurnId: undefined,
            });
            if (context.activeTurnId) {
              await emitTurnCompleted(context, context.activeTurnId, "failed", {
                errorMessage: message,
                raw: event,
              });
            }
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "runtime.error",
              payload: {
                message,
                class: "provider_error",
                detail: event.data,
              },
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.state.changed",
              payload: {
                state: "error",
                reason: message,
                detail: event.data,
              },
            });
            return;
          }
          case "session.idle": {
            if (context.activeTurnId) {
              await emitTurnCompleted(
                context,
                context.activeTurnId,
                event.data.aborted ? "cancelled" : "completed",
                {
                  raw: event,
                  stopReason: event.data.aborted ? "aborted" : null,
                },
              );
            }
            updateProviderSession(context, {
              status: context.stopped ? "closed" : "ready",
              activeTurnId: undefined,
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.state.changed",
              payload: {
                state: context.stopped ? "stopped" : "ready",
                reason: event.data.aborted ? "Copilot turn aborted." : "Copilot idle.",
              },
            });
            return;
          }
          case "session.title_changed": {
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "thread.metadata.updated",
              payload: {
                name: event.data.title.trim(),
              },
            });
            return;
          }
          case "session.warning": {
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "runtime.warning",
              payload: {
                message: event.data.message.trim(),
                detail: event.data,
              },
            });
            return;
          }
          case "session.model_change": {
            updateProviderSession(context, {
              model: trimToUndefined(event.data.newModel) ?? context.session.model,
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.configured",
              payload: {
                config: {
                  model: event.data.newModel,
                  reasoningEffort: event.data.reasoningEffort ?? null,
                  previousModel: event.data.previousModel ?? null,
                },
              },
            });
            return;
          }
          case "session.mode_changed": {
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                raw: event,
              }),
              type: "session.configured",
              payload: {
                config: {
                  mode: event.data.newMode,
                  previousMode: event.data.previousMode,
                },
              },
            });
            return;
          }
          case "session.plan_changed": {
            if (event.data.operation === "delete") {
              return;
            }
            await emitPlanSnapshot(context, event);
            return;
          }
          case "session.usage_info": {
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId: context.activeTurnId,
                raw: event,
              }),
              type: "thread.token-usage.updated",
              payload: {
                usage: usageSnapshotFromUsageInfo(event),
              },
            });
            return;
          }
          case "assistant.turn_start": {
            const turnId = resolveTurnIdForSdkTurn(context, event.data.turnId);
            updateProviderSession(context, {
              status: "running",
              activeTurnId: turnId,
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId,
                raw: event,
              }),
              type: "session.state.changed",
              payload: {
                state: "running",
                reason: "Copilot turn started",
              },
            });
            return;
          }
          case "assistant.reasoning_delta": {
            const turnId = resolveTurnIdForEvent(context, {
              sdkTurnId: context.activeSdkTurnId,
              providerItemId: event.data.reasoningId,
            });
            if (!turnId) {
              return;
            }
            const itemId = `copilot-reasoning-${event.data.reasoningId}`;
            context.turnIdByProviderItemId.set(event.data.reasoningId, turnId);
            await emitTextDelta({
              context,
              turnId,
              itemId,
              itemType: "reasoning",
              streamKind: "reasoning_text",
              nextText: (context.emittedTextByItemId.get(itemId) ?? "") + event.data.deltaContent,
              raw: event,
            });
            return;
          }
          case "assistant.reasoning": {
            const turnId = resolveTurnIdForEvent(context, {
              sdkTurnId: context.activeSdkTurnId,
              providerItemId: event.data.reasoningId,
            });
            if (!turnId) {
              return;
            }
            const itemId = `copilot-reasoning-${event.data.reasoningId}`;
            context.turnIdByProviderItemId.set(event.data.reasoningId, turnId);
            await emitTextDelta({
              context,
              turnId,
              itemId,
              itemType: "reasoning",
              streamKind: "reasoning_text",
              nextText: event.data.content,
              raw: event,
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId,
                itemId,
                raw: event,
              }),
              type: "item.completed",
              payload: {
                itemType: "reasoning",
                status: "completed",
              },
            });
            appendTurnItem(context, turnId, {
              type: "reasoning",
              reasoningId: event.data.reasoningId,
              content: event.data.content,
            });
            return;
          }
          case "assistant.message_delta": {
            const turnId = resolveTurnIdForEvent(context, {
              sdkTurnId: context.activeSdkTurnId,
              providerItemId: event.data.messageId,
              parentProviderItemId: event.data.parentToolCallId,
            });
            if (!turnId) {
              return;
            }
            const itemId = `copilot-message-${event.data.messageId}`;
            context.turnIdByProviderItemId.set(event.data.messageId, turnId);
            await emitTextDelta({
              context,
              turnId,
              itemId,
              itemType: "assistant_message",
              streamKind: "assistant_text",
              nextText: (context.emittedTextByItemId.get(itemId) ?? "") + event.data.deltaContent,
              raw: event,
            });
            return;
          }
          case "assistant.message": {
            const turnId = resolveTurnIdForEvent(context, {
              sdkTurnId: context.activeSdkTurnId,
              providerItemId: event.data.messageId,
              parentProviderItemId: event.data.parentToolCallId,
            });
            if (!turnId) {
              return;
            }
            const itemId = `copilot-message-${event.data.messageId}`;
            context.turnIdByProviderItemId.set(event.data.messageId, turnId);
            await emitTextDelta({
              context,
              turnId,
              itemId,
              itemType: "assistant_message",
              streamKind: "assistant_text",
              nextText: event.data.content,
              raw: event,
            });
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId,
                itemId,
                raw: event,
              }),
              type: "item.completed",
              payload: {
                itemType: "assistant_message",
                status: "completed",
              },
            });
            if (event.data.reasoningText?.trim()) {
              const reasoningItemId = `copilot-message-reasoning-${event.data.messageId}`;
              await emitTextDelta({
                context,
                turnId,
                itemId: reasoningItemId,
                itemType: "reasoning",
                streamKind: "reasoning_text",
                nextText: event.data.reasoningText,
                raw: event,
              });
              await emitAsync({
                ...createBaseEvent({
                  threadId: context.threadId,
                  turnId,
                  itemId: reasoningItemId,
                  raw: event,
                }),
                type: "item.completed",
                payload: {
                  itemType: "reasoning",
                  status: "completed",
                },
              });
            }
            appendTurnItem(context, turnId, {
              type: "assistant_message",
              messageId: event.data.messageId,
              content: event.data.content,
            });
            return;
          }
          case "assistant.turn_end": {
            const turnId =
              context.sdkTurnIdsToTurnIds.get(event.data.turnId) ?? context.activeTurnId;
            if (!turnId) {
              return;
            }
            await emitTurnCompleted(context, turnId, "completed", {
              raw: event,
              stopReason: null,
            });
            if (context.activeSdkTurnId === event.data.turnId) {
              context.activeSdkTurnId = undefined;
            }
            return;
          }
          case "assistant.usage": {
            const turnId = resolveTurnIdForEvent(context, {
              parentProviderItemId: event.data.parentToolCallId,
              sdkTurnId: context.activeSdkTurnId,
            });
            if (!turnId) {
              return;
            }
            const usage = usageSnapshotFromAssistantUsage(event);
            context.turnUsageByTurnId.set(turnId, usage);
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId,
                raw: event,
              }),
              type: "thread.token-usage.updated",
              payload: {
                usage,
              },
            });
            return;
          }
          case "abort": {
            const turnId = context.activeTurnId;
            if (!turnId) {
              return;
            }
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId,
                raw: event,
              }),
              type: "turn.aborted",
              payload: {
                reason: event.data.reason,
              },
            });
            await emitTurnCompleted(context, turnId, "cancelled", {
              raw: event,
              stopReason: "aborted",
            });
            return;
          }
          case "tool.execution_start": {
            const turnId = resolveTurnIdForEvent(context, {
              providerItemId: event.data.toolCallId,
              parentProviderItemId: event.data.parentToolCallId,
              sdkTurnId: context.activeSdkTurnId,
            });
            if (!turnId) {
              return;
            }
            const itemId = `copilot-tool-${event.data.toolCallId}`;
            const itemType = toolItemType(event.data.toolName, event.data.mcpServerName);
            context.toolMetaById.set(event.data.toolCallId, {
              toolName: event.data.toolName,
              itemType,
            });
            context.turnIdByProviderItemId.set(event.data.toolCallId, turnId);
            context.startedItemIds.add(itemId);
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId,
                itemId,
                raw: event,
              }),
              type: "item.started",
              payload: {
                itemType,
                status: "inProgress",
                title: event.data.toolName,
                ...(event.data.arguments ? { data: event.data.arguments } : {}),
              },
            });
            return;
          }
          case "tool.execution_partial_result": {
            const turnId = resolveTurnIdForEvent(context, {
              providerItemId: event.data.toolCallId,
              sdkTurnId: context.activeSdkTurnId,
            });
            if (!turnId) {
              return;
            }
            const itemId = `copilot-tool-${event.data.toolCallId}`;
            const toolMeta = context.toolMetaById.get(event.data.toolCallId);
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId,
                itemId,
                raw: event,
              }),
              type: "content.delta",
              payload: {
                streamKind: toolStreamKind(toolMeta?.itemType),
                delta: event.data.partialOutput,
              },
            });
            return;
          }
          case "tool.execution_progress": {
            const turnId = resolveTurnIdForEvent(context, {
              providerItemId: event.data.toolCallId,
              sdkTurnId: context.activeSdkTurnId,
            });
            const toolMeta = context.toolMetaById.get(event.data.toolCallId);
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId,
                itemId: `copilot-tool-${event.data.toolCallId}`,
                raw: event,
              }),
              type: "tool.progress",
              payload: {
                toolUseId: event.data.toolCallId,
                ...(toolMeta ? { toolName: toolMeta.toolName } : {}),
                summary: event.data.progressMessage.trim(),
              },
            });
            return;
          }
          case "tool.execution_complete": {
            const turnId = resolveTurnIdForEvent(context, {
              providerItemId: event.data.toolCallId,
              parentProviderItemId: event.data.parentToolCallId,
              sdkTurnId: context.activeSdkTurnId,
            });
            if (!turnId) {
              return;
            }
            const itemId = `copilot-tool-${event.data.toolCallId}`;
            const toolMeta = context.toolMetaById.get(event.data.toolCallId);
            const detail =
              trimToUndefined(event.data.result?.detailedContent) ??
              trimToUndefined(event.data.result?.content) ??
              trimToUndefined(event.data.error?.message);
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                turnId,
                itemId,
                raw: event,
              }),
              type: "item.completed",
              payload: {
                itemType: toolMeta?.itemType ?? "dynamic_tool_call",
                status: event.data.success ? "completed" : "failed",
                title: toolMeta?.toolName ?? "tool",
                ...(detail ? { detail } : {}),
                ...(event.data.toolTelemetry || event.data.result || event.data.error
                  ? {
                      data: {
                        ...(event.data.result ? { result: event.data.result } : {}),
                        ...(event.data.error ? { error: event.data.error } : {}),
                        ...(event.data.toolTelemetry
                          ? { toolTelemetry: event.data.toolTelemetry }
                          : {}),
                      },
                    }
                  : {}),
              },
            });
            appendTurnItem(context, turnId, {
              type: "tool_execution",
              toolCallId: event.data.toolCallId,
              toolName: toolMeta?.toolName,
              success: event.data.success,
              detail,
            });
            return;
          }
          case "permission.requested": {
            if (event.data.resolvedByHook === true) {
              return;
            }
            const signature = permissionSignature(
              event.data.permissionRequest as SessionPermissionRequest & { kind: string },
            );
            const queue = context.pendingPermissionEventsBySignature.get(signature) ?? [];
            queue.push(event.data);
            context.pendingPermissionEventsBySignature.set(signature, queue);
            await bindPermissionRequests(context, signature);
            return;
          }
          case "permission.completed": {
            const binding = context.pendingPermissionBindings.get(event.data.requestId);
            if (!binding) {
              return;
            }
            context.pendingPermissionBindings.delete(event.data.requestId);
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                requestId: binding.requestId,
                raw: event,
              }),
              type: "request.resolved",
              payload: {
                requestType: binding.requestType,
                decision: event.data.result.kind,
                resolution: event.data.result,
              },
            });
            return;
          }
          case "user_input.requested": {
            const signature = userInputSignature({
              question: event.data.question,
              ...(event.data.choices ? { choices: event.data.choices } : {}),
              ...(event.data.allowFreeform !== undefined
                ? { allowFreeform: event.data.allowFreeform }
                : {}),
            });
            const queue = context.pendingUserInputEventsBySignature.get(signature) ?? [];
            queue.push(event.data);
            context.pendingUserInputEventsBySignature.set(signature, queue);
            await bindUserInputRequests(context, signature);
            return;
          }
          case "user_input.completed": {
            const binding = context.pendingUserInputBindings.get(event.data.requestId);
            if (!binding) {
              return;
            }
            context.pendingUserInputBindings.delete(event.data.requestId);
            await emitAsync({
              ...createBaseEvent({
                threadId: context.threadId,
                requestId: binding.requestId,
                raw: event,
              }),
              type: "user-input.resolved",
              payload: {
                answers: {
                  answer: event.data.answer ?? "",
                  wasFreeform: event.data.wasFreeform ?? true,
                },
              },
            });
            return;
          }
          case "exit_plan_mode.requested": {
            await emitPlanSnapshot(context, event, event.data.planContent);
            return;
          }
          case "exit_plan_mode.completed":
          default:
            return;
        }
      };

      const startSession: CopilotAdapterShape["startSession"] = Effect.fn("startSession")(
        function* (input) {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* validationError(
              "startSession",
              `Expected provider '${PROVIDER}', received '${input.provider}'.`,
            );
          }

          if (sessions.has(input.threadId)) {
            yield* Effect.tryPromise({
              try: () => stopSessionInternal(input.threadId),
              catch: (cause) =>
                processError(
                  input.threadId,
                  cause instanceof Error
                    ? cause.message
                    : "Failed to stop existing Copilot session before restart.",
                  cause,
                ),
            });
          }

          const settings = yield* Effect.map(
            serverSettingsService.getSettings,
            (serverSettings) => serverSettings.providers.copilot,
          ).pipe(
            Effect.mapError((cause) =>
              processError(
                input.threadId,
                "Failed to read Copilot settings from the server settings store.",
                cause,
              ),
            ),
          );
          if (!settings.enabled) {
            return yield* validationError(
              "startSession",
              "Copilot is disabled in server settings.",
            );
          }

          const cwd = nodePath.resolve(input.cwd ?? serverConfig.cwd);
          const modelSelection =
            input.modelSelection?.provider === PROVIDER ? input.modelSelection : undefined;
          const reasoningEffort = getCopilotReasoningEffort(modelSelection);
          let context: CopilotSessionContext | undefined;
          const earlyEvents: Array<SessionEvent> = [];
          const onEvent: SessionConfig["onEvent"] = (event) => {
            if (!context) {
              earlyEvents.push(event);
              return;
            }
            enqueueSdkEvent(context, event);
          };
          const onSessionPermissionRequest = (_request: PermissionRequest) => {
            if (!context) {
              return Promise.resolve({
                kind:
                  input.runtimeMode === "approval-required"
                    ? "denied-interactively-by-user"
                    : "approved",
              } satisfies PermissionRequestResult);
            }
            return onPermissionRequest(context, _request);
          };
          const onSessionUserInputRequest = (_request: CopilotUserInputRequest) => {
            if (!context) {
              return Promise.resolve({
                answer: "",
                wasFreeform: true,
              } satisfies CopilotUserInputResponse);
            }
            return onUserInputRequest(context, _request);
          };

          const client = createCopilotClient({
            settings,
            cwd,
            logLevel: "error",
          });

          const baseSessionConfig = {
            clientName: "t3-code",
            ...(modelSelection?.model ? { model: modelSelection.model } : {}),
            ...(reasoningEffort ? { reasoningEffort } : {}),
            workingDirectory: cwd,
            streaming: true,
            enableConfigDiscovery: true,
            onEvent,
          } satisfies Pick<
            SessionConfig,
            | "clientName"
            | "model"
            | "reasoningEffort"
            | "workingDirectory"
            | "streaming"
            | "enableConfigDiscovery"
            | "onEvent"
          >;

          const sdkSession = yield* Effect.tryPromise({
            try: async () => {
              await client.start();
              const resume = parseCopilotResumeCursor(input.resumeCursor);
              return resume
                ? client.resumeSession(resume.sessionId, {
                    ...baseSessionConfig,
                    onPermissionRequest: onSessionPermissionRequest,
                    onUserInputRequest: onSessionUserInputRequest,
                  })
                : client.createSession({
                    ...baseSessionConfig,
                    sessionId: input.threadId,
                    onPermissionRequest: onSessionPermissionRequest,
                    onUserInputRequest: onSessionUserInputRequest,
                  });
            },
            catch: (cause) =>
              processError(
                input.threadId,
                cause instanceof Error ? cause.message : "Failed to create Copilot session.",
                cause,
              ),
          }).pipe(
            Effect.tapError(() =>
              Effect.sync(() => {
                void client.stop().catch(() => {
                  // Ignore cleanup failures while surfacing the original start error.
                });
              }),
            ),
          );

          context = {
            threadId: input.threadId,
            client,
            sdkSession,
            cwd,
            session: {
              provider: PROVIDER,
              status: "connecting",
              runtimeMode: input.runtimeMode,
              cwd,
              ...(modelSelection?.model ? { model: modelSelection.model } : {}),
              threadId: input.threadId,
              resumeCursor: toCopilotResumeCursor(sdkSession.sessionId),
              createdAt: nowIso(),
              updatedAt: nowIso(),
            },
            turns: [],
            queuedTurnIds: [],
            sdkTurnIdsToTurnIds: new Map(),
            completedTurnIds: new Set(),
            turnUsageByTurnId: new Map(),
            pendingPermissionHandlersBySignature: new Map(),
            pendingPermissionEventsBySignature: new Map(),
            pendingPermissionBindings: new Map(),
            pendingUserInputHandlersBySignature: new Map(),
            pendingUserInputEventsBySignature: new Map(),
            pendingUserInputBindings: new Map(),
            toolMetaById: new Map(),
            turnIdByProviderItemId: new Map(),
            emittedTextByItemId: new Map(),
            startedItemIds: new Set(),
            activeTurnId: undefined,
            activeSdkTurnId: undefined,
            eventChain: Promise.resolve(),
            stopped: false,
          };
          sessions.set(input.threadId, context);

          yield* Effect.tryPromise({
            try: () =>
              syncSessionMode(
                context,
                requestedCopilotMode({
                  runtimeMode: input.runtimeMode,
                }),
              ),
            catch: (cause) =>
              requestError(
                "session.mode.set",
                cause instanceof Error
                  ? cause.message
                  : "Failed to synchronize Copilot mode with the requested runtime mode.",
                cause,
              ),
          }).pipe(
            Effect.catch((cause) =>
              emit({
                ...createBaseEvent({
                  threadId: input.threadId,
                }),
                type: "runtime.warning",
                payload: {
                  message: "Failed to synchronize Copilot mode with the requested runtime mode.",
                  detail: cause,
                },
              }),
            ),
          );

          for (const event of earlyEvents) {
            enqueueSdkEvent(context, event);
          }
          yield* Effect.tryPromise({
            try: () => context.eventChain,
            catch: (cause) =>
              processError(
                input.threadId,
                cause instanceof Error
                  ? cause.message
                  : "Failed to process Copilot startup events.",
                cause,
              ),
          });
          updateProviderSession(context, {
            status: context.session.status === "connecting" ? "ready" : context.session.status,
          });

          return context.session;
        },
      );

      const sendTurn: CopilotAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
        const context = yield* requireSessionContextEffect(sessions, input.threadId);

        const text = input.input?.trim();
        const attachments = yield* Effect.forEach(input.attachments ?? [], (attachment) => {
          const filePath = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          });
          if (!filePath) {
            return Effect.fail(
              requestError("session.send", `Invalid attachment id '${attachment.id}'.`),
            );
          }
          return Effect.succeed({
            type: "file" as const,
            path: filePath,
            displayName: attachment.name,
          });
        });
        if ((!text || text.length === 0) && attachments.length === 0) {
          return yield* validationError(
            "sendTurn",
            "Copilot turns require text input or at least one attachment.",
          );
        }

        const turnId = TurnId.make(`copilot-turn-${randomUUID()}`);
        const modelSelection =
          input.modelSelection?.provider === PROVIDER ? input.modelSelection : undefined;
        const reasoningEffort = getCopilotReasoningEffort(modelSelection);
        if (modelSelection?.model) {
          yield* Effect.tryPromise({
            try: async () => {
              await context.sdkSession.setModel(
                modelSelection.model,
                reasoningEffort ? { reasoningEffort } : {},
              );
            },
            catch: (cause) =>
              requestError(
                "session.setModel",
                cause instanceof Error ? cause.message : "Failed to update Copilot model.",
                cause,
              ),
          });
          updateProviderSession(context, {
            model: modelSelection.model,
            ...(reasoningEffort ? { status: "ready" } : {}),
          });
        }

        const mode = requestedCopilotMode({
          runtimeMode: context.session.runtimeMode,
          interactionMode: input.interactionMode,
        });
        yield* Effect.tryPromise({
          try: () => syncSessionMode(context, mode),
          catch: (cause) =>
            requestError(
              "session.mode.set",
              cause instanceof Error ? cause.message : "Failed to update Copilot mode.",
              cause,
            ),
        });

        ensureTurnSnapshot(context, turnId);
        context.queuedTurnIds.push(turnId);
        context.activeTurnId = turnId;
        updateProviderSession(context, {
          status: "running",
          activeTurnId: turnId,
          ...(modelSelection?.model ? { model: modelSelection.model } : {}),
        });

        yield* emit({
          ...createBaseEvent({
            threadId: input.threadId,
            turnId,
          }),
          type: "turn.started",
          payload: {
            model: modelSelection?.model ?? context.session.model,
            ...(reasoningEffort ? { effort: reasoningEffort } : {}),
          },
        });

        const messageOptions: MessageOptions = {
          prompt: text ?? "",
          ...(attachments.length > 0 ? { attachments } : {}),
          mode: "enqueue",
        };

        yield* Effect.tryPromise({
          try: () => context.sdkSession.send(messageOptions),
          catch: (cause) =>
            requestError(
              "session.send",
              cause instanceof Error ? cause.message : "Failed to send Copilot turn.",
              cause,
            ),
        }).pipe(
          Effect.catch((error) =>
            Effect.gen(function* () {
              const queueIndex = context.queuedTurnIds.indexOf(turnId);
              if (queueIndex >= 0) {
                context.queuedTurnIds.splice(queueIndex, 1);
              }
              context.activeTurnId = undefined;
              updateProviderSession(context, {
                status: "ready",
                activeTurnId: undefined,
              });
              yield* emit({
                ...createBaseEvent({
                  threadId: input.threadId,
                  turnId,
                }),
                type: "turn.aborted",
                payload: {
                  reason: error.detail,
                },
              });
              return yield* error;
            }),
          ),
        );

        return {
          threadId: input.threadId,
          turnId,
          resumeCursor: context.session.resumeCursor,
        };
      });

      const interruptTurn: CopilotAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
        function* (threadId, turnId) {
          const context = yield* requireSessionContextEffect(sessions, threadId);

          yield* Effect.tryPromise({
            try: () => context.sdkSession.abort(),
            catch: (cause) =>
              requestError(
                "session.abort",
                cause instanceof Error ? cause.message : "Failed to abort Copilot turn.",
                cause,
              ),
          });

          const targetTurnId = turnId ?? context.activeTurnId;
          if (targetTurnId) {
            yield* emit({
              ...createBaseEvent({
                threadId,
                turnId: targetTurnId,
              }),
              type: "turn.aborted",
              payload: {
                reason: "Interrupted by user.",
              },
            });
          }
        },
      );

      const respondToRequest: CopilotAdapterShape["respondToRequest"] = Effect.fn(
        "respondToRequest",
      )(function* (threadId, requestId, decision) {
        const context = yield* requireSessionContextEffect(sessions, threadId);

        const binding = context.pendingPermissionBindings.get(requestId);
        if (!binding) {
          return yield* requestError(
            "permission.reply",
            `Unknown pending permission request: ${requestId}`,
          );
        }

        const result: PermissionRequestResult =
          decision === "accept" || decision === "acceptForSession"
            ? { kind: "approved" }
            : { kind: "denied-interactively-by-user" };
        yield* Effect.tryPromise({
          try: async () => {
            binding.promiseKit.resolve(result);
          },
          catch: (cause) =>
            requestError(
              "permission.reply",
              cause instanceof Error ? cause.message : "Failed to resolve Copilot permission.",
              cause,
            ),
        });
      });

      const respondToUserInput: CopilotAdapterShape["respondToUserInput"] = Effect.fn(
        "respondToUserInput",
      )(function* (threadId, requestId, answers) {
        const context = yield* requireSessionContextEffect(sessions, threadId);

        const binding = context.pendingUserInputBindings.get(requestId);
        if (!binding) {
          return yield* requestError(
            "user_input.reply",
            `Unknown pending user-input request: ${requestId}`,
          );
        }

        const response = answerFromUserInput(binding, answers);
        yield* Effect.tryPromise({
          try: async () => {
            binding.promiseKit.resolve(response);
          },
          catch: (cause) =>
            requestError(
              "user_input.reply",
              cause instanceof Error ? cause.message : "Failed to resolve Copilot user input.",
              cause,
            ),
        });
      });

      const stopSessionInternal = async (threadId: ThreadId): Promise<void> => {
        const context = sessions.get(threadId);
        if (!context) {
          return;
        }
        if (context.stopped) {
          sessions.delete(threadId);
          return;
        }

        context.stopped = true;
        settlePendingPermissionHandlers(context);
        settlePendingUserInputs(context);
        try {
          await context.sdkSession.disconnect();
        } catch {
          // Best effort cleanup.
        }
        try {
          await context.client.stop();
        } catch {
          // Best effort cleanup.
        }

        updateProviderSession(context, {
          status: "closed",
          activeTurnId: undefined,
        });
        await emitAsync({
          ...createBaseEvent({
            threadId,
          }),
          type: "session.state.changed",
          payload: {
            state: "stopped",
            reason: "Copilot session stopped.",
          },
        });
        await emitAsync({
          ...createBaseEvent({
            threadId,
          }),
          type: "session.exited",
          payload: {
            reason: "Copilot session stopped.",
            exitKind: "graceful",
          },
        });
        sessions.delete(threadId);
      };

      const stopSession: CopilotAdapterShape["stopSession"] = Effect.fn("stopSession")(
        function* (threadId) {
          if (!sessions.has(threadId)) {
            return yield* sessionNotFoundError(threadId);
          }
          yield* Effect.tryPromise({
            try: () => stopSessionInternal(threadId),
            catch: (cause) =>
              processError(
                threadId,
                cause instanceof Error ? cause.message : "Failed to stop Copilot session.",
                cause,
              ),
          });
        },
      );

      const listSessions: CopilotAdapterShape["listSessions"] = () =>
        Effect.sync(() => Array.from(sessions.values(), (context) => context.session));

      const hasSession: CopilotAdapterShape["hasSession"] = (threadId) =>
        Effect.sync(() => sessions.has(threadId));

      const readThread: CopilotAdapterShape["readThread"] = Effect.fn("readThread")(
        function* (threadId) {
          const context = yield* requireSessionContextEffect(sessions, threadId);
          return {
            threadId,
            turns: context.turns.map((turn) => ({
              id: turn.id,
              items: [...turn.items],
            })),
          };
        },
      );

      const rollbackThread: CopilotAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
        function* (threadId, _numTurns) {
          if (!sessions.has(threadId)) {
            return yield* sessionNotFoundError(threadId);
          }
          return yield* requestError(
            "thread.rollback",
            "Copilot SDK does not expose thread rollback.",
          );
        },
      );

      const stopAll: CopilotAdapterShape["stopAll"] = () =>
        Effect.tryPromise({
          try: async () => {
            await Promise.all(
              Array.from(sessions.keys(), (threadId) => stopSessionInternal(threadId)),
            );
            if (managedNativeEventLogger) {
              await runWithContext(managedNativeEventLogger.close());
            }
          },
          catch: (cause) =>
            requestError(
              "stopAll",
              cause instanceof Error ? cause.message : "Failed to stop Copilot sessions.",
              cause,
            ),
        });

      return {
        provider: PROVIDER,
        capabilities: {
          sessionModelSwitch: "in-session",
        },
        startSession,
        sendTurn,
        interruptTurn,
        respondToRequest,
        respondToUserInput,
        stopSession,
        listSessions,
        hasSession,
        readThread,
        rollbackThread,
        stopAll,
        get streamEvents() {
          return Stream.fromPubSub(runtimeEventPubSub);
        },
      } satisfies CopilotAdapterShape;
    }),
  );
}

export const CopilotAdapterLive = makeCopilotAdapterLive();
