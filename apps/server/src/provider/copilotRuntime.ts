import {
  CopilotClient,
  type CopilotClientOptions,
  type GetAuthStatusResponse,
  type GetStatusResponse,
  type ModelInfo,
} from "@github/copilot-sdk";
import type {
  CopilotSettings,
  ModelCapabilities,
  ServerProviderAuth,
  ServerProviderModel,
  ServerProviderState,
} from "@t3tools/contracts";

import { providerModelsFromSettings } from "./providerSnapshot.ts";

export const EMPTY_COPILOT_MODEL_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  contextWindowOptions: [],
  promptInjectedEffortLevels: [],
};

const COPILOT_REASONING_LABELS = {
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
} as const;

function trimOrUndefined(value: string | null | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

function authTypeLabel(authType: GetAuthStatusResponse["authType"]): string | undefined {
  switch (authType) {
    case "user":
      return "Signed-in user";
    case "env":
      return "Environment token";
    case "gh-cli":
      return "GitHub CLI";
    case "hmac":
      return "HMAC key";
    case "api-key":
      return "API key";
    case "token":
      return "Bearer token";
    default:
      return undefined;
  }
}

export function createCopilotClient(input: {
  readonly settings: CopilotSettings;
  readonly cwd?: string;
  readonly env?: Record<string, string | undefined>;
  readonly logLevel?: CopilotClientOptions["logLevel"];
  readonly onListModels?: CopilotClientOptions["onListModels"];
}) {
  const cliPath = trimOrUndefined(input.settings.binaryPath);
  const cliUrl = trimOrUndefined(input.settings.serverUrl);

  return new CopilotClient({
    ...(cliUrl ? { cliUrl } : {}),
    ...(!cliUrl && cliPath ? { cliPath } : {}),
    ...(input.cwd ? { cwd: input.cwd } : {}),
    ...(input.env ? { env: input.env } : {}),
    ...(input.logLevel ? { logLevel: input.logLevel } : {}),
    ...(input.onListModels ? { onListModels: input.onListModels } : {}),
  });
}

export function versionFromCopilotStatus(status: GetStatusResponse): string | null {
  return trimOrUndefined(status.version) ?? null;
}

export function capabilitiesFromCopilotModel(
  model: Pick<ModelInfo, "supportedReasoningEfforts" | "defaultReasoningEffort">,
): ModelCapabilities {
  const reasoningEffortLevels =
    model.supportedReasoningEfforts?.map((effort) => ({
      value: effort,
      label: COPILOT_REASONING_LABELS[effort],
      ...(model.defaultReasoningEffort === effort ? { isDefault: true } : {}),
    })) ?? [];

  return {
    reasoningEffortLevels,
    supportsFastMode: false,
    supportsThinkingToggle: false,
    contextWindowOptions: [],
    promptInjectedEffortLevels: [],
  };
}

export function modelsFromCopilotSdk(input: {
  readonly models: ReadonlyArray<ModelInfo>;
  readonly customModels: ReadonlyArray<string>;
}): ReadonlyArray<ServerProviderModel> {
  const builtInModels = input.models.map((model) => ({
    slug: model.id.trim(),
    name: trimOrUndefined(model.name) ?? model.id.trim(),
    isCustom: false,
    capabilities: capabilitiesFromCopilotModel(model),
  })) satisfies ReadonlyArray<ServerProviderModel>;

  return providerModelsFromSettings(
    builtInModels,
    "copilot",
    input.customModels,
    EMPTY_COPILOT_MODEL_CAPABILITIES,
  );
}

export function authSnapshotFromCopilotSdk(authStatus: GetAuthStatusResponse): {
  readonly status: Exclude<ServerProviderState, "disabled">;
  readonly auth: ServerProviderAuth;
  readonly message?: string;
} {
  const authType = trimOrUndefined(authStatus.authType);
  const label = [
    authTypeLabel(authStatus.authType),
    authStatus.login ? `@${authStatus.login}` : undefined,
    trimOrUndefined(authStatus.host)?.replace(/^https?:\/\//, ""),
  ]
    .filter((part): part is string => typeof part === "string" && part.length > 0)
    .join(" - ");

  if (!authStatus.isAuthenticated) {
    return {
      status: "error",
      auth: {
        status: "unauthenticated",
        ...(authType ? { type: authType } : {}),
        ...(label ? { label } : {}),
      },
      message:
        trimOrUndefined(authStatus.statusMessage) ??
        "GitHub Copilot is not authenticated. Sign in with the Copilot CLI or provide a supported token.",
    };
  }

  return {
    status: "ready",
    auth: {
      status: "authenticated",
      ...(authType ? { type: authType } : {}),
      ...(label ? { label } : {}),
    },
  };
}

export function formatCopilotProbeError(input: {
  readonly cause: unknown;
  readonly settings: CopilotSettings;
}): {
  readonly installed: boolean;
  readonly message: string;
} {
  const message =
    input.cause instanceof Error ? input.cause.message.trim() : String(input.cause ?? "");
  const lower = message.toLowerCase();
  const cliUrl = trimOrUndefined(input.settings.serverUrl);
  const cliPath = trimOrUndefined(input.settings.binaryPath);

  if (cliUrl) {
    if (
      lower.includes("econnrefused") ||
      lower.includes("enotfound") ||
      lower.includes("fetch failed") ||
      lower.includes("network") ||
      lower.includes("timed out") ||
      lower.includes("timeout")
    ) {
      return {
        installed: true,
        message: `Couldn't reach the configured Copilot server at ${cliUrl}. Check that it is running and the URL is correct.`,
      };
    }

    return {
      installed: true,
      message: message || "Failed to connect to the configured Copilot server.",
    };
  }

  if (
    lower.includes("enoent") ||
    lower.includes("spawn") ||
    lower.includes("not found") ||
    lower.includes("could not find")
  ) {
    return {
      installed: false,
      message: cliPath
        ? `The configured Copilot binary could not be started: ${cliPath}.`
        : "The bundled GitHub Copilot CLI could not be started.",
    };
  }

  return {
    installed: true,
    message: message || "GitHub Copilot SDK probe failed.",
  };
}
