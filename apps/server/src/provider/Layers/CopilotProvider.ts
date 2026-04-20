import type { CopilotSettings, ServerProvider } from "@t3tools/contracts";
import { Effect, Equal, Layer, Stream } from "effect";

import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { buildServerProvider } from "../providerSnapshot.ts";
import { CopilotProvider } from "../Services/CopilotProvider.ts";
import {
  authSnapshotFromCopilotSdk,
  createCopilotClient,
  formatCopilotProbeError,
  modelsFromCopilotSdk,
  toCopilotProbeError,
  versionFromCopilotStatus,
} from "../copilotRuntime.ts";

const PROVIDER = "copilot" as const;
const COPILOT_REFRESH_INTERVAL = "1 hour";

function makePendingCopilotProvider(settings: CopilotSettings): ServerProvider {
  const checkedAt = new Date().toISOString();
  const models = modelsFromCopilotSdk({
    models: [],
    customModels: settings.customModels,
  });

  if (!settings.enabled) {
    return buildServerProvider({
      provider: PROVIDER,
      enabled: false,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Copilot is disabled in T3 Code settings.",
      },
    });
  }

  return buildServerProvider({
    provider: PROVIDER,
    enabled: true,
    checkedAt,
    models,
    probe: {
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      message: "Checking GitHub Copilot SDK availability...",
    },
  });
}

export function checkCopilotProviderStatus(input: {
  readonly settings: CopilotSettings;
  readonly cwd: string;
}): Effect.Effect<ServerProvider> {
  if (!input.settings.enabled) {
    return Effect.succeed(makePendingCopilotProvider(input.settings));
  }

  const checkedAt = new Date().toISOString();
  const fallback = (cause: unknown, version: string | null = null) => {
    const failure = formatCopilotProbeError({
      cause,
      settings: input.settings,
    });
    return buildServerProvider({
      provider: PROVIDER,
      enabled: true,
      checkedAt,
      models: modelsFromCopilotSdk({
        models: [],
        customModels: input.settings.customModels,
      }),
      probe: {
        installed: failure.installed,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: failure.message,
      },
    });
  };

  return Effect.acquireUseRelease(
    Effect.sync(() =>
      createCopilotClient({
        settings: input.settings,
        cwd: input.cwd,
        logLevel: "error",
      }),
    ),
    (client) =>
      Effect.tryPromise({
        try: async () => {
          await client.start();
          const [status, authStatus, models] = await Promise.all([
            client.getStatus(),
            client.getAuthStatus(),
            client.listModels(),
          ]);
          const authSnapshot = authSnapshotFromCopilotSdk(authStatus);
          const providerModels = modelsFromCopilotSdk({
            models,
            customModels: input.settings.customModels,
          });
          const hasBuiltInModels = models.length > 0;

          return buildServerProvider({
            provider: PROVIDER,
            enabled: true,
            checkedAt,
            models: providerModels,
            probe: {
              installed: true,
              version: versionFromCopilotStatus(status),
              status:
                authSnapshot.status !== "ready"
                  ? authSnapshot.status
                  : hasBuiltInModels
                    ? "ready"
                    : "warning",
              auth: authSnapshot.auth,
              ...(authSnapshot.message
                ? { message: authSnapshot.message }
                : hasBuiltInModels
                  ? {}
                  : { message: "Copilot did not report any available models for this account." }),
            },
          });
        },
        catch: toCopilotProbeError,
      }).pipe(Effect.catch((cause) => Effect.succeed(fallback(cause)))),
    (client) => Effect.promise(() => client.stop()).pipe(Effect.ignore({ log: true })),
  );
}

const makeCopilotProvider = Effect.gen(function* () {
  const serverConfig = yield* ServerConfig;
  const serverSettingsService = yield* ServerSettingsService;

  return yield* makeManagedServerProvider({
    getSettings: serverSettingsService.getSettings.pipe(
      Effect.map((settings) => settings.providers.copilot),
      Effect.orDie,
    ),
    streamSettings: serverSettingsService.streamChanges.pipe(
      Stream.map((settings) => settings.providers.copilot),
    ),
    haveSettingsChanged: (previous, next) => !Equal.equals(previous, next),
    initialSnapshot: makePendingCopilotProvider,
    checkProvider: serverSettingsService.getSettings.pipe(
      Effect.map((settings) => settings.providers.copilot),
      Effect.flatMap((settings) =>
        checkCopilotProviderStatus({
          settings,
          cwd: serverConfig.cwd,
        }),
      ),
    ),
    refreshInterval: COPILOT_REFRESH_INTERVAL,
  });
});

export const CopilotProviderLive = Layer.effect(CopilotProvider, makeCopilotProvider);
