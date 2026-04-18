import { Effect, Layer, Schema } from "effect";

import {
  type ChatAttachment,
  type CopilotModelSelection,
  TextGenerationError,
} from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import { createCopilotClient } from "../../provider/copilotRuntime.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "../Prompts.ts";
import { type TextGenerationShape, TextGeneration } from "../Services/TextGeneration.ts";
import {
  getModelSelectionStringOptionValue,
  extractJsonObject,
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
  toJsonSchemaObject,
} from "../Utils.ts";

const COPILOT_TIMEOUT_MS = 180_000;

function isTextGenerationError(error: unknown): error is TextGenerationError {
  return (
    typeof error === "object" &&
    error !== null &&
    "_tag" in error &&
    error._tag === "TextGenerationError"
  );
}

function copilotJsonPrompt(prompt: string, outputSchemaJson: Schema.Top): string {
  const schemaDocument = JSON.stringify(toJsonSchemaObject(outputSchemaJson), null, 2);
  return `${prompt}

Return exactly one JSON object matching this schema:
${schemaDocument}

Do not wrap the JSON in markdown fences or include any other text.`;
}

const makeCopilotTextGeneration = Effect.gen(function* () {
  const serverConfig = yield* ServerConfig;
  const serverSettingsService = yield* ServerSettingsService;

  const runCopilotJson = <S extends Schema.Top>(input: {
    readonly operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    readonly cwd: string;
    readonly prompt: string;
    readonly outputSchemaJson: S;
    readonly modelSelection: CopilotModelSelection;
    readonly attachments?: ReadonlyArray<ChatAttachment> | undefined;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const settings = yield* Effect.map(
        serverSettingsService.getSettings,
        (value) => value.providers.copilot,
      ).pipe(Effect.catch(() => Effect.undefined));
      if (!settings?.enabled) {
        return yield* new TextGenerationError({
          operation: input.operation,
          detail: "Copilot is disabled in server settings.",
        });
      }

      const fileAttachments = (input.attachments ?? [])
        .map((attachment) => {
          const path = resolveAttachmentPath({
            attachmentsDir: serverConfig.attachmentsDir,
            attachment,
          });
          return path
            ? {
                type: "file" as const,
                path,
                displayName: attachment.name,
              }
            : null;
        })
        .filter((attachment): attachment is NonNullable<typeof attachment> => attachment !== null);

      const rawContent = yield* Effect.tryPromise({
        try: async () => {
          const reasoningEffort = getModelSelectionStringOptionValue(
            input.modelSelection,
            "reasoningEffort",
          );
          const client = createCopilotClient({
            settings,
            cwd: input.cwd,
            logLevel: "error",
          });

          try {
            await client.start();
            const session = await client.createSession({
              clientName: "t3-code-git-text",
              model: input.modelSelection.model,
              ...(reasoningEffort ? { reasoningEffort } : {}),
              workingDirectory: input.cwd,
              streaming: false,
              availableTools: [],
              enableConfigDiscovery: false,
              onPermissionRequest: () => ({
                kind: "denied-no-approval-rule-and-could-not-request-from-user",
              }),
            });

            try {
              const response = await session.sendAndWait(
                {
                  prompt: copilotJsonPrompt(input.prompt, input.outputSchemaJson),
                  ...(fileAttachments.length > 0 ? { attachments: fileAttachments } : {}),
                },
                COPILOT_TIMEOUT_MS,
              );
              return response?.data.content.trim() ?? "";
            } finally {
              await session.disconnect().catch(() => {
                // Best effort cleanup.
              });
            }
          } finally {
            await client.stop().catch(() => {
              // Best effort cleanup.
            });
          }
        },
        catch: (cause) =>
          isTextGenerationError(cause)
            ? cause
            : new TextGenerationError({
                operation: input.operation,
                detail:
                  cause instanceof Error
                    ? cause.message
                    : "Copilot text generation request failed.",
                cause,
              }),
      });

      if (rawContent.length === 0) {
        return yield* new TextGenerationError({
          operation: input.operation,
          detail: "Copilot returned empty output.",
        });
      }

      return yield* Schema.decodeEffect(Schema.fromJsonString(input.outputSchemaJson))(
        extractJsonObject(rawContent),
      ).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation: input.operation,
              detail: "Copilot returned invalid structured output.",
              cause,
            }),
          ),
        ),
      );
    }).pipe(
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({
              operation: input.operation,
              detail: "Copilot text generation request failed.",
              cause,
            }),
      ),
    );

  const generateCommitMessage: TextGenerationShape["generateCommitMessage"] = Effect.fn(
    "CopilotTextGeneration.generateCommitMessage",
  )(function* (input) {
    if (input.modelSelection.provider !== "copilot") {
      return yield* new TextGenerationError({
        operation: "generateCommitMessage",
        detail: "Copilot text generation requires a Copilot model selection.",
      });
    }

    const { prompt, outputSchema } = buildCommitMessagePrompt({
      branch: input.branch,
      stagedSummary: input.stagedSummary,
      stagedPatch: input.stagedPatch,
      includeBranch: input.includeBranch === true,
    });
    const generated = yield* runCopilotJson({
      operation: "generateCommitMessage",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: outputSchema,
      modelSelection: input.modelSelection,
    });

    return {
      subject: sanitizeCommitSubject(generated.subject),
      body: generated.body.trim(),
      ...(input.includeBranch && "branch" in generated && typeof generated.branch === "string"
        ? { branch: sanitizeFeatureBranchName(sanitizeBranchFragment(generated.branch)) }
        : {}),
    };
  });

  const generatePrContent: TextGenerationShape["generatePrContent"] = Effect.fn(
    "CopilotTextGeneration.generatePrContent",
  )(function* (input) {
    if (input.modelSelection.provider !== "copilot") {
      return yield* new TextGenerationError({
        operation: "generatePrContent",
        detail: "Copilot text generation requires a Copilot model selection.",
      });
    }

    const { prompt, outputSchema } = buildPrContentPrompt({
      baseBranch: input.baseBranch,
      headBranch: input.headBranch,
      commitSummary: input.commitSummary,
      diffSummary: input.diffSummary,
      diffPatch: input.diffPatch,
    });
    const generated = yield* runCopilotJson({
      operation: "generatePrContent",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: outputSchema,
      modelSelection: input.modelSelection,
    });

    return {
      title: sanitizePrTitle(generated.title),
      body: generated.body.trim(),
    };
  });

  const generateBranchName: TextGenerationShape["generateBranchName"] = Effect.fn(
    "CopilotTextGeneration.generateBranchName",
  )(function* (input) {
    if (input.modelSelection.provider !== "copilot") {
      return yield* new TextGenerationError({
        operation: "generateBranchName",
        detail: "Copilot text generation requires a Copilot model selection.",
      });
    }

    const { prompt, outputSchema } = buildBranchNamePrompt({
      message: input.message,
      attachments: input.attachments,
    });
    const generated = yield* runCopilotJson({
      operation: "generateBranchName",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: outputSchema,
      modelSelection: input.modelSelection,
      attachments: input.attachments,
    });

    return {
      branch: sanitizeFeatureBranchName(sanitizeBranchFragment(generated.branch)),
    };
  });

  const generateThreadTitle: TextGenerationShape["generateThreadTitle"] = Effect.fn(
    "CopilotTextGeneration.generateThreadTitle",
  )(function* (input) {
    if (input.modelSelection.provider !== "copilot") {
      return yield* new TextGenerationError({
        operation: "generateThreadTitle",
        detail: "Copilot text generation requires a Copilot model selection.",
      });
    }

    const { prompt, outputSchema } = buildThreadTitlePrompt({
      message: input.message,
      attachments: input.attachments,
    });
    const generated = yield* runCopilotJson({
      operation: "generateThreadTitle",
      cwd: input.cwd,
      prompt,
      outputSchemaJson: outputSchema,
      modelSelection: input.modelSelection,
      attachments: input.attachments,
    });

    return {
      title: sanitizeThreadTitle(generated.title),
    };
  });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGenerationShape;
});

export const CopilotTextGenerationLive = Layer.effect(TextGeneration, makeCopilotTextGeneration);
