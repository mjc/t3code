import { Context } from "effect";
import type { Effect } from "effect";

import type { CodexDiscoverySnapshot } from "../codexAppServer.ts";

export interface CodexDiscoveryShape {
  readonly probe: (input: {
    readonly binaryPath: string;
    readonly homePath?: string;
    readonly cwd: string;
  }) => Effect.Effect<CodexDiscoverySnapshot, Error>;
}

export class CodexDiscovery extends Context.Service<CodexDiscovery, CodexDiscoveryShape>()(
  "t3/provider/Services/CodexDiscovery",
) {}
