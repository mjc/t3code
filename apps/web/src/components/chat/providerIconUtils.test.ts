import { describe, expect, it } from "vitest";

import { GithubCopilotIcon } from "../Icons";
import { getProviderLabel, PROVIDER_ICON_BY_PROVIDER } from "./providerIconUtils";

describe("providerIconUtils", () => {
  it("uses the dedicated GitHub Copilot icon for the copilot provider", () => {
    expect(PROVIDER_ICON_BY_PROVIDER.copilot).toBe(GithubCopilotIcon);
  });

  it("uses the GitHub Copilot display label for copilot models", () => {
    expect(
      getProviderLabel("copilot", {
        slug: "gpt-4.1",
        name: "GPT-4.1",
      }),
    ).toBe("GitHub Copilot");
  });
});
