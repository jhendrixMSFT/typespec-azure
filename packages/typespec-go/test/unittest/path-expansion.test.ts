import { describe, expect, it } from "vitest";
import { emitGoFor } from "./scenario-runner.js";

describe("path expansion helper emission", () => {
  it("does not emit payload models or serde for query-only parameter groups", async () => {
    const files = await emitGoFor(`
      @service
      namespace Expansions;
      model BaseFilter { field: string; }
      model Filter extends BaseFilter { count?: int32; }
      @route("/required{?filter*}")
      op required(filter: Filter): void;
      @route("/optional{?filter}")
      op optional(filter?: Filter): void;
    `);
    expect(files.has("zz_models.go")).toBe(false);
    expect(files.has("zz_models_serde.go")).toBe(false);
    expect(files.has("zz_options.go")).toBe(true);
  });

  it.each([
    '@post @route("/body") op body(@body payload: Payload): void;',
    '@get @route("/response") op response(): Payload;',
  ])("preserves payload models and serde for shared nested models: %s", async (operation) => {
    const files = await emitGoFor(`
      @service
      namespace Expansions;
      model Filter { count: int32; }
      model Payload { filter: Filter; }
      @route("/query{?filter*}")
      op query(filter: Filter): void;
      ${operation}
    `);
    expect(files.has("zz_models.go")).toBe(true);
    expect(files.has("zz_models_serde.go")).toBe(true);
  });

  it.each([
    { location: "path", collection: "(int32 | null)[]" },
    { location: "query", collection: "(int32 | null)[]" },
    { location: "path", collection: "Record<int32 | null>" },
    { location: "query", collection: "Record<int32 | null>" },
  ])("rejects nullable entries in $location $collection", async ({ location, collection }) => {
    await expect(
      emitGoFor(`
      @service
      namespace Expansions;
      @route("/items")
      op read(@${location} value: ${collection}): void;
    `),
    ).rejects.toThrow("nullable entries in path/query collections are not supported");
  });

  it.each([
    { name: "scalar paths", parameter: "@path value: string", required: false },
    {
      name: "reserved scalar paths",
      parameter: "@path(#{ allowReserved: true }) value: string",
      required: false,
    },
    { name: "query arrays", parameter: "@query value: string[]", required: false },
    { name: "path arrays", parameter: "@path value: string[]", required: true },
    { name: "path maps", parameter: "@path value: Record<int32>", required: true },
  ])("emits helpers when needed for $name", async ({ parameter, required }) => {
    const files = await emitGoFor(`
      @service
      namespace Expansions;
      @route("/items")
      op read(${parameter}): void;
    `);
    expect(files.has("zz_path_helpers.go")).toBe(required);
  });

  it("places helpers in the containing package with the configured file prefix", async () => {
    const files = await emitGoFor(
      `
        @service
        namespace Expansions;
        @route("/items{/values*}")
        op read(values: string[]): void;
      `,
      {
        "containing-module": "example.com/parent",
        "emitter-output-dir": "{output-dir}/child",
        "file-prefix": "generated",
      },
    );
    expect(files.has("child/generated_path_helpers.go")).toBe(true);
  });
});
