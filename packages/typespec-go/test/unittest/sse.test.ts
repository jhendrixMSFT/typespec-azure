// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

import { describe, expect, it } from "vitest";
import { emitGoFor } from "./scenario-runner.js";

describe("SSE resumption options", () => {
  const spec = `
    using TypeSpec.SSE;
    @service namespace Test;
    model Info { message: string; }
    @TypeSpec.Events.events union Events { message: Info }
    @route("/events") @get op receive(): SSEStream<Events>;
  `;

  it("emits a last event ID header option without automatic reconnection", async () => {
    const files = await emitGoFor(spec, { "generate-fakes": true });
    const options = files.get("zz_options.go");
    expect(options).toContain("LastEventID string");
    expect(options).toContain("The default is empty");
    const client = files.get("zz_test_client.go");
    expect(client).toContain('options != nil && options.LastEventID != ""');
    expect(client).toContain('Header.Set("Last-Event-ID", options.LastEventID)');
    expect(client).not.toContain("Reconnect:");
    expect(client).not.toContain("Connect:");
    expect(client).not.toContain("lastEventID string");
    const server = files.get("fake/zz_test_server.go");
    expect(server).toContain('LastEventID: req.Header.Get("Last-Event-ID"),');
  });

  it("uses the actual options parameter name and avoids modeled field collisions", async () => {
    const files = await emitGoFor(
      spec.replace("receive()", "receive(@query options: string, @query lastEventID?: string)"),
      { "generate-fakes": true },
    );
    const options = files.get("zz_options.go");
    expect(options).toContain("LastEventID *string");
    expect(options).toContain("LastEventID1 string");
    const client = files.get("zz_test_client.go");
    expect(client).toContain('opts != nil && opts.LastEventID1 != ""');
    expect(client).toContain('Header.Set("Last-Event-ID", opts.LastEventID1)');
    expect(client).toContain('reqQP.Set("lastEventID"');
    const server = files.get("fake/zz_test_server.go");
    expect(server).toContain('LastEventID1: req.Header.Get("Last-Event-ID"),');
  });
});
