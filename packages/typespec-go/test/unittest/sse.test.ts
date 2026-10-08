// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

import { describe, expect, it } from "vitest";
import { emitGoFor } from "./scenario-runner.js";

describe("SSE connectors and resumption options", () => {
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
    expect(client).toContain("connectionOptions := TestClientOpenReceiveOptions{}");
    expect(client).toContain("connectionOptions = *options");
    expect(client).toContain(
      "connect := func(ctx context.Context, lastEventID string) (io.ReadCloser, error)",
    );
    expect(client).toContain("connectionOptions.LastEventID = lastEventID");
    expect(client).toContain("return client.receive(ctx, &connectionOptions)");
    expect(client).toContain("streaming.NewEventReader(ctx, connect,");
    expect(client).toContain(
      "&streaming.EventReaderOptions{LastEventID: connectionOptions.LastEventID}",
    );
    expect(client).toContain(
      "receive(ctx context.Context, options *TestClientOpenReceiveOptions) (io.ReadCloser, error)",
    );
    expect(client).toContain("return runtime.SSEResponse(httpResp, err, http.StatusOK)");
    expect(client).toContain("runtime.SkipBodyDownload(req)");
    expect(client).not.toContain("Reconnect:");
    expect(client).not.toContain("Connect:");
    expect(client).not.toContain("streaming.NewEventReader(resp,");
    expect(client).not.toContain("options.LastEventID =");
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
    expect(client).toContain("connectionOptions = *opts");
    expect(client).toContain("connectionOptions.LastEventID1 = lastEventID");
    expect(client).toContain("return client.receive(ctx, options, &connectionOptions)");
    expect(client).toContain(
      "&streaming.EventReaderOptions{LastEventID: connectionOptions.LastEventID1}",
    );
    const server = files.get("fake/zz_test_server.go");
    expect(server).toContain('LastEventID1: req.Header.Get("Last-Event-ID"),');
  });

  it("avoids shadowing required request parameters in the connector", async () => {
    const files = await emitGoFor(
      spec.replace(
        "receive()",
        "receive(@query connectionOptions: string, @query connectionOptions1: string, @query connect: string, @query lastEventID: string, @query reader: string)",
      ),
    );
    const client = files.get("zz_test_client.go");
    expect(client).toContain("connectionOptions2 := TestClientOpenReceiveOptions{}");
    expect(client).toContain(
      "connect1 := func(ctx context.Context, lastEventID1 string) (io.ReadCloser, error)",
    );
    expect(client).toContain("connectionOptions2.LastEventID = lastEventID1");
    expect(client).toContain(
      "return client.receive(ctx, connectionOptions, connectionOptions1, connect, lastEventID, reader, &connectionOptions2)",
    );
    expect(client).toContain("reader1, err := streaming.NewEventReader(ctx, connect1,");
    expect(client).toContain("return TestClientOpenReceiveResponse{Stream: reader1}, nil");
  });

  it("preserves required request bodies and optional request parameters", async () => {
    const files = await emitGoFor(
      spec
        .replace("model Info", "model Request { query: string; } model Info")
        .replace(
          "@get op receive()",
          "@post op receive(@body request: Request, @query filter?: string)",
        ),
    );
    const client = files.get("zz_test_client.go");
    expect(client).toContain("return client.receive(ctx, request, &connectionOptions)");
    expect(client).toContain("connectionOptions = *options");
    expect(client).toContain("runtime.MarshalAsJSON(req, request)");
    expect(client).toContain('reqQP.Set("filter"');
  });

  it("passes authored success status codes to the runtime helper", async () => {
    const files = await emitGoFor(
      spec
        .replace(
          '@route("/events")',
          'model AcceptedStream extends SSEStream<Events> { @statusCode statusCode: 201 | 202; } @route("/events")',
        )
        .replace("receive(): SSEStream<Events>", "receive(): AcceptedStream"),
    );
    const client = files.get("zz_test_client.go");
    expect(client).toContain(
      "return runtime.SSEResponse(httpResp, err, http.StatusCreated, http.StatusAccepted)",
    );
    expect(client).not.toContain("runtime.SSEResponse(httpResp, err, http.StatusOK)");
  });

  it("passes the operation context to the reader when tracing and fakes are enabled", async () => {
    const files = await emitGoFor(spec, { "generate-fakes": true, "inject-spans": true });
    const client = files.get("zz_test_client.go");
    expect(client).toContain(
      "ctx = context.WithValue(ctx, runtime.CtxAPINameKey{}, operationName)",
    );
    expect(client).toContain("ctx, endSpan := runtime.StartSpan(ctx, operationName,");
    expect(client).toContain("streaming.NewEventReader(ctx, connect,");
    expect(client).toContain("return client.receive(ctx, &connectionOptions)");
    expect(client).not.toContain("resp.Request.Context()");
  });
});
