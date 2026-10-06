# @azure-tools/typespec-go

TypeSpec emitter for Go SDKs

## Install

```bash
npm install @azure-tools/typespec-go
```

## Emitter usage

1. Via the command line

```bash
tsp compile . --emit=@azure-tools/typespec-go
```

2. Via the config

```yaml
emit:
  - "@azure-tools/typespec-go"
```

The config can be extended with options as follows:

```yaml
emit:
  - "@azure-tools/typespec-go"
options:
  "@azure-tools/typespec-go":
    option: value
```

## Server-sent event streams

Generated SSE operation options include a `LastEventID` string. A non-empty value
sets the `Last-Event-ID` request header; the default empty value omits the header.
Use this option only when the service supports resuming from an event ID. Its
presence does not imply that the service supports resumption.

Streams do not automatically reconnect. Reaching the end of the response body
ends the stream with `io.EOF`, even if events contain IDs, and read errors are
returned to the caller.

To resume, save the reader's `LastEventID()` after successfully processing an
event, close the reader when finished, and call the operation again with that
checkpoint in its `LastEventID` option. The service determines which event IDs
remain valid and where the resumed stream starts.

The TypeSpec SSE library does not currently expose a resumability annotation, so
resumption support cannot be inferred from the operation's TypeSpec definition.

## Emitter options

### `emitter-output-dir`

**Type:** `absolutePath`

Defines the emitter output directory. Defaults to `{output-dir}/@azure-tools/typespec-go`
See [Configuring output directory for more info](https://typespec.io/docs/handbook/configuration/configuration/#configuring-output-directory)

### `azcore-version`

**Type:** `string`

Semantic version of azcore without the leading 'v' to use if different from the default version (e.g. 1.2.3).

### `containing-module`

**Type:** `string`

The module into which the package is being emitted. Mutually exclusive with module.

### `disallow-unknown-fields`

**Type:** `boolean`

When true, unmarshalers will return an error when an unknown field is encountered in the payload. The default is false.

### `emit-content-type-header`

**Type:** `boolean`

Includes the Content-Type header in response envelopes for modeled responses. The default is false.

### `file-prefix`

**Type:** `string`

Optional prefix to file names. For example, if you set your file prefix to "zzz_", all generated code files will begin with "zzz_".

### `generate-fakes`

**Type:** `boolean`

When true, enables generation of fake servers. The default is false.

### `go-generate`

**Type:** `string`

Path to a post-generation 'go generate' script. The path is relative to the emitter-output-dir.

### `head-as-boolean`

**Type:** `boolean`

When true, HEAD requests will return a boolean value based on the HTTP status code. The default is false.

### `inject-spans`

**Type:** `boolean`

Enables generation of spans for distributed tracing. The default is false.

### `module`

**Type:** `string`

The module identity to use in go.mod. Mutually exclusive with containing-module.

### `omit-constructors`

**Type:** `boolean`

When true, client constructors are not emitted. The default is false.

### `rawjson-as-bytes`

**Type:** `boolean`

When true, properties that are untyped (i.e. raw JSON) are exposed as []byte instead of any or map[string]any. The default is false.

### `slice-elements-byval`

**Type:** `boolean`

When true, slice elements will not be pointer-to-type. The default is false.

### `single-client`

**Type:** `boolean`

Indicates package has a single client. This will omit the Client prefix from options and response types. If multiple clients are detected, an error is returned. The default is false.

### `stutter`

**Type:** `string`

Uses the specified value to remove stuttering from types and funcs instead of the built-in algorithm.

### `fix-const-stuttering`

**Type:** `boolean`

When true, fix stuttering for `const` types and values. The default is false.

### `generate-examples`

**Type:** `boolean`

Deprecated. Use generate-samples instead.

### `generate-samples`

**Type:** `boolean`

When true, generate example tests. The default is false.

### `factory-gather-all-params`

**Type:** `boolean`

**Default:** `true`

When true, the `NewClientFactory` constructor gathers all parameters. When false, it only gathers common parameters of clients. The default is true.
