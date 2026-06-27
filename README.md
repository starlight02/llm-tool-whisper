# xml-tool-bridge

`xml-tool-bridge` is a transparent Rust proxy that lets upstream models without
native tool calling participate in the OpenAI / Anthropic tool protocols.

The client still owns tools. The proxy does not configure tools, execute tools,
or call tool backends. It only bridges representations:

1. the client sends normal tool definitions in its request
2. the proxy explains those client-provided tools to the upstream model as XML
3. the upstream model emits one or more `<tool_call>...</tool_call>` blocks
4. the proxy converts those XML blocks into the current protocol's native
   tool-call response (single OR parallel calls)
5. the client executes the tools and sends the normal tool result(s) back
6. the proxy converts those tool results into XML for the upstream model

Requests without tools are forwarded with the original body bytes. The proxy
does not change streaming flags or rewrite request bodies unless tool bridging
is required.

## API

Configured providers declare exactly one protocol:

- `chat` -> `POST /v1/chat/completions`
- `responses` -> `POST /v1/responses`
- `messages` -> `POST /v1/messages`

There is no protocol conversion. A request to `/v1/responses` will not be routed
to a `chat` provider.

`GET /v1/models` returns configured models in OpenAI list format. Model ids are
exposed as:

```text
provider/model
```

`GET /health` returns `{"ok": true}` for liveness probes.

## Configuration

Configuration is TOML only.

```toml
[server]
bind = "127.0.0.1:8787"

[log]
level = "info"

[[providers]]
name = "openai"
protocol = "chat"
base_url = "https://api.openai.com/v1"
api_key = "sk-your-openai-key"
models = ["gpt-4.1", "gpt-4.1-mini"]

[[providers]]
name = "anthropic"
protocol = "messages"
base_url = "https://api.anthropic.com/v1"
api_key = "sk-ant-your-anthropic-key"
auth_header = "x-api-key"
auth_scheme = ""
headers = { "anthropic-version" = "2023-06-01" }
models = [
  "claude-fable-5",
  "claude-opus-4-8",
  "claude-sonnet-4-6",
  "claude-haiku-4-5",
]
```

Provider auth is a default. If the client already sends the same header, the
client header wins. This lets the proxy run with configured credentials while
still allowing per-request overrides.

For Anthropic Messages requests, the client request body still needs the normal
Anthropic fields such as `max_tokens` and `messages`; the proxy does not inject
them.

## Run

```bash
cp config.example.toml config.toml
cargo run --release -- config.toml
```

If no path is passed, the proxy reads `config.toml`.

## Docker

```bash
cp config.example.toml config.toml
docker build -t xml-tool-bridge .
docker run --rm -p 8787:8787 \
  -v "$PWD/config.toml:/etc/xml-tool-bridge/config.toml:ro" \
  xml-tool-bridge
```

Compose:

```bash
cp config.example.toml config.toml
docker compose up --build
```

## Tool Bridge

The upstream model is instructed to emit each tool call in its own XML block:

```xml
<tool_call>
  <name>tool_name</name>
  <arguments><![CDATA[{"key":"value"}]]></arguments>
</tool_call>
```

The model may emit several `<tool_call>` blocks back-to-back in one turn for
parallel work; the proxy collects every block in source order.

The proxy converts XML blocks into native tool calls for the request protocol:

- Chat Completions: `choices[0].message.tool_calls` (array)
- Responses: each call is one `output[].type = "function_call"` item
- Messages: each call is one `content[].type = "tool_use"` block

When the client sends tool results back, the proxy converts those result
messages into:

```xml
<tool_result>
  <name>tool_name</name>
  <content><![CDATA[{"ok":true,"content":"result"}]]></content>
</tool_result>
```

Then it forwards the request to the same upstream protocol. Any extra metadata
the client attached to the result (stdout / stderr / exit_code / status /
citations / usage / etc.) is preserved verbatim inside the payload — the
upstream model receives the full side-channel context.

### Robustness

The bridge handles several real-world failure modes:

- Tool arguments that contain the literal `]]>` sequence are split across two
  CDATA sections, so payloads never accidentally close the wrapper.
- Visible prose that precedes or surrounds the `<tool_call>` blocks is
  forwarded to the client alongside the synthesized tool calls (Chat
  `message.content`, Responses `output[].message`, Messages `content[].text`).
- A leaked `Thinking...\n> ...` preamble is lifted into the protocol's
  reasoning surface (Chat `reasoning_content`, Responses `reasoning` item,
  Messages `thinking` block) — never dropped silently.
- Streaming responses scan for `<tool_call>` markers without leaking partial
  XML to the client, and emit native tool-call SSE chunks once a complete
  block is in.
- Argument aliases (`cmd` → `command`, `q` → `query`, `file` → `path`, …) are
  rewritten only when the tool's declared schema accepts the canonical name
  AND does not declare the alias as a distinct property.

## Performance

- No-tool requests are raw byte-body passthrough and stream upstream responses
  without buffering.
- Two long-lived `reqwest` clients (one for streaming, one with a 5-minute
  total cap for JSON turns) reuse upstream connections.
- Tool requests make one upstream request per client request. Tool execution
  and multi-step orchestration stay in the client.

