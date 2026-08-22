# Issue #6558: POST /api/v1/ai/chat's response content type is request-body dependent

## Problem

`AiChatHandler` defaults request-body field `mode` to `auto`, and that branch returns
`text/event-stream` while every other `mode` value returns `application/json`. OpenAPI 3.0's
`content` map models Accept-header negotiation; it cannot express a response type selected by a
request-body field. A generated client binds `application/json` for the 200 response and breaks
on the default (`auto`) path.

Mitigation in #4895 only hoisted a warning into the operation description - documentation, not a
fix; a generated client still can't act on it.

## Resolution chosen

**Split the operation** (chosen by the developer over Accept-header negotiation, which would have
changed the wire contract for existing callers with no client-visible benefit beyond spec purity):

- `POST /api/v1/ai/chat` - always returns the JSON `AiChatResponse` body (the former
  "review-first" path). The request-body `mode` field is no longer read by the handler.
- `POST /api/v1/ai/chat/stream` (new) - always returns the `text/event-stream` SSE stream (the
  former "auto" / client-orchestrated tool-use path).

Each operation now has exactly one honest response content type, so a generated client's return
type binding is correct for both.

## Changes

- `server/src/main/java/com/arcadedb/server/ai/AiChatHandler.java` - takes a `streaming` boolean
  in its constructor; `execute()` branches on that field instead of the request-body `mode` value.
- `server/src/main/java/com/arcadedb/server/http/HttpServer.java` - registers two routes,
  `POST /chat` (`streaming=false`) and `POST /chat/stream` (`streaming=true`), both backed by
  `AiChatHandler`.
- `server/src/main/java/com/arcadedb/server/http/handler/openapi/AiApiSpec.java` - splits
  `createChatPath()` into a JSON-only `/api/v1/ai/chat` and a new SSE-only
  `/api/v1/ai/chat/stream`; drops the now-unused `mode` property from `AiChatRequest`.
- `studio/src/main/resources/static/js/studio-ai.js` - `aiSendMessageStreaming()` now posts to
  `api/v1/ai/chat/stream` instead of `api/v1/ai/chat`.
- Tests: `AiChatHandlerStreamingTest` updated to hit `/chat/stream`; `OpenApiSpecGenerationIT`'s
  anti-drift inventory gains `POST /api/v1/ai/chat/stream` (63 -> 64 operations).

## Test plan

- `AiChatHandlerStreamingTest#streamingAutoModeExecutesToolLocallyAndPostsResultBack` against
  `/api/v1/ai/chat/stream`.
- New test: `POST /api/v1/ai/chat` returns JSON even when the request body still sets
  `mode: "auto"` (regression test for the exact bug reported).
- `OpenApiSpecGenerationIT` full suite (anti-drift + operation-count + operationId-uniqueness
  checks).
- `AiServerTest` (unaffected paths still pass: protocol-version rejection, not-configured
  rejection).
