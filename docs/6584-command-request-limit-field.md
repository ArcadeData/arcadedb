# Issue #6584 — OpenAPI: CommandRequest omits 'limit', which PostCommandHandler honors

## Root cause

`server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java`
declares two request schemas for the two `POST` command/query endpoints:

- `createQueryRequestSchema()` — declares `command`, `language`, `params`,
  `serializer`, and `limit`.
- `createCommandRequestSchema()` — declares only `command`, `language`, `params`.

Both `/api/v1/query/{database}` and `/api/v1/command/{database}` are served by
`PostCommandHandler.execute()` (`PostQueryHandler` extends `PostCommandHandler` and
overrides only `executeCommand()`, not `execute()`), which reads and honors a `limit`
field from the request body identically for both endpoints:

```java
final Integer requestLimit = optionalIntField(requestMap, "limit");
...
final int autoLimit = requestLimit != null ? applyMaxResultRows(requestLimit, maxResultRows) : getDefaultRowLimit();
```

So the command endpoint accepts and honors `limit` exactly like the query endpoint, but
the OpenAPI contract never told a generated client that. This is the second instance of
this class of asymmetry between `CommandRequest` and `QueryRequest` — #6562 was the
first, where `CommandRequest` omitted the required `language` field.

## Affected components

- `server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java`
  (`createCommandRequestSchema()`)

## Expected vs actual behavior

- **Expected**: `CommandRequest` documents a `limit` field, mirroring
  `QueryRequest`'s, since the same handler code path honors it identically for both
  endpoints.
- **Actual**: `CommandRequest` has no `limit` property, so a client generated strictly
  from the OpenAPI contract has no typed way to cap a command's result set, even though
  the server supports it.

## Fix

Added a `limit` property to `createCommandRequestSchema()` in `CoreApiSpec.java`,
using the same description/example as `QueryRequest`'s `limit` property (the behavior
is identical because both endpoints share `PostCommandHandler.execute()`).

## Scope note

The issue body also suggests auditing the rest of `CommandRequest` against
`PostCommandHandler` "in the same pass". `PostCommandHandler.execute()` also reads
`serializer`, `profileExecution`, `typeHints`, and `awaitResponse` from the request
body, none of which are declared on `QueryRequest` either (only `serializer` is, on
`QueryRequest`). Broadening either schema to cover all of those is a larger, separate
change with its own design questions (e.g. whether `awaitResponse` and `typeHints`
make sense to advertise on the query endpoint), so it is intentionally left out of this
fix and left for a dedicated follow-up issue, consistent with how #6562 scoped itself to
just the `language` field.

## Tests

Added `commandRequestDeclaresLimitMatchingQueryRequest()` to
`server/src/test/java/com/arcadedb/server/http/handler/openapi/CoreApiSpecTest.java`,
following the existing `commandRequestDeclaresLanguageAsRequired()` pattern:
asserts `CommandRequest` declares a `limit` property, and that its type/description
matches `QueryRequest`'s `limit` property (same underlying behavior, same contract
language).

## Test results

See "Review cycles" section below for the verification run once implemented.
