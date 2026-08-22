# #6562 - OpenAPI CommandRequest omits the required 'language' field

## Root cause

`CoreApiSpec.createCommandRequestSchema()` declared only `command` and `params` on the
`CommandRequest` OpenAPI schema. But `PostCommandHandler.execute` reads `language` with
`requireStringField(requestMap, "language")` (no default) and rejects the request with a
400 ("Language is null") when it is missing or empty. A client generated strictly from the
OpenAPI contract therefore never sends `language` and every `POST /api/v1/command/{database}`
call fails at runtime.

`createQueryRequestSchema()` already models `language` correctly, as an optional property
(the query handler defaults it), which is why only `CommandRequest` needed the fix.

## Affected components

- `server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java` - schema
  generation for the `/api/v1/command/{database}` request body.

## Fix

Added `language` as a documented, **required** property of the `CommandRequest` schema,
matching what `PostCommandHandler` actually enforces at runtime.

## Tests

- `server/src/test/java/com/arcadedb/server/http/handler/openapi/CoreApiSpecTest.java`:
  added `commandRequestDeclaresLanguageAsRequired()`, which fails against the pre-fix schema
  (confirmed: property absent) and passes after the fix.
- Full `com.arcadedb.server.http.handler.openapi.*Test` + `OpenApiSpecGeneratorTest` suite
  (132 tests) run green, no regressions.

## Out of scope

The TypeScript client's widened-cast workaround referenced in the issue lives in the
`arcadedb-clients` repo (tracked under #4894), not in this repository.
