# #7570 - `/batch` request body has no schema, and its natural misreading silently stores wrong data

Issue: https://github.com/ArcadeData/arcadedb/issues/7570

## Finding ledger

- [x] 1. **Fixed.** The request body of `POST /api/v1/batch/{database}` is declared as `type: string` on all three
  media types. Nothing in the contract names `@type`, `@class`, `@id`, `@from`, `@to`, nor says that a
  vertex line and an edge line are different shapes. The gRPC sibling (`GraphBatchRecord`) is
  schematized; only the HTTP encoding of the same model is not.
- [x] 2. **Fixed.** The natural misreading - nesting properties under a `properties` key, because
  `GraphBatchRecord` has a `properties` map - is accepted with a 200 and stores a property literally
  named `properties`. The counters are right and the data is wrong.
- [x] 3. **Fixed.** The `text/csv` variant is equally undocumented: "Header row followed by data rows" does not say
  how a header names `@class`, a temp id, or an edge's endpoints.
- [x] 4. **Fixed.** The issue's closing note - that the temporary-id error message "is better documentation than the
  contract carries" and "would be worth lifting into the operation's `description`" - is now a paragraph of the
  request-body description: a temporary id resolves only within the request that declared it and only for a vertex
  that appeared earlier in the same payload, and a vertex from an earlier request must be referenced by RID.

## Analysis

### Where the payload is parsed

Two parsers, both under `server/src/main/java/com/arcadedb/server/http/handler/batch/`:

- `JsonlBatchRecordStream` - serves `application/x-ndjson` and `application/jsonl`.
  `META_KEYS = {"@type","@class","@id","@from","@to"}`; every key **not** in that set is copied into
  the record as a property (`parseLine`, line 164). So `properties` becomes a property named
  `properties`, and so does any unrecognised `@`-prefixed key (`@rid`, `@cat`, a typo'd `@clas`).
- `CsvBatchRecordStream` - serves `text/csv`. `parseHeader` recognises the same five column names;
  every other column becomes a property (`parseLine`, line 197), including an unrecognised
  `@`-prefixed one.

Both are constructed in exactly one place, `PostBatchHandler.streamRecords` lines 380-381, selected by
content type.

### Where the contract is declared

`CoreApiSpec.createBatchPath()` lines 448-457: one `MediaType` shared by `application/x-ndjson` and
`application/jsonl` carrying `Schema.type("string").description("One JSON object per line")`, and a
second for `text/csv` with `"Header row followed by data rows"`.

### Root cause

There is no notion of a *reserved* key in either parser. "Not one of the five meta keys" and "is data"
are the same predicate, so a control key the parser does not understand, and the nested `properties`
object that the flat encoding never produces, both fall through into the property list. Nothing can
fail, because storing an arbitrary key is the documented behaviour of the fall-through.

## Completeness

### Invariant

> A `/batch` line key that the loader cannot interpret as data - an `@`-prefixed key outside the five
> it understands, or a `properties` key whose value is a JSON object, which is the nested-form
> misreading and never the flat encoding - is refused with a 400 naming the line, instead of being
> stored as a property under that name.

### Enumeration

```
$ grep -rn "addProperty(" --include='*.java' server/src/main/java/com/arcadedb/server/http/handler/batch/
server/.../CsvBatchRecordStream.java:197:        record.addProperty(headers[i], parseValue(value));
server/.../BatchRecord.java:51:  public void addProperty(final String key, final Object value) {
server/.../JsonlBatchRecordStream.java:164:      record.addProperty(key, unwrap(json.get(key)));
```

Two producers of a property key, one per encoding. `BatchRecord.addProperty` is the sink, not a
producer.

```
$ grep -rn "implements BatchRecordStream" --include='*.java' .
server/.../CsvBatchRecordStream.java:46
server/.../JsonlBatchRecordStream.java:46
```

Two implementations, no others in the tree.

```
$ grep -rn "new JsonlBatchRecordStream\|new CsvBatchRecordStream" --include='*.java' server/src/main grpc/src/main
server/.../PostBatchHandler.java:380:        ? new CsvBatchRecordStream(inputStream)
server/.../PostBatchHandler.java:381:        : new JsonlBatchRecordStream(inputStream);
```

One construction site in production code, so the HTTP `/batch` endpoint is the only live caller.

Same-shape siblings - request/response bodies still declared as an opaque `string`:

```
$ grep -rn 'type("string")' --include='*.java' server/src/main/java/com/arcadedb/server/http/handler/openapi/ \
    | grep -v "queryParam\|pathParam\|headerParam"
SpecBuilders.java:97,104,122        (parameter/header schemas, not bodies)
SpecBuilders.java:189               rawBody(...)
PrometheusApiSpec.java:105          Snappy protobuf ReadResponse, format: binary
CoreApiSpec.java:451                the /batch JSONL body            <- this issue
CoreApiSpec.java:455                the /batch CSV body              <- this issue
AiApiSpec.java:154                  text/event-stream SSE response
PluginApiSpec.java:360              binary backup upload
```

Seven hits, five of them fine:

- `PrometheusApiSpec:105` and `PluginApiSpec:360` are `format: binary` opaque blobs; an OpenAPI
  `string/binary` is the correct encoding for one.
- `SpecBuilders.rawBody` is used three times (`TimeSeriesApiSpec:71`, `PrometheusApiSpec:71,98`), each
  for a payload defined by an **external, named** specification - InfluxDB Line Protocol, Prometheus
  remote-write/remote-read protobuf. A client can look those up; the `/batch` grammar is
  ArcadeDB-proprietary and written down nowhere.
- `AiApiSpec:154` is the genuine sibling: an ArcadeDB-proprietary, line-oriented SSE stream
  (`session`, `tool_call`, `tool_start`, `tool_end`, `done`) declared as one `string`. Out of scope
  here - different domain, a response rather than a request, and no silent-corruption half. **Filed as
  #7573.**

Existing payloads that this change could refuse:

```
$ grep -rn '@type.*@class' --include='*.java' server/src/test | grep -o '@[a-zA-Z]*' | sort | uniq -c
 162 @type
 162 @class
  69 @id
  50 @from
  49 @to
```

Every `@`-key in every batch payload in the test tree is one of the five understood ones, and no
payload carries a `properties` key. The rejection adds no new failure to existing usage.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `POST /api/v1/batch/{db}` `application/x-ndjson` / `application/jsonl` → `JsonlBatchRecordStream.parseLine`, unknown `@`-key | yes | yes - `Issue7570ReservedBatchKeyTest`, `Issue7570BatchReservedKeyIT` |
| same → `JsonlBatchRecordStream.parseLine`, nested `properties` object | yes | yes - `Issue7570ReservedBatchKeyTest`, `Issue7570BatchReservedKeyIT` |
| `POST /api/v1/batch/{db}` `text/csv` → `CsvBatchRecordStream.parseHeader`, unknown `@`-column | yes | yes - `Issue7570ReservedBatchKeyTest`, `Issue7570BatchReservedKeyIT` |
| OpenAPI contract for the three request media types (`CoreApiSpec.createBatchPath`) | yes | yes - `Issue7570BatchRequestBodySchemaTest`, plus the swagger-parser validation in `OpenApiSpecGenerationIT` |
| `text/csv` → `CsvBatchRecordStream`, a column literally named `properties` | **argued** | n/a |
| gRPC `InsertBidirectional` → `GraphBatchRecord` | **argued** | n/a |
| Streaming (`Accept: application/x-ndjson`) answer for a rejected line | yes (same throw site) | yes - IT asserts the buffered 400; the streaming encoding reports it as the terminal `error` line by the existing mechanism |
| AI `text/event-stream` response schema | **filed** - #7573 | n/a |
| `@id` on an edge line / `@from`,`@to` on a vertex line - understood meta keys, silently unused | **filed** - #7574 | n/a |

Arguments:

- **CSV `properties` column.** `CsvBatchRecordStream.parseValue` returns only `Boolean`, `Long`,
  `Double`, `String` or `null` - never a `Map`. The nested-form misreading has no CSV spelling, so a
  `properties` column in CSV is an ordinary scalar property and refusing it would break data that is
  not a mistake. The JSONL rule is deliberately keyed on the value being an object for the same
  reason: `{"properties": "public"}` is data, `{"properties": {...}}` is the mistake.
- **gRPC.** `GraphBatchRecord` (`grpc/src/main/proto/arcadedb-server.proto:744`) has typed
  `kind`, `type_name`, `temp_id`, `from_ref`, `to_ref` fields and a separate
  `map<string, GrpcValue> properties`. Control and data live in different fields, so no key can be
  mistaken for the other kind. The defect is specific to the flat HTTP encoding.

### Residual risk

1. A **known** meta key in the wrong place is still silently ignored: `@id` on an edge line, and
   `@from`/`@to` on a vertex line. It is a silent drop rather than silent corruption, and CSV's
   documented shared-header form makes a placement rule genuinely ambiguous there, so it is filed as
   #7574 rather than fixed.
2. A line may still carry a property name that the schema does not declare; the batch loader creates
   it. That is the documented behaviour of a schemaless load, not a defect.
3. The OpenAPI `oneOf` describes **one line**, not the whole body. OpenAPI 3.0 cannot express
   "newline-delimited instances of this schema" for a request body; the media-type schema being the
   single-line schema is the convention this spec already uses for its NDJSON *responses*
   (`NdJsonQueryEvent`, `NdJsonBatchEvent`), so request and response now read the same way. The
   line-orientation is stated in the request body description.

### Why the '@' namespace is the right thing to reserve

Not an aesthetic choice: the engine already treats it as metadata everywhere a property map crosses a boundary.

```
$ grep -rn 'startsWith("@")' --include='*.java' engine/src/main/java/com/arcadedb/database/MutableDocument.java
113:      if (key.startsWith("@"))        // fromMap(Map)   - SKIP METADATA
361:      if (propertyName.startsWith("@"))  // set(Map)    - SKIP METADATA
```

`MutableDocument.fromMap` and `set(Map)` drop `@`-prefixed keys as metadata. The batch loader reached the record
through `set(String, Object)` instead (`GraphBatch.java:563`), which applies no such filter, so it was the one door
in that let an `@`-prefixed property name through. Refusing it at the parser closes the hole at the boundary where
the client can still be told about it.

## Implementation

| File | Change |
|---|---|
| `JsonlBatchRecordStream` | `rejectReservedKey(key, value)` in the property loop: refuses an `@`-prefixed key outside `META_KEYS`, and a `properties` key whose value is a `Map`. Throws plain `IllegalArgumentException`, never the `MalformedBatchRecordException` subclass that `PostBatchHandler` may report as a 408. |
| `CsvBatchRecordStream` | `rejectReservedColumn(column)` from the header switch's `default` branch: refuses an unrecognised `@`-prefixed column, at the header where CSV names its control keys. |
| `CoreApiSpec` | `BatchLine` (`oneOf` + `@type` discriminator, all four accepted spellings mapped), `BatchVertexLine`, `BatchEdgeLine`; the two JSON line media types now `$ref` the line schema instead of `type: string`; the CSV media type describes the real grammar; both carry an `example`; the request body and the 400 state the reserved-key rule. |

## Test results

```
mvn -o -pl server test  -Dtest='com.arcadedb.server.http.**'            -> Tests run: 856, Failures: 0, Errors: 0, Skipped: 2
mvn -o -pl server verify -DskipITs=false -Dit.test='Issue7570BatchReservedKeyIT' -> Tests run: 7,  Failures: 0, Errors: 0
mvn -o -pl server verify -DskipITs=false -Dit.test='*Batch*IT,OpenApi*IT,...'    -> Tests run: 133, Failures: 0, Errors: 1 (see below)
```

Before the fix, the same two new unit-test classes reported **15 failures** out of 20 - every refusal assertion, on
every entry point - while the five "this still works" guards passed. That is the proof that the parsers accepted all
of it.

The one error in the wide IT sweep is `RemoteGraphBatchIT.mapAndListPropertiesRoundTrip`, failing in `endTest` on
`RemoteServer.drop` with `EOFException: EOF reached while reading`. It is **not** this change:

- the failure is in teardown, not in the test body, and nothing in this diff is on the `drop` path;
- the class hardcodes `127.0.0.1:2480` in 17 places, and a Homebrew ArcadeDB 26.9.1 server had held `*:2480` on this
  machine for over a day, so the test was talking to that server rather than to its own;
- re-run alone, the class is green: `Tests run: 15, Failures: 0, Errors: 0`.

The new IT deliberately does **not** repeat that mistake: it derives its URL from
`getServer(0).getHttpServer().getPort()`.

## Breaking change

This tightens a **write** path. A payload that carried an `@`-prefixed key outside the five, or a nested
`properties` object, used to load with a 200 and now answers 400. That is the point of the issue - the 200 was the
defect - but it is a behaviour change and not only a documentation one.

The blast radius was measured rather than assumed:

```
$ grep -rhoE '\{[^{}]*"@type"[^{}]*\}' --include='*.java' --include='*.md' --include='*.js' --include='*.py' \
      --include='*.json' --include='*.ts' . | grep -v node_modules | grep -E '"(vertex|edge|v|e)\\?"' \
  | grep -o '"@[a-zA-Z_]*"' | sort | uniq -c | sort -rn
  86 "@type"     86 "@class"     46 "@id"     24 "@to"     24 "@from"     2 "@cat"
```

Every batch payload literal in the whole tree uses only the five control keys. The two `@cat` hits are the new tests
in this branch. Nothing shipped in this repository sends a payload the refusal would now reject.

## Adversarial pass

The `Task` tool is not available in this environment, so the isolated subagent the workflow asks for could not be
spawned. The pass was run by re-reading the diff against the issue instead; that is weaker - the reviewer had already
been convinced by its own reasoning - and is recorded as such.

| Finding | Disposition |
|---|---|
| `@id` on an edge line, `@from`/`@to` on a vertex line: understood control keys used on the wrong kind, still silently dropped | **Real, out of scope** - filed as #7574 |
| The AI chat SSE response is still one opaque `string`, the same defect this issue reports for the request | **Real, out of scope** - filed as #7573 |
| `{"properties": ["..."]}` - an ARRAY rather than an object - is still accepted as a list property named `properties` | **Not fixed, argued.** The misreading this refusal targets comes from `GraphBatchRecord.properties`, which is a `map`; a client guessing from it sends an object. Refusing a list too would break a domain whose `properties` field is genuinely a list, with no corresponding mistake to prevent |
| The IT copied `http://127.0.0.1:2480` from `Issue5618BatchLineAccountingIT` | **Real, fixed here** before the first run: the IT now derives the port from `getServer(0).getHttpServer().getPort()`. The hardcoded form would have been answered by the machine's standing server |
| The IT asserted `countType("Person") == 0` on a database shared across the class's methods, so it would pass or fail on execution order | **Real, fixed here**: every method now has its own vertex type |
| `assertThat(...query("SELECT properties FROM ..."))` in the IT leaned on `properties` not being a reserved SQL word | **Real, fixed here**: replaced with an `iterateType` read, which cannot be confused by the parser |
| The error message embeds literal JSON with quotes and has to survive being placed in a JSON error body | **Not a defect** - verified empirically: the IT parses the 400 body with `JSONObject` and reads `error` back |
| A generated client may still treat `application/x-ndjson` as opaque regardless of the schema | **Real limitation, stated** in residual risk 3. The schema is still the only written description of the payload, and it matches how the spec already documents its NDJSON responses |

## Review cycles

PR: https://github.com/ArcadeData/arcadedb/pull/7583

### Cycle 1 - 16bb1ef3d7

`claude` reviewed and found no correctness bug and nothing blocking the merge. Three notes, all non-blocking:

| Note | Disposition |
|---|---|
| The control keys are matched case-sensitively, so `@Type` now lands in the "unknown control key" branch. Not a regression (it misbehaved before the fix too), but worth a line in the request-body description | **Applied.** Verified first: `META_KEYS` is a `Set.of` queried with `contains`, `@type` values go through `equals`, and the CSV header is an exact `switch`; only the CSV boolean literals use `equalsIgnoreCase`. That asymmetry is unguessable and the refusal makes it visible as a 400, so the contract now states it, with an assertion in `Issue7570BatchRequestBodySchemaTest` |
| The PR body's Test plan boxes were unticked although the runs are recorded as passing | **Applied.** Ticked, with the measured counts inline |
| `rejectReservedKey`'s `@`-branch and `rejectReservedColumn` are near-duplicate logic in two classes; the reviewer flagged it as *not* worth extracting today | **Declined, agreed with the reviewer.** The two differ in what they inspect (a JSON key on a data line vs a column name on a header row), in when they run (per property vs once per header), and in their message wording. The classes share no base type, so a helper would mean a new utility for about four lines of code and would put the JSONL and CSV messages under one roof where they would drift toward a generic wording. Worth revisiting only if a third encoding is added, which is exactly what the reviewer said |

No item was unclear, so nothing was deferred for the developer and no `review-deferred-*.md` notes file was produced.

### Cycle 2 - 270082b530

`claude` reviewed again and found no correctness bug and nothing blocking. It independently re-verified the three
claims this fix rests on - that `parseValue` genuinely never returns a `Map`, that the
`MalformedBatchRecordException`-vs-`IllegalArgumentException` split is what actually drives 408 vs 400 in
`PostBatchHandler`, and that the control-key matching really is case-sensitive - and confirmed the OpenAPI
`required` lists match the parsers field-for-field. Two notes:

| Note | Disposition |
|---|---|
| `Issue7570BatchReservedKeyIT.post(...)` reimplements a raw `HttpURLConnection` POST helper that other ITs in the package also have; CLAUDE.md asks for reuse. Flagged by the reviewer itself as a nice-to-have, not a blocker, and as "already widespread across ~57 test files" rather than something this PR introduces | **Declined, with the base class checked first.** `BaseGraphServerTest` has no helper that posts a body: `readResponse`/`readError` only drain a stream and collapse newlines, and `executeCommand` is pinned to `/api/v1/command/graph`, asserts 200 so it cannot express the 400 these tests are about, and builds its URL as `248<serverIndex>` - the hardcoded-port form this IT deliberately avoids. Reusing it is not possible; adding a new protected helper to a base class shared by dozens of suites is a wider change than this PR should carry, and it would be the right change to make once, for all ~57 files, rather than as a side effect here |
| The cycle-1 decision not to extract the shared `@`-prefix check | **Confirmed by the reviewer**, no action |

No item was unclear; nothing deferred for the developer; no `review-deferred-*.md` notes file was produced by
either cycle. (The `docs/review-deferred-*.md` files present in the tree predate this branch - they came in with
PR #7210 and others.)

## Final state

**clean-approval** after 2 review cycles.

| | |
|---|---|
| PR | https://github.com/ArcadeData/arcadedb/pull/7583 |
| Branch | `fix/7570-openapi-batch-request-body-schema` |
| Cycle 1 | `16bb1ef3d7` - 3 non-blocking notes, 2 applied, 1 declined with reasoning |
| Cycle 2 | `270082b530` - 2 non-blocking notes, both declined with reasoning; no correctness bug found |
| Follow-ups filed | #7573, #7574 |

### Outstanding for the developer

- **CodeRabbit never reviewed this PR.** It hit its free-tier rate limit at the first push ("Review limit reached,
  next included review available in 32 minutes") and its check reports `pass` only because it was skipped. There are
  therefore zero CodeRabbit threads to resolve - which reads the same as "all resolved" but is not. The repository's
  own merge bar asks for CodeRabbit threads to be resolved, so it is worth a `@coderabbitai review` once the limit
  resets before merging.
- **Downstream drivers.** The blast-radius grep covers this repository only. The refusal is a behaviour change on a
  write path, so the arcadedb-drivers clients - the reporter's own starting point - are worth a look before release.
- Merge is the developer's. This workflow does not merge.
