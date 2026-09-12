# #7568 - OpenAPI: vector/hybrid/full-text response schemas have no `required` list, and three properties are bare `object`

- Issue: https://github.com/ArcadeData/arcadedb/issues/7568
- Branch: `fix/7568-openapi-vector-response-schema-shapes`
- Type: bug (contract defect; the server's wire behaviour is correct and unchanged)

## Finding ledger

- [x] 1. `VectorSearchResponse`, `HybridSearchResponse` and `FullTextSearchResponse` declare no `required` list - **fixed**, on all three plus the two hit schemas they reach through `results[]`.
- [x] 2. `HybridSearchRequest.weights` is a bare `object` - **fixed**, declared as a closed object of three named numbers.
- [x] 3. `HybridSearchResponse.legs` is a bare `object` - **fixed**, declared as three named sub-objects with `vector` required.
- [x] 4. `results[].properties` is a bare `object` on all three response schemas - **fixed**, declared an open map (`additionalProperties: true`) on both hit schemas.

## Analysis

`server/src/main/java/com/arcadedb/server/http/handler/openapi/VectorApiSpec.java` builds all six
components. The three *request* schemas call `schema.setRequired(...)`; the three *response* schemas
never do. Nothing about the running server changes - the JSON the handlers emit is the same before
and after - only the document that describes it.

Ground truth for "always sent" is the three engine writers, read directly:

| Response | Writer | Unconditional keys | Conditional keys |
|---|---|---|---|
| `VectorSearchResponse` | `VectorSearch.search` (`engine/.../query/search/VectorSearch.java:110-117`) | `indexName`, `sparse`, `scoring`, `candidateLimit`, `truncated`, `count`, `results` | none |
| `HybridSearchResponse` | `HybridSearch.search` (`engine/.../query/search/HybridSearch.java:199-237`) | `vectorIndexName`, `sparse`, `scoring`, `legs`, `fused`, `truncated`, `count`, `results` | `fulltextIndexName` (only when the full-text leg ran), `fusionStrategy` (only when `fused` is true) |
| `FullTextSearchResponse` | `FullTextQuery.search` (`engine/.../query/search/FullTextQuery.java:154-158`) | `indexName`, `similarity`, `count`, `results` | none |
| hit (`results[]`, vector + full-text) | `VectorSearch:147-153`, `FullTextQuery:149-152` | `rid`, `properties` | exactly one of `score` / `distance` |
| fused hit (`results[]`, hybrid) | `HybridSearch:216-220` (unfused), `HybridSearch:667-679` (fused) | `rid`, `sources`, `properties` | `fusedScore` \| `score` \| `distance`, plus `depth`/`path` for an expansion hit |

Two values that could have made a "required" key vanish at runtime were checked and cannot be null:

- `VectorLeg.ResolvedVectorIndex.scoring()` is assigned from a string literal on both the dense and
  the sparse branch (`VectorLeg.java:207-217`), and the method throws rather than returning a record
  with a null scoring.
- `FullTextSearch.getSimilarity` returns `SIMILARITY_BM25` or falls through to `SIMILARITY_CLASSIC`
  (`FullTextSearch.java:394-400`); there is no null path.

For the bare objects, the shape is knowable in every case:

- `weights` is a **closed** map. `HybridSearch.validateWeights` (`HybridSearch.java:292-321`) refuses
  any key that is not `vector`/`fulltext`/`expand`, refuses a weight for a leg the request did not
  ask for, and refuses a value that is not a finite, non-negative number. The defaults come from the
  `weightOf(...)` call sites: `vector` 1.0, `fulltext` 1.0, `expand` 0.5. The gRPC mirror declares it
  `map<string, float>` (`grpc/src/main/proto/arcadedb-server.proto:307`), which agrees on the value type.
- `legs` is a **fixed** shape with three known sub-objects: `vector` `{count}`, `fulltext`
  `{indexName, similarity, count}` (present only when that leg ran), `expand`
  `{direction, edgeTypes, maxDepth, truncated, seedCount, seedsTruncated, count}` (present only when
  the request asked to expand). `vector` is always present.
- `results[].properties` is `JsonSerializer.serializeDocument(document)` - genuinely arbitrary record
  data of arbitrary value types, so it is an **open** map: `additionalProperties: true`.

## Completeness

### 1. Invariant

Every field the vector, hybrid and full-text routes always send is named in its response schema's
`required` list, and no property of those six schemas is typed as a bare `object` carrying neither
`properties` nor `additionalProperties`.

### 2. Enumeration

```
$ grep -rc 'setRequired' server/src/main/java/com/arcadedb/server/http/handler/openapi/*.java
AiApiSpec.java:3      AuthApiSpec.java:1     CoreApiSpec.java:3     GrafanaApiSpec.java:1
McpApiSpec.java:0     OpenApiContributor.java:0                     PluginApiSpec.java:1
PrometheusApiSpec.java:2                     SecurityAdminApiSpec.java:0
SpecBuilders.java:6   TimeSeriesApiSpec.java:1                      VectorApiSpec.java:3
```

`VectorApiSpec`'s three `setRequired` calls are all on request schemas (lines 158, 217, 246 before
the fix) - confirming finding 1.

```
$ grep -rn 'addSchemas("' server/src/main/java/com/arcadedb/server/http/handler/openapi/*.java | wc -l
55
$ grep -rn 'setRequired' server/src/main/java/com/arcadedb/server/http/handler/openapi/*.java | grep -v SpecBuilders | wc -l
15
```

So 40 of 55 component schemas across the whole document carry no `required` list. Six of them are
this issue's; the rest are the same defect in other specs.

```
$ grep -rn 'addProperty([^,]*, *SpecBuilders\.object(' server/src/main/java/com/arcadedb/server/http/handler/openapi/*.java | wc -l
15
```

Fifteen bare-`object` properties in total. Four are in `VectorApiSpec` (`weights` :189, `legs` :213,
`hitSchema().properties` :253, `fusedHitSchema().properties` :271 - the last two are the two code
sites behind the issue's "`results[].properties` on all three response schemas"). The other eleven
are in `AiApiSpec` (1), `CoreApiSpec` (5), `GrafanaApiSpec` (1), `PluginApiSpec` (3) and
`TimeSeriesApiSpec` (1).

Sibling *surfaces* that describe the same three responses:

```
$ grep -rn 'outputSchema\|"output_schema"' --include='*.java' mcp/src/main/java   # no hits
$ grep -rn 'message VectorSearchResponse\|message HybridSearchResponse\|message FullTextSearchResponse' --include='*.proto' .
grpc/src/main/proto/arcadedb-server.proto:258,319,365
```

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `VectorSearchResponse.required` vs what `VectorSearch.search` emits | yes | yes |
| `HybridSearchResponse.required` vs what `HybridSearch.search` emits (unfused, fused, expanding) | yes | yes |
| `FullTextSearchResponse.required` vs what `FullTextQuery.search` emits | yes | yes |
| shared hit schema (`results[]` of the vector and full-text responses) `required` | yes | yes |
| fused hit schema (`results[]` of the hybrid response) `required` | yes | yes |
| `HybridSearchRequest.weights` bare object | yes | yes |
| `HybridSearchResponse.legs` bare object | yes | yes |
| `results[].properties` bare object, both hit schemas, all three responses | yes | yes |
| The other 11 bare-`object` properties in `AiApiSpec`/`CoreApiSpec`/`GrafanaApiSpec`/`PluginApiSpec`/`TimeSeriesApiSpec` | no - filed as **#7577** | n/a |
| The other ~34 component schemas across the document with no `required` list | no - filed as **#7578** | n/a |
| The five closed value sets in these same schemas still typed as free `string` (`fusionStrategy`, `direction`, `sources[]`, `similarity`) | no - filed as **#7579** | n/a |
| MCP tool surface for the same three searches | argued: `grep -rn 'outputSchema' mcp/src/main/java` returns no hits - MCP declares input schemas only, so there is no response contract there to under-describe |
| gRPC surface for the same three searches | argued: `arcadedb-server.proto:258/319/365` declares each response as a protobuf message with typed scalar fields, `map<string, float> weights` and `map<string, GrpcValue> legs` - protobuf has no optional-by-omission problem for scalars and no bare-object type, so the defect cannot exist there |

### 4. Reachability

`VectorApiSpec` is registered as an `OpenApiContributor`, and `VectorApiSpecTest.theThreeRoutesAreDocumentedAndReachTheGeneratedDocument`
already asserts that the routes it contributes appear in the document `OpenApiSpecGenerator` actually
generates. The new assertions ride the same contributor, so they reach the served document, not only
a locally-constructed `OpenAPI`.

The whole generated document still validates clean after the change. Serialized with
`io.swagger.v3.core.util.Json.pretty(new OpenApiSpecGenerator(null).generateSpec())` and fed to
`OpenAPIV3Parser` with `resolve=true`, `SwaggerParseResult.getMessages()` came back `[]`, and the
serialized JSON carries the new keywords in the right places:

```
"weights" : { "type" : "object", "additionalProperties" : false,
              "properties" : { "vector" : { "minimum" : 0, "type" : "number", "default" : 1.0 }, ... } }
"required" : [ "count", "fused", "legs", "results", "scoring", "sparse", "truncated", "vectorIndexName" ]
"legs" : { ..., "required" : [ "vector" ] }
hit: "required" : [ "properties", "rid", "sources" ], "properties" : { ..., "additionalProperties" : true }
```

(That check was a throwaway run, not a committed test: `OpenApiSpecGenerationIT` already validates the
served document with the same parser, and this machine has something else on port 2480.)

### 5. Residual risk

The same two defects exist elsewhere in the document - 11 more bare objects (**#7577**) and roughly 34
more component schemas with no `required` list (**#7578**). This PR does not touch them, so a client
generated against any other part of the API keeps the same ergonomics problem until those are fixed.

These six schemas also still type five closed value sets as free `string` (**#7579**), which is a third
kind of under-description this issue did not name and this PR does not address.

Nothing about the server's behaviour changes: no handler, no engine class and no wire format is
touched. The only risk a contract change of this kind carries is marking required something the server
may omit, which would make the document lie in the other direction; that is what
`Issue7568VectorResponseContractMatchesBehaviourTest` exists to rule out, and it exercises the unfused
hybrid response specifically because that is the response where `fusionStrategy` and
`fulltextIndexName` are genuinely absent.

## Implementation

- `SpecBuilders.freeFormObject(String)` - new. An object declared as an open map
  (`additionalProperties: true`), for keys that are not knowable ahead of time. Sits next to
  `object(String)`, which stays the starting point for an object whose properties are spelled out.
- `VectorApiSpec.createSearchResponseSchema` / `createHybridResponseSchema` /
  `createFullTextResponseSchema` - each now calls `setRequired(...)` with the fields its writer emits
  unconditionally.
- `VectorApiSpec.hitSchema` - `required: [rid, properties]`, `properties` becomes a `freeFormObject`.
- `VectorApiSpec.fusedHitSchema` - `required: [rid, sources, properties]`, same change to `properties`.
- `VectorApiSpec.weightsSchema` - new. The three named leg weights as `number` with `minimum: 0` and
  the fallback `HybridSearch.weightOf` applies as `default` (1.0 / 1.0 / 0.5), plus
  `additionalProperties: false` because `validateWeights` refuses any other key outright.
- `VectorApiSpec.legsSchema` - new. `vector` `{count}`, `fulltext` `{indexName, similarity, count}`,
  `expand` `{direction, edgeTypes, maxDepth, truncated, seedCount, seedsTruncated, count}`, with
  `required: [vector]`.

## Tests

Two new classes in `server/src/test/java/com/arcadedb/server/http/handler/openapi/`:

- `Issue7568VectorResponseSchemaShapeTest` (8 tests) - the document's side. One test per fixed row, plus
  `noSchemaInThisContributorIsABareObject`, which walks every schema this contributor registers and
  fails with the list of bare ones. Before the fix that list read exactly
  `[VectorSearchResponse.results[].properties, HybridSearchRequest.weights, HybridSearchResponse.legs,
  HybridSearchResponse.results[].properties, FullTextSearchResponse.results[].properties]`.
- `Issue7568VectorResponseContractMatchesBehaviourTest` (7 tests) - the engine's side. Builds a real
  database with a vector index, a full-text index and an edge type, runs `VectorSearch.search`,
  `FullTextQuery.search` and `HybridSearch.search` (unfused, fused, and expanding), and asserts that
  every name in the corresponding schema's `required` list is a key the response actually carries. The
  expectation is read out of `VectorApiSpec`, so it cannot drift from the document.

The fourteen written before the fix all failed against the unfixed spec and pass after it; the
fifteenth (`aHitsPropertiesMapCarriesTheRecordMarkersAlongsideTheTypesOwnProperties`) came out of the
adversarial pass and pins the corrected description:

```
$ ./mvnw -o -pl server -am test -Dtest='Issue7568*' -Dsurefire.failIfNoSpecifiedTests=false
   before: Tests run: 14, Failures: 11, Errors: 3
   after:  Tests run: 15, Failures: 0, Errors: 0

$ ./mvnw -o -pl server -am test -Dtest='*ApiSpec*Test,SpecBuildersTest,OpenApiSpecGeneratorTest,Issue7568*,Issue7400*' \
      -Dsurefire.failIfNoSpecifiedTests=false
   Tests run: 168, Failures: 0, Errors: 0, Skipped: 0 - BUILD SUCCESS
```

## Adversarial pass

The orchestrator's isolated `general-purpose` subagent could not be spawned in this session (no `Task`
tool was available to it), so the pass was run by the author re-reading the patch as the reporter
would. That is a weaker pass by construction - the reader had already been convinced - and it is
recorded as such. Four findings, each verified against the tree:

1. **`additionalProperties: false` on `weights` claims something the server may not enforce.**
   *Real concern, checked and held.* `HybridSearch.search` calls `validateArguments` on entry
   (`HybridSearch.java:147`), which calls `validateWeights` (`:132`), which rejects any key that is
   not `vector`/`fulltext`/`expand` (`:296-298`). The HTTP route reaches it through
   `PostVectorHybridSearchHandler` -> `HybridSearch.search`. `theServerRefusesExactlyTheWeightsKeysTheSchemaRefuses`
   now pins both halves: the three documented keys are accepted, and `graph` is refused.

2. **The `properties` description said "the keys are the type's own properties". It does not hold.**
   *Real, in scope, fixed here.* `JsonSerializer.serializeDocument` writes `@rid` (when the record has
   an identity) and `@type` into every document it serializes (`JsonSerializer.java:100-102`,
   `Property.RID_PROPERTY`/`TYPE_PROPERTY`). A caller reading the description would have built a map
   key set that does not match reality. The description now names both markers, and
   `aHitsPropertiesMapCarriesTheRecordMarkersAlongsideTheTypesOwnProperties` asserts them on a real
   hit, so the description is verified rather than asserted. This is also the reason `properties`
   cannot be given a closed property set instead of `additionalProperties`.

3. **Five closed value sets are still documented as free `string` with the values only in prose** -
   `fusionStrategy`, `expand.direction` (request and `legs.expand`), `results[].sources[]`, and
   `similarity` (response and `legs.fulltext`). *Real, out of scope* - it is a third kind of
   under-description, not one of the two this issue names. Filed as **#7579**.

4. **`expand` should be closed the way `weights` is.** *Not real.* `requireExpandObject`
   (`HybridSearch.java:260-267`) only checks that `expand` is an object; `validateExpand` (`:274-284`)
   checks `maxDepth` and `direction` and ignores anything else. The server accepts an unknown key
   there, so `additionalProperties: false` would make the document stricter than the server and
   reject requests that actually work.

## Review cycles

### Cycle 1 - `f90129b`

`claude` reviewed the patch against the engine code and confirmed every `required` list, the
`additionalProperties: false` on `weights`, the 1.0/1.0/0.5 defaults and the open-map choice for a
hit's `properties`. Two items, both addressed in `d?` (next commit):

1. *Nit, applied.* `assertThatThrownBy` was reached through its fully-qualified name in
   `Issue7568VectorResponseContractMatchesBehaviourTest` while `assertThat` from the same class was
   already statically imported. Now imported.
2. *"Possible follow-up, not a blocker" - applied here instead.* `legsSchema()`'s `fulltext` and
   `expand` sub-objects declared no `required` list of their own. The reviewer suggested folding it
   into #7578; it is fixed here instead, because `legsSchema()` is code this PR introduces and
   shipping it under-described would reproduce the exact defect the PR is about, one level down.
   Verified first: `HybridSearch` writes each sub-object as a single expression
   (`:166` for `vector`, `:575-578` for `fulltext`, `:185-194` for `expand`), so a leg that is present
   is present whole. `eachLegSubObjectRequiresEverythingItCarriesWhenItIsPresentAtAll` pins the
   schemas and `theLegsAccountingMatchesTheDocumentedSubObjects` already pinned the engine's key sets
   against them.

No deferred items. CodeRabbit had not posted a review at the time of this push; it re-reviews on every
push.

`./mvnw -o -pl server -am test -Dtest='*ApiSpec*Test,SpecBuildersTest,OpenApiSpecGeneratorTest,Issue7568*,Issue7400*'`
-> Tests run: 170, Failures: 0, Errors: 0.
