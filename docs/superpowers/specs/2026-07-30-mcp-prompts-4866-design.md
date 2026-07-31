# MCP: add Prompts (graphrag_query, build_knowledge_graph) (#4866)

**Issue:** [#4866](https://github.com/ArcadeData/arcadedb/issues/4866)
**Epic:** [#4859 - MCP GraphRAG & Agent-Memory Surface](https://github.com/ArcadeData/arcadedb/issues/4859) (step 4, last before #4868)
**Date:** 2026-07-30
**Status:** implemented

## Problem

ArcadeDB's MCP server implements two of the three protocol primitives. Tools have been there since the
beginning; Resources arrived with [#4865](https://github.com/ArcadeData/arcadedb/issues/4865). Prompts are
absent: `MCPDispatcher.dispatch` has no `prompts/list` or `prompts/get` arm, and `initialize` advertises no
`prompts` capability.

Wave 1 of the epic added six tools whose value depends on the model choosing them. `vector_search`,
`full_text_search` and `hybrid_search` are only reached if the model knows retrieval is available and knows
ArcadeDB will not embed text for it. `upsert_entity` and `upsert_relationship` are only *safe* if the model
understands match keys: an upsert whose match key differs by one property silently creates a duplicate
vertex instead of updating the intended one. Absent that guidance, a model reaches for `execute_command`
with hand-written `vector.neighbors` or `MERGE` syntax, which is the failure mode the epic's governing
principle exists to prevent.

Prompts are the primitive for that guidance. This issue is epic justification **#1 (ergonomic guardrail)**
delivered through the protocol rather than through another tool, and, like #4865, it adds capability without
lengthening the tool list.

## Governing constraints

- **The MCP layer stays schema-agnostic**, per the maintainer's ruling on the epic thread: MCP is "just
  another protocol/api, like the http api or grpc or bolt." Prompt text describes ArcadeDB's tools and their
  semantics; it imposes no governance, memory or domain vocabulary.
- **Dependencies have landed.** #4860, #4861, #4864, #4865 and #4867 are all closed and on `main`, so the
  tool names the prompt text hard-codes are final.

## Design

Three pieces. Two are new; the third is two switch arms and a capability line.

### 1. `com.arcadedb.server.mcp.prompts` (new package)

One class per prompt, mirroring the `tools/` package: static methods, no instance state, no interface.

```java
public class GraphRagQueryPrompt {
  public static JSONObject getDefinition();                 // name, description, arguments[]
  public static JSONArray  getMessages(JSONObject args);    // the rendered message array
  public static Set<String> referencedTools();              // tools named in the text
  public static boolean isAvailable(Predicate<String> toolAllowed, MCPPermissions permissions);
}
```

`BuildKnowledgeGraphPrompt` has the identical shape. Keeping `isAvailable` on the prompt rather than in the
registry means each prompt owns the statement of what it needs, next to the text that needs it.

`isAvailable` takes a `Predicate<String>` rather than the dispatcher's profile type on purpose.
`EffectiveToolProfile` is a record private to `MCPDispatcher`; the dispatcher passes `profile::allows`, so
the method reference is formed inside the class that owns the record and the prompts package never names a
dispatcher internal. `MCPPermissions` is the public interface `MCPConfiguration` already implements.

### 2. `MCPPrompts` (new, `com.arcadedb.server.mcp`)

The exact analogue of `MCPResources`: the registry, the name switch, and the two entry points. Neither
transport learns anything about prompts, and `MCPDispatcher` - already 25KB - does not grow a third
subsystem.

```java
public static JSONObject list(MCPPermissions permissions, Predicate<String> toolAllowed);
public static JSONObject get(MCPPermissions permissions, Predicate<String> toolAllowed, String name, JSONObject args);
```

Neither takes an `ArcadeDBServer` or a `ServerSecurityUser`: prompt text is static (see *Prompt body*
below), so nothing here touches a database. The `EffectiveToolProfile` the dispatcher already computes per
request carries everything principal-specific that matters, and reaches here as `profile::allows`.

### 3. `MCPDispatcher` (modified)

```java
case "prompts/list" -> promptsList(id, effectiveProfile(user)::allows);
case "prompts/get"  -> promptsGet(id, params, effectiveProfile(user)::allows);
```

plus, in `initialize`:

```java
capabilities.put("prompts", new JSONObject().put("listChanged", false));
```

`listChanged` is `false` for the same reason `tools` claims it. Prompt availability does vary by principal
and by configuration, but a profile change made through the config endpoint takes effect on subsequent
requests rather than being pushed, and `tools/list` already lives with exactly that.

Both transports gain Prompts with no change of their own, because both already route through the dispatcher.

## Availability rule

A prompt is listed only when **every** tool its text names is allowed by the caller's `EffectiveToolProfile`
**and** every permission flag those tools require is on.

| Prompt | Declared tools | Required flags | `all` | `rag` | `admin` |
|---|---|---|---|---|---|
| `graphrag_query` | `get_schema`, `query`, `vector_search`, `full_text_search`, `hybrid_search` | `allowReads` | visible | visible | hidden |
| `build_knowledge_graph` | `get_schema`, `upsert_entity`, `upsert_relationship` | `allowReads`, `allowInsert`, `allowUpdate` | visible | visible | hidden |

This mirrors `tools/list`: a prompt is a script naming tools by name, so advertising one whose tools the
caller cannot see hands the agent a workflow that fails on its first call. The `admin` profile contains
neither the search tools nor the upsert tools, so it sees no prompts at all.

Defaults stay safe. A stock server has `enabled=false` and every write flag `false`; once enabled it shows
`graphrag_query` alone, and `build_knowledge_graph` appears only after insert and update are turned on. This
issue adds no configuration flag of its own.

`build_knowledge_graph` requires `allowReads` even though its purpose is writing, because step 1 of its text
instructs a schema read before match keys are chosen. That coupling is deliberate: a graph built without
first looking at the existing types is how parallel duplicate types get created.

`prompts/get` re-evaluates the same rule, the way `tools/call` re-checks the profile after `tools/list` has
already filtered. A hidden prompt cannot be fetched by name.

**The rule is strict on purpose and has a cost.** If a future profile admitted `vector_search` but not
`hybrid_search`, `graphrag_query` would disappear rather than degrade. With the three profiles that exist
that case cannot arise, and a prompt that names an unavailable tool is worse than no prompt, so strictness
wins until a profile actually splits the search tools.

## Prompt body

**Static text only.** Messages are fixed templates with arguments substituted; `prompts/get` performs no
database access. Hydrating prompts with live schema was considered and rejected: #4865 already delivers
schema without spending a turn, so embedding it here would create a second place schema is emitted, and
would pull read permissions, unknown-database errors and unbounded message size into the Prompts path.

Each prompt renders as a single `role: "user"` message of `content.type = "text"`. Protocol revision
`2025-03-26` also allows an embedded resource in a prompt message; nothing here needs one.

### `graphrag_query`

Arguments: `database` (required), `question` (required).

```
Answer the following question using the ArcadeDB database '{database}'.

<question>
{question}
</question>

Procedure:
1. Load the schema first: read the MCP resource arcadedb://{database}/schema, or call
   get_schema. Note which types carry a vector index, which carry a full-text index,
   and which properties hold the text you would search.
2. Pick the retrieval tool that fits the question:
   - vector_search for semantic similarity, when you can supply an embedding vector.
   - full_text_search when the question names specific terms, phrases or identifiers.
   - hybrid_search when both apply; it fuses a vector leg and a full-text leg into one
     ranked list.
   - query, in SQL or Cypher, when the answer needs traversal, filtering or aggregation
     that no search tool expresses.
3. ArcadeDB does not generate embeddings. Where a tool needs a vector, produce it with
   your own embedding model, using the same model and dimensionality as the indexed data.
4. Where neighbouring records would strengthen the answer, traverse from the retrieved
   records with query rather than issuing a second unrelated search.
5. Answer only from what you retrieved, and cite every claim with the @rid of the record
   it came from. If retrieval returns nothing relevant, re-check type and property names
   against the schema, then say the database does not contain the answer.
```

### `build_knowledge_graph`

Arguments: `database` (required), `sourceText` (required).

```
Extract a knowledge graph from the source text and write it into the ArcadeDB database
'{database}'.

<source_text>
{sourceText}
</source_text>

Procedure:
1. Read the MCP resource arcadedb://{database}/schema, or call get_schema, before writing
   anything. Reuse the types and properties that already exist instead of creating
   parallel ones.
2. List the entities in the source text. For each, choose a vertex type and a match key:
   the smallest set of property:value pairs that identifies it uniquely and stably, such
   as an identifier, a URL or a normalized name. The match key is what prevents
   duplicates, because two calls carrying the same match key resolve to the same vertex.
3. Prefer a match key backed by a UNIQUE index. Without one, matching is a full type scan
   and two concurrent upserts with the same key can each create a vertex. If that index is
   missing, report it rather than creating it, unless schema changes were requested.
4. Call upsert_entity once per entity: the match key in 'matchKeys', everything else in
   'setProperties'. Never fold two entities into one call.
5. Call upsert_relationship once per relationship, reusing exactly the match keys from
   step 4 as 'fromMatchKeys' and 'toMatchKeys'. The tool creates a missing endpoint, so a
   match key that differs by even one property silently produces a duplicate vertex
   instead of connecting the intended one.
6. Record only what the source text states. Do not infer relationships it does not
   contain. Close by reporting the entities and relationships you wrote, and anything you
   deliberately skipped.
```

Both texts name only the tools their prompt declares. Step 3 of `build_knowledge_graph` and step 5's
duplicate-endpoint warning restate, in imperative form, the hazards already documented in
`UpsertEntityTool` and `UpsertRelationshipTool` descriptions: those descriptions are read when a tool is
selected, which is too late to influence how the model decided to model the graph.

## The `instructions` block

Two of the three instruction texts gain one sentence pointing at `prompts/list`, appended as a final
numbered rule:

- `INSTRUCTIONS` (5 rules today): `6. Guided prompt templates may be available for retrieval and
  knowledge-graph construction: call prompts/list to see which ones your profile exposes.`
- `RAG_INSTRUCTIONS` (6 rules today): `7. Call prompts/list for guided templates: graphrag_query for
  retrieval, build_knowledge_graph for writing extracted entities and relationships.`

`RESTRICTED_INSTRUCTIONS` gains nothing: a restricted surface is where prompts are most likely filtered out
entirely, and advertising a discovery call that returns an empty list is noise.

`INSTRUCTIONS` is shared by the `all` and the `admin` profiles, and `admin` sees no prompts. Splitting the
constant in two to spare `admin` one sentence is not worth the divergence, so the sentence is hedged
("may be available ... which ones your profile exposes") and reads correctly for both. An `admin` agent may
spend one call on an empty `prompts/list`; that is the same cost `resources/list` already carries on a
reads-disabled server.

## Permission scope

Availability is evaluated against the server-global permission flags, never against a per-database override
from `getPermissionsForDatabase`. Prompts do not resolve a database - `{database}` is substituted
verbatim - so there is no database whose overrides could be consulted. A server whose global `allowInsert`
is `false` but which grants insert to one database therefore does not offer `build_knowledge_graph`; the
prompt is guidance, and the tools it names still enforce their own per-database checks when called.

## Argument contract and errors

Both arguments of both prompts are declared `required: true` in `getDefinition()`, and enforced in
`getMessages`.

The `database` argument is substituted verbatim, with no existence or authorization check. The text is
static, so a name that does not exist produces a rendered prompt mentioning a database the agent will fail
to open on its first tool call, which is where that error belongs. Checking here would make Prompts depend
on server and user state and would reopen the probe surface #4865 deliberately closed.

`MCPPrompts` signals failure with exception types; the dispatcher maps them to JSON-RPC codes, the same
division `MCPResources` uses.

| Condition | Signal | JSON-RPC code |
|---|---|---|
| nothing available to this caller, on `prompts/list` | none, returns `{"prompts": []}` | 200 (success) |
| prompt hidden by profile or permissions, on `prompts/get` | `SecurityException` | `-32600` |
| unknown prompt name | `IllegalArgumentException` | `-32602` |
| missing required argument | `IllegalArgumentException` | `-32602` |
| anything else | `Exception` | `-32603` |

**`list` stays quiet, `get` fails loudly** - the asymmetry #4865 settled. `prompts/list` is a discovery call
most clients issue unprompted at session start, so a server with reads disabled returns an empty array
rather than showing an error banner to every connecting agent. `prompts/get` is an explicit request and
fails with a message naming what is missing.

Prompt names are a fixed, documented, server-wide set, so a denial that says which prompt was refused
discloses nothing about databases or data. That is why `prompts/get` names the refused prompt where
`resources/read` deliberately collapses unknown and unauthorized into one indistinguishable `-32002`.

## Cross-reference test

The issue asks that a test fail when a tool named in prompt text is renamed or removed. Two assertions
build that link, in both directions:

```java
// forward: every tool the prompt declares is registered
assertThat(GraphRagQueryPrompt.referencedTools()).isSubsetOf(MCPDispatcher.REGISTERED_TOOL_NAMES);

// reverse: every registered tool name appearing in the text is declared
for (final String toolName : MCPDispatcher.REGISTERED_TOOL_NAMES)
  if (containsWord(renderedText, toolName))
    assertThat(GraphRagQueryPrompt.referencedTools()).contains(toolName);
```

Renaming `vector_search` in `VectorSearchTool.getDefinition()` fails the forward assertion, because the
prompt still declares the old name. Removing the tool fails it identically. Text that grows a mention of a
tool the prompt does not declare fails the reverse assertion, which is what keeps `referencedTools()` an
honest input to the availability rule rather than a hand-maintained list that drifts.

`containsWord` matches on word boundaries (`\b` around the quoted name). A plain `contains` would report
`query` inside `graphrag_query`, and would miss nothing in exchange.

Sharing a compile-time constant between `getDefinition()` and the prompt text was considered. It makes
drift impossible, but it also rewrites prompt wording silently on a rename with no human reading the result,
and it touches all sixteen tool classes. Declared-set plus scan keeps a rename a review event.

`REGISTERED_TOOL_NAMES` is `private static final` today and widens to package-private for these assertions.
`MCPPromptsTest` lives in `com.arcadedb.server.mcp`, the same package.

## Files touched

**New**
- `server/src/main/java/com/arcadedb/server/mcp/prompts/GraphRagQueryPrompt.java`
- `server/src/main/java/com/arcadedb/server/mcp/prompts/BuildKnowledgeGraphPrompt.java`
- `server/src/main/java/com/arcadedb/server/mcp/MCPPrompts.java`

**Modified**
- `server/src/main/java/com/arcadedb/server/mcp/MCPDispatcher.java` (two switch arms, the `prompts`
  capability, two `instructions` sentences, `REGISTERED_TOOL_NAMES` visibility)

**New test**
- `server/src/test/java/com/arcadedb/server/mcp/MCPPromptsTest.java`

**Modified test** (additions only)
- `server/src/test/java/com/arcadedb/server/mcp/MCPServerPluginTest.java`
- `server/src/test/java/com/arcadedb/server/mcp/MCPStdioServerTest.java`

`MCPPermissionsTest` is deliberately untouched, for the reason #4865 recorded: it is a pure unit test over a
bare `MCPConfiguration`. Permission-driven prompt visibility is covered in `MCPPromptsTest`, which can
construct an `MCPConfiguration` and an `EffectiveToolProfile` without a server, and over the wire in
`MCPServerPluginTest`.

## Testing

**No existing test method may be edited.** New coverage arrives as new methods, so the current suites stay
an untainted regression net, exactly as #4865 required. The existing `capabilities` assertions use
`capabilities.has("tools")` rather than asserting an exhaustive key set, so adding `prompts` alongside is a
no-op for them; verify that during implementation rather than assuming it.

| Suite | Coverage |
|---|---|
| `MCPPromptsTest` (new) | The two cross-reference assertions above, for both prompts. `getDefinition()` declares both arguments `required: true`. `getMessages` substitutes `{database}`, `{question}`, `{sourceText}` and leaves no placeholder behind. A missing required argument raises `IllegalArgumentException`; an unknown prompt name raises `IllegalArgumentException`. The availability matrix: both visible under `all` and `rag` with reads and writes on; both hidden under `admin`; `build_knowledge_graph` hidden with `allowInsert=false`, with `allowUpdate=false`, and with `allowReads=false`; both hidden with `allowReads=false`. `list` returns only available prompts; `get` on an unavailable prompt raises `SecurityException` |
| `MCPServerPluginTest` (HTTP) | `initialize` advertises `capabilities.prompts` with `listChanged: false`; `prompts/list` returns both prompts with argument schemas; `prompts/get` returns a well-formed `messages` array whose text carries the substituted arguments; `prompts/get` on an admin-profile server returns `-32600`; missing argument returns `-32602`; unknown prompt name returns `-32602`; `prompts/list` under the `admin` profile is empty |
| `MCPStdioServerTest` | `prompts/list` and `prompts/get` work over stdio; `initialize` advertises `prompts` |
| all existing MCP tests | Regression net: `tools/list` count unchanged, `tools/call` unaffected, `resources/*` unaffected, `ping` and notification handling unchanged |

`git diff --stat main -- server/src/test/java/com/arcadedb/server/mcp/` at the end of the work must show
additions only, plus the new `MCPPromptsTest.java`.

## Risks

**The strict availability rule.** Recorded above under *Availability rule*: a future profile that admits
some but not all of a prompt's tools hides the prompt outright. No such profile exists today.

**Prompt text is prose no test can judge.** The cross-reference test proves the text names real tools; it
cannot prove the text is good advice. Correctness rests on the review the literal text received in this
design, which is why the full text is reproduced here rather than described.

**Instruction-block growth.** Every sentence added to `instructions` is paid on every session of every
agent. One sentence per profile is the budget; further prompt documentation belongs in the prompt
descriptions returned by `prompts/list`, which are fetched only when asked for.

## Out of scope

- **`notifications/prompts/list_changed`.** `listChanged` is advertised `false`; no notification machinery is
  built, matching the Resources decision.
- **Any third prompt.** The epic enforces surface discipline, and two prompts cover the retrieval and the
  construction halves of the GraphRAG loop.
- **Per-database prompt variants.** Prompt text is server-wide; per-database scoping is
  [#4868](https://github.com/ArcadeData/arcadedb/issues/4868).
- **Schema hydration inside prompts.** Rejected under *Prompt body*; #4865 covers that need.
- **Embedding generation.** Unchanged epic non-goal: prompt text tells the model to bring its own vectors.
- **Documentation.** The MCP pages live in the out-of-tree `arcadedb-docs` repository and are a follow-up,
  consistent with #4862 and #4865.
- **Any relaxation of default permissions.** `enabled=false` and every write flag `false` are unchanged, and
  this issue adds no new configuration flag.
