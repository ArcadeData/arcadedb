---
name: arcadedb
description: Use when the user asks about data in their ArcadeDB database, wants to query, explore, model, search or update it (graph, documents, vectors, full text), or when an ArcadeDB MCP tool (list_databases, get_schema, query, execute_command, vector_search, upsert_entity, ...) fails or is missing. Covers the tool workflow, query languages, write safety and connection troubleshooting.
---

# Working with ArcadeDB

The `arcadedb` MCP server of this plugin talks to the user's own ArcadeDB server at `${user_config.server_url}`. ArcadeDB is a
multi-model database: graphs (vertices and edges), documents, key/value, vectors and time series, all in the same database, and
queryable with several languages.

## Workflow

1. **Find the database.** Call `list_databases` unless the user named one. Never guess a database name.
2. **Learn the schema before writing a query.** Call `get_schema` (or read the `arcadedb://<database>/schema` resource) to see
   the vertex, edge and document types, their properties and indexes. Type and property names are case-sensitive.
3. **Look at real values when shapes are unclear.** `sample_records` returns the first records of a few types, which shows
   how dates, embeddings, nested maps and lists are actually stored. It is not a random sample.
4. **Read with `query`, write with `execute_command`.** `query` refuses anything that modifies data. Keep result sets small with
   `limit`, and aggregate on the server (`count`, `GROUP BY`, `RETURN ... LIMIT`) instead of pulling rows to count them yourself.
5. **Empty result?** Check the type and property names against the schema before concluding the data does not exist.

## Choosing a query language

The `language` argument accepts `cypher` (the default), `sql`, `gremlin`, `graphql` and `mongo`. Scripting languages
(`js`, `python`, ...) are refused: the server cannot check what they do without running them.

- **Cypher** for graph patterns and traversals: `MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name, count(p) AS staff`.
- **ArcadeDB SQL** when the user asks for SQL, for schema changes, and for document-style work. It is not ANSI SQL:
  - types instead of tables: `SELECT FROM Person WHERE age > 30`
  - graph traversal functions: `SELECT expand(out('WORKS_AT')) FROM Person WHERE name = 'Ada'`
  - schema DDL: `CREATE VERTEX TYPE Person`, `CREATE PROPERTY Person.email STRING`, `CREATE INDEX ON Person (email) UNIQUE`
  - records have a `@rid` such as `#12:0`, plus `@type` and, on edges, `@out` / `@in`
- Use the language the user writes in when they paste a query; do not translate it unless asked.

Pass user-supplied values as literals carefully: quote strings, never concatenate untrusted text into a command.

## Search and agent memory

- `vector_search` runs a nearest-neighbour search on an LSM vector index; `full_text_search` searches a full-text index;
  `hybrid_search` fuses both. `get_schema` shows which indexes exist.
- ArcadeDB does not generate embeddings. Ask the user for the embedding model used to build the index and supply vectors
  of the same dimension.
- `upsert_entity` and `upsert_relationship` create or update a vertex or an edge addressed by match keys, without
  duplicating it, which makes them the right tools to maintain a knowledge graph or agent memory. They still overwrite
  the properties of a record they match, so the write-safety rules below apply to them too, and `setProperties` must not
  change a match key, or the next call with the same keys creates a new record. A UNIQUE index on the match keys avoids
  a full scan per call and duplicates under concurrent writers.

## Write safety

`execute_command`, `upsert_entity`, `upsert_relationship` and `set_server_setting` change the database or the server.
Before running one:

- say what will change, and for `UPDATE` / `DELETE` first run the matching `SELECT` / `MATCH` with `query` and report how
  many records it touches;
- never run `DROP`, `TRUNCATE` or a `DELETE` without a `WHERE` clause unless the user explicitly asked for exactly that;
- prefer one command per call, so a failure points at one statement.

## When a tool fails

The server enforces two layers of permissions. The MCP tool hints never grant anything.

| Message | Cause | What the user does |
|---|---|---|
| `MCP server is disabled` | MCP is off (the default) | ArcadeDB Studio > Server > MCP: set MCP Server to enabled and save |
| `User not authorized for MCP access` | the API token is not in the MCP allowed users | Studio > Server > MCP: add `apitoken:<token name>` to the allowed users |
| `Insert/Update/Delete/Schema operations are not allowed by MCP configuration` | that operation is switched off for MCP | Studio > Server > MCP: tick the operation, only if the user wants the assistant to do it |
| `Admin operations are not allowed by MCP configuration`, or a server-admin refusal | `server_status`, the profiler and settings tools need admin access and the root user | expected with an API token: use Studio for administration |
| `... not authorized to access database ...` or a permission error on a type | the API token's own permissions do not cover it | Studio > Security > API Tokens: create a token with the needed database and type permissions |
| connection refused, timeout, 401 | wrong URL or token | `/plugin` > ArcadeDB > configure: check the server URL (no `/api/v1/mcp` suffix) and paste a valid token |

Explain the cause to the user instead of retrying the same call. Do not suggest widening permissions unless the user asked
for the operation that needs them.
