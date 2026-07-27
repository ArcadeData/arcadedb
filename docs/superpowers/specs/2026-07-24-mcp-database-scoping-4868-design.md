# MCP per-database configuration scoping (#4868)

**Issue:** [#4868](https://github.com/ArcadeData/arcadedb/issues/4868)
**Epic:** [#4859 - MCP GraphRAG & Agent-Memory Surface](https://github.com/ArcadeData/arcadedb/issues/4859)
**Date:** 2026-07-24
**Status:** implemented

## Decision

Database overrides are restrictions on the server-global Model Context Protocol (MCP)
configuration. The global configuration is a hard permission ceiling:

- A local `false` denies an operation allowed globally.
- A local `true` cannot grant an operation denied globally.
- A local `allowedUsers` entry must also pass the global `allowedUsers` list.
- An omitted local field inherits the corresponding global value.
- A database with no override retains the existing global behavior.

This is the conservative choice for a server-wide endpoint. A root administrator can reason
about the global settings as the maximum authority the endpoint can ever exercise, and adding
an override cannot accidentally widen that authority.

## Configuration

Overrides live under `databases` in `config/mcp-config.json`:

```json
{
  "enabled": true,
  "allowReads": true,
  "allowInsert": true,
  "allowUpdate": true,
  "allowDelete": false,
  "allowSchemaChange": false,
  "allowAdmin": false,
  "allowedUsers": ["root", "tenant-agent"],
  "databases": {
    "tenant_graph": {
      "allowInsert": false,
      "allowUpdate": false,
      "allowedUsers": ["tenant-agent"]
    }
  }
}
```

For `tenant_graph`, the example permits reads to `tenant-agent` but denies writes. `root`
remains globally authorized but is excluded by the database allowlist. Other databases inherit
the global policy.

The supported override fields are:

- `allowReads`
- `allowInsert`
- `allowUpdate`
- `allowDelete`
- `allowSchemaChange`
- `allowAdmin`
- `allowedUsers`

Unknown override fields are rejected so a misspelled security setting cannot silently inherit a
more permissive global value. An explicit `allowedUsers: null` is an empty local allowlist,
matching the existing global configuration behavior. API token names retain the existing bare
token matching behavior.

Configuration API updates merge the `databases` object by database name: an update to one
database leaves other overrides intact. A supplied database object replaces that database's
override, an explicit null value removes that one override, and `"databases": null` clears all
database overrides. The serialized configuration omits `databases` when no overrides exist.
At startup, an override whose database is not currently loaded produces a warning but remains
valid so it can apply if the database is created later.

## Enforcement

`MCPConfiguration.getPermissionsForDatabase()` returns one effective-permission snapshot, or the
global configuration itself when the database carries no override. Database-targeted tools resolve
that snapshot together with the authenticated database, each declaring the access it requires:

| Tool | Required access |
| --- | --- |
| `get_schema` | `READ` |
| `query` | `READ` |
| `sample_records` | `READ` |
| `vector_search` | `READ` |
| `full_text_search` | `READ` |
| `execute_command` | `ACCESS` |
| `upsert_entity` | `ACCESS` |
| `upsert_relationship` | `ACCESS` |

The write tools request `ACCESS` rather than `READ` because their permission gate is the
operation-type check that follows, which resolves `CREATE`/`UPDATE` against the same per-database
snapshot. `execute_command` covers both cases: a read command is still gated on the database-local
`allowReads` through that same check.

Database discovery in `list_databases`, `server_status`, and schema resources omits databases
whose effective policy denies the user or read access. ArcadeDB's native database authorization
remains an independent required check; MCP configuration does not replace it.

Future database-targeted tools must use the same resolver and explicitly select `READ` or
`ACCESS` as their required access. There is no default resolver mode, so a new read tool cannot
silently bypass database-local read restrictions.

Tools without a database argument remain server-global and continue to use only global
permissions: profiler controls, server settings, and server-level status details. Per-database
`allowAdmin` still applies when `execute_command` analyzes a database command as an
administrative operation.

## Out of scope

- No `/mcp/{database}` HTTP route is added. A database-bound endpoint needs a separate design
  for transport configuration, discovery, initialization, and tool-list behavior.
- Tool profiles and browser-origin controls remain server-global because the current endpoint
  advertises one tool surface and has one HTTP origin policy.
- Overrides do not grant authority above the global ceiling.

## Validation

Focused tests cover persistence, inheritance, stricter overrides, attempted grants above the
global ceiling, user restrictions, API-token matching, invalid settings, all database read
surfaces, command/upsert writes, resource discovery, database listing, and server-status
filtering.
