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

## Enforcement

`MCPConfiguration.getPermissionsForDatabase()` returns one immutable effective-permission
snapshot. Database-targeted tools resolve that snapshot together with the authenticated
database:

- `get_schema`
- `query`
- `execute_command`
- `full_text_search`
- `upsert_entity`
- `upsert_relationship`

Database discovery in `list_databases`, `server_status`, and schema resources omits databases
whose effective policy denies the user or read access. ArcadeDB's native database authorization
remains an independent required check; MCP configuration does not replace it.

Future database-targeted tools must use the same resolver so they cannot bypass database-local
read, write, or user restrictions.

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
