# MCP tool profiles (#4867)

**Issue:** [#4867](https://github.com/ArcadeData/arcadedb/issues/4867)
**Epic:** [#4859 - MCP GraphRAG & Agent-Memory Surface](https://github.com/ArcadeData/arcadedb/issues/4859)
**Date:** 2026-07-24
**Status:** implemented

## Decision

The Model Context Protocol (MCP) configuration supports three named tool profiles:

- `all`: every registered tool; this is the default and preserves existing behavior.
- `rag`: retrieval and agent-memory tools.
- `admin`: the existing operator and database-administration surface.

Profiles are allowlists. They filter `tools/list` and are enforced again by `tools/call`, so a
client cannot invoke a hidden tool by name. Unknown tools remain rejected independently of the
selected profile.

## Configuration

Set `profile` in `config/mcp-config.json` or through the existing MCP configuration endpoint:

```json
{
  "enabled": true,
  "profile": "rag",
  "allowReads": true,
  "allowedUsers": ["retrieval-agent"]
}
```

The field is persisted with the rest of the MCP configuration. Values are case-insensitive when
parsed and serialized as lowercase. Unknown values are rejected before any other setting in the
same update is applied.

## Profile contents

The `rag` profile permits these registered tool names:

- `list_databases`
- `get_schema`
- `query`
- `full_text_search`
- `sample_records`
- `vector_search`
- `hybrid_search`
- `upsert_entity`
- `upsert_relationship`

Only tools actually registered by the running server are advertised. This lets retrieval tools
join the profile as they are installed without exposing an unavailable tool.

The `admin` profile contains:

- `list_databases`
- `get_schema`
- `query`
- `execute_command`
- `server_status`
- `profiler_start`
- `profiler_stop`
- `profiler_status`
- `get_server_settings`
- `set_server_setting`

Schema resources remain available through the MCP Resources capability, while `get_schema`
supports clients that expose only tools. Existing read, write, schema, administrative, user,
and origin permissions remain independent mandatory checks; a profile never grants an
operation. In particular, the two upsert tools remain unusable unless their existing write
permissions are enabled.

## Transport behavior

Both HTTP and standard input/output transports use the shared `MCPDispatcher`, so discovery,
execution denial, and profile-specific initialization instructions have one implementation.
Changing a profile through the configuration endpoint takes effect on subsequent requests.

## Validation

Tests cover default compatibility, persistence, case-insensitive parsing, invalid-update
atomicity, HTTP and standard input/output discovery filtering, execution denial for hidden
tools, and successful execution of allowed tools.

## Out of scope

The profile setting remains server-global in this implementation. It cannot yet present a
retrieval agent and an administrative agent with different tool lists when they share one MCP
endpoint, so it does not by itself satisfy the per-agent part of #4859. Per-principal profile
selection and enforcement is tracked in
[#5445](https://github.com/ArcadeData/arcadedb/issues/5445).
