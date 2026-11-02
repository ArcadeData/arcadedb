# ArcadeDB plugin for Claude Code

Connects Claude Code to **your own** ArcadeDB server through the MCP endpoint every ArcadeDB server ships
(`/api/v1/mcp`), and adds a skill that teaches the assistant how to work with ArcadeDB: discover the schema first, read
with `query`, write with `execute_command` only after saying what will change, and explain permission errors instead of
retrying them.

Claude Code calls your server directly with the API token you configure: there is no hosted relay in between. What the
assistant reads through the tools (schema, query results) becomes part of the conversation, so scope the token to the
data you are comfortable sharing with it.

## 1. Prepare the server

In ArcadeDB Studio (ArcadeDB 26.11.1 or later recommended):

1. **Security > API Tokens**: create a token for the assistant. Give it only the databases and type permissions it
   needs (read-only is a good start). Copy the token value, it is shown once.
2. **Server > MCP**: set **MCP Server** to enabled, add `apitoken:<token name>` to the allowed users, tick the
   operations the assistant may perform (reads are on by default; insert, update, delete and schema changes are off)
   and save.

## 2. Install the plugin

From a shell:

```
claude plugin marketplace add ArcadeData/arcadedb --sparse .claude-plugin claude-plugin
claude plugin install arcadedb@arcadedb
```

`--sparse` checks out only the plugin folders instead of the whole ArcadeDB repository. Inside Claude Code,
`/plugin marketplace add ArcadeData/arcadedb` followed by `/plugin install arcadedb@arcadedb` does the same with a full
checkout.

Claude Code then asks for:

| Setting | Example |
|---|---|
| ArcadeDB server URL | `http://localhost:2480` or `https://arcadedb.example.com` (no `/api/v1/mcp` suffix) |
| ArcadeDB API token | `at-...`, stored in the operating system's credential store |

Use `https://` for any server that is not on your own machine: the token travels in an `Authorization` header.

## 3. Use it

Ask in plain language, for example "which databases do I have?", "show me the schema of `movies`", "who are the five
most connected people in the `social` graph?". Read-only tools run freely; tools that change data are marked as
destructive, so Claude Code asks before running them.

## Versioning

The plugin is versioned on its own, not with ArcadeDB releases: `version` in `.claude-plugin/plugin.json` is bumped
whenever a file of the plugin changes (skill, server config, settings). Claude Code keeps an installed plugin on its
version until that field changes, so a change without a bump does not reach existing users.

## Permissions

The server decides what the assistant can do, twice: the MCP configuration (which operations MCP may perform at all)
and the API token's own permissions (which databases and types). The tool hints this server publishes only drive the
confirmation prompts in the client; they never grant access. Administration tools (server settings, profiler) need the
root user and are refused to an API token by design: use Studio for those.
