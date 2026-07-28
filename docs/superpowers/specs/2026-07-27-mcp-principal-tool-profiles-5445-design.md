# MCP per-principal tool profiles (#5445)

**Issue:** [#5445](https://github.com/ArcadeData/arcadedb/issues/5445)
**Epic:** [#4859 - MCP GraphRAG & Agent-Memory Surface](https://github.com/ArcadeData/arcadedb/issues/4859)
**Date:** 2026-07-27
**Status:** implemented

## Decision

The Model Context Protocol (MCP) configuration may restrict individual authenticated
principals to one of the existing `all`, `rag`, or `admin` tool profiles. The server-global
`profile` remains the backward-compatible default and a hard ceiling.

For a principal with an override, a tool is available only when both the global profile and
the principal profile contain it. This intersection means an override can narrow the endpoint
but cannot expose a tool hidden globally. Existing read, write, schema, administrative,
database, user, and origin permissions remain independent mandatory checks.

## Configuration

Overrides live under `principalProfiles` in `config/mcp-config.json`:

```json
{
  "enabled": true,
  "profile": "all",
  "allowedUsers": ["retrieval-agent", "retrieval-token"],
  "principalProfiles": {
    "retrieval-agent": "rag",
    "apitoken:retrieval-token": "rag"
  }
}
```

Named users use their user name. API tokens use the canonical authenticated principal name
`apitoken:<token-name>`, which avoids ambiguity with a named user that happens to share the
token's display name. Profile names are case-insensitive when parsed and serialized as
lowercase.

The global `allowedUsers` gate still decides whether a principal may reach MCP at all. Its
existing bare-token convenience remains unchanged; the canonical prefix is required only for
the profile map because that map identifies one exact principal.

## Update semantics

Configuration API updates merge `principalProfiles` by principal name:

- A supplied profile replaces that principal's override.
- A null principal value removes that one override.
- `"principalProfiles": null` clears every override.
- Omitting `principalProfiles` leaves the map unchanged.

Blank principal names, non-string values, and unknown profile names are rejected before any
other setting in the same update is applied. The serialized configuration omits
`principalProfiles` when no overrides exist.

An override for a principal that does not currently exist remains valid and inert. It starts
applying only if authentication later produces that exact principal name. This supports
pre-provisioning and token rotation without making configuration persistence depend on the
current user registry.

## Enforcement

The shared `MCPDispatcher` resolves the effective tool set after authentication on every
request. That one resolution path governs:

- `initialize` instructions;
- `tools/list` discovery; and
- `tools/call` direct invocation.

HTTP supplies the user authenticated for that request. Standard input/output mode supplies
the user authenticated when the process starts. Both transports therefore use identical
selection and enforcement rules.

The global profile and principal override are both allowlists. For example, global `rag` plus
principal `admin` exposes only the tools common to both profiles. A hidden tool remains denied
when called directly by name.

## Validation

Focused tests cover:

- persistence, canonical named-user and API-token identities;
- merge, single-entry removal, and full-map clearing;
- invalid names, values, and profiles without partial updates;
- two named users receiving different HTTP tool lists on one endpoint;
- API-token discovery and direct-call enforcement;
- standard input/output principal resolution;
- global-profile ceiling intersection;
- fallback for principals without an override; and
- initialization instructions matching the effective surface.
