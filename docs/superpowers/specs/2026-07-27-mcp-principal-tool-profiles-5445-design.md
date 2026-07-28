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

The global `allowedUsers` gate still decides whether a principal may reach MCP at all, and it
accepts either the canonical or the bare token spelling. Profile lookup accepts both spellings
too: an API-token principal is matched first by `apitoken:<token-name>` and, failing that, by
the bare token name. The canonical entry therefore stays authoritative when both are present,
while an operator who writes the same name in `allowedUsers` and in `principalProfiles` gets
the restriction they configured instead of a silently inert entry. Matching the bare form
cannot widen the surface, because a principal profile is only ever intersected with the global
one.

## Update semantics

Configuration API updates merge `principalProfiles` by principal name:

- A supplied profile replaces that principal's override.
- A null principal value removes that one override.
- `"principalProfiles": null` clears every override.
- Omitting `principalProfiles` leaves the map unchanged.

Blank principal names, non-string values, unknown profile names, and a `principalProfiles` value
that is not an object are rejected before any other setting in the same update is applied. All
four are reported as a client error, so the configuration endpoint answers `400` rather than
surfacing a malformed payload as an internal error. The same rule now covers the `databases`
key, which shares the parsing path. The serialized configuration omits `principalProfiles` when
no overrides exist.

An override for a principal that does not currently exist remains valid and inert. It starts
applying only if authentication later produces that principal name. This supports
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

Profiles gate tools only. The MCP Resources surface stays governed by the read, database, and
user permission layers alone. That is currently equivalent, because every profile exposes
`get_schema` and the only resource is a database schema, so no profile can deny content the
resource would reveal. A resource added later without a matching tool would break that
equivalence and would need its own profile mapping.

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
