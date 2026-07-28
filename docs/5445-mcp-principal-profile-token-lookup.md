# Issue #5445 follow-up - a principal profile written with a bare API-token name is silently inert

PR: (to be filled after opening)

Follow-up to #5468, which implemented per-principal MCP tool profiles and merged as `ec3447a96`.
Two defects found reviewing that change after the merge.


## Symptom 1 - the restriction an operator writes does nothing

MCP accepts an API token under either spelling in `allowedUsers`: the canonical authenticated
principal name `apitoken:<token-name>`, or the bare token name. So this configuration admits the
token and looks like it confines it to the retrieval surface:

```json
{
  "profile": "all",
  "allowedUsers": ["retrieval-token"],
  "principalProfiles": { "retrieval-token": "rag" }
}
```

It does not. The token authenticates as `apitoken:retrieval-token`, the profile map is keyed on
`retrieval-token`, and the lookup is an exact match, so no override is found and the principal falls
back to the global profile - `all`. `tools/list` shows `execute_command`, `set_server_setting` and
the rest, and each is callable. There is no error, no warning, and nothing in the serialized
configuration that reads as wrong: the entry the operator wrote is still there, and still says
`rag`.

The two keys spell the same principal the same way and disagree about who they address. That is the
whole defect: a configuration that reads as a restriction is not one.

## Symptom 2 - a malformed configuration value answers 500

`POST /api/v1/mcp/config` with a `principalProfiles` that is not an object:

```json
{ "principalProfiles": "rag" }
```

answers `500` with an internal-error body instead of `400`. The same holds for the pre-existing
`databases` key.

## Root cause

**Symptom 1.** `MCPConfiguration` resolves the two lookups differently.

`isUserAllowed` delegates to `matchesUser`, which falls back to the bare token name:

```java
return username.startsWith("apitoken:")
    && users.contains(username.substring("apitoken:".length()));
```

`getPrincipalToolProfile` does not:

```java
public ToolProfile getPrincipalToolProfile(final String principalName) {
  return principalName == null ? null : principalProfiles.get(principalName);
}
```

The design note for #5445 called the canonical prefix "required only for the profile map because
that map identifies one exact principal". The disambiguation argument holds, but requiring the
canonical form and then silently ignoring anything else turns a typo-shaped mistake into a
privilege the operator did not intend to leave open.

**Symptom 2.** Both `load()` and `updateFrom()` read the nested object with
`json.getJSONObject(name, null)`, whose default applies only when the value is JSON null. A string
value reaches Gson's `getAsJsonObject()` and raises `IllegalStateException`, which `MCPConfigHandler`
does not catch - it catches `IllegalArgumentException | JSONException` and maps those to `400`.

## Fix

1. `getPrincipalToolProfile` matches the canonical name first, then falls back to the bare token
   name for an `apitoken:` principal - the same convention `isUserAllowed` already accepts. An
   explicit `apitoken:<name>` entry therefore stays authoritative when both spellings are present.

2. A new `objectValue(json, name)` helper reads a nested configuration object, maps absent and
   explicitly null to `null` (the caller's clear-everything intent) and rejects any other non-object
   value with `IllegalArgumentException`. Used for `principalProfiles` and for `databases`, in
   `load()` and in `updateFrom()`.

### Why the bare-name fallback cannot widen the surface

A principal profile is only ever intersected with the global one
(`MCPDispatcher.EffectiveToolProfile.allows`), so finding an override can only remove tools, never
add them. The worst case for a collision - a named user and an API token sharing a name - is that
the token is confined to the named user's profile. That is a narrowing, and it is the collision
`matchesUser` already lives with on the allowlist.

The reverse direction cannot collide at all: `PostUserHandler` rejects creating a user whose name
starts with `apitoken:`, so an `apitoken:<name>` key can only ever denote a token.

## Tests

- `MCPConfigurationTest.apiTokenPrincipalProfileMatchesBareTokenName` - a bare-name entry resolves
  for an `apitoken:` principal; a canonical entry wins when both are configured; a named user is
  never matched by the fallback; an unrelated token still resolves to no override.
- `MCPConfigurationTest.invalidPrincipalProfileIsRejectedWithoutPartialUpdate` - extended: a
  non-object `principalProfiles` and a non-object `databases` are both rejected as
  `IllegalArgumentException` (so the endpoint answers `400`), and a sibling setting in the same
  update is not applied. This replaces the merged assertion on `IllegalStateException`, which locked
  in the `500`.
- `MCPServerPluginTest.apiTokenPrincipalProfileAcceptsBareTokenName` - end to end over HTTP: a token
  allowlisted and profiled by its bare name sees the `rag` tool list and is denied `server_status`
  on a direct `tools/call`.

`apiTokenPrincipalProfileMatchesBareTokenName` was confirmed red against the unfixed code before the
fix was applied.

## Verification

| Suite | Result |
|---|---|
| `server` `MCPConfigurationTest` | 24/24 |
| `server` `MCPPermissionsTest` | 15/15 |
| `server` `MCPToolUtilsTest` | 4/4 |
| `server` MCP suite (`MCPConfigurationTest,MCPStdioServerTest,MCPServerPluginTest,MCPPermissionsTest,MCPResourcesTest,MCPToolUtilsTest`) | 161/161 |

## Known gaps

- **MCP Resources are not profile-gated.** `arcadedb://{database}/schema` is governed by the read,
  database and user permission layers alone. That is currently equivalent to the tool gate, because
  every profile exposes `get_schema` and a database schema is the only resource, so no profile can
  deny content the resource would reveal. A resource added later without a matching tool would break
  the equivalence and would need its own profile mapping. Recorded in the design note; not addressed
  here.
- **`principalProfiles` is undocumented.** The MCP reference page lives in the separate
  `ArcadeData/arcadedb-docs` repository at `src/main/asciidoc/reference/mcp/mcp.adoc` and documents
  the `mcp-config.json` keys. It does not mention `principalProfiles`, nor the `profile` key from
  #5402, nor the `databases` key from #5401. A docs PR is owed there.
