# #5482 - MCP: `updateFrom` applies a prefix of a rejected configuration payload

## Problem

`MCPConfiguration.updateFrom` was not atomic. It parsed `databases`, `profile` and `principalProfiles` into
locals before mutating anything, but assigned the `allow*` booleans **inline**, then committed the three maps,
then assigned `allowedUsers` / `allowedOrigins` inline. A payload whose invalid field was a boolean therefore
committed every boolean that preceded it and then threw:

```java
final MCPConfiguration config = new MCPConfiguration(root);   // isEnabled() == false
config.updateFrom(new JSONObject()
    .put("enabled", true)
    .put("allowReads", "yes"));                                // throws
// isEnabled() was left true, from a payload the endpoint answers 400 for
```

Over HTTP that is `POST /api/v1/mcp/config` returning `400` while having already enabled the MCP endpoint. The
client has no way to know which prefix of its payload stuck short of re-reading the configuration.

The inline boolean block predates #5468 and #5479; it arrives with #5402 (`68d6596dc`).

## Root cause

Mutation was interleaved with validation. The three up-front fields were rejected atomically only as an accident
of ordering, which is why `invalidPrincipalProfileIsRejectedWithoutPartialUpdate` passed deterministically even
though its name promised a guarantee the class did not offer.

A second, related exposure sat in the same method: `allowedUsers` / `allowedOrigins` were read with
`json.getJSONArray(name, null)`, which delegates to Gson's `getAsJsonArray()` and throws `IllegalStateException`
on a non-array value. `MCPConfigHandler` catches only `IllegalArgumentException | JSONException`, so
`{"allowedUsers": "editor"}` produced an HTTP **500** instead of a 400. This is the same defect class that
#5479 fixed for `databases` / `principalProfiles` via the `objectValue()` helper.

## Fix

`server/src/main/java/com/arcadedb/server/mcp/MCPConfiguration.java`

1. `updateFrom` now parses **every** field into a local before assigning any of them, extending the shape the
   three up-front fields already used. All twelve assignments happen in one block at the end, after the last
   thing that can throw. A field absent from the payload resolves to its current value, so the assignment block
   is a no-op for it and the existing list instance is reused rather than reallocated.
2. New `stringListValue()` helper, mirroring the existing `objectValue()`: an explicitly null value still means
   "clear the list", any other non-array value is now an `IllegalArgumentException` and therefore a 400.

The method stays `synchronized` and each field stays `volatile`. That is what guarantees a *rejected* payload
commits nothing, and that a reader of any single field sees either the old value or the new one.

It is deliberately not a cross-field snapshot. The commit block writes twelve fields one at a time and the
getters are not synchronized, so a reader racing a *successful* update can still observe some fields updated and
others not (`enabled` new while `allowedUsers` is still old, say). That is pre-existing behaviour, unchanged by
this PR and out of its scope: closing it would mean holding every field in one immutable snapshot object swapped
in by a single volatile write.

### Deliberately not done: the `booleanValue` exception type

The issue notes that seven throw sites in the class use `IllegalArgumentException` while only `booleanValue`
uses `JSONException`, and suggests aligning them. That change is **not** in this PR: the existing test
`MCPConfigurationTest.invalidBooleanTypeIsRejected` asserts `isInstanceOf(JSONException.class)`, and the
resolve-issue workflow forbids modifying existing tests. Both types are caught by `MCPConfigHandler`'s combined
catch and map to 400, so nothing observable depends on it. Worth a separate cosmetic PR if wanted.

## Tests

Three tests added to `MCPConfigurationTest`, all confirmed RED against the unfixed code:

| Test | Asserts |
|---|---|
| `invalidBooleanIsRejectedWithoutPartialUpdate` | the issue's exact repro: `enabled` does not stick when `allowReads` is invalid |
| `invalidBooleanLeavesEveryOtherSettingUnchanged` | an invalid boolean mid-payload leaves booleans, profile, principalProfiles, allowedUsers and allowedOrigins all untouched |
| `nonArrayUserAndOriginListsAreRejectedWithoutPartialUpdate` | a non-array `allowedUsers` / `allowedOrigins` is a client error, and commits nothing |

The scoping comment on `invalidPrincipalProfileIsRejectedWithoutPartialUpdate` was removed, since the guarantee
it disclaimed is now general (acceptance criterion 3).

### Results

- `MCPConfigurationTest`: 27/27 green (was 24 before this change).
- Full MCP suite (`MCP*Test`, 9 classes): **224/224 green**, no regressions.

## Impact

Behaviour change for clients: a `POST /api/v1/mcp/config` carrying a non-array `allowedUsers` or
`allowedOrigins` now returns 400 instead of 500. Every other rejection keeps the same status code, and every
accepted payload behaves identically. No change to the on-disk format, and `load()` is untouched.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5515

### Review cycles

**Cycle 1 - `8ef50f23`.** `claude[bot]`: **LGTM**, no blocking findings. It confirmed the parse-then-assign
shape composes with the existing merge helpers (they already build fresh maps via `new LinkedHashMap<>(current)`,
so nothing is committed until the last throwing statement has run), and endorsed `stringListValue()` as
consistent with the #5479 `objectValue()` pattern. Three non-blocking observations:

1. *Applied.* The tracking doc claimed a concurrent reader "never [sees] a half-applied payload". That is true
   per field but not across fields: the commit block writes twelve volatile fields one at a time and the getters
   are unsynchronized, so a reader racing a **successful** update can see a mix. Verified against the source
   (`isEnabled`, `isAllowReads`, `getAllowedUsers` are all unsynchronized) and corrected the wording above. No
   code change: the behaviour is pre-existing and the rejected-payload guarantee this PR adds is unaffected.
2. *No action.* Agreed the `booleanValue` / `IllegalArgumentException` inconsistency is correctly deferred.
3. *No action.* Confirmed `stringListValue` keeping `array.getString(i)` preserves the prior coercion behaviour.

`gemini-code-assist` did not review. It has been silent on every recent PR in this repository, so this is the
expected outcome rather than a signal about the change.

`codacy-production`: 0 new issues, 0 added complexity.

**Cycle 2 - `76f5c953`.** Docs-only: the observation-1 wording correction, carrying no source change.
`claude[bot]`: **LGTM** again. It re-derived the atomicity argument independently and confirmed the composition
is "correct, not accidental", and accepted the corrected concurrency wording. Two non-blocking observations,
both of which explicitly recommend leaving the code alone:

1. *No action, as advised.* The commit block self-assigns fields absent from the payload (`allowedUsers =
   updatedAllowedUsers` where both are the same reference). A redundant volatile store, harmless, and the
   reviewer preferred the uniform block over `json.has(...)`-guarded writes for readability. Agreed.
2. *Recorded as a follow-up.* `load()` keeps the old inline-assignment shape, so the two methods could drift.
   Deferring is fine because `load()` is wrapped in a `catch (Exception)` fallback to defaults and a partially
   applied file is not client-observable the way a 400 response is. See Follow-ups below.

It also confirmed conformance to `CLAUDE.md` (imports over FQNs, `final` on locals and params, fluent
`assertThat` assertions, tracking doc matching the `docs/NNNN-*.md` convention) and flagged no performance or
security concerns.

`gemini-code-assist` did not review either cycle.

### Final state

`clean-approval` - two consecutive `claude[bot]` LGTMs with zero blocking findings, an empty working tree and no
deferred items. The `resolve-issue-with-review` gate nominally also requires `gemini-code-assist`, which has
been silent on every recent PR in this repository; its absence carries no signal about this change.

**Merge is the developer's decision. This workflow does not merge PRs.**

## Follow-ups

- Align `booleanValue` to `IllegalArgumentException` (cosmetic, see above).
- `load()` retains its own inline-assignment shape, but it is guarded by a `catch (Exception)` that falls back
  to defaults, and a partially applied file is not client-observable the way a 400 response is. Raised again in
  cycle-2 review as a drift risk now that `updateFrom` has moved to parse-then-assign; worth its own issue.
