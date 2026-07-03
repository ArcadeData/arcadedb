# Bolt server identity documentation (#4884)

Part of epic #4882 (Bolt Driver Compatibility Certification), Group A
("certification backbone"), alongside the merged #4883 conformance spec.

## Problem

`BoltNetworkExecutor` advertises `"Neo4j/5.26.0 compatible (ArcadeDB
<version>)"` in the HELLO/SUCCESS response and hardcodes matching values
(`"5.26.0"`, edition `"community"`) in the synthetic `CALL dbms.components()`
response. Official drivers gate feature negotiation and compatibility
warnings off this string, so every certification result produced by the
#4883 conformance spec is only meaningful relative to this stated envelope.
The decision is real and already shipped, but nowhere written down: no doc
states the string, its source location, why `5.26.0` was picked, or which
Neo4j-5.26-era features ArcadeDB does not implement behind that claim.

## Non-goals

- No code change. This is a documentation-only task (issue effort: S).
- Not re-deciding the advertised string - that is a separate, deliberate
  future decision per the epic's non-goals.
- Not a general `bolt` module README/getting-started guide - scope is
  strictly the identity/envelope documentation the epic asks for.

## Design

### File: `bolt/SERVER_IDENTITY.md` (new)

A standalone file at the bolt module root, next to
`BoltNetworkExecutor.java`, rather than folded into
`bolt/conformance/README.md`. Rationale: the conformance README is scoped to
"how a human writes a test against `spec.yaml`" for test authors; this doc
is about a server-side design decision for anyone reading the bolt module's
code or filing a compatibility bug. Keeping them separate keeps each
file's job single-purpose. The two are cross-linked (see below) so neither
is an orphan.

No top-level `docs/bolt-server-identity.md` (the alternative the issue text
also floats): the repo's `docs/` root is exclusively per-issue
bug-investigation write-ups (`docs/<issue-number>-<slug>.md`, confirmed
against ~30 existing files); this is a permanent reference doc, not a bug
investigation, so it doesn't belong there.

### Content (4 sections)

**1. The advertised identity, verbatim, with source**
- HELLO/SUCCESS `server` metadata:
  `"Neo4j/5.26.0 compatible (ArcadeDB " + Constants.getRawVersion() + ")"` -
  `BoltNetworkExecutor.java:422` (`handleHello`).
- Synthetic `CALL dbms.components()` response: name `"Neo4j Kernel"`,
  versions `["5.26.0"]`, edition `"community"` -
  `BoltNetworkExecutor.java:1016-1021` (`handleSystemQuery`).
- Explicitly note these two are independently hardcoded (not derived from a
  shared constant) - a future change to one without the other would be a
  real inconsistency, not a formatting nit.

**2. Rationale for `5.26.0`**
- Re-derived, since no commit or issue documents it: the only relevant
  history is issue #3471 (Neo4j Desktop 1.6.0+ connectivity fix,
  commit `19a97bb9a`), which explains *that* a version string was needed for
  Neo4j Desktop/driver feature-gating to succeed, but not *why 5.26.0*
  specifically.
- State the re-derived rationale plainly: 5.26 is Neo4j's 5.x LTS release
  line, a reasonable choice for a stable, widely-deployed feature-gating
  baseline rather than pinning to a fast-moving Neo4j 5.x/2025.x edge
  version.
- Document the distinction this doc surfaces: the advertised string is a
  **Neo4j server version claim** used by drivers/tools for feature
  detection, which is a *different axis* from the **Bolt wire protocol
  version** actually negotiated (`SUPPORTED_VERSIONS` in
  `BoltNetworkExecutor.java:96` tops out at 4.4). ArcadeDB claims a 5.26
  server identity while only speaking the 3.0/4.0/4.4 wire protocol - this
  is exactly the kind of nuance the epic wants captured explicitly rather
  than left implicit.

**3. Explicit "does not implement" list**
Sourced directly from the 4 confirmed gaps, cross-referenced to their
`expected-fail` scenario IDs already catalogued (with exact source
references) in `bolt/conformance/spec.yaml`:
- No Bolt 5.x protocol negotiation (`PROTO-002`) -
  `SUPPORTED_VERSIONS` never advertises 5.x; current 5.x-capable drivers
  work only by silently downgrading to 4.4.
- `ROUTE` is single-node only (`CONN-004`) - `handleRoute`
  (`BoltNetworkExecutor.java:903-939`) always returns the local node's own
  address as WRITE/READ/ROUTE regardless of actual HA cluster topology.
- Temporal/spatial type-fidelity gaps (`TYPE-007`..`TYPE-012`) - Date,
  Time, LocalDateTime, DateTime serialize as ISO-8601 strings instead of
  native Bolt structures; `Duration` and `Point` have no handling at all in
  `BoltStructureMapper`/`PackStreamWriter`.
- No `Neo.TransientError.*` codes (`TX-005`, `ERR-004`) -
  `BoltErrorCodes.java` defines only `Neo.ClientError.*`/
  `Neo.DatabaseError.*` (7 codes total); driver-side transient-retry logic
  can never be exercised.

Each bullet states the gap, its scenario ID(s), source file/line, and links
to tracking issue `#4890` (all four gaps are already tracked there per the
merged #4883 spec).

**4. Cross-links**
- From `bolt/SERVER_IDENTITY.md`: link to `bolt/conformance/spec.yaml` and
  `bolt/conformance/README.md`, and to tracking issue `#4890`.
- From `bolt/conformance/README.md`: one added line/paragraph pointing to
  `bolt/SERVER_IDENTITY.md`, satisfying the issue's acceptance criterion
  "Linked from the A1 conformance spec."

## Verification

Documentation-only change - no tests to run. Verification is:
- The 4 gap bullets match `spec.yaml`'s `expected-fail` scenarios exactly
  (re-check against `spec.yaml` at write time, since #4883 is a moving
  target only in the sense that new gaps could theoretically be added
  later - not expected here but worth a final diff check before commit).
- Both source line references (`:422`, `:1016-1021`, `:96`, `:903-939`)
  verified against current `main` at doc-writing time, not copied stale
  from this design doc.
- Markdown renders correctly (visual check) and both cross-links resolve to
  real files/anchors.

## Acceptance criteria (from issue #4884)

- [ ] Doc committed stating the advertised identity, its source location,
      and rationale.
- [ ] Explicit "does not implement" list matching the 4 confirmed gaps.
- [ ] Linked from the #4883 conformance spec.
