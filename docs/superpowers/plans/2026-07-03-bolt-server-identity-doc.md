# Bolt Server Identity Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Document ArcadeDB's advertised Bolt server identity (`"Neo4j/5.26.0 compatible..."`), its source, rationale, and the 4 confirmed compatibility gaps behind that claim, so every certification result produced by the #4883 conformance spec is interpretable against a stated envelope.

**Architecture:** Two markdown files, no code changes. A new standalone `bolt/SERVER_IDENTITY.md` holds the identity documentation; a small addition to the existing `bolt/conformance/README.md` links to it (satisfying the issue's "linked from the conformance spec" acceptance criterion).

**Tech Stack:** Markdown only. No build, no tests to run - verification is grep-based confirmation that every source-line reference in the doc matches the current code, plus the repo's existing pre-commit hooks (trailing-whitespace, end-of-file-fixer) which run automatically on commit.

## Global Constraints

- No code changes anywhere in this plan - documentation only (per issue #4884 effort estimate: S).
- Every source-line reference in the doc must be verified against the current `main`-derived worktree at write time, not copied stale from planning research.
- Do not commit without running `git status`/`git diff` first to confirm only the two intended files changed (per repo-wide instruction to never commit unrelated changes).
- Do not use the em dash character (`—`) anywhere in the doc content - use a normal dash, comma, or rephrase (per repo CLAUDE.md).
- User commits nothing themselves during this plan's execution unless explicitly asked - this plan performs the git add/commit steps as authored, but do not push or open a PR without being asked.

---

### Task 1: Create `bolt/SERVER_IDENTITY.md`

**Files:**
- Create: `bolt/SERVER_IDENTITY.md`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: the file path `bolt/SERVER_IDENTITY.md` and its section anchors (`#advertised-identity`, `#why-52600`, `#known-gaps-behind-this-claim`, `#related-documents`), which Task 2 links to from `bolt/conformance/README.md`.

- [ ] **Step 1: Verify the source line references are still current**

Run these from the repo root of this worktree:

```bash
grep -n 'metadata.put("server"' bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java
grep -n 'dbms.components' -A5 bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java
grep -n 'SUPPORTED_VERSIONS' bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java
grep -n 'private void handleRoute' bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java
grep -n 'public static final String' bolt/src/main/java/com/arcadedb/bolt/BoltErrorCodes.java
grep -n 'Handle date/time types' bolt/src/main/java/com/arcadedb/bolt/structure/BoltStructureMapper.java
```

Expected (as of this plan's authoring - if any line number differs, use the actual current number in Step 2 instead):
- `metadata.put("server"...` at line 422
- `dbms.components` block at lines 1016-1021
- `SUPPORTED_VERSIONS` declaration at line 96
- `handleRoute` at line 903
- `BoltErrorCodes.java` has exactly 7 `public static final String` constants, all `Neo.ClientError.*`/`Neo.DatabaseError.*`
- `Handle date/time types` comment at line 118 in `BoltStructureMapper.java`

- [ ] **Step 2: Write `bolt/SERVER_IDENTITY.md`**

Create the file with this exact content (adjust only the line numbers if Step 1 found drift):

```markdown
# Bolt Server Identity

ArcadeDB's Bolt module advertises a specific Neo4j server identity to
official Neo4j drivers. This is a deliberate, already-shipped decision -
not a placeholder - documented here so every result produced by the
[conformance spec](conformance/spec.yaml) (issue #4883) is interpretable
against a stated envelope, per epic #4882.

## Advertised identity

Two independent places hardcode a Neo4j `5.26.0` identity. They are not
derived from a shared constant - a future change to one without the other
would be a real inconsistency, not a formatting nit.

1. **HELLO/SUCCESS response** - the `server` metadata field returned to
   every client on connection:

   ```java
   metadata.put("server", "Neo4j/5.26.0 compatible (ArcadeDB " + Constants.getRawVersion() + ")");
   ```

   `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java:422`
   (`handleHello`).

2. **Synthetic `CALL dbms.components()` response** - intercepted and
   answered without touching the query engine:

   ```java
   syntheticResults.add(List.of("Neo4j Kernel", List.of("5.26.0"), "community"));
   ```

   `bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java:1016-1021`
   (`handleSystemQuery`).

## Why `5.26.0`

No commit or issue documents the specific choice of `5.26.0`. The only
relevant history is issue #3471 (Neo4j Desktop 1.6.0+ connectivity fix,
commit `19a97bb9a`), which establishes *that* a Neo4j-shaped version string
is required for Neo4j Desktop and official drivers to complete their
feature-negotiation handshake, but not *why this specific version*.

Re-derived rationale, documented here for the first time: `5.26` is
Neo4j's 5.x LTS (long-term support) release line - a reasonable choice for
a stable, widely-deployed feature-gating baseline, rather than pinning to a
fast-moving Neo4j 5.x/2025.x edge release that could introduce
driver-side feature checks ArcadeDB has no matching implementation for.

**This is a different axis from the Bolt wire protocol version actually
negotiated.** The advertised string is a *Neo4j server version claim* read
by drivers/tools for feature detection. The *Bolt wire protocol version*
is negotiated separately during the handshake and tops out at 4.4
(`SUPPORTED_VERSIONS` in
`bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java:96` lists
only `0x00000404`/`0x00000004`/`0x00000003` - Bolt 4.4, 4.0, 3.0). ArcadeDB
claims a Neo4j 5.26 server identity while speaking the 3.0/4.0/4.4 Bolt
wire protocol only. This gap between the two axes is exactly the kind of
thing this doc exists to make explicit instead of leaving implicit.

## Known gaps behind this claim

A driver that trusts the advertised `5.26.0` identity will find these four
areas do not behave like a real Neo4j 5.26 server. Each is already tracked
as an `expected-fail` scenario with a source reference in
[`conformance/spec.yaml`](conformance/spec.yaml) and rolled up under
tracking issue #4890.

- **No Bolt 5.x protocol negotiation** (`PROTO-002`) - `SUPPORTED_VERSIONS`
  (`BoltNetworkExecutor.java:96`) never advertises any Bolt 5.x version.
  Drivers that support Bolt 5.x today work only by silently downgrading to
  4.4 - an undocumented, untested compatibility stance until now.
- **`ROUTE` is single-node only** (`CONN-004`) - `handleRoute`
  (`BoltNetworkExecutor.java:903-939`) always returns the local node's own
  address as the `WRITE`, `READ`, and `ROUTE` role, regardless of actual HA
  cluster topology. `neo4j://` routing against a real cluster is unproven.
- **Temporal and spatial type-fidelity gaps** (`TYPE-007` through
  `TYPE-012`) - `LocalDate`, `LocalTime`, `LocalDateTime`,
  `OffsetDateTime`/`ZonedDateTime` serialize as ISO-8601 strings instead of
  native Bolt structures (`BoltStructureMapper.java`, from line 118).
  `Duration` and spatial `Point` have no handling anywhere in
  `BoltStructureMapper`/`PackStreamWriter` - not even a string fallback.
- **No `Neo.TransientError.*` codes** (`TX-005`, `ERR-004`) -
  `BoltErrorCodes.java` defines exactly 7 codes, all
  `Neo.ClientError.*`/`Neo.DatabaseError.*`. Driver-side transient-error
  retry logic (e.g. `executeRead`/`executeWrite` managed transaction
  functions) can never be triggered by ArcadeDB today.

## Related documents

- [`conformance/spec.yaml`](conformance/spec.yaml) - the full certification
  scenario matrix, including the `expected-fail` entries referenced above.
- [`conformance/README.md`](conformance/README.md) - how the conformance
  spec is consumed by each language's test suite.
- Tracking issue [#4890](https://github.com/ArcadeData/arcadedb/issues/4890) -
  closing these gaps.
- Epic [#4882](https://github.com/ArcadeData/arcadedb/issues/4882) - Bolt
  Driver Compatibility Certification.
```

- [ ] **Step 3: Visually confirm the file renders as expected**

Run:

```bash
cat bolt/SERVER_IDENTITY.md
```

Expected: file prints with the 4 sections (`# Bolt Server Identity`,
`## Advertised identity`, `## Why 5.26.0`, `## Known gaps behind this
claim`, `## Related documents`), no `TBD`/`TODO` markers, no em dash
characters.

- [ ] **Step 4: Confirm no em dash characters were introduced**

Run:

```bash
grep -n $'—' bolt/SERVER_IDENTITY.md
```

Expected: no output (grep exits 1, meaning no matches).

- [ ] **Step 5: Stage and commit**

```bash
git add bolt/SERVER_IDENTITY.md
git commit -m "$(cat <<'EOF'
docs(#4884): document advertised Bolt server identity and known gaps

Documents the hardcoded Neo4j/5.26.0 identity strings, why 5.26.0 (LTS
line, re-derived - no prior rationale existed), and the 4 confirmed
compatibility gaps already tracked in the #4883 conformance spec and
issue #4890.
EOF
)"
```

Expected: commit succeeds, pre-commit hooks (trailing-whitespace,
end-of-file-fixer, etc.) pass with no modifications needed.

---

### Task 2: Cross-link from `bolt/conformance/README.md`

**Files:**
- Modify: `bolt/conformance/README.md`

**Interfaces:**
- Consumes: `bolt/SERVER_IDENTITY.md` (Task 1's deliverable) - links to it by relative path `../SERVER_IDENTITY.md`.
- Produces: nothing further downstream - this is the last task in the plan.

- [ ] **Step 1: Read the current top of the file to find the insertion point**

Run:

```bash
sed -n '1,10p' bolt/conformance/README.md
```

Expected output (current content, for reference - insert after this
paragraph):

```
# Bolt Conformance Spec

Shared, language-neutral certification scenarios for ArcadeDB's Bolt
protocol implementation, referenced by issue #4883 (part of epic #4882).
`spec.yaml` in this directory is the single source of truth for what
"certified" means across all five official Neo4j driver ecosystems (Java,
JavaScript, Python, C#, Go).

## Consumption model
```

- [ ] **Step 2: Insert the cross-link paragraph**

Using an editor, insert this new paragraph immediately after the first
paragraph (the one ending `...JavaScript, Python, C#, Go).`) and before the
`## Consumption model` heading, so the file reads:

```markdown
# Bolt Conformance Spec

Shared, language-neutral certification scenarios for ArcadeDB's Bolt
protocol implementation, referenced by issue #4883 (part of epic #4882).
`spec.yaml` in this directory is the single source of truth for what
"certified" means across all five official Neo4j driver ecosystems (Java,
JavaScript, Python, C#, Go).

Every scenario here is only meaningful relative to the server identity
ArcadeDB advertises to drivers - see
[`../SERVER_IDENTITY.md`](../SERVER_IDENTITY.md) for the exact advertised
strings, why `5.26.0` was chosen, and the 4 confirmed gaps behind that
claim (already reflected in this spec's `expected-fail` scenarios below).

## Consumption model
```

- [ ] **Step 3: Verify the edit**

Run:

```bash
sed -n '1,16p' bolt/conformance/README.md
```

Expected: output matches Step 2's full block exactly, including the new
paragraph, with `## Consumption model` still present immediately after.

- [ ] **Step 4: Confirm the link target exists and no em dash was introduced**

Run:

```bash
test -f bolt/SERVER_IDENTITY.md && echo "link target exists"
grep -n $'—' bolt/conformance/README.md
```

Expected: `link target exists` printed; the grep prints no output (exits
1).

- [ ] **Step 5: Stage and commit**

```bash
git add bolt/conformance/README.md
git commit -m "$(cat <<'EOF'
docs(#4884): link conformance README to Bolt server identity doc

Satisfies #4884's acceptance criterion that the identity doc be linked
from the #4883 conformance spec.
EOF
)"
```

Expected: commit succeeds, pre-commit hooks pass.

- [ ] **Step 6: Final review of both commits**

Run:

```bash
git log --oneline -3
git status --short
git diff main --stat
```

Expected: two new commits on top of `main` (`docs(#4884): document
advertised...` and `docs(#4884): link conformance README...`), clean
working tree, `git diff main --stat` shows exactly two files changed:
`bolt/SERVER_IDENTITY.md` (new) and `bolt/conformance/README.md`
(modified).

## Self-Review Notes

- **Spec coverage:** design doc section 2 ("Content, 4 sections") maps
  1:1 to Task 1 Step 2's file content; section "Cross-links" maps to
  Task 2; the design's "Verification" section maps to Task 1 Step 1/3/4
  and Task 2 Step 3/4.
- **No placeholders:** every step above contains literal, final content -
  no "TBD", no "add appropriate X".
- **Type consistency:** N/A (no code, no function signatures across
  tasks) - the only cross-task contract is the file path
  `bolt/SERVER_IDENTITY.md` and its relative link `../SERVER_IDENTITY.md`
  from `bolt/conformance/README.md`, which match exactly between Task 1
  and Task 2.
