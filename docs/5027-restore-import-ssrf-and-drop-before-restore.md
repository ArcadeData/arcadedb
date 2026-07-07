# Issue #5027 - restore/import database: SSRF + drop-before-restore data loss

## Symptom
Two defects in the server-command restore/import path (`PostServerCommandHandler`):

1. **SSRF / LFI (MEDIUM security):** `restore database <name> <url>` and `import database <name> <url>`
   pass a client-supplied URL straight to the reflective `Restore` / `Importer`, with no scheme or
   host allow-listing. `file://` (arbitrary local-file read) and `http://` to private/link-local
   hosts (e.g. `169.254.169.254` cloud metadata) are all accepted, turning a root DB-admin action
   into a network-pivot / local-file-read primitive.
2. **Drop-before-restore data loss (MEDIUM durability):** `restore backup ... overwrite:true` drops
   the target database *before* the restore runs. If the restore then fails (corrupt zip, disk full,
   missing jar), the original database is already gone and no new one exists.

## Root cause
1. No validation of the client URL before it is handed to the integration layer that fetches it.
2. `restoreBackup` called `dropDatabase(targetDatabase)` and only then invoked `performRestore`,
   which restores directly into the final database directory. A failure mid-restore leaves nothing.

## Fix
- New server config `arcadedb.server.restoreImportAllowLocalUrls` (Boolean, default `false`).
  When `false`, the client-supplied URLs used by `restore database` / `import database` are validated:
  only `http`/`https` to public hosts are allowed; `file://` and any private / loopback / link-local /
  site-local / multicast / any-local / unresolvable host is rejected with a `SecurityException` (HTTP 403).
  Setting the flag `true` restores the previous unrestricted behaviour for trusted operators.
  `restore backup` builds its `file://` URL from a server-resolved path and is intentionally NOT
  subject to the guard.
- `performRestore` now restores into a temporary sibling directory first and only swaps it into the
  final database directory after the restore succeeds. The pre-existing target database is dropped
  only after a successful restore (moved from `restoreBackup`), so a failed restore leaves the
  original database intact. On failure the temp directory is cleaned up.

## Tests
- `RestoreImportSecurityDurabilityIT` (new):
  - `restore database` with `file://` rejected (403) by default.
  - `import database` with private-range/link-local `http://` host rejected (403) by default.
  - failed overwrite `restore backup` of an existing DB (corrupted backup zip) preserves the original
    database and its data.
- `PostServerCommandHandlerIT`: added an `onServerConfiguration` override enabling the allow flag so
  the existing legitimate `restore database ... file://<local-backup>` happy-path test still exercises
  the opt-in path (no existing test method changed).

## Impact
Client-supplied restore/import URLs are SSRF/LFI-safe by default; overwrite restores are crash-safe.
No change to the wire format or to `restore backup`'s server-resolved behaviour.

## PR
https://github.com/ArcadeData/arcadedb/pull/5090

## Review cycles
- **Cycle 1 (head `ab179894`)** - `gemini-code-assist` + `claude` reviewed. Applied:
  - Reserved-prefix (`.restore-tmp-*`) temp dir so a crash-orphaned dir is skipped by
    `loadDatabases()` at startup (was: would fail server startup trying to open it).
  - IPv6 ULA `fc00::/7` and IPv4 CGNAT `100.64.0.0/10` added to the blocked ranges in both the
    server guard and the shared `ImportSecurityValidator`; CGNAT regression test added.
  - Guaranteed temp-dir cleanup on swap failure.
  - (partially reverted in cycle 2) ran the drop under `getDatabasesLock()`.
- **Cycle 2 (head `95470f26`)** - `claude` re-reviewed (Gemini did not re-review the new head within
  the cycle budget). Applied:
  - **HA lock-ordering fix:** the HA-capable `dropDatabase()` (which round-trips through Raft, whose
    apply thread also takes `databasesLock`) now runs OUTSIDE `databasesLock`; only the local
    `deleteRecursively` + `Files.move` run under the lock, avoiding a deadlock / data-loss window.
  - `REPLACE_EXISTING` on the non-atomic `Files.move` fallback.
  - Cross-reference comments linking the duplicated blocked-range logic in the two modules.
  - Removed the committed `review-deferred-*.md` scratch file (repo convention keeps this rationale
    in the tracking doc instead).
- **Cycle 3 (head `4fe991cd`)** - `claude` re-reviewed (Gemini again did not re-review the head).
  Applied:
  - Consolidated the duplicated blocked-range logic into a shared `SsrfProtectionUtils` in the
    `engine` module; both the server guard and `ImportSecurityValidator` now delegate to it (removes
    the drift hazard flagged across all three reviews).
  - Startup sweep: `loadDatabases()` now reclaims crash-orphaned `.restore-tmp-*` directories instead
    of letting them accumulate; new `ReservedInternalDatabaseTest` case covers it.
  - The swap no longer deletes the temp directory on a swap-phase failure (it may be the only
    surviving copy); the error now reports its path for recovery, and the startup sweep reclaims a
    genuinely-orphaned one. `REPLACE_EXISTING` added to the non-atomic move fallback.
  - Softened the `restoreImportAllowLocalUrls` config description so operators do not over-trust the
    HTTP(S) check (redirect/DNS-rebinding caveat spelled out).

## Deferred (rationale)
- **Full consolidation with `ImportSecurityValidator` / unify on `importBlockLocalNetworks`.**
  `arcadedb-integration` is a *test-scope* dependency of `server` (the handler loads `Restore` /
  `Importer` reflectively via `Class.forName`), so importing the validator into handler main code
  would break the modular boundary; and this issue requires `file://` blocked *by default*, which
  differs from the validator's default posture. Partial win: the validator's ULA/CGNAT gap was fixed.
- **HA integration test for `restore backup ... overwrite:true`.** `RestoreImportSecurityDurabilityIT`
  is single-node; an HA test for the overwrite happy/failure paths is a larger follow-up.
- **HTTP redirect / DNS-rebinding SSRF bypass.** Lives in the integration fetch layer
  (`FullRestoreFormat` / `HttpURLConnection`) and affects the importer too; broader hardening.
- **IPv6 literal-host false-block check and a positive public-host allow test.** Low priority; the
  positive path is hard to assert without network access.

## Final state
timeout (cycle 2): Gemini did not re-review head `95470f26` within the 15-minute budget; Claude's
cycle-2 feedback was addressed. PR left open for the developer to merge.
