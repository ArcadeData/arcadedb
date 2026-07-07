# Review notes for PR #5090 - head ab179894

Feedback from `claude[bot]` and `gemini-code-assist[bot]` on head `ab179894`. Items applied as new
commits vs. deferred (with rationale), following superpowers:receiving-code-review (verify first).

## Applied
- **Orphaned temp dir breaks startup (claude #2, CONFIRMED).** The temp restore dir was a non-reserved
  child of the database directory, so `ArcadeDBServer.loadDatabases()` would try to open the orphan
  as a user database after a crash. Fixed: temp dir is now named with the reserved-database prefix
  (`.restore-tmp-<db>-<nanos>`), which `isReservedDatabaseName()` already skips at startup.
- **IPv6 ULA fc00::/7 + IPv4 CGNAT 100.64/10 gap (gemini high, claude #5).** Added explicit raw-byte
  checks to the server-side `isBlockedHost` AND to the shared `ImportSecurityValidator.isBlockedAddress`
  (claude noted the same gap exists there). New regression test covers the CGNAT literal.
- **Temp dir leak on swap failure (gemini medium).** `swapRestoredDatabase` now wraps the whole
  drop+move body in try/catch and deletes the temp dir on any exception.
- **Swap not serialized on the registry lock (claude #3).** The drop + on-disk move now runs inside
  `synchronized (server.getDatabasesLock())`, matching the snapshot-installer pattern (issue #4832).

## Deferred (with rationale)
- **Consolidate with `ImportSecurityValidator` / unify on `importBlockLocalNetworks` (claude #1).**
  Two blockers make a full merge inappropriate here:
  1. `arcadedb-integration` is a **test-scope** dependency of the `server` module (the handler loads
     `Restore`/`Importer` reflectively via `Class.forName` precisely because integration is not on the
     server compile classpath). Importing `ImportSecurityValidator` into handler main code would force
     a compile-scope dependency and break that deliberate modular boundary.
  2. This issue's acceptance criteria require `file://` to be **rejected by default**, whereas the
     existing validator permits `file://` by default (only restricting when
     `importAllowedLocalPaths` is set). Unifying onto `importBlockLocalNetworks` would silently change
     the required default posture.
  Partial win applied: the shared validator's address-range gap (ULA/CGNAT) was fixed too. A full
  cross-module consolidation is a maintainer design decision, left for follow-up.
- **Narrow drop-before-move window (claude #4).** After dropping the old target, a failing
  `Files.move` would leave neither DB. The move is a same-filesystem atomic rename immediately after a
  successful restore, so failure is very unlikely; closing it fully needs close-vs-drop plus an
  HA-aware rename-aside, which is out of proportion for this fix. Temp cleanup on failure is in place;
  the primary bug (restore failure *before* the swap) is fully fixed.
- **HTTP redirect / DNS-rebinding SSRF bypass (claude #5).** These live in the integration fetch layer
  (`FullRestoreFormat`/`HttpURLConnection`), not in the changed surface, and also affect the existing
  importer. Disabling redirects / per-hop re-validation is a broader hardening tracked separately.
- **`URI.create` vs `new URL()` parsing (claude minor).** The `URI` path fails safe: a host `URI`
  cannot parse (e.g. containing `_`) yields a null host, which is treated as blocked. Kept as-is.
