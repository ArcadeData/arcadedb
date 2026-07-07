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
