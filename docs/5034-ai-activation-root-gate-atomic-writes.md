# Issue #5034 - AI module: activation not root-gated + non-atomic, unsynchronized chat/config writes

## Symptom
- `POST /api/v1/ai/activate` was reachable by any authenticated (non-root) user, letting them push a
  subscription key to the gateway and overwrite server-wide `config/ai.json`.
- `ChatStorage.saveChat` used a truncating `FileOutputStream` with no lock, no atomic rename, and it
  swallowed write errors: concurrent saves/reads produced spliced or partial JSON ("chat disappears"),
  and failures were reported as success.
- `AiConfiguration.save`, `MCPConfiguration.save`, and `PostServerCommandHandler.setBackupConfig` all
  rewrite config files in place; a crash mid-write corrupts the previously valid file.

## Root cause
- `AiActivateHandler.execute` never called `checkRootUser(user)` (unlike `MCPConfigHandler`, `PostUserHandler`).
- `ChatStorage.saveChat` did a direct in-place truncating write with no serialization and swallowed exceptions.
- Config writers used `Files.writeString` / `FileOutputStream` directly onto the live file.

## Fix
- `AiActivateHandler.execute`: call `checkRootUser(user)` first; non-root -> 403.
- `ChatStorage.saveChat`: write to `<chat>.json.tmp` then `Files.move(..., ATOMIC_MOVE)` (fallback to a
  non-atomic replacing move if the FS rejects ATOMIC_MOVE); serialize per-`(user, chatId)` writers via a
  lock stripe; propagate write failures as an unchecked exception instead of swallowing them.
- `AiConfiguration.save`, `MCPConfiguration.save`, `PostServerCommandHandler.setBackupConfig`: reuse an
  atomic write helper (temp file + `ATOMIC_MOVE`) so a crash mid-write leaves the prior file intact.

## Tests
- `AiServerTest.activateRejectsNonRootUser` - non-root POST /api/v1/ai/activate returns 403.
- `ChatStorageTest` - atomic-save concurrency: concurrent saves never corrupt the file; a save failure surfaces.
- `AiConfigurationTest` / `MCPConfigurationTest` - config file stays valid, temp file cleaned up.

## Impact
- Closes a MEDIUM privilege-escalation hole and a MEDIUM durability/concurrency defect in the AI module,
  plus hardens the remaining non-atomic config writes.

## PR
- https://github.com/ArcadeData/arcadedb/pull/5057

## Review cycles
- Cycle 1 - head `00d9e5303`: gemini-code-assist COMMENTED with two actionable items on
  `FileUtils.atomicWriteFile` (both verified correct and applied):
  - HIGH: `ATOMIC_MOVE` must be paired with `REPLACE_EXISTING` to overwrite an existing target on
    platforms such as Windows (otherwise `FileAlreadyExistsException`).
  - MEDIUM: resolve the target to an absolute path so the temp file lands on the same file store
    (a relative path gave a null parent -> system temp dir -> non-atomic fallback).
  The `claude` bot did not post a review within the 15-minute window.
- Cycle 2 - head `2ed4fb11f` (after applying the fixes): gemini-code-assist re-posted the same HIGH
  comment, which is now stale/already-resolved (HEAD already passes `ATOMIC_MOVE, REPLACE_EXISTING`);
  no action required. The `claude` bot again did not post within the 15-minute window.
- Cycle 3 - head `ae22c687` (docs update): `claude` LGTM with 6 minor/non-blocking observations;
  gemini's inline comment on this SHA was the stale re-anchored `REPLACE_EXISTING` note (already
  resolved). Applied three: (1) streaming path now logs (not throws) a `saveChat` failure because the
  SSE exchange is already committed - re-throwing would attempt a second response on the committed
  exchange; (2) dropped the redundant `mkdirs()` in `saveChat` (`atomicWriteFile` already creates the
  parent dir); (3) tightened the lock-stripe comment to state the anti-splice guarantee comes from the
  atomic rename, not the lock. All 24 AI/config tests pass. Pushed as `e11e0473`.
- Cycle 4 - head `e11e0473`: `claude` fresh review (LGTM, "none are blockers"). Its one substantive
  item (non-streaming path returns 500 on a persistence failure, discarding a possibly-billed AI
  answer) is a deliberate design decision that directly matches issue #5034's acceptance criterion
  "a save failure surfaces as an error" - not auto-reversed; deferred to the developer. Remaining
  items (parent-dir fsync, dropping the lock stripe, @Tag("slow"), 0600 perms) were skipped with
  rationale (dropping the lock would contradict the issue's "add a per-(user, chatId) lock" criterion;
  the concurrency test measured 0.201 s so is not "slow"). No code change this cycle. `gemini` did not
  post a fresh review on this HEAD within the window (only the stale re-anchored inline comment).

## Deferred items
- `docs/review-deferred-e11e0473.md` (local-only, not committed) - Item 1: non-streaming persistence
  failure returns 500 vs. preserving a billed AI answer. Default recommendation: keep current behavior
  (matches issue acceptance criteria). Also records the skipped optional items with rationale.

## Final state
- `deferred-items`: all clearly-actionable feedback was applied in cycle 3 and verified (24 tests
  green). The one remaining substantive suggestion (cycle 4, item 1) is a conscious product decision
  surfaced to the developer rather than auto-applied, because reversing it would contradict issue
  #5034's explicit acceptance criteria. PR left open. Merge remains the developer's responsibility.
