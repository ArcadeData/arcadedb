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

## Deferred items
- None.

## Final state
- `timeout`: all actionable review feedback (gemini's two items) was applied in cycle 1 and verified;
  the gating `claude` bot never responded within the per-cycle 15-minute window on either commit.
  PR left open for the developer. Merge remains the developer's responsibility.
