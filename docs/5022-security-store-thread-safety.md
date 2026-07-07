# Issue #5022 - User/group security store: thread-unsafe users map + non-atomic persistence

## Symptom
- `ServerSecurity.users` was a plain `HashMap` read unsynchronized on every request and mutated by
  three threads (HTTP handlers, config-reload timer, Raft apply). Reads landing in the
  `clear()`->repopulate window returned `null` -> spurious `User/Password not valid`; concurrent
  structural mutation could corrupt the map or lose entries.
- `SecurityUserFileRepository.save()` / `SecurityGroupFileRepository.save()` truncated and rewrote
  the live file in place (plain `FileWriter`, no temp file, no fsync, no rename). A crash mid-write
  left a truncated/empty file, and on restart the parse failure fell back to `createDefault()` -
  losing all users/hashes/grants (root reset) or resetting group policy to the permissive default.
- `ServerSecurity.saveUsers()` swallowed `IOException`, so an HTTP `create user` could return 200
  while the user was never persisted.
- Concurrent writers (synchronized local mutations vs. non-synchronized Raft apply) could interleave
  `FileWriter` output and corrupt the JSONL.

## Root cause
Shared mutable state without a publish-safe data structure, plus non-atomic single-file rewrites.

## Fix
- `ServerSecurity.users` is now a `volatile Map` holding a `ConcurrentHashMap`. `loadUsers()` and
  `applyReplicatedUsers()` build a fresh map and publish it in one atomic reference swap instead of
  `clear()`+repopulate, so readers never observe an empty/torn window.
- `SecurityUserFileRepository.save()` and `SecurityGroupFileRepository.save()` write to a sibling
  temp file, `FileChannel.force(true)`, then `Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)` with a
  plain-move fallback. Both serialize writers via an internal lock. The users file is written with
  POSIX owner-only (600) permissions, matching `ApiTokenConfiguration`.
- `ServerSecurity.saveUsers()` now propagates a failed save as a `ServerSecurityException` so the
  HTTP caller sees an error instead of a false 200.
- `SecurityGroupFileRepository.getGroups()` lazy-init uses double-checked locking on the volatile
  field.

## Tests
- `SecurityUserFileRepositoryTest` - round-trip, no leftover temp files, POSIX 600 perms, concurrent
  saves always yield a complete parseable file.
- `SecurityGroupFileRepositoryTest` - atomic round-trip + concurrent saves stay valid.
- `ServerSecurityUsersConcurrencyTest` - authenticate() never spuriously fails while
  applyReplicatedUsers() rebuilds the map; failed save propagates to the caller.

## Impact
Server security module only. No API signature changes except `saveUsers()` now throwing a runtime
exception on persistence failure (previously swallowed).
