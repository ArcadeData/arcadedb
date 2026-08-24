# Issue #6560: Redis wire follow-ups from #6493

Three independent, pre-existing follow-ups found while reviewing/fixing #6493. None of these are caused by
#6493.

## 1. RESP error replies duplicate/mask the error kind

`RedisNetworkExecutor.respErrorPrefix()` unconditionally derived the RESP error kind (the token right after
`-`) from `ErrorCategory.of(error)`, which only recognizes a handful of real engine exception types. Several
call sites (`AUTH`, `HELLO`, `getAuthorizedDatabase`) instead baked their own kind word (`WRONGPASS`,
`NOAUTH`, `NOPROTO`, `NOPERM`) into the *message* of a plain `RedisException` - a type `ErrorCategory` has no
special case for, so it always fell through to the generic `ERR` default. The wire reply came out as e.g.
`-ERR WRONGPASS ...` instead of `-WRONGPASS ...`: the kind a client can actually branch on was always `ERR`,
never the specific one, even though the message text still happened to mention the real kind further along -
which is exactly why the existing `.contains("WRONGPASS")`-style assertions never caught it.

**Fix:** `RedisException` gained a `withKind(kind, message)` factory that carries an explicit RESP kind
separate from the message. `respErrorPrefix()` now checks for it first, before falling back to
`ErrorCategory`. Every call site that used to embed a kind word in its message now uses `withKind` instead
(and the two sites that only repeated the already-default `ERR` had it dropped from the message entirely).

**Tests:**
- `RedisErrorClassificationTest` (unit): explicit-kind precedence over the `ErrorCategory` default, and that a
  plain `RedisException` without an explicit kind is unaffected.
- `RedisAuthenticationTest` (integration, real server + Jedis client): the wire reply for wrong credentials
  and for `NOPERM` starts with the real kind and NOT with `ERR`.

## 2. Redis-wire `SET k v NX` is not a real distributed lock across an HA/Raft cluster

`RaftReplicatedDatabase`'s four global-variable accessors (`getGlobalVariable`, `setGlobalVariable`,
`setGlobalVariableIfAbsent`, `setGlobalVariableIfPresent`) delegate straight to the local node with no Raft
consensus/replication involved - unlike every other mutating method on that class. Global variables (and
therefore Redis-wire `SET`/`GET`, including `SET k v NX`) are purely per-node state today, so `SET lock 1 NX`
issued against two different nodes of an HA cluster (or the same logical key resolved differently after a
failover) can each succeed independently.

A full fix means replicating global variables through Raft - a materially bigger effort and its own design
discussion, out of scope here. Per the issue's own suggested action, this item is a **documentation-only**
fix: `DatabaseInternal`'s four global-variable methods and `RaftReplicatedDatabase`'s four overrides now
carry an explicit Javadoc caveat that they are node-local, not cluster-wide, and must not be relied on as a
real distributed lock in an HA deployment. No behavior changed, so no new automated test accompanies this
item - the existing behavior (and its existing test coverage) is unchanged; only what is documented about it.

## 3. `RedisQueryLanguageTest` hardcodes port 2480

`executeCommand`/`executeQuery` built their target URL as `"http://127.0.0.1:248" + serverIndex + ..."`,
assuming the server bound the default port 2480 (or 2481 for a second server). `SERVER_HTTP_INCOMING_PORT` is
actually a range (2480-2489 by default) and binds the first free port in it, so with 2480 already held by
anything else - another local ArcadeDB instance, an IDE debug session for a different project - this test's
own server listens elsewhere and every test in the class fails with a confusing `403 Too many failed
authentication attempts` / `User/Password not valid`, which reads as an auth bug rather than a port
collision. Same class of bug as #6426, fixed the same way for `ConsoleAsyncInsertTest` in #6437.

**Fix:** both helpers now ask the server for the port it actually bound
(`getServer(serverIndex).getHttpServer().getPort()`) instead of assuming the default.

**Verification:** the whole class (19 tests) was run in this environment with port 2480 genuinely held by an
unrelated process (`lsof -nP -iTCP:2480 -sTCP:LISTEN` showed a live listener) - all 19 passed, which the
pre-fix hardcoded URL could not have done.

## Test results (this environment, isolated `-Dmaven.repo.local`)

- `RedisErrorClassificationTest`: 7/7 passed (later 7/7 unchanged)
- `RedisAuthenticationTest`: 9/9 passed initially, 11/11 after cycle 1's review addition
- `RedisQueryLanguageTest`: 19/19 passed, with port 2480 occupied by an unrelated process
- Full non-IT `redisw` unit-test set (`RedisAuthenticationTest`, `RedisErrorClassificationTest`,
  `RedisPortConfigurationTest`, `RedisProtocolLimitsTest`, `RedisQueryLanguageTest`,
  `RedisRespCorrectnessTest`, `RedisWTest`): 76/76 passed, no regressions
- `ha-raft` unit tests touching `RaftReplicatedDatabase` (`RaftReplicatedDatabaseTest`,
  `RaftReplicatedDatabaseLeaderWaitTest`): passed, unaffected by the Javadoc-only change

## PR

https://github.com/ArcadeData/arcadedb/pull/6691

## Review cycles

- **Cycle 1** - head `7080e167`: initial PR. `claude` review: solid overall; flagged a coverage gap - only
  `WRONGPASS`/`NOPERM` were asserted at the wire level (via `RedisAuthenticationTest`), not `NOPROTO`/plain
  `NOAUTH` (the `HELLO` paths). Addressed in `057a321d`: added `helloWithoutAuthErrorKindIsNotMaskedByErr`
  and `helloWithBadProtocolVersionErrorKindIsNotMaskedByErr` to `RedisAuthenticationTest` (11/11 passed).
- **Cycle 2** - head `057a321d`: `claude` review: approved overall; nitpicked that the shared "these four
  accessors" Javadoc comment was physically placed above only `getGlobalVariable()`, so it would not surface
  in generated docs/IDE hover for the other three overrides (a Javadoc comment attaches to the single
  declaration it precedes). Also noted `getGlobalVariables()` (plural) was outside the "four accessors"
  comment's scope. Addressed in `71b99af8`: gave each of the five node-local accessors its own short Javadoc
  pointing back to the caveat on `DatabaseInternal`.
- **Cycle 3** - head `71b99af8`: `claude` review: "Approving from a code-review standpoint"; one minor
  non-blocking nit that `DatabaseInternal.getGlobalVariables()` itself (the interface method, not just the
  Raft override) still lacked its own "not replicated" caveat. Addressed in `d1129bac`: added the caveat to
  the interface method's own Javadoc for full symmetry with the other three.
- **Cycle 4** - head `d1129bac`: `claude` review: clean; the only note was an explicitly non-blocking
  "optional polish" observation that `respErrorPrefix()`'s explicit-kind check looks only at the outer
  exception, not the full cause chain the way `ErrorCategory.of()` does - a latent gotcha only if a future
  call site wraps a `withKind()` exception in another type before it reaches `respErrorPrefix()`, which none
  do today. Categorized as a nitpick (bot's own words: "not worth blocking on"), not applied. **Clean
  approval** - working tree empty, no actionable items remaining.

## Deferred items

None. The one item not applied (cycle 4's cause-chain observation) was explicitly framed by the reviewer
itself as optional polish rather than a defect, so it was categorized as a nitpick and skipped rather than
deferred to a notes file.

## Final state

`clean-approval` after 4 review cycles (the maximum configured). Merge remains the developer's
responsibility.
