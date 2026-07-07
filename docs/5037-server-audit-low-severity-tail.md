# Issue #5037 - Server audit low-severity tail

Grab-bag of small, independent hardening items from the 2026-07 `server` module audit.

## Items

### Item 1 (security) - verbose exception details leaked in HTTP error responses
- **Symptom:** Error responses always include the exception class name and a `detail` built from the full
  cause chain, regardless of server mode. An attacker probing endpoints harvests internal class names,
  file paths and engine errors.
- **Root cause:** `AbstractServerHttpHandler.sendErrorResponse` unconditionally serialized `exception`,
  `detail` and `exceptionArgs`. The `SERVER_MODE` gate only lowered log verbosity, not response content.
- **Fix:** New package-private `buildErrorBody(verbose, ...)` helper. In `production` mode it conceals only the
  free-form `detail` cause chain (where file paths and engine internals actually leak) and adds a `requestId`
  correlation id (echoed from the `X-Request-Id` response header). The bounded `exception` class name and the
  structured `exceptionArgs` are emitted in every mode: they are a wire contract consumed by the remote Java
  driver (`RemoteHttpComponent.manageException`) and by HA leader-exception reconstruction
  (`RaftReplicatedDatabase.reconstructLeaderException`) to rebuild typed exceptions, leader-redirect hints and
  duplicate-key details, so stripping them in production would silently break the driver and HA forwarding.
  `development`/`test` mode additionally returns the full `detail` to aid debugging. `buildDetailChain` extracts
  the cause-chain rendering for reuse and uses an identity-based visited set so cyclic cause chains terminate.
- **Tests:** `AbstractServerHttpHandlerErrorBodyTest` asserts production conceals the `detail` cause chain (no
  leaked file path) while preserving `exception`/`exceptionArgs` and the correlation id, that verbose mode still
  exposes the full detail, and that `buildDetailChain` terminates on a deep cyclic cause chain.

### Item 2 (correctness) - `POST /begin` returned 401 for an already-started transaction
- **Symptom:** Re-issuing `/begin` on an existing session returned HTTP 401 (makes clients re-authenticate for
  a state error). A payload without `isolationLevel` was rejected with 400 though the field is optional.
- **Root cause:** `PostBeginHandler` returned `401` and required `isolationLevel`.
- **Fix:** Return `409` (Conflict) for an already-started transaction. Treat `isolationLevel` as optional -
  fall back to a default `begin()` when absent.
- **Tests:** `Issue5037BeginHandlerIT` - second `/begin` on the same session returns 409; `/begin` with an
  empty JSON body (no `isolationLevel`) returns 204.

### Item 3 (correctness) - malformed HA read bookmark header threw 500
- **Symptom:** A non-numeric `X-ArcadeDB-Read-After` (or legacy `X-ArcadeDB-Commit-Index`) threw
  `NumberFormatException` -> HTTP 500 instead of 400.
- **Root cause:** `DatabaseAbstractHandler` called `Long.parseLong` on the raw header with no validation.
- **Fix:** New package-private static `parseReadBookmark(String)` returns `-1` for absent input and throws a
  descriptive `IllegalArgumentException` on malformed input; the handler returns a generic HTTP 400 instead of
  propagating a 500.
- **Tests:** `DatabaseAbstractHandlerBookmarkParseTest` unit test covers null/blank/valid/malformed inputs.

### Item 4 (performance/availability) - forced `System.gc()` in the server monitor
- **Symptom:** When available heap dropped below 10% the monitor called `System.gc()` - a stop-the-world
  collection precisely when the server is already struggling, which can cascade into HA election timeouts.
- **Root cause:** `ServerMonitor.checkHeapRAM` invoked `System.gc()`.
- **Fix:** Removed the forced collection; the low-memory condition is logged as a WARNING and the JVM collector
  is left to manage heap.

### Item 5 (security guidance) - cluster forwarded-user auth rests on the cluster token
- **Symptom:** When `X-ArcadeDB-Cluster-Token` matches, the leader trusts `X-ArcadeDB-Forwarded-User` with no
  password check. Not directly exploitable (constant-time compare, blank tokens rejected), but a weak root
  password plus a reachable replication HTTP port could let an attacker forge a PBKDF2-derived default token.
- **Fix (docs/config):** Expanded the `HA_CLUSTER_TOKEN` configuration guidance to recommend an explicit
  high-entropy token and to warn that the replication HTTP port must not be exposed to untrusted networks.

## Impact
All changes are server-side and backward compatible. Default (`development`) mode error responses are unchanged
except for an additive `requestId` field. Concealment only engages when an operator explicitly sets
`arcadedb.server.mode=production`; the default mode stays verbose.

Production mode conceals only the free-form `detail` cause chain; the `exception` class name and `exceptionArgs`
are preserved so the remote-driver / HA typed-exception contract still holds. The "no leak in production" property
therefore covers `detail` only: `exceptionArgs` is intentionally NOT concealed because the driver and HA need its
structured tokens to reconstruct the typed exception. This is a deliberate, bounded trade-off - for
`DuplicatedKeyException` the args carry the index name, the colliding key (caller-supplied) and the pre-existing
record's RID, so a production error response still discloses that RID to the client. One deliberate trade-off follows
from concealing `detail`: consumers that read the cause-chain message degrade gracefully against a
production-mode server. Specifically the remote Java driver (`RemoteHttpComponent.manageException`) parses the RID
for `RecordNotFoundException` out of `detail`, so it reconstructs the typed exception with a `null` RID; and both
the driver and HA leader-exception reconstruction (`RaftReplicatedDatabase.reconstructLeaderException`) fall back
to a generic message (`"Unknown"` / the top-level `error`) for message-only exceptions. `exceptionArgs`-based
exceptions (`DuplicatedKeyException`, `ServerIsNotTheLeaderException`) are unaffected because their structured args
are preserved. `Issue5037ProductionErrorBodyIT` locks in the production wiring end-to-end.
