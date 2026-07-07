# #5100 - HA offline bootstrap: bootstrap baseline lost on restart

## Symptom

After a cluster completes first-formation bootstrap, restarting a peer with persistent Raft storage
leaves `ArcadeStateMachine.getBootstrapBaseline(db)` returning `null`: the committed, durable baseline
is invisible after restart. Reproduced by `RaftBootstrapDoesNotEngageOnRestartIT`.

## Root cause

`applyBootstrapFingerprintEntry` records the baseline only into the in-memory `bootstrapBaselines`
map, and `getBootstrapBaseline` reads that map. On restart with persistent storage:

- the `BOOTSTRAP_FINGERPRINT_ENTRY` sits below the Ratis snapshot index, so Ratis restores from the
  snapshot instead of replaying the entry, and `applyBootstrapFingerprintEntry` is never called again;
- the map is not part of the state-machine snapshot, so it comes back empty.

## Fix

Persist the baselines next to the existing applied-index bookkeeping and reload them lazily, mirroring
the `.raft/applied-index` machinery:

- new `.raft/bootstrap-baselines` JSON file keyed by database name (`{fingerprint, lastTxId}`);
- `applyBootstrapFingerprintEntry` loads-before-mutate then persists after recording;
- `getBootstrapBaseline` lazily loads the file on first access (session-applied entries win via
  `putIfAbsent`);
- `applyDropDatabaseEntry` evicts the dropped database's baseline and rewrites the file, keeping it
  honest (mirrors the per-database applied-index eviction);
- writes go through a temp file + atomic rename; a corrupt/missing file degrades to "no baselines".

Written only when a baseline is recorded/evicted (rare), never on the hot apply path.

## Tests

- `ArcadeStateMachineBootstrapBaselinePersistenceTest` (new, fast unit): baseline survives a simulated
  restart, no-file returns null, on-disk JSON shape, single-database rewrite preserves others.
- `RaftBootstrapDoesNotEngageOnRestartIT` (existing IT reproduction).

## Test results

- New unit test: 4/4 pass.
- `ArcadeStateMachine*Test` + `BootstrapElection*Test`: 72 pass.
- `RaftBootstrapDoesNotEngageOnRestartIT`: passes (failed before the fix).
- `RaftBootstrapFromLocalDatabaseIT`, `RaftBootstrapLateNewerJoinerIT`, `RaftDropDatabase3NodesIT`: pass.
- `RaftBootstrapPicksHighestLastTxIdIT` fails on the base commit too (pre-existing timing flake, not a
  regression of this change).

## Impact

Scoped to `ha-raft` bootstrap persistence. No change to the hot apply path or wire format. The persisted
baseline makes the offline-bootstrap decision durable across restarts, closing the last of the three
first-formation baseline bugs (bug 1 = #5098, bug 2 = #5099).
