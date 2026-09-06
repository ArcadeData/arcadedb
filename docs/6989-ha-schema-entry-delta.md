# 6989 - HA: every SCHEMA_ENTRY ships the full schema JSON instead of a delta

## Problem

Every `SCHEMA_ENTRY` carried a complete serialization of the schema document, no matter how small the
DDL was. Split out of #6982, where a 1209-type schema took ~3 hours to replicate.

The full document is paid for four times: leader `toJSON().toString()`, the Raft entry itself
(wire + every node's log segment), the follower's `new JSONObject(json)` parse, and the follower's
full rewrite of `schema.json`.

## Root cause

`RaftReplicatedDatabase.recordFileChanges` (`:1774`) and the compaction path (`:2612`) both do
`proxied.getSchema().getEmbedded().toJSON().toString()` unconditionally whenever the schema version
moved. `SCHEMA_ENTRY` has no other way to express a schema change: the `schemaJson` slot is a whole
document or nothing.

## Approach

Add an OPTIONAL trailing `SCHEMA_ENTRY` section carrying a **schema delta** instead of the full
document, using the same unframed self-describing-section mechanism #4382, #5443 and #4416 already
use (`SCHEMA_ENTRY` is excluded from the #7138 framed-extension mechanism by construction).

Delta shape (`SchemaDelta`):

```
{ "base": <schemaVersion the delta was computed against>,
  "put":   { "<rootKey>": <value> },              // changed non-object root values
  "merge": { "<rootKey>": { "<child>": <val> } }, // upserted children (one type, one trigger, ...)
  "keys":  { "<rootKey>": [ "<child>", ... ] },   // authoritative child key set
  "rootKeys": [ "<rootKey>", ... ] }              // authoritative root key set
```

`keys`/`rootKeys` make `apply` **structure-authoritative**: the merged document has exactly the
leader's key set regardless of what the receiver started from, so a removal never needs a separate
drop list and a stale receiver cannot end up with a phantom type. They cost one name per type
(~1% of a multi-MB schema), which is what keeps the entry proportional to the change in practice.

## When the leader falls back to the whole document

The leader keeps, per replicated database, the document it last SHIPPED plus the **Raft term** it shipped it in,
and diffs against that. It ships the whole document instead whenever:

- `arcadedb.ha.schemaDelta` is off (the default);
- nothing has been shipped yet in this database instance's lifetime, so there is no base;
- the Raft term moved since the cache was filled - another node has been leader in between, so what the
  followers hold is whatever IT shipped. An unreadable term (a division restarting in place) counts as moved;
- the delta is not at least half the size of the document, which is the bulk-drop / settings-rewrite case;
- the change rides the compaction path, whose sealed-payload budget is measured against the serialized schema.

Two defects the integration test caught that no unit test could have:

1. **The HA setting was read off the wrong configuration.** `HA_SCHEMA_DELTA` is `SCOPE.SERVER`, and the
   per-database `ContextConfiguration` does not carry what a server-scoped setting was set to - so
   `proxied.getConfiguration()` silently answered "default", the gate was always false, and nothing failed.
   It is read off `server.getConfiguration()` now, like `HA_QUORUM_TIMEOUT` and
   `HA_FORWARD_LEADER_WAIT_TIMEOUT_MS` beside it.
2. **The schema version cannot be the staleness guard**, which is why the guard is the Raft term: `saveConfiguration()` defers
   while a transaction is active, so the version bump from a DDL run inside `db.transaction(...)` lands AFTER
   the entry has already shipped. A version-based guard refused every single delta. The version is carried in
   the payload as a diagnostic only.

On the follower the version is a diagnostic for the mirror-image reason: `load()` re-saves the schema whenever
it repaired anything, and every save increments `versionSerial`, so a follower legitimately runs one or more
versions AHEAD of the document the leader shipped. Refusing a delta on a mismatch would refuse the common case,
and there would be nothing to refuse into - a delta entry carries no whole document to fall back to. What makes
that safe is `keys`/`rootKeys`: the merged document has the leader's structure whatever the receiver started
from.

## Known limitation

A whole-document entry re-imposed the leader's rendering of EVERY type on the follower. A delta only replaces
the children the leader saw change, so content drift in an unchanged child now persists until the next
whole-document entry (a leader change, a compaction, a restart) instead of being overwritten on the next DDL.
Structure - which types, triggers, views and functions exist - is still re-imposed on every delta. Genuine
divergence remains the business of the WAL-version-gap detection and `checkDatabase`.

## Mixed-version safety

An older decoder stops after the sealed-slice section and silently ignores anything after it, so a
delta entry would look to it like a `SCHEMA_ENTRY` with an EMPTY `schemaJson` - it would apply
nothing and diverge silently. There is no peer-version negotiation anywhere in `ha-raft` today
(nothing in the Raft envelope, no peer capability exchange), so the leader cannot detect an older
follower on its own.

The emission is therefore gated on `arcadedb.ha.schemaDelta` (`GlobalConfiguration.HA_SCHEMA_DELTA`),
**default `false`**: the leader ships the full document until an operator turns deltas on, which is
the "leader falls back to the full document until every peer is upgraded" arm of the issue's
acceptance criteria. The DECODE side is unconditional, so upgrading is a one-way ratchet: every node
understands deltas before any node is allowed to emit one.

## Deferred

- Automatic peer-capability negotiation, which would let the gate default to on.
- The follower still parses and rewrites the whole document (`update()` + `load()` round-trip
  through `schema.json`) - that is the companion "incremental follower apply" issue, and it is why
  `LocalSchema.update()`'s write cannot simply be coalesced: `load()` reads the file back.
