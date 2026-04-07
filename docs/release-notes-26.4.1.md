# ArcadeDB 26.4.1 - Release Notes

## Breaking Changes

### HA Quorum modes reduced to `majority` and `all`

The HA quorum configuration (`arcadedb.ha.quorum`) now only supports two values:

- `majority` (default) - requires a majority of nodes to acknowledge writes
- `all` - requires all nodes to acknowledge writes

The following values are **no longer supported** and will cause a startup error:

- `none`
- `one`
- `two`
- `three`

These granular quorum modes were part of the old custom HA protocol. Apache Ratis, which now powers ArcadeDB's High Availability, supports `majority` (standard Raft quorum) and `all` (all-committed watch). If you were using any of the removed values, update your configuration before upgrading.
