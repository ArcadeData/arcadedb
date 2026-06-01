# Deferred review items - cycle 1 (head 75cb6b3)

## Item 1 - @Tag("IntegrationTest") on RaftStorageDirectoryIT

Source: claude[bot] review comment.

> Several peer tests in the same package carry `@Tag("IntegrationTest")` (e.g. `RaftHAServerIT`,
> `RaftReplicationIT`, `RaftHAComprehensiveIT`). `RaftStorageDirectoryIT` starts a full 2-node cluster
> and fits that profile. Note that `RaftBootstrapDoesNotEngageOnRestartIT` (which the PR cites as a
> regression test) also lacks the tag, so this may be intentional - worth double-checking against
> CI filter config.

Rationale for deferral: Claude itself flags this as "may be intentional." The test class
`RaftBootstrapDoesNotEngageOnRestartIT` used as the style reference for `RaftStorageDirectoryIT`
also lacks the tag. Consistent with the existing pattern in the same test group. Whether the
existing tests lacking the tag are a gap or a deliberate choice depends on the CI filter
configuration - a human should decide.

## Item 2 - Startup warning for relative raftStorageDirectory paths

Source: claude[bot] minor suggestion.

> A startup-time warning log when `!Paths.get(raftDir).isAbsolute()` would help operators catch
> misconfigurations before they wonder why raft data landed in the working directory.

Rationale for deferral: The config description already calls out "e.g. /var/lib/arcadedb/raft"
as the intended form. Adding a runtime warning goes beyond what the issue requested and would
change observable startup behavior. Relative paths ARE valid (they work in development/tests
with `"./target/custom-raft-storage"`). This can be addressed as a follow-up if operators
report confusion.
