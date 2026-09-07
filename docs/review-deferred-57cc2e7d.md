# Review notes for PR #7254 (issue #7209), head 57cc2e7d

Cycle 2, reviewer `claude`. One PR issue comment, no formal review and no inline comments. Three
points; one applied, two need no code.

## Applied

1. **The sweep was not guarded against unchecked failures at the registration call site.** Correct and
   worth fixing. `pruneSnapshotMarkersAtStartup()` caught `RuntimeException`, but the call from
   `registerSnapshotMarker` sat inside a `try` that catches only `IOException`, so a `SecurityException`
   out of `File.listFiles()` would have propagated out of `registerSnapshotMarker` - the exact opposite
   of the "never fail a checkpoint over a cosmetic cleanup" guarantee its javadoc makes.

   The guard now lives inside `pruneObsoleteSnapshotMarkers` itself rather than at either call site, so
   the promise is carried by the method that makes it and cannot be lost by a future third caller. The
   startup wrapper keeps its own catch for the lookups above the sweep (`storage.getSnapshotFile` throws
   when Ratis has no state-machine directory). Pinned by `aFilesystemFailureDuringTheSweepIsSwallowed`,
   which drives a `File` whose `listFiles()` raises; verified armed - with the guard removed the test
   errors with `java.lang.SecurityException: denied`.

## No code needed

2. **The two `docs/` files should get a conscious yes/no before merge.** Agreed, and that is exactly
   where it is left: `docs/review-deferred-5a9e9a8f.md` records why the workflow produces them and asks
   the developer to drop `docs/7209-raft-snapshot-marker-pruning.md` and both notes files in one commit
   before merging if tracking docs are no longer wanted here. Nothing in the fix or the tests references
   any of them. This is the developer's call, not one to make from inside the workflow that mandates the
   file.
3. **Nothing to flag on dependencies, license, or the Studio/JS surface.** No action; this branch adds
   no dependency and touches only `ha-raft` Java plus `docs/`.
