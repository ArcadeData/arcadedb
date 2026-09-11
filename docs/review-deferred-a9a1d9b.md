# Review notes - PR #7442, head `a9a1d9b`

Cycle 1, `claude` review. Nothing in it blocked merging; the disposition of each point is below.

## Applied

- **Javadoc link used as a mid-sentence verb.** `{@link #databaseNameIsTaken is taken}` at
  `ServerControlPlane.java:1216` and `:1245` used the link *label* to carry the sentence's verb, a
  convention that appears nowhere else in the class. Reworded to "A target name that is already taken -
  see `{@link #databaseNameIsTaken}` - ..." and "... already taken, as `{@link #databaseNameIsTaken}`
  defines it", so the link is a reference and the sentence reads on its own.
- **Tracking doc skipped section 4.** It numbered 1, 2, 3, then 5. Section 4 of
  `completeness-checklist.md` is "test per entry point, not per issue" and it had been folded into the
  coverage table instead of written out. Added as its own section, listing which test drives which
  fixed row.

## Skipped, with rationale

- **`Issue7395GrpcRestoreTargetExistsIT` hardcodes `GRPC_PORT = 50051`.** Not changed, and the reviewer
  says so too ("matching the existing pattern across most other `grpcw` IT classes in this package, so
  it is not a new issue"). Verified rather than assumed:

  ```
  $ grep -rln "GRPC_PORT *= *50051" grpcw/src/test/java | wc -l
  43
  ```

  Forty-three IT classes in this package share that constant, and the module's ITs run one JVM per class
  in sequence. Picking a different port for this one class alone would not make the package
  concurrency-safe - it would only make this class the odd one out - and moving all seventeen to a
  free-port helper is a change to the `grpcw` test fixture, not to this fix. The `server`-module half of
  this PR does derive its port from `getServer(0).getHttpServer().getPort()`, which is the pattern that
  base class actually supports.
