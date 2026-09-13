# Deferred review items - PR #7585, head abde77d9

## 1. Run the `server` module before merge (claude review, cycle 1)

> The `server` module wasn't run (port 2480 was occupied). Low risk since remote DDL goes through the same
> `LocalDocumentType` funnel, but worth a green run before merge if the port frees up, since it's the one module this
> PR doesn't have direct evidence for.

**Deferred - blocked by the environment, not by judgement.** Port 2480 is held for the whole session by a long-running
ArcadeDB server process (PID 41389, started 2026-09-11 16:36, `/opt/homebrew/opt/openjdk/bin/java -server ...`) that
this branch did not start and must not kill. Server tests bind fixed ports from 2480 up, and a suite started against
an occupied port fails as `403` / "Too many failed authentication attempts" rather than as a port conflict, so the run
would be noise rather than evidence.

The reviewer's own risk assessment is the one recorded in the PR body: `RemoteDocumentType.createProperty` emits
`create property ...` SQL that the server executes through the same `LocalDocumentType` funnel the 14920-test engine
run covers, so the module adds no entry point of its own. **Action for the developer: run
`mvn -o -pl server -am test` once the port is free, before merging.**

## Items considered and NOT deferred

- *"`TIMESERIES_ROLE_CUSTOM_KEY` is declared mid-class rather than with other constants at the top"* (claude review,
  and the single Codacy CodeStyle notice on `LocalProperty.java:295`). Applied rather than deferred - the field now
  sits directly under the class declaration.
- *"The Javadoc on the three new guard methods is long"* - the reviewer's own conclusion was to leave it ("the WHY
  genuinely is non-obvious"), and the comments are what stop the next reader from removing a guard that looks
  redundant. Left as written.
- *"Worth confirming `LocalSchema.dropType()` was checked"* - it was, and the reviewer verified it independently:
  `dropType()` discards indexes and buckets and removes the type from the `types` map without ever calling
  `dropProperty`, so the new refusal cannot block dropping the type. No change needed.
