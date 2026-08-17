# Issue #6189 - Reclaim unreferenced files

https://github.com/ArcadeData/arcadedb/issues/6189

## Root cause / gap

#6143 shipped `UnreferencedFiles.scan()` and wired it into `CHECK DATABASE` as a report-only `unreferencedFiles`
result key. It never deletes anything: an operator who reads the finding still has to stop the node and remove
files by hand. The issue explicitly rejects an *inferred*, always-on reclaim (a follower tracking its own abandoned
instalment's files and dropping them on the next publish) as too risky given the "silent divergence" history of that
code path (#4083, #4743, #5443, #5492), and instead floats an **operator-triggered** reclaim as the safer
alternative: `CHECK DATABASE FIX` reclaiming what `UnreferencedFiles.scan` already proves, on the node the operator
chose, with the finding printed first.

## Design decision (this PR)

Implemented the operator-triggered design, narrowed to the one shape a raw file delete can prove safe:

- New SQL clause `CHECK DATABASE FIX RECLAIM UNREFERENCED FILES`, same pattern as #6090's `DELETE ORPHANS` (its own
  clause, requires `FIX`, refused with a clear error otherwise).
- `UnreferencedFiles.UnreferencedFile` now carries a `Kind` enum (`NO_SCHEMA_COMPONENT`, `UNOWNED_BUCKET`,
  `UNOWNED_INDEX`) alongside its existing human-phrased `reason`, so a caller can branch on the shape instead of
  parsing the sentence.
- The reclaim deletes only `NO_SCHEMA_COMPONENT` findings via `FileManager.dropFile` - the shape an abandoned
  instalment sequence leaves, and the only one where nothing in the schema's registries names the file, so a raw
  delete can never leave a dangling schema reference. `UNOWNED_BUCKET` / `UNOWNED_INDEX` stay report-only: their
  file is only half of what would need to go (the schema component itself is still registered), and the safe way
  to remove those is the existing `DROP BUCKET` / index tooling the finding's reason already names.
- New result key `reclaimedUnreferencedFiles`, seeded empty like `unreferencedFiles`, listing what was actually
  removed. The finding is logged/recorded before any deletion (code-order guarantee), and one file's I/O failure
  is a warning, not an abort of the rest.

## Explicit scope boundary (documented, not silently assumed)

Per the issue's closing line ("whichever way it goes, the choice should be explicit"): this PR does **not** touch
HA command forwarding. `CheckDatabaseStatement` stays non-idempotent, so under HA the statement forwards to the
leader exactly as plain `CHECK DATABASE FIX` already does today - this reclaims what the node actually executing
the check holds (the leader, under HA forwarding; whichever node under embedded/non-HA). Targeting an arbitrary
follower directly (the scenario that actually accumulates most of these files) needs a change to forwarding
semantics, which is a separate, larger decision than this PR's scope and is called out here for a future issue
rather than bundled in silently.

## Files touched

- `engine/src/main/java/com/arcadedb/engine/UnreferencedFiles.java` - `Kind` enum, `NO_SCHEMA_COMPONENT` /
  `UNOWNED_BUCKET` / `UNOWNED_INDEX` classification threaded through `walk()`/`scan()`/`count()`.
- `engine/src/main/java/com/arcadedb/engine/DatabaseChecker.java` - `setReclaimUnreferencedFiles`,
  `reclaimedUnreferencedFiles` result key, `reclaimUnreferencedFiles()` deletion step.
- `engine/src/main/java/com/arcadedb/query/sql/parser/CheckDatabaseStatement.java` - `reclaimUnreferencedFiles`
  field, FIX-required validation, `toString()` round trip.
- `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLLexer.g4` / `SQLParser.g4` - `RECLAIM`, `UNREFERENCED`,
  `FILES` tokens and the new clause, plus the identifier-keyword-reuse list entries.
- `engine/src/main/java/com/arcadedb/query/sql/antlr/SQLASTBuilder.java` - parses the new clause.
- `docs/release-26.9.1.md` - release note appended at end of file.

## Tests

- `engine/src/test/java/com/arcadedb/query/sql/parser/Issue6189CheckDatabaseReclaimUnreferencedFilesParserTest.java`
  - grammar coverage, mirroring `Issue6090CheckDatabaseDeleteOrphansParserTest`.
- `engine/src/test/java/com/arcadedb/engine/Issue6189ReclaimUnreferencedFilesTest.java` - engine-level regression:
  reclaim removes a `NO_SCHEMA_COMPONENT` file, leaves `UNOWNED_BUCKET`/`UNOWNED_INDEX` findings untouched, refuses
  without FIX, is a no-op on a clean database, and one file's failure does not block the others.

## Verification

- `mvn -pl engine -am compile` - clean compile, ANTLR grammar regenerates without error.
- `mvn -pl server -am compile` - downstream consumer of `UnreferencedFiles`/`DatabaseChecker` (`HAReplicationStatsProvider`,
  `HAReplicationMetrics`) still compiles against the changed `UnreferencedFile` record shape.
- `mvn -pl engine -am test -Dtest=Issue6189ReclaimUnreferencedFilesTest,Issue6189CheckDatabaseReclaimUnreferencedFilesParserTest,Issue6143UnreferencedFilesTest,Issue6168MemoizedUnreferencedCountTest,Issue6090CheckDatabaseDeleteOrphansParserTest,Issue6090OrphanEdgeRecordCheckTest`
  - 33/33 passed.
- `mvn -pl engine -am test -Dtest='com.arcadedb.query.sql.parser.**' -DexcludedGroups=benchmark,slow,vector`
  - 362/362 passed (2 skipped, pre-existing).
- `mvn -pl engine -am test -Dtest='com.arcadedb.engine.**' -DexcludedGroups=benchmark,slow,vector`
  - 751/751 passed.
