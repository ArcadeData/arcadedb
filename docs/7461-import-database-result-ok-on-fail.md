# #7461 - IMPORT DATABASE reports result=OK for the one failure it means to report as FAIL

## The defect

`ImportDatabaseStatement.executeSimple` wrote `result=FAIL` inside the `InvocationTargetException`
handler and then, two lines below the enclosing `try`, assigned `result=OK` unconditionally. The
`FAIL` branch was therefore unobservable by any caller: an `IMPORT DATABASE` whose importer raised an
`IllegalArgumentException` answered `{"result":"OK"}` with no rows and no statistics.

## Root cause and history

The `FAIL` branch arrived with `probeOnly` (#1401, Dec 2023). `probeOnly` asks "can this source be
parsed?" without importing anything, so `Importer.load()` deliberately converts a failed probe into an
`IllegalArgumentException`:

```java
} catch (final Exception e) {
  if (settings.probeOnly)
    throw new IllegalArgumentException(e);
  else
    throw new ImportException("Error on parsing source '" + source + "'", e);
}
```

so the statement could report it as a **row** rather than as an error. The same commit added
`result.setProperty("result", "FAIL")` above an `result.setProperty("result", "OK")` that was already
unconditional, and never moved the latter. It has been dead since the day it was written; the `#7443`
maintenance-slot change (#7460) re-indented the block but did not touch the behaviour.

## The invariant

> `IMPORT DATABASE` never answers `result=OK` for an import that did not run.

## Completeness

### Sweep commands and output

Producers of the in-band `FAIL` - the reflective invokes inside the guarded block:

```
$ grep -n 'invoke(importer' engine/src/main/java/com/arcadedb/query/sql/parser/ImportDatabaseStatement.java
103:        clazz.getMethod("setAllowLocalUrls", boolean.class).invoke(importer, !blockLocalNetworks);
113:        clazz.getMethod("setSettings", Map.class).invoke(importer, settingsToString);
114:        final Map<String, Object> statistics = (Map<String, Object>) clazz.getMethod("load").invoke(importer);
```

`setAllowLocalUrls` is a field assignment and throws nothing. That leaves **two** producers, not the
one the issue names - `setSettings` reaches `ImporterSettings.parseParameter`, which raises the same
exact type:

```
$ grep -rn 'IllegalArgumentException' --include='*.java' integration/src/main/java/com/arcadedb/integration/importer/ | head
.../Importer.java:98:        throw new IllegalArgumentException(e);                       <- probeOnly
.../ImporterSettings.java:191:  throw new IllegalArgumentException("Invalid value '" + value + "' for -onRowError. ...")
.../SourceDiscovery.java:723,734,793 ... (all inside Importer.load()'s try, so wrapped in ImportException)
```

```
$ grep -n 'setSettings' integration/src/main/java/com/arcadedb/integration/importer/*.java
AbstractImporter.java:61:  public void setSettings(final Map<String, String> parameters) {
      -> settings.parseParameter(entry.getKey(), entry.getValue())   // outside load()'s try
```

Siblings - the same statement shape elsewhere:

```
$ grep -rn 'setProperty("result"' --include='*.java' . | grep -v /target/
BackupDatabaseStatement.java:165:  result.setProperty("result", "OK");    // inside the success branch, before return
ImportDatabaseStatement.java:128:  result.setProperty("result", "FAIL");  <- THE BUG
ImportDatabaseStatement.java:136:  result.setProperty("result", "OK");    <- overwrites it
ExportDatabaseStatement.java:113: result.setProperty("result", "OK");     // its catch always throws
SleepStatement.java:49/53:        OK / "failure" in mutually exclusive branches
AlterTypeStatement.java:334:      result.setProperty("result", "OK");     // no catch at all
```

```
$ grep -rn '"FAIL"' --include='*.java' . | grep -v /target/
engine/src/main/java/com/arcadedb/query/sql/parser/ImportDatabaseStatement.java:128
```

`ImportDatabaseStatement` is the only statement in the tree with an in-band `FAIL`, so there is no
sibling carrying the same defect.

Readers of the row:

```
$ grep -rn 'getProperty("result")' --include='*.java' engine/src server/src console/src integration/src | grep -v /target/ | grep -v src/test
(no hits)
```

No production code gates on the value - the console, Studio and the HTTP/gRPC layers render the row as
it comes - so making `FAIL` survive cannot break a programmatic caller.

### Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `IMPORT DATABASE <url> WITH probeOnly = true`, unparseable source -> `Importer.load()` raises `IllegalArgumentException` | yes - `FAIL` survives, `reason` added | yes - `aFailedProbeReportsFailWithAReason` |
| `IMPORT DATABASE <url> WITH probeOnly = true`, good source -> `load()` returns `null` | yes - still `OK` | yes - `aSuccessfulProbeStillReportsOk` |
| `IMPORT DATABASE <url> WITH onRowError = 'bogus'` -> `setSettings` raises exact `IllegalArgumentException` before the import runs | yes - `FAIL` survives, `reason` added | yes - `anUnusableSettingValueReportsFailWithAReason` |
| `IMPORT DATABASE <url>` (no `probeOnly`), unparseable source | argued - `load()` wraps it in `ImportException`, which never reached the `FAIL` branch and still throws `CommandExecutionException` | yes, as a regression guard - `aGenuineImportFailureStillThrows` |
| `IMPORT DATABASE <url> WITH commitEvery = 'abc'` -> `setSettings` raises `NumberFormatException` | argued - the exact class-name match excludes `IllegalArgumentException` subclasses, so this always threw and still does. Widening to `instanceof` would turn these errors into rows, which is why the comparison was deliberately left as-is | yes, as a regression guard - `aNonNumericNumericSettingStillThrows` |
| Importer raises `SecurityException` (SSRF/LFI guard) | argued - the rethrow loop above the branch is unchanged apart from hoisting `e.getTargetException()` into a local; it still short-circuits to HTTP 403 | covered by the existing `ImportDatabaseSecurityIT` (not run here, see Verification) |
| Startup `import:` default-database command (`ArcadeDBServer.loadDefaultDatabases`) | **filed - #7484**. It executes this statement and discards the result set (`// drain not needed`), so a `FAIL` row is as silent after this fix as it was before it | no |
| `BACKUP` / `EXPORT DATABASE` siblings | argued - neither has an in-band `FAIL`; `EXPORT`'s catch always throws and `BACKUP` sets `OK` inside its success branch (greps above) | n/a |
| Server `import database <name> <url>` command (HTTP/gRPC) | argued - routes through `ServerControlPlane.importDatabase`, which drives `Importer` directly and never reaches this statement | n/a |

## The decision

The issue offers two designs. This PR takes the first - keep the in-band shape, make `FAIL` survive,
add a `reason` - because the second would break what the branch exists for. `probeOnly`'s whole point
is to answer "can this be parsed?" **without** raising; turning a failed probe into a
`CommandExecutionException` would leave no way to ask the question.

Three parts:

1. `result=OK` moves into the success path, immediately after the statistics are folded in, which is
   the shape `BackupDatabaseStatement` already has. There is no flag and no second assignment.
2. `reason` carries the failure message alongside `FAIL`. `FAIL` on its own tells a caller nothing it
   can act on, and the server log was the only place the message existed. `failureReason()` reports the
   failure's **own** message and deliberately not the cause's: `Importer.load()` builds its probe
   failure as `new IllegalArgumentException(cause)`, so that message is already `cause.toString()` -
   type included, which is the entire answer when the cause is a `FileNotFoundException` whose own
   message is nothing but the path (`SourceDiscovery.openLocalStream` throws
   `new FileNotFoundException(filePath)`). Probing a missing file therefore yields
   `reason = "java.io.FileNotFoundException: /path/nodes.csv"`, and a refused setting yields its own
   sentence, `"Invalid value 'bogus' for -onRowError. Supported values are 'abort' and 'skip'"`.
3. `e.getTargetException()` is hoisted into `target` and null-guarded. `e.getCause()` was dereferenced
   without a check, and the two accessors were being mixed for the same object.

The exact class-name comparison is deliberately **kept**, not widened to `instanceof` - see the
`commitEvery` row above.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent deliberately kept ignorant of the author's reasoning.
**No `Task` tool was available in this session**, so the pass was run by the author against the diff
instead - weaker by construction, and recorded as such. Four findings, all real, all fixed before the
PR opened:

1. **`failureReason()` discarded the informative half of the message.** The first version preferred the
   cause's message over the failure's. For the probe that is exactly backwards: the cause is a
   `FileNotFoundException` whose message is a bare path, while the `IllegalArgumentException` wrapper's
   own message already carries `type: path`. The helper now reads `failure.getMessage()` and is three
   lines shorter. Verified by running the test with a canary assertion and reading the actual string.
2. **`aSuccessfulProbeStillReportsOk` passed for the wrong reason.** A build that ignored `probeOnly`
   entirely and imported the file would also answer `OK`, so the test proved nothing about the probe.
   It now asserts `db.getSchema().existsType("Document")` is false - an actual import of the fixture is
   what creates that type, as the success test one method below relies on.
3. **Three tests asserted only the exception TYPE, which several unrelated guards also produce.**
   `aGenuineImportFailureStillThrows` now pins `hasRootCauseInstanceOf(FileNotFoundException)` on top of
   the message, `aNonNumericNumericSettingStillThrows` pins `NumberFormatException` (the subclass the
   exact class-name match must not absorb), and `aFailedProbeReportsFailWithAReason` pins both halves of
   the reason string. Without those, each could pass against a statement that refused the command for a
   completely different reason.
4. **`anUnusableSettingValueReportsFailWithAReason` did not prove the import had not run.** It now
   asserts the schema is untouched as well.

Two further checks that found nothing:

- `ImporterContext.toMap()` emits `parsedRecords`, `errors`, `warnings`, `createdDocuments`,
  `createdVertices`, `createdEdges`, `createdTimeSeriesSamples` - neither `result` nor `reason`, so
  moving the `OK` assignment below `setPropertiesFromMap(statistics)` collides with nothing and the new
  `reason` key cannot shadow a statistic.
- Studio's import button sends the server command `import database <name> <url>`
  (`studio-database.js:672`), not this statement, so no UI special-cases the row. Console and Studio
  render a result row generically.

## Verification

```
$ mvn -o -pl integration -am test -Dtest=Issue7461ImportDatabaseFailResultTest ...   # BEFORE the fix
Tests run: 6, Failures: 2   <- the two FAIL rows; the four guard tests were already green,
                               which is the evidence for the three "argued" rows
$ ... same command AFTER the fix
Tests run: 6, Failures: 0
```

- `integration` module, full suite minus the `benchmark`/`slow`/`vector` lanes: **372 tests, 0
  failures, 9 skipped** (re-run after the adversarial-pass changes).
- `engine` module, full suite minus those lanes: 14870 tests, **1 failure** -
  `MultiColumnAggregationResultTest.emptySumAndCountStayZeroNotNaN` (`expected: 0.0 but was: NaN`).
  **Pre-existing and unrelated**: re-run with `ImportDatabaseStatement.java` restored to its `HEAD`
  content, it fails identically. Time-series aggregation, nothing to do with this change.
- Server ITs that exercise the import path (`ImportDatabaseSecurityIT`, `Issue7443SqlMaintenanceSlotIT`)
  were **not** run: port 2480 was already held by another process on this machine
  (`lsof -nP -iTCP:2480 -sTCP:LISTEN` -> a live `java` listener), and a server IT run against an
  occupied port produces authentication errors rather than meaningful results. The `SecurityException`
  rethrow they cover is unchanged in semantics - the loop reads the same object through a local.

## Reachability

`ImportDatabaseStatement` is instantiated by the SQL parser for every `IMPORT DATABASE` statement; the
new tests reach the changed lines through `database.command("sql", ...)`, i.e. the same live path a
client uses. No feature flag gates it.

## Residual risk

- A startup `import:` command still swallows the row - **#7484**. The fix gives that path an answer
  worth reading; reading it is a server-module change and is out of scope here.
- `IMPORT DATABASE ... WITH commitEvery = 'abc'` throws while
  `IMPORT DATABASE ... WITH onRowError = 'bogus'` returns a `FAIL` row. That asymmetry is inherited,
  documented in the code comment, and pinned by two tests. Unifying it means deciding whether a
  refused setting is an error or a row for *every* setting, which is a contract change this PR
  deliberately does not make.
- `reason` is a new property on the row. It is absent on `OK`, so nothing that reads the existing
  shape changes.

## Finding ledger

- [x] 1. `result=FAIL` overwritten by an unconditional `result=OK` - fixed, on both producers of the
      branch, with a `reason` property and 6 tests.
