# #5262 - Tests hand-roll their database lifecycle on repo-relative `./databases/` paths

## Symptom

57 test classes across `engine`, `server`, `integration` and `gremlin` built their own
`DatabaseFactory` on a repo-relative `./databases/<name>` path and hand-rolled the create/drop
lifecycle instead of extending `TestHelper`.

Two consequences:

1. A leftover directory poisons every subsequent run of that class - `LocalDatabase.create()`
   throws `Database './databases/x' already exists`. Any path that skips teardown (JVM crash,
   `kill -9`, surefire fork timeout, OOM, a failure inside `drop()` itself) wedges the class.
2. `mvn clean` cannot recover, because the databases live outside `target/`. The only cure was a
   manual `rm -rf`.

Reproduction (pre-fix):

```bash
cd engine
mkdir -p databases/test-issue5213 && echo '{}' > databases/test-issue5213/schema.json
mvn test -Dtest=Issue5213NestedCallScopeTest    # 5 errors, and no mvn clean fixes it
```

## Root cause

The tests only did one of the four things a database-backed test needs. `TestHelper` already does
all four:

| | hand-rolled | `TestHelper` |
|---|---|---|
| clean **before** test | no | `FileUtils.deleteRecursively` in ctor |
| drop **after** test | yes | `afterTest()` |
| path under `target/` (so `mvn clean` works) | no (`./databases/x`) | `target/databases/<ClassName>` |
| assert no `Database` instance leaked | no | `checkActiveDatabases()` |
| database integrity check | no | `checkDatabaseIntegrity()` |

## Fix

**48 `engine` classes** migrated onto `TestHelper`: the `@BeforeEach`/`@AfterEach` pair and the
private `Database` field are deleted, the class `extends TestHelper`, and seeding moves into the
`beginTest()` hook. The inherited `database` field and the default
`target/databases/<SimpleClassName>` path are used throughout.

```java
class FooTest extends TestHelper {
  @Override
  protected void beginTest() {
    database.command("opencypher", "CREATE (:A {id: 1})");   // was @BeforeEach
  }
  // @Test methods unchanged
}
```

**9 classes outside `engine` cannot extend `TestHelper`** (the `server` and `gremlin` test modules
do not depend on the engine test-jar, and three of the `integration` classes are `main()`
utilities, not JUnit tests). They are repointed under `target/` instead:

- `server`: `AsyncInsertTest`, `RemoteQueriesIT`, `RemoteSafeCloseDatabaseIT` now set
  `GlobalConfiguration.SERVER_ROOT_PATH` to `./target` - the convention already used by the other
  server tests. `SERVER_DATABASE_DIRECTORY` defaults to `${arcadedb.server.rootPath}/databases`, so
  this relocates both the pre-created database and the server's own database directory in one move.
  In `AsyncInsertTest` and `RemoteQueriesIT` this also let a vestigial `./databases/<name>` drop
  block be deleted: the database it guarded is already wiped by the existing
  `deleteRecursively(rootPath + "/databases")`.
- `integration`: `SQLLocalExporterTest`, `SQLLocalImporterIT`, `GloVeTest`, `SimpleGloVeTest`,
  `FastTextDatabase` repointed to `target/databases/...`.
- `gremlin`: `CypherEngineComparisonBenchmark` repointed to `target/databases/...`.

## Regression guard

`.github/scripts/check-test-database-paths.sh` has two complementary checks:

- **static** (`--static`, in the `setup` job): greps test sources for a string literal rooted at a
  repo-relative `databases/` dir - `"databases/`, `"./databases/` or `"../databases/`. Matching the
  literal rather than the `new DatabaseFactory(...)` call site means it also catches the
  `private static final String DB_PATH = "databases/..."` form. It deliberately does *not* match
  `"/databases/`, which is the suffix half of the correct server idiom
  `new DatabaseFactory(rootPath + "/databases/" + name)`.
- **runtime** (`--runtime`, after the unit-test job): asserts no module grew a top-level
  `databases/` directory. This is the backstop for the case no grep can see - a server test whose
  path is *derived* from `SERVER_ROOT_PATH` rather than written as a literal.

## PR

https://github.com/ArcadeData/arcadedb/pull/5264

## Review cycles

| # | Head | What changed | Outcome |
|---|---|---|---|
| 1 | `863febdd` | Initial: 48 engine classes onto `TestHelper`, 9 non-engine repointed under `target/`, static guard | Claude: guard too narrow, six classes still leak via `String DB_PATH`. Gemini: a `@Nested` class creates its DB with no pre-clean |
| 2 | `e3beea4e` | Guard matches the *literal*, not the call site; added the runtime backstop; fixed the 6 exposed classes, 5 server tests leaking via a derived path, `ConsoleTest` dead cleanup, 4 nested classes onto `TestHelper.createDatabase` | Claude: 3 polish items, none blocking |
| 3 | `130e8173` | Runtime guard excludes `*/target/*`; documented the static grep's false-alarm bias. Declined the `reopenDatabase()` DRY suggestion (see below) | Claude: runtime backstop missing from the jobs that need it most |
| 4 | `4a9a5108` | Runtime guard added to `slow-unit-tests`, `integration-tests`, `ha-integration-tests`; documented the second semantic change | Claude: no blockers |

### Declined

**Reuse `TestHelper.reopenDatabase()` in `RecordRecyclingTest.createAndDeleteGraph()`.** Not
equivalent. `reopenDatabase()` closes and reopens with no window in between, but the test must
delete the statistics file *between* the close and the open - that gap is the point of the test,
which exercises recovery when the stats file is missing. Reusing the helper would reopen before the
delete and stop exercising that path. Explained on the PR; no pushback.

### Known follow-up (not in scope)

`OpenCypherCollectUnwindTest` and `OpenCypherPatternPredicateTest` have `@Nested` classes that
cannot fold into the outer `TestHelper` database (the outer seed data would change their
expectations). They keep their own lifecycle, so the outer database is now created, seeded,
integrity-checked and dropped once per nested test. Correct but wasteful. The clean fix is to pull
the nested classes out into top-level `TestHelper` subclasses, which is a bigger refactor than this
issue warrants.

## Review follow-up (cycle 1)

The first version of the guard only matched the inline-constructor form, so it reported `OK` while
six classes still put databases on repo-relative paths via a `String` constant. Broadening it to
match the literal exposed them, and the runtime check exposed a seventh case the static grep is
structurally incapable of catching:

- `engine`: `LSMVectorIndexPersistenceTest`, `LSMVectorIndexChunkedWriteTest`, `ContiguousPageIOTest`
  (`DB_PATH` constants) and three `@Test` methods in `LSMVectorIndexTest` (local `dbPath`) repointed
  under `target/`. These already self-heal, so they only needed the path change.
- `integration`: `GloVeReopenVerificationTest` used `"../databases/glovedb"` - repo *root*, one level
  above the module. Repointed to `target/databases/glovedb`, pairing it with `GloVeTest`.
- `console`: two `deleteRecursively(new File("databases/" + DATABASE_PATH))` calls in `ConsoleTest`
  were dead cleanup of a legacy location. The console writes to `./target/databases/`, which the
  test's own setup already wipes wholesale. Removed.
- `server`: `SelectOrderTest`, `CompositeIndexTest`, `BatchInsertUpdateTest`, `RemoteDateIT` and
  `RemoteCollectionTemporalIT` call `IntegrationUtils.setRootPath(...)` without setting
  `SERVER_ROOT_PATH` first. `ServerPathUtils.setRootPath` only honours an already-set value and
  otherwise returns `"."`, so `rootPath + "/databases/"` resolved to the repo-relative
  `server/databases/`. Confirmed empirically: a full `mvn -pl server test` left
  `server/databases/SelectOrderTest` behind. All five now set `SERVER_ROOT_PATH` to `./target` in
  their `@BeforeEach`, and `SelectOrderTest`'s database was verified to land in
  `server/target/databases/` afterwards.
- `OpenCypherCollectUnwindTest`: four `@Nested` classes created their database with a bare
  `new DatabaseFactory(...).create()` and no pre-clean, so a crashed run wedged them until
  `mvn clean`. They now use `TestHelper.createDatabase(...)`, which drops-if-exists then creates.
  (Gemini flagged one of the four; the same defect was present in three more.)

## Tests

The migration is validated by the existing suites: the tests must keep passing while now running
against a `target/`-rooted database, with `TestHelper`'s stricter teardown
(`checkActiveDatabases()` + `CHECK DATABASE`) newly applied to all 48 engine classes.

No assertion, query or expected value was changed. Two edits are *not* pure moves, and are called
out here so "lifecycle-only" is not read too literally:

1. `RecordRecyclingTest`: `databaseFactory.getActiveDatabaseInstances()` ->
   `DatabaseFactory.getActiveDatabaseInstances()`. The same static method, now called through the
   class because the local variable was removed.
2. `OrderByTest` and `TestInsertAndSelectWithThreadBucketSelectionStrategy`: `beginTest()` now calls
   `database.getSchema().setDateTimeFormat(...)` in addition to setting the global
   `DATE_TIME_FORMAT`. This is a genuine addition, and it is required: `TestHelper` builds the
   database in its **constructor**, before any subclass code runs, and `LocalSchema` captures
   `DATE_TIME_FORMAT` at construction and persists it into `schema.json`. Setting only the global
   from `beginTest()` would arrive too late for the already-built schema, so the format has to be
   written onto the schema itself to survive the `reopenDatabase()` these tests perform.
