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

`.github/scripts/check-test-database-paths.sh` fails the build when any file under
`*/src/test/java` constructs a `DatabaseFactory` on a repo-relative `databases/` path. It is wired
into the `setup` job of `.github/workflows/mvn-test.yml`, so it gates every PR before the test jobs
run. Verified failing on the pre-fix tree (57 files listed) and passing after.

## Tests

No test logic, assertion, query or expected value was changed - this is a pure lifecycle refactor.
The migration is itself validated by the existing suites: the tests must keep passing while now
running against a `target/`-rooted database, with `TestHelper`'s stricter teardown
(`checkActiveDatabases()` + `CHECK DATABASE`) newly applied to all 48 engine classes.
