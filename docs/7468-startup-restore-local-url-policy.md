# #7468 - the startup `restore:` local-URL policy now resolves from the server's `ContextConfiguration`

It used to resolve from the static `GlobalConfiguration` value. What follows is the defect as reported, the
sweep for every other path that could share it, and the fix.

## Problem

`ArcadeDBServer.restoreDatabaseFromStartupCommand` - the executor of the `restore:` startup command of
`arcadedb.server.defaultDatabases` - never calls `Restore.setAllowLocalUrls(...)`. `RestoreSettings.allowLocalUrls`
therefore stays `null`, and `FullRestoreFormat.openInputFile()` falls back to the **static**
`GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS` value:

```java
// integration/src/main/java/com/arcadedb/integration/restore/format/FullRestoreFormat.java:234
final boolean allowLocalUrls = settings.allowLocalUrls != null ?
    settings.allowLocalUrls : GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getValueAsBoolean();
```

Every other server-side restore/import path resolves the flag against the server instead, via
`ServerControlPlane.isRestoreImportLocalUrlsAllowed()` - whose own javadoc calls itself "the single source of truth
for `SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS` on this server instance". A per-server `ContextConfiguration` override
therefore reaches the HTTP/gRPC `restore database` verb but not the same server's boot-time restore, and the two
disagree in whichever direction the override went.

## Completeness

### 1. The invariant

> Every restore this server starts - by any command, startup or client - resolves
> `SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS` from **this server's own** `ContextConfiguration`, never from the
> process-wide static `GlobalConfiguration` value.

### 2. Enumerating every way to violate it

Every place that reflectively loads the restorer or the importer:

```console
$ grep -rn 'integration.restore.Restore"' --include="*.java" . | grep -v /test/
server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:1693:      final Class<?> clazz = Class.forName("com.arcadedb.integration.restore.Restore");
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1712:        final Class<?> clazz = Class.forName("com.arcadedb.integration.restore.Restore");

$ grep -rn 'integration.importer.Importer"' --include="*.java" . | grep -v /test/
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1564:      final Class<?> clazz = Class.forName("com.arcadedb.integration.importer.Importer");
engine/src/main/java/com/arcadedb/query/sql/parser/ImportDatabaseStatement.java:90:        final Class<?> clazz = Class.forName("com.arcadedb.integration.importer.Importer");
```

Every `setAllowLocalUrls` call site in main sources:

```console
$ grep -rn 'setAllowLocalUrls' --include="*.java" . | grep -v /test/ | grep invoke
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1574:      clazz.getMethod("setAllowLocalUrls", boolean.class).invoke(importer, isRestoreImportLocalUrlsAllowed());
server/src/main/java/com/arcadedb/server/ServerControlPlane.java:1717:        clazz.getMethod("setAllowLocalUrls", boolean.class).invoke(restorer, isRestoreImportLocalUrlsAllowed());
engine/src/main/java/com/arcadedb/query/sql/parser/ImportDatabaseStatement.java:109:        clazz.getMethod("setAllowLocalUrls", boolean.class).invoke(importer, !blockLocalNetworks);
```

Four reflective construction sites, three `setAllowLocalUrls` calls: `ArcadeDBServer:1693` is the one with no call.

Callers of the method under repair:

```console
$ grep -rn "restoreDatabaseFromStartupCommand" --include="*.java" .
./server/src/test/java/com/arcadedb/server/Issue7440StartupRestoreProgressIT.java:231
./server/src/test/java/com/arcadedb/server/Issue7440StartupRestoreProgressIT.java:264
./server/src/main/java/com/arcadedb/server/RestoreProgress.java:32   (javadoc reference)
./server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:1618  (the `restore:` startup command)
./server/src/main/java/com/arcadedb/server/ArcadeDBServer.java:1688  (the declaration)
```

The other transports that can start a restore all route through the control plane rather than through this method:

```console
$ grep -rn "controlPlane.restore" --include="*.java" grpcw/src/main/java
grpcw/.../ArcadeDbGrpcAdminService.java:804:          controlPlane.restoreBackup(...)
grpcw/.../ArcadeDbGrpcAdminService.java:828:          controlPlane.restoreDatabase(req.getDatabase(), req.getUrl(), listener);
```

### 3. Coverage table

| Entry point | Resolves the flag from | Covered by fix? | Covered by a test? |
|---|---|---|---|
| startup `restore:` -> `ArcadeDBServer.restoreDatabaseFromStartupCommand` (`:1693`) | **was: static global** -> now the server's `ContextConfiguration` | **yes** | yes - `Issue7468StartupRestoreLocalUrlPolicyIT`, both directions |
| HTTP/gRPC `restore database` / `restore backup` -> `ServerControlPlane.performRestore` (`:1717`) | server's `ContextConfiguration` already (`isRestoreImportLocalUrlsAllowed()`) | n/a - already correct (#6380/#6381) | pre-existing `Issue6381RestoreSsrfTest` |
| HTTP/gRPC `import database` -> `ServerControlPlane.runImport` (`:1574`) | server's `ContextConfiguration` already | n/a - already correct (#6474) | pre-existing `Issue6474ImportSsrfFlagDivergenceTest` |
| SQL `IMPORT DATABASE` -> `ImportDatabaseStatement` (`:108`) | the command's own `ContextConfiguration`; a server command threads its resolved answer down | n/a - already correct (#6474) | pre-existing |
| startup `import:` -> `database.command("sql", "import database ...")` -> `ImportDatabaseStatement` | **the static global**, via `LocalDatabase:2099`'s `new ContextConfiguration()` | **no** - filed as #7632 | no |

Argued rows carry their evidence above; the one uncovered row is filed rather than left blank.

### 4. The uncovered row, in detail

`LocalDatabase.command(String, String, Object...)` builds a **fresh empty** `ContextConfiguration` for every
command:

```java
// engine/src/main/java/com/arcadedb/database/LocalDatabase.java:2099
return getQueryEngine(language).command(query, new ContextConfiguration(), parameters);
```

so the `import:` startup command at `ArcadeDBServer:1628`, which calls exactly that overload, reaches
`ImportDatabaseStatement:108` with nothing overlaid and reads `SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS` off
the static global. That is the same *shape* of divergence as this issue, on the other startup command and the
other setting, and its fix is a different one (threading a `ContextConfiguration` into `database.command(...)`,
as `PostServerCommandHandler` already does for the client-issued form). Out of scope here; filed as **#7632**.

### 5. Reachability

`restoreDatabaseFromStartupCommand` is called from `loadDefaultDatabases()` (`ArcadeDBServer:1618`), which
`start()` calls on every boot, for every `restore:` entry in `arcadedb.server.defaultDatabases`. No flag gates
it. The value is read at the moment of the restore, not bound at class-init, so an override applied to the
server's `ContextConfiguration` before `start()` - which is where an embedded or multi-instance deployment
applies it - is the one the fetch sees.

### 6. Residual risk

- The `file://` branch of `FullRestoreFormat.openInputFile()` is **not** gated by `allowLocalUrls` at all; only the
  `http(s)` branch is. A `file://` startup restore was permitted before this change and still is. That is
  deliberate and unchanged: the startup URL comes from the operator's own configuration file rather than from a
  client, and `validateClientRestoreImportUrl` - the check that rejects `file://` - is a *client*-URL gate that the
  startup command deliberately does not run. This fix changes only which configuration the `http(s)` fetch's
  local-address policy is read from.
- The startup `import:` command keeps reading its own policy from the static global (row 5 of the table).

## The fix

`ArcadeDBServer.restoreDatabaseFromStartupCommand` now calls `setAllowLocalUrls` on the restorer with
`new ServerControlPlane(this).isRestoreImportLocalUrlsAllowed()` - the same accessor `performRestore` and
`runImport` use, rather than a second copy of the `server.getConfiguration().getValueAsBoolean(...)` expression,
so there stays exactly one source of truth for the setting on a server instance. `ServerControlPlane` holds only
a reference to the server, so constructing one here costs an object per startup restore.

The javadoc on the method gained a paragraph saying which configuration the fetch runs under, and - because the
question comes up every time - that this command deliberately does **not** run
`validateClientRestoreImportUrl`: that gate refuses a URL a *client* supplied, and the `restore:` URL comes from
the operator's own configuration file.

### Behaviour, by case

| Server override | Static global | Before | After |
|---|---|---|---|
| none | `false` (default) | refuses local URLs | refuses local URLs (unchanged) |
| none | `true` | allows | allows (unchanged) |
| `true` | `false` | **refuses a URL the operator allowed** | allows |
| `false` | `true` | **fetches a URL the operator disallowed** | refuses |

`ContextConfiguration.getValueAsBoolean` falls through to the live static value when nothing is overlaid
(`ContextConfiguration:267-275`), which is why the first two rows are untouched.

## Tests

`server/src/test/java/com/arcadedb/server/Issue7468StartupRestoreLocalUrlPolicyIT.java`, three methods, each
driving `restoreDatabaseFromStartupCommand` against a real archive served over `http://127.0.0.1`:

| Test | Override | Global | Asserts |
|---|---|---|---|
| `aStartupRestoreFollowsAPerServerOverrideThatAllowsLocalUrls` | `true` | `false` | the restore runs and the records are there |
| `aStartupRestoreFollowsAPerServerOverrideThatForbidsLocalUrls` | `false` | `true` | `CommandExecutionException` with a `SecurityException` root cause, and no database left behind |
| `aStartupRestoreWithNoOverrideStillFollowsTheGlobal` | none | both values | the global still decides, so the default deployment is unaffected |

The archive is served over `http`, not handed over as a `file://` path, on purpose:
`FullRestoreFormat.openInputFile()` gates only its `http(s)` branch on `allowLocalUrls`, so a `file://` URL would
exercise no policy at all and would pass against the unfixed code.

### Proof the tests can fail

Run against the tree **before** the fix, the two override tests fail in exactly the two directions the issue
names, and the no-override test passes:

```
[ERROR] Tests run: 3, Failures: 1, Errors: 1, Skipped: 0 -- in Issue7468StartupRestoreLocalUrlPolicyIT
[ERROR]   ...ThatForbidsLocalUrls:144   (the restore ran; the global said yes)
[ERROR]   ...ThatAllowsLocalUrls:124 » CommandExecution Error on restoring database   (the global said no)
```

After the fix, with the sibling `Issue7440StartupRestoreProgressIT`:

```
[INFO] Tests run: 6, Failures: 0, Errors: 0, Skipped: 0
```

### Regression runs

| Command | Result |
|---|---|
| `mvn -o -pl integration -am verify -DskipITs=false -Dtest='Issue6381RestoreSsrfTest,Issue6474ImportSsrfFlagDivergenceTest,Issue7385RestoreProgressCallbackTest,RestoreSettingsTest,FullRestoreFormatThreadSizingTest' -Dit.test='Issue6086ParallelRestoreIT'` | 28 + 19 tests, 0 failures |
| `mvn -o -pl server -am verify -DskipTests -DskipITs=false -Dit.test='Issue7468...,Issue7440...,ServerRestoreDatabaseIT,ServerImportDatabaseIT,ServerBackupDatabaseIT,Issue7385RestoreProgressIT,Issue7395RestoreTargetExistsIT,Issue7308RestoreTargetNameIT,RestoreImportSecurityDurabilityIT,BackupRestoreDeleteApiIT,PostServerCommandPathTraversalIT'` | 41 tests, 0 failures |
| `mvn -o -pl server test -Dtest='Issue7484StartupImportFailureTest,PostServerCommandHandlerSsrfTest,Issue7483ImportProgressCounterMonotonicTest,Issue7384OperationAdmissionTest'` | 16 tests, 0 failures |

## Note on an existing test comment

`Issue7440StartupRestoreProgressIT.onServerConfiguration` carried a comment saying the startup restore "reads
this from the static global (it has no ContextConfiguration of its own to resolve against)". That sentence is
false after this change, so it was rewritten to say what is now true - the restore resolves against the server's
`ContextConfiguration`, which with no overlay falls through to the global, which is why setting the global is
still what that test needs. **No assertion, method or fixture in that class was touched**; the edit is the
comment text alone.

## Ledger

- [x] 1. Startup `restore:` **resolved** the local-URL policy from the static global instead of the server's
  `ContextConfiguration` - **fixed**: the static-global fallback is gone, and the policy now resolves through
  `ServerControlPlane.isRestoreImportLocalUrlsAllowed()` like every other server-side restore. Tested in both
  directions.
- [x] 2. Sibling: startup `import:` resolves `SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS` from the static
  global - **filed as #7632**, out of scope here.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not seen the author's reasoning. **No `Task` tool was
available in this session**, so the pass was run by hand instead, against the diff alone, and is recorded here
with that caveat: it carries less weight than an agent that was never persuaded.

| Probe | Finding | Disposition |
|---|---|---|
| Does a `file://` startup restore now behave differently? | No. `FullRestoreFormat.openInputFile()` gates only its `http(s)` branch on `allowLocalUrls`; the `file://` branch reads the path unconditionally, before and after. | Not a defect. Recorded under Residual risk; it is also why the tests serve over `http` - a `file://` fixture would have passed against the unfixed code. |
| Does the new reflective `getMethod("setAllowLocalUrls", ...)` add a classpath requirement? | Yes, strictly: an `arcadedb-integration` jar predating #6381 would now take the `ReflectiveOperationException` arm and report "restore libs not found in classpath" for a startup restore that used to run. | Accepted. `ServerControlPlane.performRestore` has carried the identical call and the identical catch since #6381, so the server already requires the method for its HTTP/gRPC restore verbs, and that arm's existing comment already reads "the optional arcadedb-integration module is absent **or does not match**". The reflection exists because the module is optional, not because versions skew. |
| Does a refused startup restore leave a half-built directory that blocks a retry? | No directory survived the run (`server/target/` has no `databases0` afterwards), and the test asserts `existsDatabase` is false. The refusal happens in `openInputFile()`, before any extraction. Whatever is true here is true identically for the "archive does not exist" refusal that `Issue7440StartupRestoreProgressIT.aFailedStartupRestoreRetiresTheOperation` already pins. | Not a defect, and not a regression. |
| Is the `hasValue` guard in the no-override test vacuous? | No. `ContextConfiguration.setValue(GlobalConfiguration, ...)` stores under `iConfig.getKey()` and `hasValue(String)` looks up `normalizeKey(iName)`, which resolves a declared key to itself (#7297), so the two agree on the key. | Verified, no change. |
| Is `new ServerControlPlane(this)` a real cost? | Its constructor assigns one field. One object per startup restore, of which there is one per `restore:` entry per boot. | Not a defect. Chosen over a second copy of the `getConfiguration().getValueAsBoolean(...)` expression so the setting keeps one accessor. |
| Is any entry point still resolving a restore/import policy from the static global? | Yes - the startup `import:` command, on the other setting. | Real and out of scope: filed as **#7632** before this PR opened. |

## Review cycles

### Cycle 1 - `da8d23c` (PR #7633)

| Reviewer | Outcome |
|---|---|
| `claude` | **No blocking issues found.** Verified the fallback, the accessor, the reflective signature and the exception shape against the real sources, and confirmed `BaseGraphServerTest` gives each method a fresh server so the per-server override cannot leak between test methods. |
| `coderabbitai` | 1 actionable comment, `Minor`, on the tracking doc. |
| `codacy-production` | 0 new issues, 0 complexity. |

**Applied.** CodeRabbit (thread `4018356122`): the doc's title and ledger entry stated the pre-fix behaviour in
the present tense, so a reader arriving after the merge could take them for a description of current code. Real,
if cosmetic. The title now names the server `ContextConfiguration` as the source and says the static-global
resolution is what it replaced; the ledger entry is past-tense about the defect and names the accessor the policy
now resolves through.

**Skipped, with the reason.** `claude` observed that every other `ServerControlPlane` caller holds one as a field
rather than constructing it inline, and suggested hoisting a field onto `ArcadeDBServer` - explicitly "in a future
pass. Not worth blocking on." Not done here: the other callers are HTTP/gRPC handlers that construct one per
handler and serve many requests from it, whereas this path runs at most once per `restore:` entry per boot, so the
field would buy nothing and would add a server-lifetime object whose relationship to `getConfiguration()` is a
wider decision than this fix needs. Recorded here rather than in a `review-deferred-*.md` notes file because there
is nothing for the developer to act on - the reviewer classified it as non-blocking and future-scoped.

No `review-deferred-*.md` notes file was produced in this cycle.
