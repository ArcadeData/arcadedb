# 5543 - Expose `arcadedb.log.impl` through `GlobalConfiguration`

## Problem

Since #5361 the logger implementation is selected with the `arcadedb.log.impl` system property, read in
`LogManager.createLogger()` through a direct `System.getProperty(...)` call. There is no matching
`GlobalConfiguration` entry, which makes it the only ArcadeDB setting outside ArcadeDB's own configuration
system:

- it never appears in `dumpConfigAtStartup`, `toJSON()`/`fromJSON()`, or any config-file mechanism;
- `LogManager.createLogger()` runs from the `LogManager` static initialiser, so the property has to be set
  before the class is first touched. A framework-managed application (Spring Boot) configures later than
  that, so the value cannot come from the application's own configuration.

`LogManager.setLogger(Logger)` is the existing escape hatch, but it forces the embedder to construct the
implementation instead of just expressing the choice as configuration.

## Solution

Add a `GlobalConfiguration.LOG_IMPL` entry (`arcadedb.log.impl`, `SCOPE.JVM`, default `default`) whose
callback installs the corresponding logger on the running `LogManager`. `LogManager.createLogger(String)`
is extracted so both the configuration callback and the existing static-initialiser path share one
resolution routine.

### Why `LogManager` still reads the raw system property

`LogManager` deliberately does **not** query `GlobalConfiguration` from its static initialiser. Doing so
would force `GlobalConfiguration.<clinit>` (and every one of its `readConfiguration()` callbacks, some of
which touch `PageManager` and log) to run *inside* `LogManager.<clinit>`, at a point where
`LogManager.instance()` is still `null`. `DefaultLogger` avoids `GlobalConfiguration` for the same reason
(see the `DEFAULT_LOG_DIR` comment there).

The wiring is therefore one-directional:

- **before class load** - the system property is read directly by `createLogger()`, exactly as today;
- **at `GlobalConfiguration` init** - `readConfiguration()` picks up the same system property (the key is
  literally `arcadedb.log.impl`) and the callback re-installs the logger, a no-op in practice;
- **at any later point** - `GlobalConfiguration.LOG_IMPL.setValue("slf4j")`, a config file, `fromJSON()`,
  or a server setting installs the logger on the already-loaded `LogManager`. This is the case that was
  impossible before.

The callback null-guards `LogManager.instance()` so it stays safe if it ever runs re-entrantly from
`LogManager`'s own static initialiser.

### Unknown values

`createLogger(String)` keeps the #5361 behaviour: an unrecognized value is reported on `System.err` and
falls back to `java.util.logging`. The entry deliberately does **not** declare an `allowed` set:
`setValue()` runs the callback *before* the allowed-values check, so a rejected value would throw after the
logger had already been swapped.

## Changes

- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` - new `LOG_IMPL` entry.
- `engine/src/main/java/com/arcadedb/log/LogManager.java` - `createLogger(String)` extracted;
  `LOG_IMPL_PROPERTY` javadoc updated; `setLogger(Logger)` javadoc added (the nit from the issue).
- `engine/src/test/java/com/arcadedb/log/LogImplConfigurationTest.java` - new regression test.
- `examples/logging/README.md` - documents the configuration route.

Because `SCOPE.JVM` settings are listed by `GetServerHandler`, the setting also shows up in the server
settings API, and setting it there swaps the logger on a running server.

## Verification

**Test first.** `LogImplConfigurationTest` was written before the fix and did not compile
(`cannot find symbol: variable LOG_IMPL`). After the fix, the callback was then neutered
(`if (false && logManager != null)`) to prove the assertions are load-bearing: 4 of the 6 tests failed,
the two remaining being the registration/default-value checks that do not exercise the callback.

**Class-initialisation orderings**, probed in four separate JVMs against the built classes (this is the
part unit tests inside one JVM cannot cover, since `LogManager` is already loaded there):

| system property | first class touched | installed logger | `LOG_IMPL` value |
|---|---|---|---|
| unset | `LogManager` | `DefaultLogger` | `default` |
| `slf4j` | `LogManager` | `Slf4jLogger` | `slf4j` |
| `slf4j` | `GlobalConfiguration` | `Slf4jLogger` | `slf4j` |
| `bogus` | `GlobalConfiguration` | `DefaultLogger` | `bogus` |

The `bogus` row prints the fallback warning twice, once from the static initialiser and once from the
configuration callback that re-resolves the same value. Only a mistyped value reaches it, and both lines
say the same thing, so it is left as is.

**Test results**

- `mvn -pl engine test`: 10690 tests, 0 failures, 0 errors, 23 skipped.
- `mvn compile` over the full reactor: green.

## Impact

Additive. The default is `default`, which is what an unset system property already resolved to, and the
system property keeps being honoured through the exact same code path. Nothing installs a logger unless
the value is explicitly set. `GlobalConfiguration.reset()` restores the value but does not re-install a
logger, so a caller that swapped the implementation and wants the original back should keep the
`LogManager.getLogger()` it replaced.
