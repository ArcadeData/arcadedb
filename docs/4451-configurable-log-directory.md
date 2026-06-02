# Issue #4451 - Configurable Server Log Directory

## Goal

Allow the server's file-log directory to be configured via
`arcadedb.server.logsDirectory` (or the `ARCADEDB_LOG_DIR` environment variable),
enabling Kubernetes deployments with `readOnlyRootFilesystem` to write logs to a
writable mount instead of crashing on a read-only working directory.

## Problem

The packaged `arcadedb-log.properties` hardcodes the JUL FileHandler pattern as
`./log/arcadedb.log`. The Java logging framework resolves that path relative to the
process working directory the first time any log call fires - which happens during
`GlobalConfiguration` static initialization, before the server root path is even set.
On a read-only root filesystem the working directory is not writable, so JUL fails to
open the log file:

```
java.nio.file.FileSystemException: ./log/arcadedb.log.0.lck: Read-only file system
```

The directory cannot simply be tied to `arcadedb.server.rootPath`, because that value
is still unset at the moment logging initializes. It must be resolvable from the
environment alone.

## Solution

Resolve `${...}` placeholders in the FileHandler pattern inside `DefaultLogger` - the
one place ArcadeDB controls the configuration stream before handing it to JUL - using
the existing `SystemVariableResolver` (system property, then environment variable, then
`GlobalConfiguration`), defaulting to `./log` so existing behavior is unchanged.

The packaged pattern becomes:
`java.util.logging.FileHandler.pattern=${arcadedb.server.logsDirectory}/arcadedb.log`

Resolution is applied both when pre-creating the log directory and in the pattern handed
to JUL, so the two agree. A pattern with no placeholder (e.g. an absolute literal path) is
returned unchanged.

Operators relocate the log directory by setting the environment variable:
`ARCADEDB_LOG_DIR=/var/lib/arcadedb/log` (the launch scripts forward it as
`-Darcadedb.server.logsDirectory=...`), or by setting the system property directly.
When unset, logs default to `./log`, identical to prior releases.

## Affected Files

- `engine/src/main/java/com/arcadedb/log/DefaultLogger.java` - `resolveConfigurableLogDir` helper, wired into `createLogDirectoryFromConfig()` and `installCustomFormatter()`
- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` - new `SERVER_LOGS_DIRECTORY` config entry (`arcadedb.server.logsDirectory`, default `./log`)
- `engine/src/test/java/com/arcadedb/LoggerTest.java` - placeholder resolution + integration regression tests
- `engine/src/test/java/com/arcadedb/GlobalConfigurationTest.java` - config entry test
- `package/src/main/config/arcadedb-log.properties` - FileHandler pattern uses the placeholder
- `package/src/main/scripts/server.sh`, `package/src/main/scripts/server.bat` - forward `ARCADEDB_LOG_DIR`

## Verification

- `LoggerTest` (8 tests) and `GlobalConfigurationTest` (10 tests): all PASS. Includes the
  issue-#3732 regression tests, which prove literal patterns (the default `./log` behavior)
  are unchanged.
- Read-only-CWD smoke test against the real packaged `arcadedb-log.properties`: with a
  read-only `./log` in the working directory and `arcadedb.server.logsDirectory` pointing at
  a writable directory, the server logs to the writable directory, leaves `./log` untouched,
  and does not throw the `Read-only file system` exception.

## Backward Compatibility

The default remains `./log`, so single-host and standard Docker deployments (where the
working directory is writable) behave exactly as before. The change is additive.
