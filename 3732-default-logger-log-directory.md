# Fix #3732 — DefaultLogger always creates `.log` directory

## Problem

`DefaultLogger.init()` unconditionally tried to create a `./log` directory on every startup,
even when no file-based logging was configured. If that path was not writable, the following
error appeared in the output:

```
Cannot create log directory: /./log
```

Because the directory creation happened **before** reading the log configuration, users who
provided their own `java.util.logging.config.file` (e.g. a console-only setup) could not
prevent the error.

## Root Cause

`init()` hard-coded `new File("./log")` and tried to `mkdirs()` it regardless of what the
actual logging configuration required.

## Fix

**File:** `engine/src/main/java/com/arcadedb/log/DefaultLogger.java`

Changed `init()` to:

1. Call `findConfiguredFileHandlerPattern()` — reads the **pending** log-properties source
   (same three locations that `installCustomFormatter()` will later use: system property,
   classpath resource, or `config/` directory) and extracts
   `java.util.logging.FileHandler.pattern` without loading it into the `LogManager`.
2. If a pattern is found, resolve JUL substitutions (`%t`, `%h`, `%g`, `%u`, `%%`) and
   pre-create the parent directory (identical logic to before, but targeted at the configured
   path).
3. Call `installCustomFormatter()` to load the configuration. JUL's `FileHandler` can now
   open the file successfully because the directory already exists.

If no `FileHandler` pattern is found in the configuration (console-only, custom external
logging framework, etc.) **no directory is created and no error is printed**.

## Tests

Two regression tests added to `LoggerTest`:

- `noLogDirectoryCreatedWithConsoleOnlyConfig` — verifies that a console-only configuration
  results in no `FileHandler.pattern` being registered and no spurious directory creation.
- `logDirectoryCreatedFromConfiguredFileHandlerPattern` — verifies that a FileHandler
  configuration pointing to a custom temp directory causes that directory to be created
  before the config is loaded into the LogManager.

## Test Results

```
Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```
