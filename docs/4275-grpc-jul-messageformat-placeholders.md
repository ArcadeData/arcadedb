# Issue #4275: gRPC ManagedChannelImpl log messages contain unsubstituted placeholders

## Root cause

`AnsiLogFormatter.customFormatMessage()` and `LogFormatter.customFormatMessage()` use
`String.formatted()` (Java printf-style, `%s`/`%d`) to substitute `LogRecord` parameters into
the message template.

gRPC's internal loggers (e.g. `ManagedChannelImpl`) log via `java.util.logging` using
**MessageFormat-style** placeholders (`{0}`, `{1}`):

```java
logger.log(Level.WARNING, "[{0}] Failed to resolve name. status={1}",
           new Object[]{ channelId, status });
```

`String.formatted("[{0}] Failed to resolve name. status={1}", channelId, status)` treats
`{0}` as literal text (not a printf specifier), silently drops both args, and returns the
raw template — which is what appears in the log.

## Fix

Replace the manual `iRecord.getMessage()` + `iRecord.getParameters()` +
`String.formatted()` pattern in both formatters with a call to the JDK base-class
method `Formatter.formatMessage(LogRecord)`, which already handles both styles:

- If `iRecord.getParameters()` is null or empty → returns `iRecord.getMessage()` as-is.
- If the message contains `{0}` … `{3}` → delegates to `MessageFormat.format()`.
- Otherwise → returns the raw message.

ArcadeDB's own `DefaultLogger` always pre-formats messages before calling JUL
(`log.log(level, preformattedMsg)`, no parameters array), so `formatMessage()` is a
no-op for those records and existing behaviour is preserved.

## Files changed

- `engine/src/main/java/com/arcadedb/log/LogFormatter.java`
- `engine/src/main/java/com/arcadedb/utility/AnsiLogFormatter.java`

## Test

- `engine/src/test/java/com/arcadedb/LogFormatterMessageFormatTest.java`
  - verifies `{0}`, `{1}` are substituted by both formatters
  - verifies pre-formatted (no-parameter) messages are passed through unchanged
  - verifies printf-style `%s`/`%d` messages with parameters still work via the fallback path
