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
  - verifies templates containing only non-whitelisted printf conversions (`%n`, `%t`) are returned unchanged (defensive guard against tainted format strings)

## PR

- https://github.com/ArcadeData/arcadedb/pull/4281

## Review cycles

| Cycle | HEAD | Summary | Bot outcome |
|---|---|---|---|
| 1 | `faa219157` | Initial fix: switch `LogFormatter`/`AnsiLogFormatter` to `Formatter.formatMessage()` | gemini-code-assist COMMENTED - 3 inline (doc path typo, printf regression risk, request printf test) |
| 2 | `4eefd6c3a` | Override `formatMessage()` with printf fallback, add 3 printf tests, fix doc path | gemini-code-assist COMMENTED - 2 stale duplicates of cycle 1 (override they recommended is already present); github-advanced-security flagged CodeQL `tainted-format-string` |
| 3 | `098ef0315` | Added `SAFE_PRINTF_PATTERN` whitelist (mitigates CodeQL by rejecting `%n`/`%t`/modifier templates); 1 new test for unsafe specifiers; deferred-items file with rationale | gemini-code-assist COMMENTED - same 2 stale duplicates re-emitted on new line numbers; github-advanced-security re-flagged same CodeQL alert (taint flow technically still present, mitigation is runtime) |

## Deferred items

All cycle 2/3 review comments are documented in
[`docs/review-deferred-4eefd6c3a.md`](review-deferred-4eefd6c3a.md):

- gemini-code-assist printf-regression comment: skipped - the override added in cycle 2
  already provides the exact fallback the bot is asking for; the bot's analysis appears
  not to follow the inheritance into the override.
- gemini-code-assist missing-printf-test comment: skipped - three printf-style tests were
  added in cycle 2 (the bot's line anchor lands on the no-parameter test by accident).
- github-advanced-security CodeQL `tainted-format-string`: runtime-mitigated in cycle 3
  via `SAFE_PRINTF_PATTERN`. The static taint flow still exists; if the alert remains the
  developer should dismiss it in the GitHub UI as a known false positive (log templates
  are application code, not user input).

## Final state

`deferred-items` - bots have reached a stable state where they re-emit the same comments
each cycle. Further commits would trigger the same loop. Loop exited at cycle 3 of 4.
Merge is the developer's responsibility.
