# Deferred review items - PR #4281, HEAD 4eefd6c3a

## Cycle 2 review

Two of the three comments below are stale duplicates of cycle 1 feedback that has already
been addressed in commit `4eefd6c3a`. The third (CodeQL) is mitigated in the cycle 3 commit.

### 1. gemini-code-assist on `engine/src/main/java/com/arcadedb/log/LogFormatter.java:96` - skipped (already addressed)

> Switching to `formatMessage(iRecord)` introduces a regression for any JUL log records that
> use printf-style placeholders (`%s`) with parameters... If maintaining backward
> compatibility for printf-style is desired, you would need to check if `formatMessage`
> performed any substitution or attempt a fallback to `String.formatted()`.

**Rationale for skipping:** The `formatMessage(iRecord)` call on line 96 resolves to the
override added in this same commit at lines 79-92, NOT the JDK default. The override
already does exactly what the reviewer asks for: delegate to `super.formatMessage()` for
`{0}` MessageFormat substitution, then fall back to `String.formatted(parameters)` when
the JDK path returned the raw template unchanged. The gemini bot's static analysis
appears not to have followed the inheritance.

Verification: `LogFormatter.java` lines 79-92 contain the override; tests
`printfStylePlaceholdersWithParametersAreSubstituted` and
`ansiFormatterPrintfStylePlaceholdersAreSubstituted` exercise the fallback.

### 2. gemini-code-assist on `engine/src/test/java/com/arcadedb/LogFormatterMessageFormatTest.java:83` - skipped (already addressed)

> The PR description mentions that existing printf-style (`%s`) messages are unaffected, but
> the current tests only verify pre-formatted messages (where parameters are null). It would
> be beneficial to add a test case that specifically uses a `LogRecord` with `%s`
> placeholders and non-null parameters to confirm the current behavior...

**Rationale for skipping:** Three printf-style tests were already added in this same
commit:
- `printfStylePlaceholdersWithParametersAreSubstituted` - `LogFormatter` + `%s` + `%d`
- `ansiFormatterPrintfStylePlaceholdersAreSubstituted` - `AnsiLogFormatter` + `%s` + `%d`
- `malformedPrintfTemplateFallsBackToRawMessage` - `IllegalFormatException` catch path

The gemini bot's line-83 anchor lands inside the no-op-parameters test
(`preformattedMessagesPassThrough`); the printf-style tests it asked for are further down
the same file.

### 3. github-advanced-security (CodeQL) on `engine/src/main/java/com/arcadedb/log/LogFormatter.java:86` - mitigated in cycle 3

> CodeQL / Use of externally-controlled format string. Format string depends on a
> user-provided value... [Show more details](https://github.com/ArcadeData/arcadedb/security/code-scanning/1842)

**Rationale and action:** `LogRecord.getMessage()` is application-controlled (log
templates are hardcoded at call sites), not user input. The same `message.formatted(args)`
pattern existed in the pre-fix code without flagging - CodeQL caught it now because the
flow moved into a function where the source-to-sink path is shorter and more obvious.

Cycle 3 commit adds a regex-based whitelist (`SAFE_PRINTF_PATTERN`) that only attempts the
printf fallback when the message contains a recognized simple format specifier, otherwise
returns the raw template. This is defense in depth: the format string is still application
code, but a stray `%n`/`%t`-style template that slips through (e.g. inside an exception
message) is rejected rather than processed. The CodeQL alert may still appear as the taint
flow still exists technically; if so, dismiss it as a known false positive in the UI.
