# Fix #4326: JavascriptFunctionDefinition JS injection via source concatenation

## Issue

`JavascriptFunctionDefinition.execute()` built the function call by string-concatenating
user-supplied arguments into JavaScript source code and passing the result to
`polyglotEngine.eval()`. A "looksLikeJson" heuristic passed strings that started with `{`/`[`
through unescaped, allowing injection of arbitrary JS (e.g. `{a:require('child_process').execSync('id'),b:1}`).
An "alreadyQuoted" heuristic did the same for strings enclosed in single quotes.

## Root cause

`toJavaScriptValue(Object)` at lines 115-159 produced JS source fragments. Any string
passing the `looksLikeJson` or `alreadyQuoted` checks escaped the string-escaping path
and was embedded verbatim into the eval'd source.

## Fix

Replace source-code concatenation in `execute()` with a direct GraalVM `Value.execute(args...)`
call. The declared function is retrieved from the JS bindings by name and called with Java
objects passed as arguments - GraalVM handles the type conversion internally, eliminating
the injection surface entirely.

- Java `Map` values are converted to `ProxyObject` (deep, recursive) so JS code can access
  map properties via dot/bracket notation.
- Java `List` values are converted to `ProxyArray` (deep, recursive).
- All other types (String, Number, Boolean, null) are passed as-is; GraalVM converts them.

The `toJavaScriptValue`, `mapToJavaScript`, and `listToJavaScript` methods are removed
as they are no longer used on the execution path.

## Tests changed

- `PolyglotFunctionTest.jsonObjectAsInput`: updated to pass a `Map` (not a JSON-encoded
  string) since the JS function now receives the map as a proper JS object.
- `PolyglotFunctionTest.stringObjectAsInput`: updated to pass a plain string (not a
  single-quote-wrapped string) since the `alreadyQuoted` heuristic is gone.
- `PolyglotFunctionTest.jsInjectionPrevented`: new regression test that verifies a
  string matching the old `looksLikeJson` pattern is treated as a JS string, not as
  source code.

## Files changed

- `engine/src/main/java/com/arcadedb/function/polyglot/JavascriptFunctionDefinition.java`
- `engine/src/test/java/com/arcadedb/function/polyglot/PolyglotFunctionTest.java`

## PR

https://github.com/ArcadeData/arcadedb/pull/4339

## Review cycles

### Cycle 1

- Head SHA: `718d99c4cd18085bef654417af21d45280fce89f`
- gemini-code-assist: COMMENTED (3 inline comments)
  1. **high** - `ClassCastException` risk in `toJsArg` when casting `Map<?,?>` to `Map<String,Object>` if map has non-String keys. Fixed by adding `normalizeMapKeys`/`normalizeListValues`/`normalizeValue` helpers.
  2. **medium** - null check for `fn = getMember(functionName)`. Fixed by adding `if (fn == null) throw new FunctionExecutionException(...)`.
  3. **medium** - original exception cause swallowed in catch block. Fixed by passing `e` to `FunctionExecutionException(String, Throwable)` constructor.
- Follow-up commit: `0468570ea` - address review: normalize map keys, add null guard, preserve exception cause
- All 23 polyglot tests pass after the follow-up commit.

## Final state

`cycle-1-feedback-addressed` - gemini-code-assist reviewed and all 3 comments were applied. Per repo convention, gemini does not re-review follow-up pushes, so the loop exits after 1 productive cycle.
