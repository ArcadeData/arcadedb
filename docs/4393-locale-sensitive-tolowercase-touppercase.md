# Issue #4393 - Locale-sensitive toLowerCase/toUpperCase in function library

## Problem

At least 10 call sites in `com.arcadedb.function` use `String.toLowerCase()` / `String.toUpperCase()` without a locale argument, relying on the JVM default locale.

On Turkish locale (`tr_TR`), `"I".toLowerCase()` produces `"ı"` (dotless-i, u0131) and `"i".toUpperCase()` produces `"İ"` (dotted-I, u0130), causing:
- `toLower("INFO")` returns `"ınfo"` instead of `"info"`
- `toUpper("info")` returns `"İNFO"` instead of `"INFO"`
- `util.compress(x, "GZIP")` throws "Unsupported compression algorithm: gzıp" (I becomes ı)
- `date.field(ts, "MINUTE")` throws "Unknown date field: mınute"
- `vector_distance(a, b, "euclidean")` throws unsupported metric "EUCLİDEAN"
- Non-deterministic results across deployments with different JVM locales

## Root Cause

All case conversion uses `String.toLowerCase()` / `String.toUpperCase()` with no locale, which uses the JVM default locale. On Turkish locale (`tr_TR`), 'I'/'i' maps to dotless/dotted Turkish characters rather than the standard ASCII equivalents.

## Fix

Replace all `.toLowerCase()` with `.toLowerCase(Locale.ROOT)` and `.toUpperCase()` with `.toUpperCase(Locale.ROOT)` at all 10 sites. For user-facing text transforms (`toLower()`/`toUpper()`), `Locale.ROOT` is also appropriate - it matches the behavior of SQL `LOWER()`/`UPPER()` in all major databases (deterministic, ASCII-folding only).

## Affected Files (21)

| File | Change |
|---|---|
| `text/ToLowerFunction.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` |
| `text/ToUpperFunction.java` | `toUpperCase()` -> `toUpperCase(Locale.ROOT)` |
| `text/ToBooleanFunction.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` |
| `convert/ConvertToBoolean.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` |
| `util/UtilCompress.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` |
| `util/UtilDecompress.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` |
| `math/RoundFunction.java` | `toUpperCase()` -> `toUpperCase(Locale.ROOT)` |
| `date/AbstractDateFunction.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` in `unitToMillis` |
| `date/DateField.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` |
| `text/NormalizeFunction.java` | `toUpperCase()` -> `toUpperCase(Locale.ROOT)` |
| `vector/VectorDistanceFunction.java` | `toUpperCase()` -> `toUpperCase(Locale.ROOT)` |
| `node/AbstractNodeFunction.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` in `parseDirection` |
| `sql/geo/SQLFunctionGeoDistance.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` |
| `sql/time/SQLFunctionTimeBucket.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` in `parseInterval` |
| `sql/vector/SQLFunctionMultiVectorScore.java` | `toUpperCase()` -> `toUpperCase(Locale.ROOT)` |
| `sql/vector/SQLFunctionVectorApproxDistance.java` | `toUpperCase()` -> `toUpperCase(Locale.ROOT)` |
| `sql/vector/SQLFunctionVectorFuse.java` | `toUpperCase()` -> `toUpperCase(Locale.ROOT)` |
| `sql/vector/SQLFunctionVectorScoreTransform.java` | `toUpperCase()` -> `toUpperCase(Locale.ROOT)` |
| `sql/vector/SQLFunctionVectorToString.java` | `toUpperCase()` -> `toUpperCase(Locale.ROOT)` |
| `FunctionRegistry.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` in name normalization |
| `CypherFunctionRegistry.java` | `toLowerCase()` -> `toLowerCase(Locale.ROOT)` in name normalization |

## Test

New: `engine/src/test/java/com/arcadedb/function/LocaleSensitivityTest.java`

Sets `Locale.setDefault(Locale.forLanguageTag("tr-TR"))` before each test and asserts correct behavior for all affected functions. 13 test methods cover each affected site with Turkish-sensitive inputs.

## Test Results

- New regression test: 13/13 passed
- TextStatelessFunctionsTest: 54/54 passed
- OpenCypherTextFunctionsTest: 86/86 passed
- OpenCypherConvertFunctionsTest: 30/30 passed
- OpenCypherVectorFunctionsComprehensiveTest: 34/34 passed
- All related function tests: 423 total, 0 failures
