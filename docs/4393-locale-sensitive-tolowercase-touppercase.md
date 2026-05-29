# Issue #4393 - Locale-sensitive toLowerCase/toUpperCase in function library

## Problem

At least 21 call sites in `com.arcadedb.function` use `String.toLowerCase()` / `String.toUpperCase()` without a locale argument, relying on the JVM default locale.

On Turkish locale (`tr_TR`), `"I".toLowerCase()` produces `"ı"` (dotless-i, u0131) and `"i".toUpperCase()` produces `"İ"` (dotted-I, u0130), causing:
- `toLower("INFO")` returns `"ınfo"` instead of `"info"`
- `toUpper("info")` returns `"İNFO"` instead of `"INFO"`
- `util.compress(x, "GZIP")` throws "Unsupported compression algorithm: gzıp" (I becomes ı)
- `date.field(ts, "MINUTE")` throws "Unknown date field: mınute"
- `vector_distance(a, b, "euclidean")` throws unsupported metric "EUCLİDEAN"
- `node.incoming("IN")` silently falls back to BOTH direction
- `geo.distance(p1, p2, "MI")` throws unsupported unit "mı" (miles)
- Non-deterministic results across deployments with different JVM locales

## Root Cause

All case conversion uses `String.toLowerCase()` / `String.toUpperCase()` with no locale, which uses the JVM default locale. On Turkish locale (`tr_TR`), 'I'/'i' maps to dotless/dotted Turkish characters rather than the standard ASCII equivalents.

## Fix

Replace all `.toLowerCase()` with `.toLowerCase(Locale.ROOT)` and `.toUpperCase()` with `.toUpperCase(Locale.ROOT)` at all 21 sites. For user-facing text transforms (`toLower()`/`toUpper()`), `Locale.ROOT` is also appropriate - it ensures deterministic behavior matching how most SQL databases implement LOWER/UPPER.

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

Sets `Locale.setDefault(Locale.forLanguageTag("tr-TR"))` before each test, annotated `@Isolated` to prevent locale state leak. 20 test methods cover all major affected sites.

## PR

https://github.com/ArcadeData/arcadedb/pull/4400

## Review Cycles

### Cycle 1 (HEAD `363abf8f`)
- Gemini: HIGH - use `"ceiling"` test for RoundFunction; MEDIUM - add `@Isolated`; MEDIUM - compare compress output not isNotNull; MEDIUM - use specific millisecond in DateField test; MEDIUM - compare vector distance upper/lower
- Claude: CRITICAL - missed `UtilDecompress.java`; IMPORTANT - additional unguarded sites (FunctionRegistry, AbstractNodeFunction, GeoDistance, etc.); MINOR - parallel test safety (`@Isolated`)

### Cycle 2 (HEAD `ad6e81cd`)
- Applied: UtilDecompress fix, FunctionRegistry/CypherFunctionRegistry, AbstractNodeFunction, SQLFunctionGeoDistance, SQLFunctionTimeBucket, 5 sql/vector files; all Gemini test improvements; `@Isolated`
- Gemini cycle 2: same 5 stale comments (already addressed)
- Claude: cycle-2 review constructive, main items: missing parseDirection test, normalize test rename, docs file (disagreed - project convention)

### Cycle 3 (HEAD `2a6a2d19`)
- Applied: added `parseDirection` test with "IN"/"INCOMING"/"OUT"; renamed normalize test to `normalizeFunctionWorksUnderTurkishLocale`
- Gemini cycle 3: same 5 stale comments again
- Claude: cycle-3 review flagged FunctionRegistry/GeoDistance tests missing, scope discrepancy (10 vs 21 files)

### Cycle 4 (HEAD `7bde69c9`)
- Applied: FunctionRegistry.normalizeApocName test, GeoDistance "MI"/"NMI" test; PR description updated to 21 files
- Gemini cycle 4: same 5 stale comments
- Claude: cycle-4 review - SQLFunctionTimeBucket and vector tests missing; docs still says 10 files

### Cycle 5 (HEAD `44c0b8d8`)
- Applied: SQLFunctionTimeBucket.parseInterval test, VectorScoreTransform "sigmoid" test, MultiVectorScore "MIN" test; docs updated to 21 files
- Gemini cycle 5: N/A (not polled)
- Claude: cycle-5 review - "the core fix is correct and the test coverage is solid"; only remaining items: CHANGELOG note (developer), docs file removal (disagreed), normalize test weak (minor), comment cleanup (minor)
- Final state: **clean-approval**
