# Test Coverage Improvement Design

## Overview

Improve test coverage for ArcadeDB focusing on two areas with significant gaps:
1. **OpenCypher implementation** (~170 untested sources)
2. **Bolt protocol** (~25 untested sources)

## Approach

Balanced mix of unit tests and integration-style tests:
- **Unit tests**: For functions, mappers, serialization (fast, granular)
- **Integration tests**: For executor steps and protocol handlers (realistic)

---

## Phase 1: Bolt Protocol Unit Tests

### 1.1 BoltMessageTest (extend existing)

Test all 17 message types for parsing and serialization.

**Request messages to test**:
- `HelloMessage` (0x01) - user_agent, credentials, routing
- `RunMessage` (0x10) - query, parameters, database extraction
- `BeginMessage` (0x11) - transaction options
- `CommitMessage` (0x12) - simple marker
- `RollbackMessage` (0x13) - simple marker
- `DiscardMessage` (0x2F) - record count
- `PullMessage` (0x3F) - record count
- `ResetMessage` (0x0F) - connection reset
- `GoodbyeMessage` (0x02) - disconnect
- `LogonMessage` (0x6A) - re-authentication
- `LogoffMessage` (0x6B) - de-authentication
- `RouteMessage` (0x66) - routing info

**Response messages to test**:
- `SuccessMessage` (0x70) - metadata
- `RecordMessage` (0x71) - data row
- `IgnoredMessage` (0x7E) - command ignored
- `FailureMessage` (0x7F) - error code/message

### 1.2 BoltStructureMapperTest (new)

Test type conversions in `BoltStructureMapper`:
- `toPackStreamValue()` - all supported types
- `ridToId()` - RID to numeric ID conversion
- `toNumber()` - numeric precision handling
- Date/time conversions to ISO strings
- Collection handling (List, Set, Map)
- Null and edge cases

### 1.3 BoltChunkedIOTest (new)

Test `BoltChunkedInput` and `BoltChunkedOutput`:
- Normal message chunking
- Large messages (>64KB, multiple chunks)
- Boundary conditions (exactly 65535 bytes)
- Empty messages
- Terminator handling (0x0000)

---

## Phase 2: OpenCypher Function Unit Tests

### 2.1 OpenCypherTextFunctionsTest (new)

Test 27 text functions:
- `TextIndexOf`, `TextSplit`, `TextJoin`
- `TextReplace`, `TextRegexReplace`
- `TextCapitalize`, `TextCamelCase`, `TextSnakeCase`
- `TextLevenshteinDistance`, `TextJaroWinklerDistance`
- `TextTrim`, `TextLTrim`, `TextRTrim`
- `TextLeft`, `TextRight`, `TextSubstring`
- `TextReverse`, `TextRepeat`
- `TextStartsWith`, `TextEndsWith`, `TextContains`
- `TextToUpper`, `TextToLower`
- `TextBase64Encode`, `TextBase64Decode`
- `TextHexEncode`, `TextHexDecode`
- `TextFormat`

### 2.2 OpenCypherMathFunctionsTest (new)

Test 9 math functions:
- `MathSigmoid`, `MathTanh`, `MathCosh`, `MathSinh`
- `MathMaxLong`, `MathMaxDouble`
- `MathMinLong`, `MathMinDouble`
- `MathLog2`

### 2.3 OpenCypherAggFunctionsTest (new)

Test 11 aggregation functions:
- `AggFirst`, `AggLast`, `AggNth`
- `AggMedian`, `AggPercentiles`
- `AggStatistics`
- `AggMaxItems`, `AggMinItems`
- `AggProduct`
- `AggStDev`, `AggStDevP`

### 2.4 OpenCypherDateFunctionsTest (new)

Test 11 date functions:
- `DateCurrentTimestamp`, `DateNow`
- `DateFormat`, `DateParse`
- `DateAdd`, `DateSubtract`
- `DateConvert`
- `DateFromISO8601`, `DateToISO8601`
- `DateTruncate`
- `DateFields` (year, month, day, etc.)

### 2.5 OpenCypherConvertFunctionsTest (new)

Test 8 conversion functions:
- `ConvertToInteger`, `ConvertToFloat`, `ConvertToBoolean`
- `ConvertToString`, `ConvertToList`
- `ConvertToJson`, `ConvertFromJson`
- `ConvertToMap`

### 2.6 OpenCypherUtilFunctionsTest (new)

Test 10 utility functions:
- `UtilMd5`, `UtilSha1`, `UtilSha256`, `UtilSha512`
- `UtilCompress`, `UtilDecompress`
- `UtilSleep`
- `UtilValidate`
- `UtilRandomUuid`
- `UtilCoalesce`

---

## Phase 3: OpenCypher Executor Steps

### 3.1 OpenCypherMatchStepsTest (new)

Test matching behavior:
- `MatchNodeStep` - label filtering, ID filtering, empty results
- `MatchRelationshipStep` - type filtering, direction handling
- `ExpandPathStep` - variable-length paths
- `ExpandIntoStep` - path expansion into existing nodes
- Optional match behavior

### 3.2 OpenCypherFilterStepsTest (new)

Test WHERE clause filtering:
- `FilterPropertiesStep` - property comparisons
- Null handling in comparisons
- Complex predicates (AND, OR, NOT)
- Pattern predicates

### 3.3 OpenCypherAggregationStepsTest (new)

Test aggregation operations:
- `AggregationStep` with COUNT, SUM, AVG, MIN, MAX
- GROUP BY with multiple fields
- DISTINCT aggregations
- Wrapped aggregations (HEAD(COLLECT(...)))
- Empty result set aggregation

### 3.4 OpenCypherOrderingStepsTest (new)

Test ordering operations:
- `OrderByStep` - single/multi-field sorting, ASC/DESC
- `LimitStep` - result limiting
- `SkipStep` - result skipping
- Combined SKIP + LIMIT

### 3.5 OpenCypherProjectionStepsTest (new)

Test projection operations:
- `ProjectReturnStep` - field selection, aliasing
- `UnwindStep` - list unwinding
- `WithStep` - intermediate result projection

### 3.6 OpenCypherMutationStepsTest (new)

Test mutation operations:
- `CreateStep` - node/relationship creation
- `SetStep` - property updates
- `DeleteStep` - node/relationship deletion
- `RemoveStep` - property/label removal
- `MergeStep` - MERGE with ON CREATE/ON MATCH

---

## File Locations

### Bolt Tests
- `bolt/src/test/java/com/arcadedb/bolt/BoltMessageTest.java` (extend)
- `bolt/src/test/java/com/arcadedb/bolt/BoltStructureMapperTest.java` (new)
- `bolt/src/test/java/com/arcadedb/bolt/BoltChunkedIOTest.java` (new)

### OpenCypher Function Tests
- `engine/src/test/java/com/arcadedb/query/opencypher/functions/OpenCypherTextFunctionsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/functions/OpenCypherMathFunctionsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/functions/OpenCypherAggFunctionsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/functions/OpenCypherDateFunctionsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/functions/OpenCypherConvertFunctionsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/functions/OpenCypherUtilFunctionsTest.java`

### OpenCypher Executor Tests
- `engine/src/test/java/com/arcadedb/query/opencypher/OpenCypherMatchStepsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/OpenCypherFilterStepsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/OpenCypherAggregationStepsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/OpenCypherOrderingStepsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/OpenCypherProjectionStepsTest.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/OpenCypherMutationStepsTest.java`

---

## Expected Coverage Improvement

| Area | Before | After |
|------|--------|-------|
| Bolt messages | ~3 tests | ~20 tests |
| Bolt structures | ~5 tests | ~15 tests |
| OpenCypher functions | 0 direct tests | ~80 tests |
| OpenCypher steps | indirect only | ~30 focused tests |

**Total new tests**: ~140+ tests
