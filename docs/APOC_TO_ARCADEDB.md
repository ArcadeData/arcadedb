# ArcadeDB OpenCypher Built-in Functions

## Introduction

ArcadeDB provides a comprehensive set of built-in functions for OpenCypher queries. These functions are designed to be compatible with common Cypher function patterns while using ArcadeDB-native namespaces (e.g., `text.indexOf`, `map.merge`, `merge.relationship`).

Unlike external procedure libraries, these functions are built directly into ArcadeDB's OpenCypher engine, providing:

- **Zero configuration** - Functions are available immediately without any installation
- **Native performance** - Direct integration with the query engine
- **Type safety** - Proper type handling and error messages
- **Consistent behavior** - Predictable results across all ArcadeDB deployments
- **APOC compatibility** - Full support for the `apoc.` prefix (see below)

### APOC Prefix Compatibility

**ArcadeDB automatically supports the `apoc.` prefix for all built-in functions and procedures.** This means you can use your existing Neo4j/APOC queries without any modifications.

When ArcadeDB encounters a function or procedure call with the `apoc.` prefix, it automatically strips the prefix and resolves to the corresponding ArcadeDB function. For example:

| APOC Call | Resolves To | Result |
|-----------|-------------|--------|
| `apoc.text.indexOf("hello", "l")` | `text.indexOf("hello", "l")` | Same function |
| `apoc.map.merge({a:1}, {b:2})` | `map.merge({a:1}, {b:2})` | Same function |
| `CALL apoc.merge.relationship(...)` | `CALL merge.relationship(...)` | Same procedure |

This compatibility layer is:
- **Automatic** - No configuration required
- **Case-insensitive** - `APOC.TEXT.INDEXOF` works the same as `apoc.text.indexOf`
- **Zero overhead** - Simple string prefix check, no performance impact

**Example - These queries are equivalent:**

```cypher
-- Neo4j/APOC style (works in ArcadeDB)
RETURN apoc.text.join(["a", "b", "c"], ",") AS result

-- ArcadeDB native style
RETURN text.join(["a", "b", "c"], ",") AS result
```

```cypher
-- Neo4j/APOC style (works in ArcadeDB)
MATCH (a:Person), (b:Person) WHERE a.name = "Alice" AND b.name = "Bob"
CALL apoc.merge.relationship(a, "KNOWS", {}, {since: 2024}, b) YIELD rel
RETURN rel

-- ArcadeDB native style
MATCH (a:Person), (b:Person) WHERE a.name = "Alice" AND b.name = "Bob"
CALL merge.relationship(a, "KNOWS", {}, {since: 2024}, b) YIELD rel
RETURN rel
```

### Function Namespaces

Functions are organized into the following namespaces:

| Namespace | Description |
|-----------|-------------|
| `text.*` | String manipulation and text processing |
| `map.*` | Map/object operations |
| `math.*` | Mathematical functions |
| `convert.*` | Type conversion functions |
| `date.*` | Date/time operations |
| `util.*` | Utility functions (hashing, compression, validation) |
| `agg.*` | Aggregation and collection functions |
| `node.*` | Node/vertex operations (degree, labels, relationships) |
| `rel.*` | Relationship/edge operations (type, endpoints) |
| `path.*` | Path operations (create, combine, slice) |
| `create.*` | Creation functions (UUIDs, virtual nodes/relationships) |
| `merge.*` | Merge procedures for nodes and relationships |
| `algo.*` | Graph algorithms (Dijkstra, A*, shortest paths) |
| `meta.*` | Schema and database introspection procedures |

---

## APOC to ArcadeDB Function Mapping

This section provides a mapping from common APOC procedures to their ArcadeDB equivalents.

### Text Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.text.indexOf` | `text.indexOf` | Identical behavior |
| `apoc.text.join` | `text.join` | Identical behavior |
| `apoc.text.split` | `text.split` | Identical behavior |
| `apoc.text.replace` | `text.replace` | Identical behavior |
| `apoc.text.regexReplace` | `text.regexReplace` | Identical behavior |
| `apoc.text.capitalize` | `text.capitalize` | Identical behavior |
| `apoc.text.capitalizeAll` | `text.capitalizeAll` | Identical behavior |
| `apoc.text.decapitalize` | `text.decapitalize` | Identical behavior |
| `apoc.text.decapitalizeAll` | `text.decapitalizeAll` | Identical behavior |
| `apoc.text.camelCase` | `text.camelCase` | Identical behavior |
| `apoc.text.snakeCase` | `text.snakeCase` | Identical behavior |
| `apoc.text.upperCamelCase` | `text.upperCamelCase` | Identical behavior |
| `apoc.text.lpad` | `text.lpad` | Identical behavior |
| `apoc.text.rpad` | `text.rpad` | Identical behavior |
| `apoc.text.format` | `text.format` | Identical behavior |
| `apoc.text.slug` | `text.slug` | Identical behavior |
| `apoc.text.random` | `text.random` | Identical behavior |
| `apoc.text.hexValue` | `text.hexValue` | Identical behavior |
| `apoc.text.byteCount` | `text.byteCount` | Identical behavior |
| `apoc.text.charAt` | `text.charAt` | Identical behavior |
| `apoc.text.code` | `text.code` | Identical behavior |
| `apoc.text.levenshteinDistance` | `text.levenshteinDistance` | Identical behavior |
| `apoc.text.levenshteinSimilarity` | `text.levenshteinSimilarity` | Identical behavior |
| `apoc.text.sorensenDiceSimilarity` | `text.sorensenDiceSimilarity` | Identical behavior |
| `apoc.text.jaroWinklerDistance` | `text.jaroWinklerDistance` | Identical behavior |
| `apoc.text.hammingDistance` | `text.hammingDistance` | Identical behavior |

### Map Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.map.merge` | `map.merge` | Identical behavior |
| `apoc.map.mergeList` | `map.mergeList` | Identical behavior |
| `apoc.map.fromLists` | `map.fromLists` | Identical behavior |
| `apoc.map.fromPairs` | `map.fromPairs` | Identical behavior |
| `apoc.map.setKey` | `map.setKey` | Identical behavior |
| `apoc.map.removeKey` | `map.removeKey` | Identical behavior |
| `apoc.map.removeKeys` | `map.removeKeys` | Identical behavior |
| `apoc.map.clean` | `map.clean` | Identical behavior |
| `apoc.map.flatten` | `map.flatten` | Identical behavior |
| `apoc.map.unflatten` | `map.unflatten` | Identical behavior |
| `apoc.map.submap` | `map.submap` | Identical behavior |
| `apoc.map.values` | `map.values` | Identical behavior |
| `apoc.map.groupBy` | `map.groupBy` | Identical behavior |
| `apoc.map.sortedProperties` | `map.sortedProperties` | Identical behavior |

### Conversion Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.convert.toJson` | `convert.toJson` | Identical behavior |
| `apoc.convert.fromJsonMap` | `convert.fromJsonMap` | Identical behavior |
| `apoc.convert.fromJsonList` | `convert.fromJsonList` | Identical behavior |
| `apoc.convert.toMap` | `convert.toMap` | Identical behavior |
| `apoc.convert.toList` | `convert.toList` | Identical behavior |
| `apoc.convert.toSet` | `convert.toSet` | Identical behavior |
| `apoc.convert.toBoolean` | `convert.toBoolean` | Identical behavior |
| `apoc.convert.toInteger` | `convert.toInteger` | Identical behavior |
| `apoc.convert.toFloat` | `convert.toFloat` | Identical behavior |

### Date Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.date.format` | `date.format` | Identical behavior |
| `apoc.date.parse` | `date.parse` | Identical behavior |
| `apoc.date.add` | `date.add` | Identical behavior |
| `apoc.date.convert` | `date.convert` | Identical behavior |
| `apoc.date.field` | `date.field` | Identical behavior |
| `apoc.date.fields` | `date.fields` | Identical behavior |
| `apoc.date.currentTimestamp` | `date.currentTimestamp` | Identical behavior |
| `apoc.date.toISO8601` | `date.toISO8601` | Identical behavior |
| `apoc.date.fromISO8601` | `date.fromISO8601` | Identical behavior |
| `apoc.date.systemTimezone` | `date.systemTimezone` | Identical behavior |

### Utility Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.util.md5` | `util.md5` | Identical behavior |
| `apoc.util.sha1` | `util.sha1` | Identical behavior |
| `apoc.util.sha256` | `util.sha256` | Identical behavior |
| `apoc.util.sha512` | `util.sha512` | Identical behavior |
| `apoc.util.compress` | `util.compress` | Supports gzip, deflate |
| `apoc.util.decompress` | `util.decompress` | Supports gzip, deflate |
| `apoc.util.sleep` | `util.sleep` | Identical behavior |
| `apoc.util.validate` | `util.validate` | Identical behavior |

### Aggregation Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.agg.first` | `agg.first` | Identical behavior |
| `apoc.agg.last` | `agg.last` | Identical behavior |
| `apoc.agg.nth` | `agg.nth` | Identical behavior |
| `apoc.agg.slice` | `agg.slice` | Identical behavior |
| `apoc.agg.median` | `agg.median` | Identical behavior |
| `apoc.agg.percentiles` | `agg.percentiles` | Identical behavior |
| `apoc.agg.statistics` | `agg.statistics` | Identical behavior |
| `apoc.agg.product` | `agg.product` | Identical behavior |
| `apoc.agg.minItems` | `agg.minItems` | Identical behavior |
| `apoc.agg.maxItems` | `agg.maxItems` | Identical behavior |

### Math Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.math.sigmoid` | `math.sigmoid` | Identical behavior |
| `apoc.math.sigmoidPrime` | `math.sigmoidPrime` | Identical behavior |
| `apoc.math.tanh` | `math.tanh` | Identical behavior |
| `apoc.math.cosh` | `math.cosh` | Identical behavior |
| `apoc.math.sinh` | `math.sinh` | Identical behavior |
| `apoc.math.maxLong` | `math.maxLong` | Identical behavior |
| `apoc.math.minLong` | `math.minLong` | Identical behavior |
| `apoc.math.maxDouble` | `math.maxDouble` | Identical behavior |

### Node Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.node.degree` | `node.degree` | Identical behavior |
| `apoc.node.degree.in` | `node.degree.in` | Identical behavior |
| `apoc.node.degree.out` | `node.degree.out` | Identical behavior |
| `apoc.node.labels` | `node.labels` | Identical behavior |
| `apoc.node.id` | `node.id` | Identical behavior |
| `apoc.node.relationship.exists` | `node.relationship.exists` | Identical behavior |
| `apoc.node.relationship.types` | `node.relationship.types` | Identical behavior |

### Relationship Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.rel.id` | `rel.id` | Identical behavior |
| `apoc.rel.type` | `rel.type` | Identical behavior |
| `apoc.rel.startNode` | `rel.startNode` | Identical behavior |
| `apoc.rel.endNode` | `rel.endNode` | Identical behavior |

### Path Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.path.create` | `path.create` | Identical behavior |
| `apoc.path.combine` | `path.combine` | Identical behavior |
| `apoc.path.slice` | `path.slice` | Identical behavior |
| `apoc.path.elements` | `path.elements` | Identical behavior |

### Create Functions

| APOC Function | ArcadeDB Function | Notes |
|---------------|-------------------|-------|
| `apoc.create.uuid` | `create.uuid` | Identical behavior |
| `apoc.create.uuidBase64` | `create.uuidBase64` | Identical behavior |
| `apoc.create.vNode` | `create.vNode` | Identical behavior |
| `apoc.create.vRelationship` | `create.vRelationship` | Identical behavior |

### Merge Procedures

| APOC Procedure | ArcadeDB Procedure | Notes |
|----------------|-------------------|-------|
| `apoc.merge.relationship` | `merge.relationship` | Compatible, uses per-row execution |
| `apoc.merge.node` | `merge.node` | Compatible |

### Algorithm Procedures

| APOC Procedure | ArcadeDB Procedure | Notes |
|----------------|-------------------|-------|
| `apoc.algo.dijkstra` | `algo.dijkstra` | Weighted shortest path |
| `apoc.algo.aStar` | `algo.astar` | A* with optional geographic heuristics |
| `apoc.algo.allSimplePaths` | `algo.allsimplepaths` | Find all simple paths between nodes |

### Path Expansion Procedures

| APOC Procedure | ArcadeDB Procedure | Notes |
|----------------|-------------------|-------|
| `apoc.path.expand` | `path.expand` | Expand paths from start node |
| `apoc.path.expandConfig` | `path.expandconfig` | Expand with configuration map |
| `apoc.path.subgraphNodes` | `path.subgraphnodes` | Get all reachable nodes |
| `apoc.path.subgraphAll` | `path.subgraphall` | Get all reachable nodes and relationships |
| `apoc.path.spanningTree` | `path.spanningtree` | Get spanning tree paths |

### Meta/Schema Procedures

| APOC Procedure | ArcadeDB Procedure | Notes |
|----------------|-------------------|-------|
| `apoc.meta.graph` | `meta.graph` | Virtual graph of schema structure |
| `apoc.meta.schema` | `meta.schema` | Detailed schema information |
| `apoc.meta.stats` | `meta.stats` | Database statistics |
| `apoc.meta.nodeTypeProperties` | `meta.nodetypeproperties` | Node type property information |
| `apoc.meta.relTypeProperties` | `meta.reltypeproperties` | Relationship type property information |

---

## Function Reference

### Text Functions

#### text.indexOf

Find the position of a substring within a string.

**Syntax:** `text.indexOf(string, substring, [start])`

**Parameters:**
- `string` - The string to search in
- `substring` - The substring to find
- `start` (optional) - Starting position for the search (default: 0)

**Returns:** Long - Position of substring, or -1 if not found

**APOC Compatible:** `apoc.text.indexOf`

**Examples:**
```cypher
RETURN text.indexOf("hello world", "world") AS pos
// Returns: 6

RETURN text.indexOf("hello hello", "hello", 1) AS pos
// Returns: 6
```

---

#### text.join

Join a list of strings with a delimiter.

**Syntax:** `text.join(list, delimiter)`

**Parameters:**
- `list` - List of strings to join
- `delimiter` - String to insert between elements

**Returns:** String - Joined string

**APOC Compatible:** `apoc.text.join`

**Examples:**
```cypher
RETURN text.join(["a", "b", "c"], ",") AS result
// Returns: "a,b,c"

RETURN text.join(["hello", "world"], " ") AS result
// Returns: "hello world"
```

---

#### text.split

Split a string by a delimiter.

**Syntax:** `text.split(string, delimiter)`

**Parameters:**
- `string` - The string to split
- `delimiter` - The delimiter to split on

**Returns:** List - List of substrings

**APOC Compatible:** `apoc.text.split`

**Examples:**
```cypher
RETURN text.split("a,b,c", ",") AS parts
// Returns: ["a", "b", "c"]

RETURN text.split("hello world", " ") AS parts
// Returns: ["hello", "world"]
```

---

#### text.replace

Replace all occurrences of a substring.

**Syntax:** `text.replace(string, search, replacement)`

**Parameters:**
- `string` - The original string
- `search` - The substring to find
- `replacement` - The replacement string

**Returns:** String - String with replacements made

**APOC Compatible:** `apoc.text.replace`

**Examples:**
```cypher
RETURN text.replace("hello world", "world", "universe") AS result
// Returns: "hello universe"

RETURN text.replace("aaa", "a", "b") AS result
// Returns: "bbb"
```

---

#### text.regexReplace

Replace all matches of a regular expression.

**Syntax:** `text.regexReplace(string, regex, replacement)`

**Parameters:**
- `string` - The original string
- `regex` - The regular expression pattern
- `replacement` - The replacement string

**Returns:** String - String with replacements made

**APOC Compatible:** `apoc.text.regexReplace`

**Examples:**
```cypher
RETURN text.regexReplace("hello123world", "[0-9]+", "-") AS result
// Returns: "hello-world"

RETURN text.regexReplace("hello", "[aeiou]", "X") AS result
// Returns: "hXllX"
```

---

#### text.capitalize

Capitalize the first letter of a string.

**Syntax:** `text.capitalize(string)`

**Parameters:**
- `string` - The string to capitalize

**Returns:** String - Capitalized string

**APOC Compatible:** `apoc.text.capitalize`

**Examples:**
```cypher
RETURN text.capitalize("hello") AS result
// Returns: "Hello"

RETURN text.capitalize("hello world") AS result
// Returns: "Hello world"
```

---

#### text.capitalizeAll

Capitalize the first letter of each word.

**Syntax:** `text.capitalizeAll(string)`

**Parameters:**
- `string` - The string to capitalize

**Returns:** String - String with all words capitalized

**APOC Compatible:** `apoc.text.capitalizeAll`

**Examples:**
```cypher
RETURN text.capitalizeAll("hello world") AS result
// Returns: "Hello World"
```

---

#### text.camelCase

Convert a string to camelCase.

**Syntax:** `text.camelCase(string)`

**Parameters:**
- `string` - The string to convert

**Returns:** String - camelCase string

**APOC Compatible:** `apoc.text.camelCase`

**Examples:**
```cypher
RETURN text.camelCase("hello world") AS result
// Returns: "helloWorld"

RETURN text.camelCase("hello_world") AS result
// Returns: "helloWorld"
```

---

#### text.snakeCase

Convert a string to snake_case.

**Syntax:** `text.snakeCase(string)`

**Parameters:**
- `string` - The string to convert

**Returns:** String - snake_case string

**APOC Compatible:** `apoc.text.snakeCase`

**Examples:**
```cypher
RETURN text.snakeCase("helloWorld") AS result
// Returns: "hello_world"

RETURN text.snakeCase("Hello World") AS result
// Returns: "hello_world"
```

---

#### text.upperCamelCase

Convert a string to UpperCamelCase (PascalCase).

**Syntax:** `text.upperCamelCase(string)`

**Parameters:**
- `string` - The string to convert

**Returns:** String - UpperCamelCase string

**APOC Compatible:** `apoc.text.upperCamelCase`

**Examples:**
```cypher
RETURN text.upperCamelCase("hello world") AS result
// Returns: "HelloWorld"
```

---

#### text.lpad

Left-pad a string to a specified length.

**Syntax:** `text.lpad(string, length, padChar)`

**Parameters:**
- `string` - The string to pad
- `length` - Target length
- `padChar` - Character to use for padding

**Returns:** String - Padded string

**APOC Compatible:** `apoc.text.lpad`

**Examples:**
```cypher
RETURN text.lpad("42", 5, "0") AS result
// Returns: "00042"
```

---

#### text.rpad

Right-pad a string to a specified length.

**Syntax:** `text.rpad(string, length, padChar)`

**Parameters:**
- `string` - The string to pad
- `length` - Target length
- `padChar` - Character to use for padding

**Returns:** String - Padded string

**APOC Compatible:** `apoc.text.rpad`

**Examples:**
```cypher
RETURN text.rpad("42", 5, "0") AS result
// Returns: "42000"
```

---

#### text.slug

Create a URL-friendly slug from a string.

**Syntax:** `text.slug(string, [delimiter])`

**Parameters:**
- `string` - The string to convert
- `delimiter` (optional) - Character to use as word separator (default: "-")

**Returns:** String - URL-friendly slug

**APOC Compatible:** `apoc.text.slug`

**Examples:**
```cypher
RETURN text.slug("Hello World!") AS result
// Returns: "hello-world"

RETURN text.slug("Hello World!", "_") AS result
// Returns: "hello_world"
```

---

#### text.levenshteinDistance

Calculate the Levenshtein (edit) distance between two strings.

**Syntax:** `text.levenshteinDistance(string1, string2)`

**Parameters:**
- `string1` - First string
- `string2` - Second string

**Returns:** Long - Number of edits required

**APOC Compatible:** `apoc.text.levenshteinDistance`

**Examples:**
```cypher
RETURN text.levenshteinDistance("kitten", "sitting") AS distance
// Returns: 3

RETURN text.levenshteinDistance("hello", "hello") AS distance
// Returns: 0
```

---

#### text.levenshteinSimilarity

Calculate the Levenshtein similarity (0-1) between two strings.

**Syntax:** `text.levenshteinSimilarity(string1, string2)`

**Parameters:**
- `string1` - First string
- `string2` - Second string

**Returns:** Double - Similarity score (0-1)

**APOC Compatible:** `apoc.text.levenshteinSimilarity`

**Examples:**
```cypher
RETURN text.levenshteinSimilarity("hello", "hallo") AS similarity
// Returns: 0.8
```

---

### Map Functions

#### map.merge

Merge two maps, with the second map's values overriding the first.

**Syntax:** `map.merge(map1, map2)`

**Parameters:**
- `map1` - First map
- `map2` - Second map (values override first)

**Returns:** Map - Merged map

**APOC Compatible:** `apoc.map.merge`

**Examples:**
```cypher
RETURN map.merge({a: 1, b: 2}, {b: 3, c: 4}) AS result
// Returns: {a: 1, b: 3, c: 4}
```

---

#### map.fromLists

Create a map from a list of keys and a list of values.

**Syntax:** `map.fromLists(keys, values)`

**Parameters:**
- `keys` - List of keys
- `values` - List of values

**Returns:** Map - New map

**APOC Compatible:** `apoc.map.fromLists`

**Examples:**
```cypher
RETURN map.fromLists(["a", "b", "c"], [1, 2, 3]) AS result
// Returns: {a: 1, b: 2, c: 3}
```

---

#### map.setKey

Add or update a key in a map.

**Syntax:** `map.setKey(map, key, value)`

**Parameters:**
- `map` - The original map
- `key` - Key to set
- `value` - Value to set

**Returns:** Map - Map with key added/updated

**APOC Compatible:** `apoc.map.setKey`

**Examples:**
```cypher
RETURN map.setKey({a: 1}, "b", 2) AS result
// Returns: {a: 1, b: 2}
```

---

#### map.flatten

Flatten a nested map using a delimiter.

**Syntax:** `map.flatten(map, delimiter)`

**Parameters:**
- `map` - The nested map
- `delimiter` - String to join nested keys

**Returns:** Map - Flattened map

**APOC Compatible:** `apoc.map.flatten`

**Examples:**
```cypher
RETURN map.flatten({a: {b: 1, c: 2}}, ".") AS result
// Returns: {"a.b": 1, "a.c": 2}
```

---

#### map.groupBy

Group a list of maps by a key.

**Syntax:** `map.groupBy(list, key)`

**Parameters:**
- `list` - List of maps
- `key` - Key to group by

**Returns:** Map - Map of groups

**APOC Compatible:** `apoc.map.groupBy`

**Examples:**
```cypher
RETURN map.groupBy([{type: "A", val: 1}, {type: "B", val: 2}, {type: "A", val: 3}], "type") AS result
// Returns: {A: [{type: "A", val: 1}, {type: "A", val: 3}], B: [{type: "B", val: 2}]}
```

---

### Math Functions

#### math.sigmoid

Calculate the sigmoid function.

**Syntax:** `math.sigmoid(x)`

**Parameters:**
- `x` - Input value

**Returns:** Double - Sigmoid value (0-1)

**APOC Compatible:** `apoc.math.sigmoid`

**Examples:**
```cypher
RETURN math.sigmoid(0) AS result
// Returns: 0.5

RETURN math.sigmoid(10) AS result
// Returns: ~0.9999
```

---

#### math.maxLong

Get the maximum long value.

**Syntax:** `math.maxLong()`

**Returns:** Long - Maximum long value (9223372036854775807)

**APOC Compatible:** `apoc.math.maxLong`

**Examples:**
```cypher
RETURN math.maxLong() AS result
// Returns: 9223372036854775807
```

---

### Convert Functions

#### convert.toJson

Convert a value to JSON string.

**Syntax:** `convert.toJson(value)`

**Parameters:**
- `value` - Value to convert (map, list, or primitive)

**Returns:** String - JSON representation

**APOC Compatible:** `apoc.convert.toJson`

**Examples:**
```cypher
RETURN convert.toJson({name: "John", age: 30}) AS result
// Returns: '{"name":"John","age":30}'

RETURN convert.toJson([1, 2, 3]) AS result
// Returns: '[1,2,3]'
```

---

#### convert.fromJsonMap

Parse a JSON string to a map.

**Syntax:** `convert.fromJsonMap(json)`

**Parameters:**
- `json` - JSON string representing an object

**Returns:** Map - Parsed map

**APOC Compatible:** `apoc.convert.fromJsonMap`

**Examples:**
```cypher
RETURN convert.fromJsonMap('{"name":"John","age":30}') AS result
// Returns: {name: "John", age: 30}
```

---

#### convert.toBoolean

Convert a value to boolean.

**Syntax:** `convert.toBoolean(value)`

**Parameters:**
- `value` - Value to convert

**Returns:** Boolean - Converted value

**APOC Compatible:** `apoc.convert.toBoolean`

**Examples:**
```cypher
RETURN convert.toBoolean("true") AS result
// Returns: true

RETURN convert.toBoolean(1) AS result
// Returns: true

RETURN convert.toBoolean(0) AS result
// Returns: false
```

---

### Date Functions

#### date.currentTimestamp

Get the current timestamp in milliseconds.

**Syntax:** `date.currentTimestamp()`

**Returns:** Long - Current timestamp in milliseconds since epoch

**APOC Compatible:** `apoc.date.currentTimestamp`

**Examples:**
```cypher
RETURN date.currentTimestamp() AS now
// Returns: 1705314600000 (example)
```

---

#### date.format

Format a timestamp to a date string.

**Syntax:** `date.format(timestamp, unit, format)`

**Parameters:**
- `timestamp` - The timestamp value
- `unit` - Unit of the timestamp (ms, s, m, h, d)
- `format` - Date format pattern (Java SimpleDateFormat)

**Returns:** String - Formatted date string

**APOC Compatible:** `apoc.date.format`

**Examples:**
```cypher
RETURN date.format(1705314600000, "ms", "yyyy-MM-dd HH:mm:ss") AS result
// Returns: "2024-01-15 10:30:00"

RETURN date.format(1705314600, "s", "yyyy-MM-dd") AS result
// Returns: "2024-01-15"
```

---

#### date.add

Add time to a timestamp.

**Syntax:** `date.add(timestamp, value, unit)`

**Parameters:**
- `timestamp` - The timestamp (in milliseconds)
- `value` - Amount to add (can be negative)
- `unit` - Unit of the value (ms, s, m, h, d)

**Returns:** Long - New timestamp

**APOC Compatible:** `apoc.date.add`

**Examples:**
```cypher
RETURN date.add(date.currentTimestamp(), 1, "d") AS tomorrow
// Returns: timestamp for tomorrow

RETURN date.add(date.currentTimestamp(), -1, "h") AS oneHourAgo
// Returns: timestamp for 1 hour ago
```

---

#### date.toISO8601

Convert a timestamp to ISO 8601 format.

**Syntax:** `date.toISO8601(timestamp)`

**Parameters:**
- `timestamp` - Timestamp in milliseconds

**Returns:** String - ISO 8601 formatted string

**APOC Compatible:** `apoc.date.toISO8601`

**Examples:**
```cypher
RETURN date.toISO8601(1705314600000) AS result
// Returns: "2024-01-15T10:30:00+00:00"
```

---

#### date.fromISO8601

Parse an ISO 8601 string to timestamp.

**Syntax:** `date.fromISO8601(dateString)`

**Parameters:**
- `dateString` - ISO 8601 formatted string

**Returns:** Long - Timestamp in milliseconds

**APOC Compatible:** `apoc.date.fromISO8601`

**Examples:**
```cypher
RETURN date.fromISO8601("2024-01-15T10:30:00Z") AS result
// Returns: 1705314600000
```

---

#### date.systemTimezone

Get the system's default timezone.

**Syntax:** `date.systemTimezone()`

**Returns:** String - Timezone ID (e.g., "America/New_York")

**APOC Compatible:** `apoc.date.systemTimezone`

**Examples:**
```cypher
RETURN date.systemTimezone() AS tz
// Returns: "America/New_York" (depends on system)
```

---

### Utility Functions

#### util.md5

Compute MD5 hash of a value.

**Syntax:** `util.md5(value)`

**Parameters:**
- `value` - Value to hash

**Returns:** String - MD5 hash (32 hex characters)

**APOC Compatible:** `apoc.util.md5`

**Examples:**
```cypher
RETURN util.md5("hello") AS hash
// Returns: "5d41402abc4b2a76b9719d911017c592"
```

---

#### util.sha256

Compute SHA-256 hash of a value.

**Syntax:** `util.sha256(value)`

**Parameters:**
- `value` - Value to hash

**Returns:** String - SHA-256 hash (64 hex characters)

**APOC Compatible:** `apoc.util.sha256`

**Examples:**
```cypher
RETURN util.sha256("hello") AS hash
// Returns: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
```

---

#### util.compress

Compress data using gzip or deflate.

**Syntax:** `util.compress(data, [algorithm])`

**Parameters:**
- `data` - String data to compress
- `algorithm` (optional) - "gzip" (default) or "deflate"

**Returns:** String - Base64-encoded compressed data

**APOC Compatible:** `apoc.util.compress`

**Examples:**
```cypher
RETURN util.compress("Hello World!") AS compressed
// Returns: base64-encoded gzip data
```

---

#### util.decompress

Decompress base64-encoded compressed data.

**Syntax:** `util.decompress(data, [algorithm])`

**Parameters:**
- `data` - Base64-encoded compressed data
- `algorithm` (optional) - "gzip" (default) or "deflate"

**Returns:** String - Decompressed string

**APOC Compatible:** `apoc.util.decompress`

**Examples:**
```cypher
WITH util.compress("Hello World!") AS compressed
RETURN util.decompress(compressed) AS original
// Returns: "Hello World!"
```

---

#### util.validate

Validate a condition and throw an error if false.

**Syntax:** `util.validate(predicate, message)`

**Parameters:**
- `predicate` - Boolean condition to check
- `message` - Error message if validation fails

**Returns:** Boolean - true if validation passes

**APOC Compatible:** `apoc.util.validate`

**Examples:**
```cypher
RETURN util.validate(1 > 0, "Value must be positive") AS valid
// Returns: true

RETURN util.validate(false, "This will throw!") AS valid
// Throws: IllegalArgumentException with message "This will throw!"
```

---

### Aggregation Functions

#### agg.first

Get the first non-null element from a list.

**Syntax:** `agg.first(list)`

**Parameters:**
- `list` - List of values

**Returns:** Any - First non-null value

**APOC Compatible:** `apoc.agg.first`

**Examples:**
```cypher
RETURN agg.first([null, "a", "b"]) AS result
// Returns: "a"
```

---

#### agg.last

Get the last non-null element from a list.

**Syntax:** `agg.last(list)`

**Parameters:**
- `list` - List of values

**Returns:** Any - Last non-null value

**APOC Compatible:** `apoc.agg.last`

**Examples:**
```cypher
RETURN agg.last(["a", "b", "c"]) AS result
// Returns: "c"
```

---

#### agg.median

Calculate the median of a list of numbers.

**Syntax:** `agg.median(list)`

**Parameters:**
- `list` - List of numbers

**Returns:** Double - Median value

**APOC Compatible:** `apoc.agg.median`

**Examples:**
```cypher
RETURN agg.median([1, 2, 3, 4, 5]) AS result
// Returns: 3.0

RETURN agg.median([1, 2, 3, 4]) AS result
// Returns: 2.5
```

---

#### agg.statistics

Get full statistics for a list of numbers.

**Syntax:** `agg.statistics(list)`

**Parameters:**
- `list` - List of numbers

**Returns:** Map - Statistics map with count, min, max, sum, mean, stdev, median

**APOC Compatible:** `apoc.agg.statistics`

**Examples:**
```cypher
RETURN agg.statistics([1, 2, 3, 4, 5]) AS stats
// Returns: {count: 5, min: 1.0, max: 5.0, sum: 15.0, mean: 3.0, stdev: 1.414, median: 3.0}
```

---

### Merge Procedures

#### merge.relationship

Create or match a relationship between two nodes.

**Syntax:** `CALL merge.relationship(startNode, relType, matchProps, createProps, endNode) YIELD rel`

**Parameters:**
- `startNode` - Starting node
- `relType` - Relationship type name
- `matchProps` - Properties to match on
- `createProps` - Properties to set on create
- `endNode` - Ending node

**Returns:** `rel` - The merged relationship

**APOC Compatible:** `apoc.merge.relationship`

**Examples:**
```cypher
MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"})
CALL merge.relationship(a, "KNOWS", {}, {since: 2024}, b) YIELD rel
RETURN rel

// With batch processing:
UNWIND $batch AS row
MATCH (a), (b) WHERE elementId(a) = row.source AND elementId(b) = row.target
CALL merge.relationship(a, row.relType, {}, row.props, b) YIELD rel
RETURN count(rel)
```

---

#### merge.node

Create or match a node by labels and properties.

**Syntax:** `CALL merge.node(labels, matchProps, createProps) YIELD node`

**Parameters:**
- `labels` - List of labels
- `matchProps` - Properties to match on
- `createProps` - Properties to set on create

**Returns:** `node` - The merged node

**APOC Compatible:** `apoc.merge.node`

**Examples:**
```cypher
CALL merge.node(["Person"], {email: "alice@example.com"}, {name: "Alice", created: date.currentTimestamp()}) YIELD node
RETURN node
```

---

### Algorithm Procedures

#### algo.dijkstra

Find the shortest weighted path between two nodes using Dijkstra's algorithm.

**Syntax:** `CALL algo.dijkstra(startNode, endNode, relType, weightProperty, [direction]) YIELD path, weight`

**Parameters:**
- `startNode` - Starting node
- `endNode` - Target node
- `relType` - Relationship type to traverse
- `weightProperty` - Edge property to use as weight
- `direction` (optional) - Traversal direction ("OUT", "IN", "BOTH", default: "BOTH")

**Returns:**
- `path` - The shortest path
- `weight` - Total path weight

**APOC Compatible:** `apoc.algo.dijkstra`

**Examples:**
```cypher
MATCH (a:City {name: 'New York'}), (b:City {name: 'Los Angeles'})
CALL algo.dijkstra(a, b, 'ROAD', 'distance') YIELD path, weight
RETURN path, weight
```

---

#### algo.astar

Find the shortest path using A* algorithm with optional geographic heuristics.

**Syntax:** `CALL algo.astar(startNode, endNode, relType, weightProperty, [latProperty], [lonProperty]) YIELD path, weight`

**Parameters:**
- `startNode` - Starting node
- `endNode` - Target node
- `relType` - Relationship type to traverse
- `weightProperty` - Edge property to use as weight
- `latProperty` (optional) - Node property for latitude (for geographic heuristic)
- `lonProperty` (optional) - Node property for longitude (for geographic heuristic)

**Returns:**
- `path` - The shortest path
- `weight` - Total path weight

**APOC Compatible:** `apoc.algo.aStar`

**Examples:**
```cypher
MATCH (a:City {name: 'Seattle'}), (b:City {name: 'Miami'})
CALL algo.astar(a, b, 'FLIGHT', 'distance', 'lat', 'lon') YIELD path, weight
RETURN path, weight
```

---

#### algo.allSimplePaths

Find all simple paths (without repeated nodes) between two nodes.

**Syntax:** `CALL algo.allsimplepaths(startNode, endNode, relTypes, maxDepth) YIELD path`

**Parameters:**
- `startNode` - Starting node
- `endNode` - Target node
- `relTypes` - Relationship type(s) to traverse (string or list)
- `maxDepth` - Maximum path length

**Returns:** `path` - Each simple path found

**APOC Compatible:** `apoc.algo.allSimplePaths`

**Examples:**
```cypher
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CALL algo.allsimplepaths(a, b, 'KNOWS', 5) YIELD path
RETURN path
```

---

### Path Expansion Procedures

#### path.expand

Expand paths from a starting node following relationship types and node labels.

**Syntax:** `CALL path.expand(startNode, relTypes, labelFilter, minDepth, maxDepth) YIELD path`

**Parameters:**
- `startNode` - Starting node
- `relTypes` - Relationship types (pipe-separated string or list, e.g., "KNOWS|WORKS_WITH")
- `labelFilter` - Node labels to include (pipe-separated string or list)
- `minDepth` - Minimum path length (non-negative)
- `maxDepth` - Maximum path length

**Returns:** `path` - Each expanded path

**APOC Compatible:** `apoc.path.expand`

**Examples:**
```cypher
MATCH (a:Person {name: 'Alice'})
CALL path.expand(a, 'KNOWS|WORKS_WITH', 'Person', 1, 3) YIELD path
RETURN path
```

---

#### path.expandConfig

Expand paths using a configuration map for more control.

**Syntax:** `CALL path.expandconfig(startNode, config) YIELD path`

**Parameters:**
- `startNode` - Starting node
- `config` - Configuration map with options:
  - `relationshipFilter` - Relationship types (string or list)
  - `labelFilter` - Node labels (string or list)
  - `minLevel` - Minimum depth (default: 0)
  - `maxLevel` - Maximum depth (default: unlimited)
  - `bfs` - Use BFS (true) or DFS (false) (default: true)
  - `limit` - Maximum number of paths to return

**Returns:** `path` - Each expanded path

**APOC Compatible:** `apoc.path.expandConfig`

**Examples:**
```cypher
MATCH (a:Person {name: 'Alice'})
CALL path.expandconfig(a, {
  relationshipFilter: 'KNOWS|WORKS_WITH',
  labelFilter: 'Person',
  minLevel: 1,
  maxLevel: 3,
  bfs: true,
  limit: 100
}) YIELD path
RETURN path
```

---

#### path.subgraphNodes

Get all nodes reachable from a starting node within configured constraints.

**Syntax:** `CALL path.subgraphnodes(startNode, config) YIELD node`

**Parameters:**
- `startNode` - Starting node
- `config` - Configuration map:
  - `relationshipFilter` - Relationship types
  - `labelFilter` - Node labels
  - `maxLevel` - Maximum depth

**Returns:** `node` - Each reachable node

**APOC Compatible:** `apoc.path.subgraphNodes`

**Examples:**
```cypher
MATCH (a:Person {name: 'Alice'})
CALL path.subgraphnodes(a, {relationshipFilter: 'KNOWS', maxLevel: 3}) YIELD node
RETURN node.name
```

---

#### path.subgraphAll

Get all nodes and relationships reachable from a starting node.

**Syntax:** `CALL path.subgraphall(startNode, config) YIELD nodes, relationships`

**Parameters:**
- `startNode` - Starting node
- `config` - Configuration map (same as subgraphNodes)

**Returns:**
- `nodes` - List of all reachable nodes
- `relationships` - List of all traversed relationships

**APOC Compatible:** `apoc.path.subgraphAll`

**Examples:**
```cypher
MATCH (a:Person {name: 'Alice'})
CALL path.subgraphall(a, {relationshipFilter: 'KNOWS', maxLevel: 2}) YIELD nodes, relationships
RETURN size(nodes) AS nodeCount, size(relationships) AS relCount
```

---

#### path.spanningTree

Get a spanning tree from the start node to all reachable nodes.

**Syntax:** `CALL path.spanningtree(startNode, config) YIELD path`

**Parameters:**
- `startNode` - Starting node
- `config` - Configuration map (same as subgraphNodes)

**Returns:** `path` - Each path in the spanning tree

**APOC Compatible:** `apoc.path.spanningTree`

**Examples:**
```cypher
MATCH (root:Category {name: 'Root'})
CALL path.spanningtree(root, {relationshipFilter: 'HAS_CHILD', maxLevel: 5}) YIELD path
RETURN path
```

---

### Meta/Schema Procedures

#### meta.graph

Get a virtual graph representing the database schema structure.

**Syntax:** `CALL meta.graph() YIELD nodes, relationships`

**Returns:**
- `nodes` - Virtual nodes representing vertex types with their counts and properties
- `relationships` - Virtual relationships representing edge types with their counts

**APOC Compatible:** `apoc.meta.graph`

**Examples:**
```cypher
CALL meta.graph() YIELD nodes, relationships
RETURN nodes, relationships
```

---

#### meta.schema

Get detailed schema information including all types and properties.

**Syntax:** `CALL meta.schema() YIELD value`

**Returns:** `value` - Map containing:
- `nodeLabels` - List of vertex types with their properties
- `relationshipTypes` - List of edge types with their properties

**APOC Compatible:** `apoc.meta.schema`

**Examples:**
```cypher
CALL meta.schema() YIELD value
RETURN value.nodeLabels AS nodeTypes
```

---

#### meta.stats

Get database statistics including counts of nodes and relationships.

**Syntax:** `CALL meta.stats() YIELD value`

**Returns:** `value` - Map containing:
- `labelCount` - Number of node labels
- `relTypeCount` - Number of relationship types
- `nodeCount` - Total number of nodes
- `relCount` - Total number of relationships
- `labels` - Map of label to count
- `relTypes` - Map of relationship type to count

**APOC Compatible:** `apoc.meta.stats`

**Examples:**
```cypher
CALL meta.stats() YIELD value
RETURN value.nodeCount AS nodes, value.relCount AS relationships
```

---

#### meta.nodeTypeProperties

Get property information for each node type.

**Syntax:** `CALL meta.nodetypeproperties() YIELD nodeType, propertyName, propertyTypes, mandatory`

**Returns:**
- `nodeType` - Name of the vertex type
- `propertyName` - Name of the property
- `propertyTypes` - List of property types
- `mandatory` - Whether the property is required

**APOC Compatible:** `apoc.meta.nodeTypeProperties`

**Examples:**
```cypher
CALL meta.nodetypeproperties() YIELD nodeType, propertyName, propertyTypes
RETURN nodeType, propertyName, propertyTypes
```

---

#### meta.relTypeProperties

Get property information for each relationship type.

**Syntax:** `CALL meta.reltypeproperties() YIELD relType, propertyName, propertyTypes, mandatory`

**Returns:**
- `relType` - Name of the edge type
- `propertyName` - Name of the property
- `propertyTypes` - List of property types
- `mandatory` - Whether the property is required

**APOC Compatible:** `apoc.meta.relTypeProperties`

**Examples:**
```cypher
CALL meta.reltypeproperties() YIELD relType, propertyName, propertyTypes
RETURN relType, propertyName, propertyTypes
```

---

## Migration Guide

### Converting APOC Queries to ArcadeDB

**Good news: In most cases, no changes are required!**

ArcadeDB automatically supports the `apoc.` prefix, so your existing Neo4j/APOC queries will work without modification:

```cypher
-- This Neo4j/APOC query works directly in ArcadeDB
RETURN apoc.text.join(["a", "b"], ",")

-- This procedure call also works directly
CALL apoc.merge.relationship(a, "KNOWS", {}, {}, b) YIELD rel
```

### Optional: Removing the APOC Prefix

If you prefer to use the cleaner ArcadeDB-native syntax, you can optionally remove the `apoc.` prefix:

1. **Functions** - Remove the `apoc.` prefix
   ```cypher
   -- Neo4j/APOC style (works in ArcadeDB)
   RETURN apoc.text.join(["a", "b"], ",")

   -- ArcadeDB native style (also works)
   RETURN text.join(["a", "b"], ",")
   ```

2. **Procedures** - Remove the `apoc.` prefix
   ```cypher
   -- Neo4j/APOC style (works in ArcadeDB)
   CALL apoc.merge.relationship(a, "KNOWS", {}, {}, b) YIELD rel

   -- ArcadeDB native style (also works)
   CALL merge.relationship(a, "KNOWS", {}, {}, b) YIELD rel
   ```

3. **Function signatures are 100% compatible** - Parameters are in the same order with the same semantics

### Unsupported APOC Functions

Some APOC procedures are not yet implemented in ArcadeDB:

- Periodic/batch operations (`apoc.periodic.*`) - Use ArcadeDB's transaction API
- Schema modification operations (`apoc.schema.*`) - Use ArcadeDB's Schema API
- Refactoring procedures (`apoc.refactor.*`) - Use ArcadeDB's native capabilities
- Export/import procedures (`apoc.export.*`, `apoc.import.*`) - Use ArcadeDB's native import/export

**Note:** Many common APOC functions are now supported, including:
- Graph algorithms (`algo.dijkstra`, `algo.astar`, `algo.allsimplepaths`)
- Path expansion (`path.expand`, `path.expandconfig`, `path.subgraphnodes`, etc.)
- Schema introspection (`meta.graph`, `meta.schema`, `meta.stats`, etc.)

---

## Version History

| Version | Changes |
|---------|---------|
| 25.x | Initial implementation of built-in functions |

---

## Support

For issues or feature requests related to Cypher functions, please file an issue on the [ArcadeDB GitHub repository](https://github.com/ArcadeData/arcadedb).
