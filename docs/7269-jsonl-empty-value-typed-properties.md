# #7269 - An empty JSONL value aborts the graph import for int/long/double/vector/list

Follow-up to #7265, which fixed the same defect shape for DATETIME one level up, in
`GraphImporter.readProperty()`.

## Finding ledger

- [x] 1. `JsonlRecordReader.getInt` - fixed, on both the vertex and the edge-source pass
- [x] 2. `JsonlRecordReader.getLong` - fixed, both passes
- [x] 3. `JsonlRecordReader.getDouble` - fixed, both passes
- [x] 4. `JsonlRecordReader.getFloatArray` - fixed, both passes
- [x] 5. `JsonlRecordReader.getList` - fixed, both passes
- [x] 6. Done: the five overrides now share one `notSet(attribute)` predicate instead of each
       repeating `isNull(...)`

## Root cause

`GraphImporter.RecordReader` declares default accessors that all treat an empty value as
"not set" (`GraphImporter.java:784-833`):

```java
default int getInt(final String attribute) {
  final String v = get(attribute);
  return v != null && !v.isEmpty() ? Integer.parseInt(v) : 0;
}
```

`JsonlRecordReader` overrides all five typed accessors and guards only against
`json.isNull(attribute)`, which is `!has(name) || isJsonNull()` (`JSONObject.java:576-578`) and
therefore **false** for an explicit `""`. The empty string then reaches
`JSONObject.getInt/getLong/getDouble/getJSONArray`, whose `getAsNumber()` on a
`JsonPrimitive("")` throws `NumberFormatException`, rethrown as `JSONException`
(`JSONObject.java:257-264`, `:280`, `:326`). `GraphImporter.readInt/readLong/readDouble` and the
`FLOAT_ARRAY`/`LIST` branches of `readProperty` catch it and rethrow it as the `badValue`
`IllegalArgumentException` from inside the row loop, ending the import.

## Completeness

### 1. The invariant

**An empty value for an `int`/`long`/`double`/`vector`/`list` property never aborts a graph
import: it means "not set" on every `RecordSource`, exactly as it already does on the interface
defaults and for DATETIME after #7265.**

### 2. Enumerate every way to violate it

Every `RecordReader` implementation in the tree:

```
$ grep -rn --include='*.java' "implements GraphImporter.RecordReader" integration/src/main/java
XmlRowSource.java:138:  private static class AttrRecordReader implements GraphImporter.RecordReader {
XmlRowSource.java:154:  private static class MapRecordReader  implements GraphImporter.RecordReader {
CsvRowSource.java:99:   private static class CsvRecordReader  implements GraphImporter.RecordReader {
JsonlRowSource.java:65: private static class JsonlRecordReader implements GraphImporter.RecordReader {
```

Which typed accessors each one overrides:

```
$ grep -rn --include='*.java' "public int getInt(\|public long getLong(\|public double getDouble(\|public float\[\] getFloatArray(\|public List<Object> getList(" integration/src/main/java
CsvRowSource.java:108:    public int getInt(
XmlRowSource.java:146:    public int getInt(
XmlRowSource.java:166:    public int getInt(
JsonlRowSource.java:91:   public int getInt(
JsonlRowSource.java:96:   public long getLong(
JsonlRowSource.java:101:  public double getDouble(
JsonlRowSource.java:110:  public float[] getFloatArray(
JsonlRowSource.java:122:  public List<Object> getList(
```

Sibling sweep - every use of the `isNull(...)`-as-"not set" predicate in the module:

```
$ grep -rn --include='*.java' "isNull(" integration/src/main/java
JsonlRowSource.java:92,97,102,111,123          <- the five overrides, all affected
format/JsonlImporterFormat.java:360            <- `aliases` array, not a typed-property accessor
Neo4jImporter.java:278,440,713                 <- `label`/`labels`, not typed-property accessors
```

Consumers of the accessors - both the vertex pass and the edge-source pass:

```
$ grep -n "readInt(record\|readLong(record\|readDouble(record\|readProperty(record" integration/src/main/java/com/arcadedb/integration/importer/graph/GraphImporter.java
1129:  final Object val = readProperty(record, pd);                         <- vertex pass
1367:  ...intProps...add(readInt(record, pd));                              <- edge-source pass
1372:  ...longProps...add(readLong(record, pd));                            <- edge-source pass
1377:  ...doubleProps...add(readDouble(record, pd));                        <- edge-source pass
1384:  ...objProps...add(readProperty(record, pd));                         <- edge-source pass (vector/list)
```

### 3. Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| JSONL vertex pass -> `intProperty` -> `JsonlRecordReader.getInt` | yes | yes |
| JSONL vertex pass -> `longProperty` -> `getLong` | yes | yes |
| JSONL vertex pass -> `doubleProperty` -> `getDouble` | yes | yes |
| JSONL vertex pass -> `floatArrayProperty` -> `getFloatArray` | yes | yes |
| JSONL vertex pass -> `listProperty` -> `getList` | yes | yes |
| JSONL edge-source pass -> `intProperty`/`longProperty`/`doubleProperty` -> `readInt`/`readLong`/`readDouble` | yes | yes |
| JSONL edge-source pass -> `floatArrayProperty`/`listProperty` -> `readProperty` default branch | yes | yes |
| CSV source, all five types | argued: unaffected | yes (pinned) |
| XML source (`AttrRecordReader`, `MapRecordReader`), all five types | argued: unaffected | yes (pinned) |
| A present-but-malformed JSONL value | argued: still a data error, must keep aborting | yes (regression) |
| `JsonlImporterFormat`, `Neo4jImporter` `isNull(...)` uses | argued: out of scope | n/a |

Arguments, with the evidence:

- **CSV.** `CsvRecordReader.get` maps `""` to `null` (`CsvRowSource.java:102-106`) and its `getInt`
  override carries the `!v.isEmpty()` guard (`:107-111`). `getLong`, `getDouble`, `getFloatArray`
  and `getList` are not overridden, so they use the interface defaults, which read through the
  same guarded `get()`. No path from an empty CSV cell to a parse.
- **XML.** `AttrRecordReader` and `MapRecordReader` override only `get` and `getInt`; the `getInt`
  overrides carry the `!v.isEmpty()` guard (`XmlRowSource.java:145-149`, `:165-169`). The other
  four accessors are the interface defaults, which guard on `isEmpty()` themselves. An empty XML
  attribute or element therefore reaches `0`/`null`, not a parse.
- **`JsonlImporterFormat:360` / `Neo4jImporter:278,440,713`.** These test `isNull` on `aliases`,
  `label` and `labels` - string and array attributes read through `getString`/`getJSONArray`
  outside the `RecordReader` contract, in importers that do not dispatch on a declared property
  type at all. An empty value there is a string or an absent label, not a number that fails to
  parse. Different contract, no defect of this shape.
- **A malformed value.** #7265 settled that empty means "not set" while a value that is present
  and not parseable stays a data error. This fix keeps that split: `isEmpty()`, not `isBlank()`,
  and only for a `String`, so a JSON `0`, `false` or `[]` is a real value.

### 4. Test per entry point

`GraphImporterEmptyValueTest` - one test per fixed row, plus the two pinning tests and the
malformed-value regression test.

### 5. Reachability

`JsonlRowSource` is public API constructed by callers (`new JsonlRowSource(path)` /
`JsonlRowSource.from(dir, file)`) and by `GraphImporterEmptyDatetimeTest`; `forEach` builds the
`JsonlRecordReader` on every import (`JsonlRowSource.java:53`), and `GraphImporter.run()` drives
it through `readProperty` on the live vertex and edge passes. Nothing gates it behind a flag.

### 6. Residual risk

The fix changes `JsonlRecordReader` only. It does not change what an empty value means for a
`String` or `BOOLEAN` property (`get()` already returns `""` verbatim for a string, and
`"True".equalsIgnoreCase("")` is already `false`), and it does not touch the older
`JsonlImporterFormat` / `Neo4jImporter` importers, which have no typed-property contract. A
whitespace-only value (`" "`) is still a data error on every type, deliberately, matching #7265.

## The fix

`JsonlRowSource.JsonlRecordReader` gains one private predicate and all five typed overrides route
through it:

```java
private boolean notSet(final String attribute) {
  if (json.isNull(attribute))
    return true;
  return json.opt(attribute) instanceof String text && text.isEmpty();
}
```

`isNull()` still covers absent and explicit JSON `null`; the second clause adds the empty string.
The `String` narrowing is deliberate - a JSON `0`, `false` or `[]` is a value in its own right and
must not become "not set", and `isEmpty()` rather than `isBlank()` keeps a whitespace-only value a
data error, the split #7265 settled for DATETIME.

`GraphImporter` is unchanged: `readInt`/`readLong`/`readDouble` and the `FLOAT_ARRAY`/`LIST`
branches of `readProperty` already drop a `null` and already accept a `0`, so both passes get the
behaviour from the accessor alone.

## Test results

`integration/src/test/java/com/arcadedb/integration/importer/GraphImporterEmptyValueTest.java`,
12 tests:

- 5 vertex-pass cases, one per overridden accessor (int, long, double, vector, list), each with a
  blank row, a populated row and a row where the attribute is absent
- 2 edge-source-pass cases: the numeric one reaches `readInt`/`readLong`/`readDouble`, the
  vector/list one reaches `readProperty`'s default branch
- 2 pinning cases: CSV and XML, all five types, blank and populated
- 1 regression: a malformed value and a whitespace-only value both still abort and still name the
  property
- 1 regression: JSON `null`, JSON `0` and `[]` keep their own meaning
- 1 end-to-end: a blank optional column does not abort the import

Before the fix: `Tests run: 12, Failures: 1, Errors: 7`. The 8 failures are exactly the 7 rows of
the coverage table marked "yes" plus the end-to-end case; the 4 that passed are the two pins and
the two regressions, which is the evidence that the pins were not silently asserting the bug.

After the fix: `Tests run: 12, Failures: 0, Errors: 0`.

Whole module, no regressions:

```
$ mvn -o test -pl integration -DexcludedGroups=benchmark,slow,vector
Tests run: 275, Failures: 0, Errors: 0, Skipped: 9
BUILD SUCCESS
```

`grep -rn --include='*.java' "JsonlRowSource" .` outside `integration/` returns nothing, so the
integration module is the whole blast radius.

## Impact

An operator loading JSONL with any optional typed column - a nullable score, an embedding only
some rows carry, a tag list - no longer has the import end on the first blank value. The failure
was total, not partial: `GraphImporter` throws from inside the row loop, so every row after the
first blank one was never loaded.

## Adversarial pass

No isolated `Task` subagent was available in this session, so the pass was run by hand against the
tree, with each finding verified by a throwaway probe test rather than by reading. Three findings.

### 1. A stringified JSON array still aborts the import - REAL, out of scope, filed as #7285

The same drift this issue is about survives for a *stringified* array. A throwaway probe on
`{"id": "1", "name": "alice", "score": "7", "embedding": "[0.1,0.2]", "tags": "[\"x\"]"}`, one
property per run:

```
PROBE score     -> OK
PROBE embedding -> Property 'embedding' is declared as a vector but attribute 'embedding' does not
                   hold a numeric array (JSONObject[embedding] is not a JSON array ("[0.1,0.2]"))
PROBE tags      -> Property 'tags' is declared as a list but attribute 'tags' does not hold an
                   array (JSONObject[tags] is not a JSON array ("[\"x\"]"))
```

The interface defaults parse exactly that textual form and document that they do
(`GraphImporter.java:802-833`), so CSV and XML accept it and JSONL does not. It is the next column
of the very file this issue's repro describes - a CSV export whose converter quoted every value.
Out of scope here (this issue is scoped to the *empty* value, and the fix would touch only two of
the five overrides and needs its own tests), so it is filed as **#7285** rather than left for the
reporter.

The probe did produce one kept test: `aQuotedNumberIsStillImported` pins that a quoted `"7"`,
`"9000000000"` and `"4.5"` import identically to their unquoted forms. The new guard sits on
exactly that path, and nothing pinned it before.

### 2. `notSet()` adds an `elementToObject` per typed property per row - REAL, accepted, not a defect

`json.opt(attribute)` runs `JSONObject.elementToObject`, which for a Gson `LazilyParsedNumber` does
three `String.indexOf` scans plus a `Long.parseLong` plus a box - and `json.getInt(attribute)` then
parses the same text again. So the guard doubles the numeric decode of every populated JSONL
numeric property.

Accepted rather than optimized, for two reasons, both checked with a command rather than recalled:

- `elementToObject` is already on this source's per-row path, several times over.
  `grep -n "record\.get(\|identity(record"` over `GraphImporter.java` returns 12 call sites, of
  which `1115`, `1116` (id and nameId, every vertex row), `1355`, `1356` (both endpoints, every
  edge row) and `1516` (every STRING property) run per row and reach `JsonlRecordReader.get`, which
  is itself `json.opt(...)`. The guard adds a call of a class the source already makes per row; it
  does not introduce a new order of cost, and it is small next to the `new JSONObject(line)` full
  Gson parse that precedes it and the page write that follows it.
- The cheap alternatives are worse. Reaching the raw `JsonElement` means importing Gson types into
  the integration module, past the `JSONObject` wrapper that exists to hide them. Testing
  emptiness with `getString(attribute, null)` instead would throw on the `JSONArray` that
  `getFloatArray`/`getList` legitimately receive, and would turn a numeric type error's message
  from "is not a int" into "is not a string" - trading a real diagnostic for a micro-optimization.

### 3. An empty *element* inside a vector, `[0.1, "", 0.3]` - REAL, argued, not filed

`array.getFloat(i)` throws on the empty element and the import aborts. This is deliberate and not
the same defect: the attribute is present and holds an array, so "not set" does not apply, and a
vector with a hole in it has no defensible value to substitute - `0.0f` would silently corrupt an
embedding rather than skip a row. Every source behaves this way, including the interface default
via `VectorUtils.toFloatArray`. No change, no issue.

## Result

- Fixed here: all 7 "yes" rows of the coverage table, 13 tests, whole `integration` module green.
- Filed: **#7285** (stringified array on a JSONL source, vector/list).
- Argued with evidence: CSV, XML, the two older importers, the `opt()` cost, the empty vector
  element.
