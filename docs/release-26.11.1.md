# ArcadeDB v.26.11.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.11.1 development cycle, so the release notes are ready at tag time.

## Breaking Changes (migration notes)

### Cypher: durations are not comparable, and a local datetime is not a zoned one (#10200)

Cypher now compares temporal values as Neo4j does:

- two durations are equal or not, component by component, but are **not comparable**: `<` and `>` answer `null`, and
  `<=` / `>=` answer `true` only for two equal durations, `null` otherwise. `ORDER BY`, `min()` and `max()` still order
  them, by average length (a month counts 365.2425 / 12 days), then by months, then by days;
- a `localdatetime()` and a `datetime()` are two types: `=` is `false`, `<>` is `true` and `<` / `>` are `null`;
- values of two temporal types sort by type in `ORDER BY` (`DATETIME`, `LOCAL DATETIME`, `DATE`, `TIME`, `LOCAL TIME`,
  `DURATION`, all after paths and before strings) instead of failing with "Cannot compare DateTime with CypherDate".

**Who sees a different answer.** A range filter on a duration, `WHERE s.d > duration({days: 2})`, used to compare
component by component and now keeps no row, with or without an index on the property. Filter on a number instead,
such as the duration's average length in seconds, `s.d.months * 2629746 + s.d.days * 86400 + s.d.seconds > 172800`, or a
single component when the durations hold nothing else (`s.d.days > 2` for durations of whole days). A `DATETIME` property read from the database is the exception to the
second point: ArcadeDB stores it as an instant and it still compares with a zoned `datetime()` literal or parameter by
instant, so `WHERE e.createdAt > datetime('2024-01-01T00:00Z')` is unchanged. So does a `java.time.LocalDateTime` query
parameter.

### Cypher: duration arithmetic is exact, and calendar units follow the local calendar (#10198)

- `duration * number` and `duration / number` are exact down to the nanosecond, truncated toward zero:
  `duration('P1D') / 24` is `PT1H` (it was `PT59M59.999999999S`). An `INTEGER` operand is no longer rounded through a
  double, and a `FLOAT` operand is the decimal it is written as.
- `duration.between()`, `duration.inMonths()` and `duration.inDays()` count months and days on the calendar of the first
  argument's time zone: across a daylight saving change, noon to noon is `P1D` (it was `PT23H`).
- `sum()` and `avg()` accept durations (`sum()` in SQL too). Mixing numbers and durations in one aggregation is an error.

### Cypher: temporal constructors (#10199)

- A `millisecond`, `microsecond` or `nanosecond` given with a base value (`time:`, `datetime:`, ...) replaces the whole
  fraction of second: `localtime({time: localtime('12:30:45.123456789'), nanosecond: 5})` is `12:30:45.000000005`
  (it was `12:30:45.123456005`). The `*.truncate()` adjustment map is unchanged.
- `datetime({epochSeconds | epochMillis, timezone})` reads the instant in that time zone (the zone was ignored).
- `date({date: ..., dayOfWeek: n})` moves the date within its ISO week, and `dayOfQuarter` within its quarter.

### PostgreSQL wire: `regtype` and `format_type()` print PostgreSQL's type names (#10228)

`format_type()` and the `regtype` cast now answer the name PostgreSQL prints for a type - `integer`,
`character varying`, `double precision`, `boolean` - instead of its internal name (`int4`, `varchar`, `float8`, `bool`),
as PostgreSQL does and as `psql`'s `\d` shows in its Type column. `pg_type.typname` is unchanged. A client that compared
the old text, such as `atttypid::regtype = 'int4'` written as a string comparison, sees the SQL name instead; compare OIDs
(`atttypid = 'int4'::regtype`) to be independent of the spelling.

### SQL: string methods count and cut characters, not UTF-16 code units (#10248)

`length()`, `left()`, `right()`, `subString()`, `indexOf()`, `lastIndexOf()` and `charAt()` count Unicode characters, as
openCypher's `size()`, `left()`, `right()` and `substring()` already did (#8998). A character outside the Basic Multilingual
Plane, such as an emoji, is one character: `'\uD83D\uDE00a'.length()` is 2 (it was 3), `.left(1)` is the emoji (it was the high
half of its surrogate pair, which compared equal to `'?'`) and `.indexOf('a')` is 1 (it was 2). A search never matches inside a
surrogate pair. Text made of characters in the Basic Multilingual Plane, which is almost all text, answers exactly as before.

**Who sees a different answer.** A query that keeps a position or a length in a property and uses it later on a text with
emojis or other characters outside the Basic Multilingual Plane (`WHERE s.length() > 140` on tweets, `s.substring(0, 280)`).
The text.* functions (`text.indexOf()`, `text.charAt()`) are unchanged: they follow APOC, which counts UTF-16 code units.

**Cost.** `length()` is constant time on Latin-1 text, which is how the JVM stores most of it. A position past the start of text with other characters (`charAt(i)`, `indexOf(s, i)`) is found by walking to it, so a query that reads every character of a long text of that kind by position costs more than before; `left()`, `right()` and `subString()` cost what they cut.

## Fixes

- **#10249** A Graph Analytical View no longer ends up a few edges short when several threads commit at the same time and some
  of the commits delete edges. A transaction reported its change to the view after releasing its locks, so two commits that
  conflicted could reach the view in the opposite order, and an edge created in the record slot a deleted one had just freed
  (the same RID) was withdrawn by the deletion that arrived after it. The changes now reach the view in the order their
  transactions committed, for `SYNCHRONOUS` (broken since the views exist) and `ASYNCHRONOUS` (since #10212) alike.
  `OrderedAfterCommitCallback` gives any after-commit callback the same guarantee. A commit on a table the view covers now
  returns once the commits ahead of it have reached the view (a few microseconds when they are running; a transaction that
  never reports, because its thread died, costs the commits behind it one wait of 30 seconds, logged as a WARNING, and is then
  skipped, and its change reaches the view out of order if it ever comes: the commit order is guaranteed until such a WARNING is logged;
  `arcadedb.gavCommitOrderMaxWait` sets the wait in milliseconds, 0 to not wait at all).
- **#10188** A `NOTUNIQUE` LSM-tree index no longer returns entries its records do not have once it has been compacted:
  `TypeIndex.iterator()`, `countEntries()`, range scans and `ORDER BY` on the indexed property returned extra rows after
  updates. A key with many values is stored in chunks that can run over a data page of the compacted series, and the scan
  applied the deletions of the newer pages to the first page of the key only. Point lookups were not affected.
- **#10228** PostgreSQL wire: `psql`'s `\dt`, `\d <table>` and `\d+ <table>`, and psqlodbc's `SQLColumns` and
  `SQLTables(SQL_ALL_TABLE_TYPES)`, answer from the emulated `pg_catalog` instead of empty results or a syntax error. The
  catalog reads parenthesised joins, applies join conditions (an INNER JOIN's `ON` filters, a LEFT JOIN that does not
  match reads NULL), models `pg_am` and the column defaults of `pg_attrdef`, rewrites `OPERATOR(pg_catalog.=)` and
  `COLLATE`, and evaluates `UNION`/`INTERSECT`/`EXCEPT` over catalog queries and literal `SELECT`s, aggregates with or
  without `GROUP BY`, and correlated sub-selects. Two columns named alike (`SELECT NULL, NULL, relkind`) are both returned.
  A catalog query that would take more work than one statement may spend fails with SQLSTATE `54000` rather than
  answering empty.

- **#10199** `datetime()` reads years outside 0000-9999 (`'-0001-12-31T23:59:59Z'`, `'+10000-01-01T00:00Z'`), and
  `time()` accepts a zone name as its `timezone` (the offset that zone has now).
- **#10203** `algo.triangleCount` and the OLTP path of `algo.localClusteringCoefficient` count triangles and degrees on the
  simple undirected graph: a self-loop, a parallel edge or an edge in both directions no longer adds triangles. The
  triangle count also no longer allocates an `n x n` bit matrix, so its working memory is the adjacency.
