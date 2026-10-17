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
component by component and now keeps no row, with or without an index on the property. Filter on a number instead:
`WHERE duration.inSeconds(...)`, `s.d.days > 2`. A `DATETIME` property read from the database is the exception to the
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

## Fixes

- **#10199** `datetime()` reads years outside 0000-9999 (`'-0001-12-31T23:59:59Z'`, `'+10000-01-01T00:00Z'`), and
  `time()` accepts a zone name as its `timezone` (the offset that zone has now).
- **#10203** `algo.triangleCount` and the OLTP path of `algo.localClusteringCoefficient` count triangles and degrees on the
  simple undirected graph: a self-loop, a parallel edge or an edge in both directions no longer adds triangles. The
  triangle count also no longer allocates an `n x n` bit matrix, so its working memory is the adjacency.
