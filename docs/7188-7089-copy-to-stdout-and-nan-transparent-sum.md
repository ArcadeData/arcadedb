# #7188 and #7089 - COPY ... TO STDOUT over the Postgres wire, and NaN-transparent TimeSeries SUM/AVG

<https://github.com/ArcadeData/arcadedb/issues/7188>
<https://github.com/ArcadeData/arcadedb/issues/7089>

Two unrelated gaps, each left behind by an earlier fix that scoped itself deliberately: #7188 was split
out of #7180 (the Arrow ADBC bootstrap) as "a feature in its own right", and #7089 was filed from the
review of #7087, which unified the MIN/MAX NaN policy and left SUM/AVG visibly outside it.

## Finding ledger

- [x] 1. `COPY (<query>) TO STDOUT` rejected by the SQL parser, so the Arrow ADBC PostgreSQL driver's
      default read path, `psql \copy` and every bulk-export tool stop at the first query - **implemented**,
      text, CSV and binary, option-list and legacy keyword spellings, simple and extended protocol
- [x] 2. TimeSeries `SUM`/`AVG` poisoned by one NaN sample: the sealed block's declared sum is NaN, the
      block-statistics fast path answers from it, and a whole Grafana bucket goes blank - **fixed**, under
      the one NaN policy, with the count of real samples added to the sealed block statistics so the fast
      path stays exact

## 1. COPY ... TO STDOUT

### What the protocol answers

`PostgresCopyStatement` recognises the statement ahead of the SQL engine, exactly where `SET`/`SHOW` and
the system queries are recognised - "COPY" is no production of ArcadeDB's grammar, so parsing it would be a
guaranteed failure. The source is a parenthesised query, run verbatim in the session's language, or a type
name with an optional column list, which is the same statement with an implicit `SELECT`. Both spellings
PostgreSQL accepts are read: the option list (`FORMAT`, `DELIMITER`, `NULL`, `HEADER`, `QUOTE`, `ESCAPE`,
`FORCE_QUOTE`, `ENCODING`) and the pre-9.0 keywords (`BINARY`, `CSV HEADER`, `DELIMITER AS`, ...), with the
combination rules `ProcessCopyOptions` applies (no `HEADER` in binary, `QUOTE` only in CSV, ...).

The executor's `copyOut` frames the answer: `CopyOutResponse` ('H') with the overall format and one
format code per column, one `CopyData` ('d') per row (plus one for the binary header and one for the
trailer), `CopyDone` ('c'), and the caller's `CommandComplete` tagged `COPY n`. CopyData is buffered and
written in 64 KB batches: one socket write per row is a system call per row on the statement whose whole
point is bulk, and one write per result would hold it all.

On the extended protocol - what `PQexecParams` sends, and therefore what the ADBC driver sends - a COPY is
a portal like any other: `Describe('S')` answers `ParameterDescription` then `NoData`, `Describe('P')`
answers `NoData`, `Execute` streams, a second `Execute` of the same portal is refused with `34000` as
PostgreSQL refuses to run a completed portal. The query INSIDE the COPY is what `Parse` hands to the SQL
engine, so a syntax error in it is reported at `Parse`, the way a plain statement's is.

### Columns are resolved the way Describe resolves them

This is the one decision that is not visible from the protocol text. The ADBC driver prepares and
describes the inner statement first, then decodes the binary COPY stream by the OIDs the description
promised. So `copyOut` resolves the columns with `getColumnsFromQuerySchema` - the same call
`Describe('S')` makes - and only when that cannot answer (a schemaless type with no sample row, another
query language) does it fall back to materializing the rows and naming the columns from them, the way a
plain query does, within the same `arcadedb.postgres.queryMaxRows` bound.

The first case is streamed: no row is held, and the cap does not apply. That is the property a bulk
export needs, and the reason a COPY of a statement that a plain SELECT is refused for (#7034's cap)
succeeds; `Issue7188CopyToStdoutIT.aCopyStreamsPastTheRowCapAPlainSelectIsRefusedBy` pins it.

### Encodings are PostgreSQL's own

The text and CSV row encodings follow `copyto.c` exactly - `CopyAttributeOutText`'s backslash escapes
(the control characters, the backslash, the delimiter), `CopyAttributeOutCSV`'s quoting rule (forced,
equal to the NULL string, holding the delimiter/quote/line break, or `\.` alone on a single-column line) -
because a reader that parses the stream relies on them. `PostgresType.toText` is the text form
`serializeAsText` already produced, extracted so COPY escapes the same string a DataRow carries; the
binary field encoding is `serializeAsBinary` unchanged, since COPY's binary field format (int32 length,
-1 for NULL, then the send-format bytes) is the DataRow's.

### Declined, with `feature_not_supported`

`COPY ... FROM STDIN` (the ingest direction, a separate statement with its own failure modes, out of scope
by the issue's own text), `COPY ... TO 'file'` and `COPY ... TO PROGRAM` (server-side writes and command
execution from a wire client) are refused with SQLSTATE `0A000` and a message that names the alternative.
So is a binary COPY of a column with no binary encoding (an array): a DataRow can mix format codes per
column, a COPY stream cannot. `CopyException` extends `CommandParsingException` so the existing error arms
catch it, and carries its own SQLSTATE so the client is not told "syntax error" for a well-formed
statement it merely cannot have.

### Alternative considered

Materializing every COPY through `browseAndCacheBoundedResultSet`, as every other statement is, would
have been the smaller change and would have kept one path. It would also have capped the one statement
whose purpose is to move a whole type, and would have named columns from rows where the ADBC driver
expects them named from the schema - the exact mismatch that desynchronizes a binary decoder.

## 2. NaN-transparent SUM and AVG

### Policy

NaN is the ABSENT marker (`TimeSeriesNaN`, since #7043), and every aggregate now skips it: `SUM` is the
sum of the real samples, `AVG` divides it by the count of REAL samples, and a window with no real sample
answers ABSENT for both rather than a NaN produced by IEEE arithmetic poisoning a real total. `COUNT`
counts rows, as SQL's `count(*)` does - which is what the SQL push-down maps it from, so no decision was
needed there. Downsampling follows the same rule: the mean of the real samples, ABSENT for a bucket with
none (it used to answer 0.0, a measurement).

This is SQL's `NULL` semantics under `SUM`/`AVG`/`count(*)`, it is what the issue's reporter and a
dashboard want, and it is the only choice that makes `MIN` and `SUM` agree on the same data again. The
PromQL layer is deliberately left out: Prometheus propagates NaN through `sum`/`avg`/`sum_over_time`, and a
PromQL query is expected to answer what Prometheus would. `TimeSeriesNaN`'s class comment records both.

### Why the sealed format changed

The block-statistics fast path answers `SUM`/`AVG` from the block header without decompressing. With
skip-NaN, `AVG` needs the count of real samples per column, and the header carried only the block's
sample count. Three ways to get there were weighed:

1. **Keep the poisoned sum in the header as a "decode me" marker** and take the slow path for any block
   holding a NaN. No format change, exact answers, but a trap: the header's `sum` would mean something
   different from the aggregate's `SUM`, and the next person to "fix" `reduceNumericStats` to skip NaN would
   silently break `AVG` on the fast path.
2. **Skip NaN in the sum, divide by the sample count.** Wrong by construction, and the issue says so.
3. **Record the count of real samples next to the sum.** Chosen. The fast path stays exact for every block,
   the header's four statistics mean what the aggregates mean, and the DEEP check verifies all four.

Each block declares its own layout through its magic: `TSBL` (the pre-#7089 `[min, max, sum]` triplet) or
`TSB2` (`[min, max, sum, count]`). A legacy block is read as it stands: its sum was a plain `+=`, so a
finite sum proves the column held no NaN and its count is the block's, while a NaN sum leaves the count
`COUNT_UNKNOWN`, which routes `SUM`/`AVG` over that block through the values and leaves `MIN`/`MAX`/`COUNT`
on the header. Every rewrite (compaction, downsampling, truncation) writes `TSB2`, recomputing the two
statistics for a column it copies with `COUNT_UNKNOWN`, so the marker cannot outlive the next rewrite. The
file header's version moved from 0 to 1 so that an older build refuses a newer file loudly, instead of
stopping its directory scan at the first magic it does not know and silently losing every block after it;
a version-0 file is read and stamped 1 on its next header rewrite. `LocalTimeSeriesType` now gates on
`>` rather than `!=`, as the mutable bucket's check already did.

### What else moved under the policy

- `MultiColumnAggregationResult` seeds `SUM`/`AVG` at ABSENT and folds with `TimeSeriesNaN.sum`; its
  per-request count is now the count of samples that CONTRIBUTED (rows for `COUNT`, real samples for the
  rest), so `finalizeAvg` divides by the right denominator and an all-NaN bucket keeps ABSENT
- `TimeSeriesVectorOps.sum` skips NaN in both the scalar and the SIMD implementation (a NaN lane is blended
  to the ADD identity, as `min`/`max` already blend theirs), answers ABSENT for an empty or all-NaN range,
  and a new `countPresent` gives the SIMD path its denominator
- the single-column `AggregationResult` path and its cross-shard merge, whose weighted `AVG` merge used to
  multiply a NaN partial by a zero count
- the DEEP integrity check verifies the declared count, and reports nothing for a legacy `COUNT_UNKNOWN`
  column: it is a block of its time, not a damaged one

`Issue7089NaNTransparentSumAvgTest` covers the fold, the statistics reducer, both vector implementations,
partial-result merging, the engine end to end on the mutable path and on the sealed fast path (asserting
the fast path was taken), the SQL push-down, a hand-written version-0 file with legacy blocks (poisoned
and not, decoded and header-answered respectively, then upgraded by a rewrite), a mixed-layout file, the
DEEP check catching a lying count, and downsampling.
