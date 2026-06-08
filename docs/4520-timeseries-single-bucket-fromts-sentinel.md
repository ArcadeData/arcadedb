# Issue #4520 - `aggregate()` anchors single-bucket result to caller-supplied `fromTs`

## Summary

When time-series aggregation runs in single-bucket mode (`bucketIntervalMs <= 0`), the resulting
bucket timestamp was anchored to the caller-supplied `fromTs`. Callers use `Long.MIN_VALUE` as the
"no lower bound" sentinel, so every row's bucket key became `Long.MIN_VALUE`. Downstream consumers
(e.g. `AggregateFromTimeSeriesStep`, which converts the bucket key to a `LocalDateTime`) then
misformatted that sentinel as a real epoch.

## Root Cause

Four single-bucket sites used `fromTs` directly as the bucket key:

- `TimeSeriesEngine.aggregate()` (mutable + sealed merge iterator)
- `TimeSeriesEngine.aggregateMulti()` sequential path (mutable rows)
- `TimeSeriesSealedStore.aggregate()` (single-column sealed blocks)
- `TimeSeriesSealedStore.aggregateMultiBlocks()` (multi-column sealed blocks, no-interval branch)

All rows in single-bucket mode must share one bucket key; that key must be a valid epoch, not the
sentinel.

## Fix

Added a shared helper `TimeSeriesEngine.singleBucketAnchor(long fromTs)`:

```java
static long singleBucketAnchor(final long fromTs) {
  return fromTs == Long.MIN_VALUE ? 0L : fromTs;
}
```

Returns `0L` (Unix epoch) when `fromTs` is the `Long.MIN_VALUE` sentinel, otherwise keeps the
caller-supplied lower bound (unchanged behavior for real bounds). All four sites now route their
single-bucket anchor through this helper, keeping sealed-store and mutable-bucket results consistent
(they must agree so sealed + mutable rows merge into the same bucket).

`0L` was chosen over "min observed ts" because the sealed store and the parallel shard path each
aggregate independently; threading a globally-observed minimum across all of them would be fragile,
and `0L` is the value endorsed by the issue, is a valid epoch, and is identical across every path.

## Tests

New regression test `TimeSeriesSingleBucketAnchorTest`:

- `singleBucketAnchorMapsSentinelToEpoch` - direct unit test of the helper contract (`MIN_VALUE` -> `0L`, real `fromTs` preserved).
- `aggregateSingleBucketWithMinValueFromTsIsNotSentinel` - mutable single-bucket SUM, MIN_VALUE fromTs.
- `aggregateSingleBucketWithRealFromTsKeepsAnchor` - real lower bound still anchors at `fromTs`.
- `aggregateSingleBucketWithRealFromTsKeepsAnchorOnSealedData` - real lower bound preserved on the sealed path after `compactAll()`.
- `aggregateMultiSingleBucketWithMinValueFromTsIsNotSentinel` - multi-column single-bucket.
- `aggregateSingleColumnSingleBucketReadsSealedData` - single-column sealed path after `compactAll()`.
- `aggregateMultiSingleBucketReadsSealedData` - multi-column sealed-store path after `compactAll()`.

Confirmed failing before the fix (bucket key `Long.MIN_VALUE`), passing after.

## Verification

- New test: 7/7 pass.
- Regression suite (78 tests): `TimeSeriesEngineTest`, `TimeSeriesSealedStoreTest`,
  `TimeSeriesAggregationPushDownTest`, `TimeSeriesAccuracyTest`, `TimeSeriesShardTest`,
  `TimeSeriesSQLTest`, `TimeSeriesPhase2SQLTest`, `SQLFunctionTimeBucketTest`,
  `ContinuousAggregateTest`, `TimeSeriesDownsamplingTest` - all pass.
