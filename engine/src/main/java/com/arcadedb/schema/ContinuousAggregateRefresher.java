/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.DateUtils;

import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.regex.Pattern;

public class ContinuousAggregateRefresher {
  // Allows letters, digits, and underscores only — consistent with ArcadeDB identifier rules.
  // Backtick, dot, hyphen, and other injection-enabling characters are excluded.
  private static final Pattern SAFE_COLUMN_NAME = Pattern.compile("[A-Za-z0-9_]+");

  /**
   * The clauses a SELECT can carry AFTER its WHERE, in the order {@code SelectStatement.toString()} writes them.
   * The first top-level occurrence of any of them is where the WHERE clause ENDS, which is where the bracket that
   * keeps the caller's own predicate intact has to close (#8156).
   */
  private static final String[] CLAUSES_AFTER_WHERE = { "GROUP BY", "ORDER BY", "UNWIND", "SKIP", "LIMIT", "TIMEOUT" };

  /**
   * The key the refresh of a continuous aggregate is registered under in a transaction
   * ({@code TransactionContext.addAfterCompletionCallbackIfAbsent}), so that every change of its source in one transaction asks for
   * one refresh when it ends.
   */
  public static String refreshCallbackKey(final String aggregateName) {
    return "ca-refresh:" + aggregateName;
  }

  /** How many times a caller runs the refresh again for the requests that arrived while it ran, see {@link #incrementalRefresh}. */
  private static final int MAX_FOLLOW_UP_REFRESHES = 5;

  /**
   * Refreshes the aggregate, or has the refresh in progress do it when it ends. #10428: a refresh asked for while another one runs
   * used to be dropped, and the one running may have read the aggregate before the change that asked (a delete recording the range
   * of buckets to recompute): the change then waited for the next refresh, which nothing may ask for. When a range is waiting to be
   * recomputed, the request is remembered and the refresh in progress runs again when it ends. A request that finds no range keeps
   * the cheap skip it always had: an append that commits while a refresh runs records its samples whatever the watermark (issue
   * #10633, see {@link ContinuousAggregateImpl#invalidateLateSamples}), so an append that finds no range has none the refresh in
   * progress could leave behind its new watermark, and the next refresh picks its rows up.
   * <p>
   * The request is made BEFORE the second attempt to begin, so one that arrives as the refresh in progress ends is either seen by
   * it (it looks again after releasing the aggregate) or finds the aggregate free.
   */
  public static void incrementalRefresh(final Database database, final ContinuousAggregateImpl ca) {
    // The first pass is this caller's own; the others are for the requests that arrived while it ran, bounded so that one thread
    // (the committer of an INSERT, say) is not kept refreshing for as long as others keep asking. What is left over stays requested,
    // and the next caller runs it. The same holds when a refresh fails: the request made during it stays, and the next refresh
    // applies it
    for (int pass = 0; pass <= MAX_FOLLOW_UP_REFRESHES; pass++) {
      if (!ca.tryBeginRefresh()) {
        if (ca.getInvalidation() != null) {
          ca.requestRefresh();
          // the refresh in progress may have ended since the attempt, and would not look at the request any more
          if (ca.tryBeginRefresh())
            ca.clearRefreshRequest();
          else {
            LogManager.instance().log(ContinuousAggregateRefresher.class, Level.FINE,
                "Concurrent refresh for continuous aggregate '%s' — already in progress, it runs again when it ends", null,
                ca.getName());
            return;
          }
        } else {
          LogManager.instance().log(ContinuousAggregateRefresher.class, Level.FINE,
              "Skipping concurrent refresh for continuous aggregate '%s' — already in progress", null, ca.getName());
          return;
        }
      } else
        // cleared once the aggregate is held and before anything is read: a request that arrives from here on is for the next run
        ca.clearRefreshRequest();

      try {
        refreshOnce(database, ca);
      } finally {
        ca.endRefresh();
      }
      if (!ca.isRefreshRequested())
        return;
    }
  }

  /** One refresh, run by the caller that holds the aggregate (see {@link ContinuousAggregateImpl#tryBeginRefresh}). */
  private static void refreshOnce(final Database database, final ContinuousAggregateImpl ca) {
    ca.setStatus(MaterializedViewStatus.BUILDING);
    final long startNs = System.nanoTime();
    try {
      final String backingTypeName = ca.getBackingTypeName();
      final String bucketColumn = ca.getBucketColumn();
      // #10634: rows in the backing type beside a watermark never set (see adoptWatermarkFromBackingRows). Before the read below
      final boolean adoptedWatermark = !ca.isWatermarkSet() && !ca.needsCleanRebuild() && adoptWatermarkFromBackingRows(database, ca);
      // ONE read of the pair: the flag decides whether to delete and the timestamp decides what, so reading them
      // through two separate getters could straddle a concurrent advance (found in review).
      final ContinuousAggregateImpl.Watermark startWatermark = ca.currentWatermark();
      final long watermark = startWatermark.timestamp();
      final boolean watermarkSet = startWatermark.set();
      // A pre-#8152 schema whose watermark is the ambiguous 0 brings its duplicate rows with it - see
      // ContinuousAggregateImpl.needsCleanRebuild. This one refresh starts from an empty backing type.
      final boolean cleanRebuild = ca.needsCleanRebuild();
      // #10428: the range samples were removed from since the buckets behind the watermark were computed. Read once: a delete
      // that widens it while this refresh runs makes the clearing below a no-op, and the next refresh recomputes the wider range
      final ContinuousAggregateImpl.Invalidation invalidated = ca.getInvalidation();

      // Validate interpolated names to prevent backtick injection
      if (!SAFE_COLUMN_NAME.matcher(backingTypeName).matches())
        throw new IllegalArgumentException("Unsafe backing type name: '" + backingTypeName + "'");
      if (!SAFE_COLUMN_NAME.matcher(bucketColumn).matches())
        throw new IllegalArgumentException("Unsafe bucket column name: '" + bucketColumn + "'");

      // Where a completed refresh's new watermark is parked until the transaction has actually COMMITTED (found in
      // review). Advancing it inside the lambda left the watermark AHEAD OF THE DATA whenever the commit itself
      // failed - retry exhaustion, say: the rows were rolled back, nothing restored the watermark, and the next
      // refresh trusted it and skipped the very window that had just been lost.
      final Long[] advancedWatermark = new Long[1];

      // #10688: IN A TRANSACTION OF ITS OWN, NEVER THE CALLER'S. Joining one open on the thread (a REFRESH, a refresh() or a CREATE
      // run inside BEGIN) returned without committing anything, and the watermark below was installed and saved over rows the
      // caller's rollback then took back: every bucket behind it was lost for good, with the aggregate reported VALID. The refresh is
      // no more part of the caller's unit of work than the samples it reads are (#7410), and the materialized view refresh made the
      // same choice (#3941). The answer is what says the rows committed: with joinCurrentTx=false it is always a transaction of its
      // own, committed before returning, or an exception
      final boolean committed = database.transaction(() -> {
        // RESET FIRST. database.transaction() retries this very lambda on a NeedRetryException, rolling back in
        // between, and advancedWatermark outlives the attempt that wrote it - so a watermark computed by an attempt
        // that was then DISCARDED would survive into one that committed a smaller result, advancing the watermark
        // past the data that actually exists (found in review). maxBucketTs and maxBucketSeen are re-initialised per
        // attempt because they are declared inside the lambda; this one has to be told.
        advancedWatermark[0] = null;

        // #10428: the buckets behind the watermark that a delete reached. A refresh that rebuilds everything (no watermark yet,
        // or a clean rebuild) recomputes them below anyway
        if (invalidated != null && watermarkSet && !cleanRebuild)
          recomputeInvalidatedBuckets(database, ca, invalidated, watermark);

        // Replace the rows of the current (possibly incomplete) bucket and of all newer buckets. #8152: the guard used to be
        // `watermark > 0`, which skipped the replacement for an aggregate legitimately anchored at the epoch and let that one bucket
        // gain a duplicate on every refresh. Issue #10804: replaced in place, not deleted and inserted again, see ReplacedRows
        final ReplacedRows replaced;
        if (cleanRebuild) {
          database.command("sql", "DELETE FROM `" + backingTypeName + "`");
          replaced = null;
        } else
          replaced = watermarkSet ? ReplacedRows.select(database, ca, bucketsFromSelect(ca), new Date(watermark)) : null;

        // Build the filtered query: append WHERE clause with watermark filter on the source timestamp
        final String filteredQuery = buildFilteredQuery(ca, watermark, watermarkSet && !cleanRebuild);

        // Execute and insert results
        long maxBucketTs = watermark;
        boolean maxBucketSeen = watermarkSet && !cleanRebuild;
        try (final ResultSet rs = database.query("sql", filteredQuery)) {
          while (rs.hasNext()) {
            final Result result = rs.next();

            // Track maximum bucket timestamp for advancing watermark
            final Object bucketVal = result.getProperty(bucketColumn);
            if (bucketVal != null) {
              final long bucketMs = toEpochMs(bucketVal, bucketColumn, ca.getName());
              if (!maxBucketSeen || bucketMs > maxBucketTs) {
                maxBucketTs = bucketMs;
                maxBucketSeen = true;
              }
              saveRow(database, backingTypeName, result, replaced != null ? replaced.take(bucketMs) : null);
            } else
              // no bucket (a sample with no timestamp, which a TIMESERIES type does not store): never one of the rows replaced, whose
              // bucket is at or after the watermark, exactly as the delete it replaces never matched it
              saveRow(database, backingTypeName, result, null);
          }
        }
        if (replaced != null)
          replaced.deleteTheRest();

        // The watermark advances to the max bucket boundary found. The FIRST bucket ever seen installs it even when
        // it is not greater than the initial 0 - that is the whole point of tracking "set" separately (#8152).
        if (maxBucketSeen && (cleanRebuild || !watermarkSet || maxBucketTs > watermark))
          advancedWatermark[0] = maxBucketTs;
      }, false);
      if (!committed)
        throw new IllegalStateException(
            "The refresh of continuous aggregate '" + ca.getName() + "' joined a transaction instead of committing its own");

      // The transaction committed, so the rows the watermark refers to are durable and it can be installed.
      if (advancedWatermark[0] != null)
        ca.setWatermarkTs(advancedWatermark[0]);
      // ... and the buckets of the invalidated range are recomputed, so the record of it can go (see saveConfiguration below)
      if (invalidated != null)
        ca.clearInvalidation(invalidated);
      final long durationMs = (System.nanoTime() - startNs) / 1_000_000;
      ca.recordRefreshSuccess(durationMs);
      ca.updateLastRefreshTime();
      ca.setStatus(MaterializedViewStatus.VALID);

      // Persist updated watermark only if it actually advanced.
      // If saveConfiguration fails, revert the in-memory watermark to the original value
      // so the next refresh re-processes the same window (delete-first design makes it safe).
      // A clean rebuild must persist even if it found no rows to advance the watermark with, so the repair is not
      // repeated on every restart.
      if (advancedWatermark[0] != null || cleanRebuild || invalidated != null || adoptedWatermark) {
        final LocalSchema schema = (LocalSchema) database.getSchema();
        try {
          schema.saveConfiguration();
        } catch (final Exception saveEx) {
          // #8152: restore the FLAG too. setWatermarkTs(watermark) would have left the aggregate claiming a
          // watermark of 0 it never had, which the next refresh would then honour by deleting nothing.
          ca.restoreWatermark(watermark, watermarkSet);
          // #10428: and the range, which the next refresh recomputes again (the recompute deletes before it inserts)
          if (invalidated != null)
            ca.restoreInvalidation(invalidated);
          throw saveEx;
        }
      }

      // The duplicates of the pre-#8152 era are gone with the rows that were just replaced - but only once the
      // schema that records it is on disk. Clearing the flag before that (found by CodeRabbit) meant a failed
      // saveConfiguration left an aggregate with a correct backing type, a watermark rolled back to unset and no
      // flag: the next refresh would have appended a full copy on top of the rows the rebuild had just committed.
      if (cleanRebuild)
        ca.cleanRebuildDone();

    } catch (final Exception e) {
      ca.recordRefreshError();
      ca.setStatus(MaterializedViewStatus.ERROR);
      LogManager.instance().log(ContinuousAggregateRefresher.class, Level.SEVERE,
          "Error refreshing continuous aggregate '%s': %s", e, ca.getName(), e.getMessage());
      throw e;
    }
  }

  /**
   * Takes the newest bucket of the backing type as the watermark of an aggregate whose watermark is not set while its backing type
   * holds rows (issue #10634), and tells whether it did. A refresh from an unset watermark runs the whole defining query and deletes
   * nothing, which is right only for a backing type that is empty: with rows in it, every bucket they hold was added a second time.
   * <p>
   * The watermark is saved with a plain schema save after each refresh, outside the schema session that ships the schema to the
   * followers under HA, while the rows of the refresh are replicated with its transaction. A follower can therefore hold the rows
   * beside a watermark that was never set (an aggregate created over a source that was empty then, refreshed later), and refresh
   * from it once promoted to leader. A schema save that failed after the refresh committed leaves the same state on a single node.
   * <p>
   * The newest bucket of the backing type is the watermark the refresh that wrote it installed: a refresh advances the watermark to
   * the newest bucket it wrote. A watermark that is set but older than the rows (the same two causes) needs nothing: the refresh
   * deletes the buckets at or after it and computes them again.
   * <p>
   * A read of every row of the backing type, done once: the watermark is set by it, and by any refresh that finds a bucket. Only a
   * backing type whose rows all lack the bucket column, which no refresh writes, is read again by every refresh.
   */
  private static boolean adoptWatermarkFromBackingRows(final Database database, final ContinuousAggregateImpl ca) {
    final String backingTypeName = ca.getBackingTypeName();
    final String bucketColumn = ca.getBucketColumn();
    if (!database.getSchema().existsType(backingTypeName))
      return false; // refused with its message by the refresh
    // Read through the records rather than a query: the refresh that follows the creation runs under the write lock of the creation's
    // schema session, where a parallel scan would wait for workers that need the read lock. Only reached while the watermark is not
    // set, when the backing type is empty or holds what the refresh is about to rebuild on top of
    boolean found = false;
    long newest = 0L;
    final Iterator<Record> records = database.iterateType(backingTypeName, false);
    while (records.hasNext()) {
      final Object value = records.next().asDocument().get(bucketColumn);
      if (value == null)
        continue;
      final long bucket = toEpochMs(value, bucketColumn, ca.getName());
      if (!found || bucket > newest) {
        newest = bucket;
        found = true;
      }
    }
    if (!found)
      return false;
    ca.setWatermarkTs(newest);
    LogManager.instance().log(ContinuousAggregateRefresher.class, Level.INFO,
        "Continuous aggregate '%s' had no watermark while its backing type holds rows: resuming from its newest bucket", null,
        ca.getName());
    return true;
  }

  /**
   * Refreshes every continuous aggregate whose source is {@code sourceTypeName}, after a change of its samples: with no transaction
   * open at once, inside one once that transaction ENDS, whether it commits or rolls back (issue #10687). The change is not part of
   * the transaction it runs in (an append commits its own, #7410; a delete or a truncate is not transactional, #10428), so a rollback
   * leaves it in place, and the refresh it owes cannot be conditional on a commit either: an after-commit callback was discarded with
   * the rollback, and left the aggregate describing samples that no longer existed, or missing samples that did. Deferred rather
   * than run at once so that every change of the transaction asks for one refresh. A failing refresh is logged, never raised: the
   * statement that scheduled it did its own work.
   */
  public static void refreshAggregatesOver(final DatabaseInternal database, final String sourceTypeName) {
    final ContinuousAggregate[] aggregates = database.getSchema().getContinuousAggregates();
    if (aggregates.length == 0)
      return;

    final boolean inTransaction = database.isTransactionActive();
    for (final ContinuousAggregate aggregate : aggregates) {
      if (!(aggregate instanceof ContinuousAggregateImpl impl) || !sourceTypeName.equals(impl.getSourceTypeName()))
        continue;
      final Runnable refresh = () -> {
        // run at the end of a transaction, which a close does not wait for: a closed database has nothing to refresh
        if (!database.isOpen())
          return;
        try {
          incrementalRefresh(database, impl);
        } catch (final Exception e) {
          LogManager.instance().log(ContinuousAggregateRefresher.class, Level.WARNING,
              "Error refreshing continuous aggregate '%s' after a change of its source type '%s': %s", e, impl.getName(),
              sourceTypeName, e.getMessage());
        }
      };
      if (inTransaction)
        database.getTransaction().addAfterCompletionCallbackIfAbsent(refreshCallbackKey(impl.getName()), refresh);
      else
        refresh.run();
    }
  }

  /**
   * Recomputes the buckets samples were removed from (issue #10428): deletes their rows from the backing type and runs the defining
   * query over the samples of exactly those buckets. Only the buckets behind the watermark are looked at, the ones at or after it
   * are deleted and recomputed by the refresh anyway.
   * <p>
   * The buckets are computed on the grid the watermark is a start of, not from the interval alone: a bucket start worked out from the
   * interval would be unaligned on any grid but the epoch's (an origin, an offset or a time zone of {@code ts.timeBucket()}), and
   * recompute its first bucket from a part of its samples. A bucket with no row is recomputed too, since an append behind the
   * watermark can add samples to it (issue #10502). A range that reaches back to the first instants is bounded by the rows of the
   * backing type instead, as #10428 did for every range: a bucket that has no row there has nothing a delete could correct.
   * <p>
   * It runs in the transaction of the refresh, so the buckets of a very wide range (two deletes far apart in time are one range, the
   * hull of both) are one transaction of the size of that range.
   */
  private static void recomputeInvalidatedBuckets(final Database database, final ContinuousAggregateImpl ca,
      final ContinuousAggregateImpl.Invalidation invalidated, final long watermark) {
    final String backingTypeName = ca.getBackingTypeName();
    final String bucketColumn = ca.getBucketColumn();
    final long interval = ca.bucketIntervalMs();

    // A bucket starting at b holds the samples in [b, b + interval): it reaches the range when b <= toTs and b + interval > fromTs.
    // Every bucket is `interval` wide: ts.timeBucket() takes fixed units only (s, m, h, d, w), and a time zone whose offset changes
    // (daylight saving), which would make some buckets 23 or 25 hours, is refused rather than approximated
    final StringBuilder where = new StringBuilder("`" + bucketColumn + "` < ?");
    final List<Object> params = new ArrayList<>(3);
    params.add(new Date(watermark));
    if (invalidated.toTs() < watermark) {
      where.append(" AND `").append(bucketColumn).append("` <= ?");
      params.add(new Date(invalidated.toTs()));
    }
    if (interval > 0 && invalidated.fromTs() > Long.MIN_VALUE + interval) {
      where.append(" AND `").append(bucketColumn).append("` > ?");
      params.add(new Date(invalidated.fromTs() - interval));
    }

    // #10502: the buckets an append behind the watermark reached may have no row yet (a bucket no sample was in, one older than every
    // bucket), and a delete may not be the only thing the range holds. Every bucket of the grid the range reaches is recomputed then:
    // the grid is the one the watermark is a start of, since every bucket is `interval` wide. The backing type still bounds the start
    // of a range that reaches back to the first instants (a TRUNCATE, a delete with no lower bound), whose buckets are not on any
    // grid a query could write. The cutoff only tells such a sentinel bound (Long.MIN_VALUE, what a delete with no lower bound
    // records) from a timestamp: half of it is about 146 million years before the epoch, below any sample a column can hold, and far
    // enough from Long.MIN_VALUE that the bucket start and the range query literal cannot overflow
    if (interval > 0 && invalidated.fromTs() > Long.MIN_VALUE / 2) {
      final long first = ContinuousAggregateImpl.bucketStart(invalidated.fromTs(), watermark, interval);
      final long last = ContinuousAggregateImpl.bucketStart(Math.min(invalidated.toTs(), watermark - 1), watermark, interval);
      if (first > last)
        return;
      recomputeBuckets(database, ca, first, last, Math.min(last + interval, watermark));
      return;
    }

    final long first;
    final long last;
    try (final ResultSet rs = database.query("sql",
        "SELECT min(`" + bucketColumn + "`) AS minBucket, max(`" + bucketColumn + "`) AS maxBucket FROM `" + backingTypeName
            + "` WHERE " + where,
        params.toArray())) {
      final Result row = rs.next();
      final Object firstValue = row.getProperty("minBucket");
      final Object lastValue = row.getProperty("maxBucket");
      if (firstValue == null || lastValue == null)
        return;
      first = toEpochMs(firstValue, bucketColumn, ca.getName());
      last = toEpochMs(lastValue, bucketColumn, ca.getName());
    }

    recomputeBuckets(database, ca, first, last,
        interval > 0 && last <= Long.MAX_VALUE - interval ? Math.min(last + interval, watermark) : watermark);
  }

  /** Replaces the rows of the buckets starting in {@code [first, last]} with the defining query over the samples of {@code [first, end)}. */
  private static void recomputeBuckets(final Database database, final ContinuousAggregateImpl ca, final long first, final long last,
      final long end) {
    final String backingTypeName = ca.getBackingTypeName();
    final String bucketColumn = ca.getBucketColumn();
    final ReplacedRows replaced = ReplacedRows.select(database, ca, bucketRangeSelect(ca), new Date(first), new Date(last));

    try (final ResultSet rs = database.query("sql", buildRangeQuery(ca, first, end))) {
      while (rs.hasNext()) {
        final Result result = rs.next();
        final Object bucketVal = result.getProperty(bucketColumn);
        saveRow(database, backingTypeName, result,
            bucketVal != null ? replaced.take(toEpochMs(bucketVal, bucketColumn, ca.getName())) : null);
      }
    }
    replaced.deleteTheRest();
  }

  /**
   * The rows of the buckets at or after a watermark, the parameter. Served by the index on the bucket column (issue #10804): without
   * one it reads every row the aggregate ever wrote, on every refresh.
   */
  static String bucketsFromSelect(final ContinuousAggregateImpl ca) {
    return "SELECT FROM `" + ca.getBackingTypeName() + "` WHERE `" + ca.getBucketColumn() + "` >= ?";
  }

  /** The rows of the buckets starting between the two parameters, both included. Served by the same index as {@link #bucketsFromSelect}. */
  static String bucketRangeSelect(final ContinuousAggregateImpl ca) {
    final String bucketColumn = ca.getBucketColumn();
    return "SELECT FROM `" + ca.getBackingTypeName() + "` WHERE `" + bucketColumn + "` >= ? AND `" + bucketColumn + "` <= ?";
  }

  /**
   * The rows a refresh replaces, by the start of their bucket (issue #10804). A refresh used to delete them and insert the recomputed
   * ones, and an ingest request triggers one: the open bucket's rows were deleted and inserted again by every request, under the same
   * bucket key. That leaves a tombstone and a new entry per request on one key of the index on the bucket column, which every later
   * lookup of the bucket walks (measured: 35 ms a request after 20k requests into one 1h bucket, from 0.3 ms without the index), and a
   * hole per request in the pages of the backing type. A recomputed row takes over a replaced row of its own bucket instead: the key
   * does not change, so the index is not written at all, and the record is rewritten in place. A recomputed row with no replaced row
   * left in its bucket is inserted, and the replaced rows nothing took over are deleted.
   * <p>
   * Only the identities are held, one per row the refresh replaces: the rows of the open bucket for the refresh an append triggers,
   * those of the recomputed range for a late sample or a delete. That is the size the transaction of the refresh has anyway, which
   * the delete it replaces held too, and which the recomputed rows it writes hold again.
   */
  private static final class ReplacedRows {
    private final Map<Long, ArrayDeque<RID>> byBucket = new HashMap<>();
    private final Database                   database;

    private ReplacedRows(final Database database) {
      this.database = database;
    }

    static ReplacedRows select(final Database database, final ContinuousAggregateImpl ca, final String select, final Object... params) {
      final ReplacedRows rows = new ReplacedRows(database);
      final String bucketColumn = ca.getBucketColumn();
      try (final ResultSet rs = database.query("sql", select, params)) {
        while (rs.hasNext()) {
          final Document row = rs.next().toElement();
          rows.byBucket.computeIfAbsent(toEpochMs(row.get(bucketColumn), bucketColumn, ca.getName()), k -> new ArrayDeque<>(2))
              .add(row.getIdentity());
        }
      }
      return rows;
    }

    /**
     * A replaced row of the bucket starting at {@code bucketMs} for a recomputed row to take over, or {@code null} when none is left.
     * Refreshes of one aggregate do not overlap ({@link ContinuousAggregateImpl#tryBeginRefresh}), so only a write to the backing type
     * outside them can remove a row between the read and here: the next one is taken, and with none left the row is inserted.
     */
    MutableDocument take(final long bucketMs) {
      final ArrayDeque<RID> rows = byBucket.get(bucketMs);
      if (rows != null)
        for (RID row = rows.poll(); row != null; row = rows.poll())
          try {
            return database.lookupByRID(row, true).asDocument().modify();
          } catch (final RecordNotFoundException e) {
            // gone already: nothing to take over
          }
      return null;
    }

    void deleteTheRest() {
      for (final ArrayDeque<RID> rows : byBucket.values())
        for (final RID row : rows)
          try {
            database.deleteRecord(database.lookupByRID(row, false));
          } catch (final RecordNotFoundException e) {
            // gone already: nothing to delete
          }
      byBucket.clear();
    }
  }

  /** Writes a recomputed row: into {@code replaced}, a row of the same bucket it takes over (issue #10804), or as a new one. */
  private static void saveRow(final Database database, final String backingTypeName, final Result result,
      final MutableDocument replaced) {
    final MutableDocument doc = replaced != null ? replaced : database.newDocument(backingTypeName);
    if (replaced != null) {
      // what the row it replaces held and the recomputed row does not, the defining query had no value for. Collected first (a removal
      // inside the walk would change the set walked), and only when there is one: the ingest refresh rewrites a row whose properties
      // are the same every time, and must not pay a copy of them per request
      List<String> stale = null;
      for (final String prop : replaced.getPropertyNames())
        if (!result.hasProperty(prop)) {
          if (stale == null)
            stale = new ArrayList<>(2);
          stale.add(prop);
        }
      if (stale != null)
        for (final String prop : stale)
          replaced.remove(prop);
    }
    for (final String prop : result.getPropertyNames()) {
      if (!prop.startsWith("@"))
        doc.set(prop, result.getProperty(prop));
    }
    doc.save();
  }

  /**
   * The defining query with the incremental filter applied, or the query unchanged when no watermark has been set
   * yet. #8152: {@code watermarkSet} is a separate argument and NOT {@code watermark > 0}, because an aggregate
   * anchored at the epoch has a watermark of 0 that must still be filtered on.
   */
  static String buildFilteredQuery(final ContinuousAggregateImpl ca, final long watermark, final boolean watermarkSet) {
    if (!watermarkSet)
      return ca.getQuery();
    return spliceTimeFilter(ca, watermark, null);
  }

  /**
   * The defining query restricted to the samples with {@code fromTs <= ts < toTs} (issue #10428), which is the buckets of that range
   * when both ends are bucket boundaries.
   */
  static String buildRangeQuery(final ContinuousAggregateImpl ca, final long fromTs, final long toTs) {
    return spliceTimeFilter(ca, fromTs, toTs);
  }

  /** The query with {@code ts >= fromTs}, and {@code ts < toTs} when it is not {@code null}, spliced into its WHERE clause. */
  private static String spliceTimeFilter(final ContinuousAggregateImpl ca, final long fromTs, final Long toTs) {
    final String query = ca.getQuery();
    final String tsColumn = ca.getTimestampColumn();

    // Validate column name to prevent backtick injection
    if (!SAFE_COLUMN_NAME.matcher(tsColumn).matches())
      throw new IllegalArgumentException("Unsafe timestamp column name: '" + tsColumn + "'");

    final String filter = "`" + tsColumn + "` >= " + fromTs + (toTs != null ? " AND `" + tsColumn + "` < " + toTs : "");

    // Find WHERE clause position at the outermost level (case-insensitive).
    // Note: CTEs and subqueries with their own WHERE clauses are not supported
    // in continuous-aggregate queries.
    final String upperQuery = toUpperCasePreservingLength(query);
    final int whereIdx = findTopLevelKeyword(upperQuery, "WHERE", 0);

    if (whereIdx >= 0) {
      // #8156: BRACKET THE CALLER'S OWN PREDICATE. 'AND' BINDS TIGHTER THAN 'OR', so splicing the watermark filter
      // in as a bare left conjunct rewrote 'WHERE a OR b' into '(ts >= W AND a) OR b' - a DIFFERENT predicate, whose
      // second disjunct escapes the watermark entirely and re-aggregates buckets OLDER than it. Those buckets are
      // not covered by the DELETE that opens the refresh, so their rows survive and the recomputed ones land next to
      // them: one duplicate per such bucket per refresh. The bracket closes at the END OF THE WHERE CLAUSE, not at
      // the end of the string, because everything from GROUP BY onwards is outside it.
      final int whereEnd = findWhereClauseEnd(upperQuery, whereIdx + 5);
      final String before = query.substring(0, whereIdx + 5); // "WHERE" is 5 chars
      final String predicate = query.substring(whereIdx + 5, whereEnd).strip();
      final String after = query.substring(whereEnd);
      // #8251: strip() drops the newline that ends a trailing line comment, which would then swallow the closing
      // bracket and every clause pasted back after it. Such a predicate gets its bracket on a line of its own.
      return before + " " + filter + " AND (" + predicate + (endsInsideLineComment(predicate) ? "\n)" : ")")
          + (after.isEmpty() ? "" : " " + after.stripLeading());
    } else {
      // No WHERE clause — insert before the first clause that can follow one, or at the end
      final int insertIdx = findWhereClauseEnd(upperQuery, 0);
      if (insertIdx < query.length()) {
        final String before = query.substring(0, insertIdx);
        final String after = query.substring(insertIdx);
        return before + "WHERE " + filter + " " + after;
      }
      // #8251: a query ending in a line comment would comment the filter out, and the refresh would silently
      // re-aggregate every bucket, older ones included, next to the rows it already holds
      return query + (endsInsideLineComment(query) ? "\n" : " ") + "WHERE " + filter;
    }
  }

  /**
   * Upper-cases for keyword matching WITHOUT changing the length, so that every index found in the mirror addresses
   * the same character in the original. {@code String.toUpperCase} does not promise that - German 'ß' upper-cases to
   * two characters - and every use of the mirror here is a {@code substring} on the original.
   */
  private static String toUpperCasePreservingLength(final String query) {
    final char[] chars = query.toCharArray();
    for (int i = 0; i < chars.length; i++)
      chars[i] = Character.toUpperCase(chars[i]);
    return new String(chars);
  }

  /**
   * Index of the first top-level clause keyword at or after {@code fromIdx} that can follow a WHERE, or the length of
   * the query when there is none - i.e. where the WHERE clause ends.
   */
  private static int findWhereClauseEnd(final String upperQuery, final int fromIdx) {
    int end = upperQuery.length();
    for (final String keyword : CLAUSES_AFTER_WHERE) {
      final int idx = findTopLevelKeyword(upperQuery, keyword, fromIdx);
      if (idx >= 0 && idx < end)
        end = idx;
    }
    return end;
  }

  /**
   * Index of the first standalone occurrence of {@code keyword} at the outermost nesting level (depth 0) at or after
   * {@code fromIdx}, or -1. String literals (single or double quoted), block comments, line comments and
   * parenthesized subqueries are skipped, so a keyword inside any of them is not mistaken for a clause.
   * E.g.: {@code SELECT func('(foo)') FROM t WHERE ts > 0}, {@code SELECT /* WHERE not here *&#47; * FROM t WHERE ts > 0}
   * <p>
   * #8156: this used to scan for WHERE only, and the other branch of {@code buildFilteredQuery} used a plain
   * {@code indexOf} for GROUP BY / ORDER BY / LIMIT that knew nothing about quoting - so a GROUP BY inside a string
   * literal was taken for the clause. One scanner now answers for every clause keyword.
   * <p>
   * The scan always starts at 0 and {@code fromIdx} only filters what it RETURNS: the quote, comment and paren
   * state at {@code fromIdx} can only be known by reading everything before it. Starting the loop at {@code fromIdx}
   * would be faster and wrong. That costs one pass per keyword, on a query string, once per refresh.
   */
  private static int findTopLevelKeyword(final String upperQuery, final String keyword, final int fromIdx) {
    int depth = 0;
    int idx = 0;
    final int len = upperQuery.length();
    while (idx < len) {
      final char ch = upperQuery.charAt(idx);
      // Skip over block comments: /* ... */
      if (ch == '/' && idx + 1 < len && upperQuery.charAt(idx + 1) == '*') {
        idx += 2;
        while (idx + 1 < len && !(upperQuery.charAt(idx) == '*' && upperQuery.charAt(idx + 1) == '/'))
          idx++;
        idx += 2; // skip closing */
        continue;
      }
      // Skip over line comments: -- ... \n
      if (isLineCommentStart(upperQuery, idx)) {
        idx = lineCommentEnd(upperQuery, idx);
        continue;
      }
      // Skip over quoted string literals AND backtick-quoted identifiers, so neither a paren nor a clause keyword
      // inside one is counted. #8156 (found by CodeRabbit): the backtick was missing, so `LIMIT` or `TIMEOUT` used
      // as a quoted column name ended the WHERE clause in the middle of the predicate. A doubled quote is SQL's own
      // escape and does not close the literal.
      if (ch == '\'' || ch == '"' || ch == '`') {
        idx = skipQuoted(upperQuery, idx);
        continue;
      }
      if (ch == '(') {
        depth++;
        idx++;
        continue;
      }
      if (ch == ')') {
        depth--;
        idx++;
        continue;
      }
      if (depth > 0) {
        idx++;
        continue;
      }
      if (ch == keyword.charAt(0)) {
        final int matchEnd = matchKeywordAt(upperQuery, keyword, idx);
        if (matchEnd > 0) {
          final boolean leftBound = idx == 0 || !isIdentifierCharacter(upperQuery.charAt(idx - 1));
          final boolean rightBound = matchEnd >= len || !isIdentifierCharacter(upperQuery.charAt(matchEnd));
          if (leftBound && rightBound && idx >= fromIdx)
            return idx;
          idx = matchEnd;
          continue;
        }
      }
      idx++;
    }
    return -1;
  }

  private static boolean isLineCommentStart(final String query, final int idx) {
    return idx + 1 < query.length() && query.charAt(idx) == '-' && query.charAt(idx + 1) == '-';
  }

  /**
   * Index of the newline that ends the line comment starting at {@code idx}, or the length of the query when the
   * comment runs to its end.
   */
  private static int lineCommentEnd(final String query, final int idx) {
    final int newLine = query.indexOf('\n', idx + 2);
    return newLine < 0 ? query.length() : newLine;
  }

  /**
   * Whether {@code text} ends inside a line comment, so that anything appended to it on the same line would be
   * commented out (#8251). Quoted literals and block comments are skipped the way {@link #findTopLevelKeyword} skips
   * them, so a {@code --} inside either does not count.
   */
  static boolean endsInsideLineComment(final String text) {
    final int len = text.length();
    int idx = 0;
    while (idx < len) {
      final char ch = text.charAt(idx);
      if (ch == '/' && idx + 1 < len && text.charAt(idx + 1) == '*') {
        final int close = text.indexOf("*/", idx + 2);
        if (close < 0)
          return false;
        idx = close + 2;
      } else if (isLineCommentStart(text, idx)) {
        idx = lineCommentEnd(text, idx);
        if (idx >= len)
          return true;
      } else if (ch == '\'' || ch == '"' || ch == '`') {
        idx = skipQuoted(text, idx);
      } else
        idx++;
    }
    return false;
  }

  /**
   * Index just past the quoted literal or backtick-quoted identifier opening at {@code idx}. A doubled quote is SQL's
   * own escape and does not close the literal, and neither does a backslash-escaped one.
   */
  private static int skipQuoted(final String query, int idx) {
    final int len = query.length();
    final char quote = query.charAt(idx);
    idx++;
    while (idx < len) {
      final char c2 = query.charAt(idx);
      idx++;
      if (c2 == '\\') {
        idx++; // skip escaped character
      } else if (c2 == quote) {
        if (idx < len && query.charAt(idx) == quote) {
          idx++; // doubled quote: an escaped quote, not the end of the literal
          continue;
        }
        break;
      }
    }
    return idx;
  }

  /**
   * Whether {@code c} can appear inside an ArcadeDB identifier, which {@code SAFE_COLUMN_NAME} spells as
   * {@code [A-Za-z0-9_]}. #8156 (found by CodeRabbit): the boundary test used {@code Character.isLetterOrDigit}
   * alone, so the {@code TIMEOUT} in a column named {@code timeout_ms} looked like a clause keyword and ended the
   * WHERE clause in the middle of the predicate.
   */
  private static boolean isIdentifierCharacter(final char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  /**
   * Matches {@code keyword} at {@code idx}, answering the index just past the match or -1. A single space in the
   * keyword matches any run of whitespace in the query, so {@code GROUP  BY} and a {@code GROUP} / {@code BY} split
   * across a line break are the clause they are, not two words.
   * <p>
   * #8156 (found in review): a literal {@code startsWith("ORDER BY")} missed those spellings, and the bracket the
   * fix adds closes at the end of the WHERE clause - so a clause boundary the scan cannot see swallows the whole
   * rest of the query, GROUP BY included, into the predicate and makes the refresh fail on invalid SQL. The branch
   * this replaced never looked past WHERE at all, so the spelling could not hurt it.
   */
  private static int matchKeywordAt(final String upperQuery, final String keyword, final int idx) {
    final int queryLen = upperQuery.length();
    final int keywordLen = keyword.length();
    int q = idx;
    for (int k = 0; k < keywordLen; k++) {
      final char kc = keyword.charAt(k);
      if (kc == ' ') {
        if (q >= queryLen || !Character.isWhitespace(upperQuery.charAt(q)))
          return -1;
        while (q < queryLen && Character.isWhitespace(upperQuery.charAt(q)))
          q++;
        continue;
      }
      if (q >= queryLen || upperQuery.charAt(q) != kc)
        return -1;
      q++;
    }
    return q;
  }

  /**
   * #8152: this was a private near-copy of the same conversion that lives in {@link DateUtils#toEpochMillis}, and it
   * knew nothing about the {@link java.time.LocalDateTime} that {@code ts.timeBucket()} actually returns - so every
   * bucket read as 0, the watermark never advanced, and each refresh appended another full copy of the aggregate on
   * top of the previous one. THE SILENT {@code return 0} IS WHAT MADE THAT A DATA DEFECT RATHER THAN AN ERROR: 0 is
   * also this class's "no watermark yet", so nothing anywhere could tell the two apart. It now refuses a value it
   * cannot read, which fails the refresh loudly and marks the aggregate ERROR.
   */
  private static long toEpochMs(final Object value, final String bucketColumn, final String aggregateName) {
    try {
      return DateUtils.toEpochMillis(value);
    } catch (final IllegalArgumentException | DateTimeParseException e) {
      // The caller skips a null bucket rather than asking about it, but the message must not be the thing that
      // throws if that ever changes (found in review).
      throw new IllegalArgumentException("Continuous aggregate '" + aggregateName + "': bucket column '" + bucketColumn
          + "' holds '" + value + "'" + (value != null ? " (" + value.getClass().getName() + ")" : "")
          + ", which is not a timestamp", e);
    }
  }
}
