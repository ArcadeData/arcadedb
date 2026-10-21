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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.PageManager;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BetweenCondition;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Loads the records the index entries of the previous step point to.
 * <p>
 * Built with a {@link ScanFallback}, the step does not trust the planner's choice of the index blindly (issue #8333).
 * The planner picks an index whenever its condition matches, but fetching most of a type through an index is one
 * random page access per record: cheap while the type fits the page cache, far slower than a plain scan once it does
 * not. So before loading anything the step reads the index entries alone - no record is touched - through a
 * {@link PhysicalOrderRidFetcher}, and:
 * <ul>
 *   <li>once more of them match than {@link com.arcadedb.GlobalConfiguration#QUERY_INDEX_MAX_SELECTIVITY} of the
 *   records the target buckets hold, it drops the index and serves the rows from a scan of the type filtered by the
 *   index condition, which is exactly the plan the statement gets without the index;</li>
 *   <li>otherwise it loads the matching records in physical order (bucket, then position), so the pages are swept
 *   forward once instead of visited in key order.</li>
 * </ul>
 * The decision is taken per execution, with the parameters bound, so a plan cached for {@code WHERE x > ?} serves a
 * selective value with the index and a non-selective one with the scan. The planner only builds the fallback when the
 * index order is not what the statement relies on and the scan is guaranteed to answer the same rows.
 * <p>
 * Both branches run in parallel where a scan would (issue #8333 on top of #8523): the scan is the parallel scan of the
 * type, and the physical-order load is cut in slices of the sorted record addresses that the workers of a parallel scan
 * load each, through their bucket, every page read once. An aggregation downstream runs in those workers too, with
 * the conditions the index does not answer, see {@link ParallelAggregationSource}. The threshold falls with the
 * workers the scan would run on, since the index entries are read by one thread whatever the parallelism.
 *
 * Created by luigidellaquila on 16/03/17.
 */
public class GetValueFromIndexEntryStep extends AbstractExecutionStep implements ParallelAggregationSource {
  /**
   * What the step needs to replace the index search with a scan of the type.
   *
   * @param typeName    the type the planner targets (scanned polymorphically, like the index buckets are)
   * @param bucketNames the buckets the planner narrowed the target to, or null for all the type's buckets
   * @param keyFilter   the conditions the index search answers, to evaluate on every scanned record instead
   */
  public record ScanFallback(String typeName, Set<String> bucketNames, WhereClause keyFilter) {
    ScanFallback copy() {
      return new ScanFallback(typeName, bucketNames, keyFilter.copy());
    }
  }

  enum Strategy {INDEX_ORDER, PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED, SCAN}

  // The key of a point lookup's streaming path before the first entry: no key, not even null
  private static final Object NO_KEY              = new Object();
  /**
   * How many times the streaming path reads one key again for the entries commits took from it (#9902). Each read is one more
   * pass over the key's entries, owed only to commits that deleted or moved records of THIS key while it was being read, so the
   * bound only matters for a key rewritten faster than it can be read: past it, the entries lost last are not looked for again.
   * A record that holds the key the whole time is never one of them: it is in the entries the lookup read, and passes its check.
   */
  private static final int    MAX_REREADS_PER_KEY = 8;
  /**
   * How many records the streaming path of a point lookup remembers it served of one key (#9902), about 6 MB at most: enough
   * for any key a consumer pulls whole in practice, and a bound on what a statement holds for a key of millions of entries.
   * What it served is what a read of the key again must not serve twice, so past the bound the key is not read again: entries
   * a commit takes from it are counted as unsettled instead (PROFILE shows them, a WARNING says so once per execution). No record
   * is served for a key it does not hold, and a record that holds the key the whole time is still served; the record that
   * replaced a deleted one may be missed, and an {@code IN} that lists the key twice serves the records past the bound twice, as
   * an {@code IN} with a repeated value did before #9902.
   */
  static final int            MAX_TRACKED_RIDS_PER_KEY = 1 << 18;
  // The served set of the previous key is cleared for the next one up to this size, and replaced past it
  private static final int    MAX_REUSED_SERVED_SET    = 64;

  private final List<Integer> filterBucketIds;
  private final ScanFallback  scanFallback;

  // runtime: plain fields, unlike the per-thread state of Cypher's NodeIndexRangeScan, because a cached SQL plan is
  // copied for every execution (ExecutionPlanCache.get()) while a cached Cypher operator is shared by all of them
  private ResultSet                   prevResult = null;
  // The rows of a point lookup, loaded against one committed state (#9369), and the entries read past what that takes
  private ArrayDeque<Result>          consistentRows;
  private boolean                     consistentTried;
  private final ArrayDeque<Result>    pendingEntries = new ArrayDeque<>();
  private boolean                     prevExhausted;
  // Rows a range scan recovered for an entry whose record could not be loaded (#9369), served before the next entry
  private final ArrayDeque<Result>    recovered      = new ArrayDeque<>();
  // The streaming path of a point lookup (#9902). Kept on the step, not on the page of rows syncPull() returns: a page ends
  // where the consumer stops pulling, not where a key does, and what was served of a key must outlive it
  private Object                      streamKey      = NO_KEY;
  // The records served for the key: so a read of the key again serves only what was not served yet
  private RidHashSet                  servedOfKey;
  // Past MAX_TRACKED_RIDS_PER_KEY: servedOfKey no longer holds every record served of the key
  private boolean                     servedOverflow;
  // MAX_TRACKED_RIDS_PER_KEY, lowered by tests to reach the bound without a key of 256K entries
  int                                 trackedRidsPerKey = MAX_TRACKED_RIDS_PER_KEY;
  // Entries the key lost since it was last read again
  private int                         pendingLosses;
  // An entry of the key lost its record to a commit: the key is read again once its entries run out
  private boolean                     keyLostEntries;
  private int                         rereadsOfKey;
  // The entries the key holds now that were not served yet, and the publication sequence they were read at
  private ArrayDeque<RID>             rereadCandidates;
  private long                        rereadSequence;
  // What reconciling cost, for PROFILE and the tests
  private long                        verifiedRows;
  private long                        lostEntries;
  private long                        keyRereads;
  private long                        rereadEntries;
  private long                        unsettledEntries;
  // The buckets the records of a point lookup were read from, so a restart releases their pinned pages whatever the bucket filter
  private final Set<Integer>          readBucketIds  = new HashSet<>();
  private Strategy                    strategy;
  private PhysicalOrderRidFetcher     fetcher;
  private FetchFromTypeWithFilterStep scanStep;
  private long                        scanThreshold;
  private long                        matchedEntries;
  // A physical-order load shared by the workers of a parallel scan, one round per chunk of the range: decided once
  // per execution, like the strategy
  private boolean                     parallelDecided;
  private ParallelTypeScan            parallelRound;
  private int                         parallelRounds;

  /**
   * @param context         the execution context
   * @param filterBucketIds only extract values from these clusters. Pass null if no filtering is needed
   */
  public GetValueFromIndexEntryStep(final CommandContext context, final List<Integer> filterBucketIds) {
    this(context, filterBucketIds, null);
  }

  /**
   * @param scanFallback when not null, the step may serve the rows from a scan or in physical order instead of in index
   *                     order, see the class comment. Requires the previous step to be a {@link FetchFromIndexStep}
   */
  public GetValueFromIndexEntryStep(final CommandContext context, final List<Integer> filterBucketIds,
      final ScanFallback scanFallback) {
    super(context);
    this.filterBucketIds = filterBucketIds;
    this.scanFallback = scanFallback;
  }

  /**
   * Read-only access to the bucket-id constraint applied to index entries (or {@code null}
   * when no constraint is active and every value passes through). Surfaced for tests that
   * need to verify partition-aware bucket pruning narrowed the per-bucket sub-index set.
   */
  public List<Integer> getFilterBucketIds() {
    return filterBucketIds;
  }

  public ScanFallback getScanFallback() {
    return scanFallback;
  }

  /**
   * How the last execution served the rows, or null before it started. Surfaced for tests.
   */
  Strategy getStrategy() {
    return strategy;
  }

  /** Rows of a point lookup checked against their key because a commit overlapped the lookup (#9902). Surfaced for tests. */
  long getVerifiedRows() {
    return verifiedRows;
  }

  /** Entries of a point lookup whose record was gone or no longer held the key. Surfaced for tests. */
  long getLostEntries() {
    return lostEntries;
  }

  /** How many times a key of a point lookup was read again for the entries it lost. Surfaced for tests. */
  long getKeyRereads() {
    return keyRereads;
  }

  /** Entries lost to a commit whose key was not read again for them, see {@link #startReread}. Surfaced for tests. */
  long getUnsettledEntries() {
    return unsettledEntries;
  }

  /** The index entries those reads returned. Surfaced for tests. */
  long getRereadEntries() {
    return rereadEntries;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();

    if (strategy == null)
      chooseStrategy(context, prevStep);

    return switch (strategy) {
      case SCAN -> scanStep.syncPull(context, nRecords);
      case PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED -> {
        if (!parallelDecided) {
          parallelDecided = true;
          parallelRound = planRound(context, true);
        }
        // Once parallel, always: after the last round the fetcher still holds the last chunk, which a sequential
        // page would serve a second time
        yield parallelRounds > 0 ? parallelPhysicalOrderResultSet(context, nRecords) : physicalOrderResultSet(context, nRecords);
      }
      case INDEX_ORDER -> indexOrderResultSet(context, prevStep, nRecords);
    };
  }

  /**
   * Plans the parallel aggregation of this execution's rows (issue #8333 on top of #8523): the index entries are read
   * first, as for a sequential execution, then a range served by a scan hands the aggregation to the parallel scan of
   * the type, and one served in physical order has its records loaded by the workers of a parallel scan, a slice of
   * the sorted addresses each, chunk after chunk when the range is too large to hold at once.
   */
  @Override
  public ParallelTypeScan planParallelAggregation(final CommandContext context) {
    if (strategy != null || scanFallback == null || !ParallelTypeScan.isAllowed(context.getDatabase()))
      return null;

    chooseStrategy(context, checkForPrevious());
    parallelDecided = true;
    return switch (strategy) {
      case SCAN -> scanStep.planParallelAggregation(context);
      case PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED -> planRound(context, true);
      case INDEX_ORDER -> null;
    };
  }

  @Override
  public ParallelTypeScan nextParallelAggregationRound(final CommandContext context) {
    return strategy == Strategy.PHYSICAL_ORDER_CHUNKED && fetcher.nextChunk() ? planRound(context, false) : null;
  }

  /**
   * Whether an execution starting now would run in parallel: never known before the index entries are read, so an
   * EXPLAIN does not claim it; a PROFILE shows how the execution went.
   */
  @Override
  public boolean wouldRunInParallel(final CommandContext context) {
    return false;
  }

  /**
   * Plans the parallel load of the records of the fetcher's current chunk, in physical order: the sorted addresses of
   * every bucket are cut in slices a worker loads each.
   *
   * @param first whether this is the first chunk, which decides for the whole execution: it goes parallel only where
   *              parallel scans are allowed and when it is large enough to share. A later chunk always does, the
   *              execution being parallel already: its entries are loaded, and answering null would drop them
   *
   * @return the scan, or null when the load stays sequential: parallel scans are not allowed here, or the first chunk
   * is too small to share
   */
  private ParallelTypeScan planRound(final CommandContext context, final boolean first) {
    final DatabaseInternal database = context.getDatabase();
    final int entriesPerUnit = ParallelTypeScan.entriesPerUnit(database, fetcher.getBufferedRids());
    // Counted before the units are built: building them hands the entries that are not record addresses over, which
    // a sequential load then would not serve
    if (first && (!ParallelTypeScan.isAllowed(database)
        || fetcher.slices(entriesPerUnit, null) + (fetcher.hasPassThrough() ? 1 : 0) < 2))
      return null;

    final ParallelTypeScan round = ParallelTypeScan.ofUnits(context, scanFallback.typeName(),
        unitsOfCurrentChunk(context, entriesPerUnit));
    if (round != null)
      ++parallelRounds;
    return round;
  }

  /**
   * The units the fetcher's current chunk is loaded in by a parallel scan: a slice of at most {@code entriesPerUnit}
   * sorted positions of one bucket each, in physical order, then the entries that are not record addresses.
   */
  private List<PhysicalOrderSliceStep> unitsOfCurrentChunk(final CommandContext context, final int entriesPerUnit) {
    final List<PhysicalOrderSliceStep> units = new ArrayList<>();
    fetcher.slices(entriesPerUnit,
        (bucketId, positions, from, to) -> units.add(PhysicalOrderSliceStep.ofPositions(context, bucketId, positions, from, to)));
    final List<Object> passThrough = fetcher.takePassThrough();
    if (passThrough != null)
      units.add(PhysicalOrderSliceStep.ofEntries(context, passThrough, GetValueFromIndexEntryStep::toResult));
    return units;
  }

  /**
   * The rows of a physical-order load shared by the workers of a parallel scan, in the order the sequential load serves
   * them: round after round, each one the parallel scan of a chunk of the range.
   */
  private ResultSet parallelPhysicalOrderResultSet(final CommandContext context, final int nRecords) {
    return new ResultSet() {
      ResultSet page    = parallelRound != null ? parallelRound.pull(context, nRecords) : null;
      int       fetched = 0;

      @Override
      public boolean hasNext() {
        while (fetched < nRecords && parallelRound != null) {
          if (page.hasNext())
            return true;
          // The page stopped short of what was asked: this round has no row left. Every unit of it has ended, so the
          // next chunk can take over the arrays its slices read
          parallelRound.close();
          parallelRound = fetcher.nextChunk() ? planRound(context, false) : null;
          if (parallelRound != null)
            page = parallelRound.pull(context, nRecords - fetched);
        }
        return false;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        fetched++;
        return page.next();
      }
    };
  }

  private ResultSet indexOrderResultSet(final CommandContext context, final ExecutionStepInternal prevStep, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    return new ResultSet() {

      public boolean finished = false;

      Result nextItem = null;
      int    fetched  = 0;

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords || finished)
          return false;

        if (nextItem == null)
          fetchNextItem();

        return nextItem != null;

      }

      @Override
      public Result next() {
        if (fetched >= nRecords || finished)
          throw new NoSuchElementException();

        if (nextItem == null)
          fetchNextItem();

        if (nextItem == null)
          throw new NoSuchElementException();

        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      private void fetchNextItem() {
        nextItem = null;
        if (finished)
          return;

        if (!recovered.isEmpty()) {
          nextItem = recovered.pollFirst();
          return;
        }

        if (!consistentTried && prevStep instanceof FetchFromIndexStep indexStep && indexStep.isPointLookup()) {
          consistentTried = true;
          consistentRows = loadPointLookup(context, indexStep, nRecords, guard);
        }
        if (consistentRows != null) {
          nextItem = consistentRows.pollFirst();
          if (nextItem == null)
            finished = true;
          return;
        }

        while (!finished) {
          // Every pass loads a record. The entries a chunk read ahead (pendingEntries) or a re-read found do not come through
          // the index step's own check, so a terminated statement or one past its deadline is stopped here (#9893)
          guard.check();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (rereadCandidates != null) {
              // The entries the key holds now, after its own ran out with some lost to a commit (#9902). Only a point lookup loses
              // entries, so the previous step is a FetchFromIndexStep here and in the re-reads below
              nextItem = nextRereadRow(context, (FetchFromIndexStep) prevStep, guard);
              if (nextItem != null)
                return;
              rereadCandidates = null;
              if (keyLostEntries)
                // a commit took more entries while they were served: settle those too, from the key as it is now
                startReread(context, (FetchFromIndexStep) prevStep, guard);
              continue;
            }

            final Result val = nextEntry(context, prevStep, nRecords);
            if (val == null) {
              if (!keyLostEntries || !startReread(context, (FetchFromIndexStep) prevStep, guard))
                finished = true;
              continue;
            }

            final Object finalVal = val.getProperty("rid");
            if (!passesBucketFilter(finalVal))
              continue;

            if (prevStep instanceof FetchFromIndexStep indexStep && finalVal instanceof RID rid) {
              final Object key = val.getProperty("key");
              if (indexStep.isPointLookup()) {
                if (!FetchFromIndexStep.isEntryOf(key, streamKey)) {
                  if (keyLostEntries) {
                    // The entries of the key ran out with some lost to a commit: settle them before moving to the next key
                    pendingEntries.addFirst(val);
                    if (!startReread(context, indexStep, guard))
                      startKey(indexStep.lookupKeyOf(key));
                    continue;
                  }
                  startKey(indexStep.lookupKeyOf(key));
                }
                nextItem = pointLookupRow(context, indexStep, rid);
              } else {
                nextItem = toResult(finalVal, context);
                if (nextItem == null) {
                  // A range scan reads its entries as it goes, so only an entry whose record is gone is reconciled (#9369)
                  recovered.addAll(reconcile(indexStep, val.getProperty("key"), rid, context, guard));
                  nextItem = recovered.pollFirst();
                }
              }
            } else
              nextItem = toResult(finalVal, context);
            if (nextItem != null)
              return;
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }
    };
  }

  /** The next index entry: one set aside first, then the previous step's, or null once both ran out. */
  private Result nextEntry(final CommandContext context, final ExecutionStepInternal prevStep, final int nRecords) {
    if (!pendingEntries.isEmpty())
      return pendingEntries.pollFirst();
    if (prevResult == null) {
      prevResult = prevStep.syncPull(context, nRecords);
      prevExhausted = !prevResult.hasNext();
    }
    // Only a page that comes back empty ends the entries: one that stops short of nRecords may not be the last
    while (!prevResult.hasNext()) {
      if (prevExhausted)
        return null;
      prevResult = prevStep.syncPull(context, nRecords);
      prevExhausted = !prevResult.hasNext();
    }
    return prevResult.next();
  }

  /** Starts serving the entries of another key of a point lookup. */
  private void startKey(final Object key) {
    streamKey = key;
    // A set grown for a large key is dropped rather than cleared: clearing costs its whole capacity, once per key
    if (servedOfKey == null || servedOfKey.size() > MAX_REUSED_SERVED_SET)
      servedOfKey = new RidHashSet(16);
    else
      servedOfKey.clear();
    servedOverflow = false;
    keyLostEntries = false;
    pendingLosses = 0;
    rereadsOfKey = 0;
  }

  /**
   * The row of an entry of a point lookup that holds too many entries to be loaded against one committed state (#9369), or null
   * when there is none to serve now (#9902). The row is served as loaded when no commit published since the lookup read its
   * entries, which is the common case and costs one volatile read. When one did, the commit may have deleted the record, moved it
   * to another key or given its slot to another record, and the row is served only if its record holds the key: a check of the
   * record alone, whatever the commit touched elsewhere. An entry that fails it is set aside, and the key is read again once,
   * when its entries run out, to serve what the key holds then and was not served yet: the record that replaced a deleted one,
   * say. Nothing is materialised up front and no lock is taken, so the work follows the rows the consumer pulls, not the size of
   * the key.
   */
  private Result pointLookupRow(final CommandContext context, final FetchFromIndexStep indexStep, final RID rid) {
    // Several entries of a key can name one record only when a lookup repeats the key (an IN listing it twice)
    if (servedOfKey.contains(rid))
      return null;
    final Result row = toResult(rid, context, false);
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    if (noCommitSince(database, indexStep.entriesSequence())) {
      if (row != null)
        return served(row, rid);
      if (!isRepeatableRead(database)) {
        // The entry and the records are one committed state: the index names a record that does not exist
        LogManager.instance().log(this, Level.WARNING, "Record %s not found. Skip it from the result set", null, rid);
        return null;
      }
      // Under REPEATABLE_READ the page the record was looked for in may be one this transaction pinned before the record existed
    } else if (row != null && holdsKey(indexStep, row, streamKey))
      return served(row, rid);
    loseEntry(rid);
    return null;
  }

  /**
   * Notes a served record of the key, unique index or not: a lookup of a prefix of a composite unique key is one key with many
   * entries, and a read of it again must not serve them twice.
   */
  private Result served(final Result row, final RID rid) {
    if (!servedOverflow) {
      if (servedOfKey.size() >= trackedRidsPerKey)
        servedOverflow = true;
      else
        servedOfKey.add(rid);
    }
    return row;
  }

  private boolean holdsKey(final FetchFromIndexStep indexStep, final Result row, final Object key) {
    ++verifiedRows;
    return row.getElement().map(record -> indexStep.isKeyOf(record, key)).orElse(false);
  }

  private void loseEntry(final RID rid) {
    keyLostEntries = true;
    ++lostEntries;
    ++pendingLosses;
    // its bucket is released under REPEATABLE_READ before the key is read again
    readBucketIds.add(rid.getBucketId());
  }

  /**
   * Reads the key of the stream again, for the entries a commit took from it (#9902): the candidates are the entries the key holds
   * now that were not served yet. Waits for a commit that is publishing to finish first, so the replacement of a record found
   * deleted - published by the same commit, maybe on a page not put in the cache yet - is among them. Index entries only: the
   * records are loaded one at a time as the consumer pulls them, see {@link #nextRereadRow}.
   *
   * @return false when the key is not read again: it was read again too many times already ({@link #MAX_REREADS_PER_KEY}), or
   * served too many records to tell which ones a read again must skip ({@link #MAX_TRACKED_RIDS_PER_KEY}). The entries lost are
   * then counted as unsettled
   */
  private boolean startReread(final CommandContext context, final FetchFromIndexStep indexStep, final WorkGuard guard) {
    keyLostEntries = false;
    if (rereadsOfKey >= MAX_REREADS_PER_KEY || servedOverflow) {
      LogManager.instance().log(this, Level.FINE, "Key %s of index '%s' lost %d entries to commits while it was read and is not read "
              + "again (read again %d times, %d records served): the records that replaced them are not looked for", null, streamKey,
          indexStep.indexName, pendingLosses, rereadsOfKey, servedOfKey.size());
      // unsettledEntries is still 0 only the first time: one WARNING per execution, however many keys give up
      if (unsettledEntries == 0 && pendingLosses > 0)
        LogManager.instance().log(this, Level.WARNING, "Point lookup on index '%s' lost entries of key %s to concurrent commits and "
                + "did not look for the records that replaced them (key read again %d times, %d records served): the answer may "
                + "miss records written during the query (logged once per execution, PROFILE counts every such entry)", null,
            indexStep.indexName, streamKey, rereadsOfKey, servedOfKey.size());
      unsettledEntries += pendingLosses;
      pendingLosses = 0;
      return false;
    }
    pendingLosses = 0;
    ++rereadsOfKey;
    ++keyRereads;

    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final PageManager pageManager = database.getPageManager();
    guard.check();
    if ((pageManager.getPublicationSequence() & 1) != 0)
      // A commit is publishing: wait for it to let go of the lock, doing nothing under it. The wait cannot deadlock a reader that
      // holds file locks in its transaction, for the reason given at loadPointLookup(): a committer takes this lock last and
      // acquires nothing under it. It lasts one commit's publication, so the guard is checked before it and not during it
      pageManager.executeInLock(() -> null);
    unpinRepeatableReadPages(database, indexStep);
    rereadSequence = pageManager.getPublicationSequence();

    final List<RID> fresh = indexStep.lookupKeyAgain(streamKey, guard);
    rereadCandidates = new ArrayDeque<>(fresh == null ? 0 : fresh.size());
    if (fresh != null) {
      rereadEntries += fresh.size();
      for (final RID rid : fresh)
        if (!servedOfKey.contains(rid) && passesBucketFilter(rid))
          rereadCandidates.add(rid);
    }
    return true;
  }

  /**
   * The next row of the entries a key was read again for, or null once they ran out. As for the entries of the lookup, a row is
   * served as loaded when no commit published since the key was read again, and otherwise only if its record holds the key.
   */
  private Result nextRereadRow(final CommandContext context, final FetchFromIndexStep indexStep, final WorkGuard guard) {
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    RID rid;
    while ((rid = rereadCandidates.pollFirst()) != null) {
      // a record load each, I/O: checked on every one (#9893)
      guard.check();
      final Result row = toResult(rid, context, false);
      if (noCommitSince(database, rereadSequence)) {
        if (row != null)
          return served(row, rid);
        // the key and the record were read in one state: a dangling entry, not one a commit took
        LogManager.instance().log(this, Level.WARNING, "Record %s not found. Skip it from the result set", null, rid);
      } else if (row != null && holdsKey(indexStep, row, streamKey))
        return served(row, rid);
      else
        loseEntry(rid);
    }
    return null;
  }

  /** True when no commit published since {@code sequence} was sampled, nor was publishing then. */
  private static boolean noCommitSince(final DatabaseInternal database, final long sequence) {
    return (sequence & 1) == 0 && database.getPageManager().getPublicationSequence() == sequence;
  }

  private static boolean isRepeatableRead(final DatabaseInternal database) {
    return database.isTransactionActive()
        && database.getTransactionIsolationLevel() == Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ;
  }

  private ResultSet physicalOrderResultSet(final CommandContext context, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    return new ResultSet() {
      Result nextItem = null;
      int    fetched  = 0;

      @Override
      public boolean hasNext() {
        if (fetched >= nRecords)
          return false;
        if (nextItem == null)
          nextItem = nextInPhysicalOrder(context, guard);
        return nextItem != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }
    };
  }

  private Result nextInPhysicalOrder(final CommandContext context, final WorkGuard guard) {
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      Object entry;
      while ((entry = fetcher.nextRecord(context.getDatabase())) != null) {
        guard.checkPeriodically((int) ++rowCount);
        final Result result = entry instanceof Record record ? new ResultInternal(record) : toResult(entry, context);
        if (result != null)
          return result;
      }
      return null;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * Reads the index entries alone, without loading a record, until either they run out or more of them match than
   * the scan threshold. See the class comment.
   */
  private void chooseStrategy(final CommandContext context, final ExecutionStepInternal prevStep) {
    // The threshold is a share of the records the target buckets hold, so it needs them named: the planner always does
    // when it builds a fallback, and a step without buckets keeps the index order
    if (scanFallback == null || !(prevStep instanceof FetchFromIndexStep indexStep) || filterBucketIds == null) {
      strategy = Strategy.INDEX_ORDER;
      return;
    }

    // The threshold falls with the workers the scan would run on: the index entries are read by one thread whatever
    // the parallelism, so the more the scan is split the sooner it wins
    final long scanThreshold = PhysicalOrderRidFetcher.scanThreshold(context.getDatabase(), filterBucketIds,
        ParallelTypeScan.plannedWorkers(context.getDatabase(), filterBucketIds));
    if (scanThreshold < 0 || !keysAreScalars(context)) {
      strategy = Strategy.INDEX_ORDER;
      return;
    }

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      final boolean[] firstPass = { true };
      fetcher = new PhysicalOrderRidFetcher(() -> {
        // The first pass reads the plan's own index step; a second one, for a range too large to buffer, a fresh copy
        final FetchFromIndexStep step;
        if (firstPass[0]) {
          firstPass[0] = false;
          step = indexStep;
        } else
          step = (FetchFromIndexStep) indexStep.copy(context);
        return new IndexStepSource(step, context);
      }, scanThreshold);

      switch (fetcher.start(WorkGuard.forCommandDeadline(context))) {
      case SCAN -> {
        this.scanThreshold = scanThreshold;
        fetcher = null;
        scanStep = new FetchFromTypeWithFilterStep(scanFallback.typeName(), scanFallback.bucketNames(),
            scanFallback.keyFilter(), context, null);
        strategy = Strategy.SCAN;
      }
      case PHYSICAL_ORDER -> strategy = Strategy.PHYSICAL_ORDER;
      case PHYSICAL_ORDER_CHUNKED -> strategy = Strategy.PHYSICAL_ORDER_CHUNKED;
      }
      // Kept on the step: PROFILE is rendered after close() released the fetcher
      matchedEntries = fetcher != null ? fetcher.getMatched() : 0;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * Whether every value the index key is compared with is a single value. The index search expands a collection -
   * {@code tag = :p} with {@code :p = ['a', 'b']} seeks each element - where evaluating the same condition on a
   * record compares the value with the collection as a whole, so a search over collections keeps the index.
   */
  private boolean keysAreScalars(final CommandContext context) {
    final BooleanExpression keys = scanFallback.keyFilter().getBaseExpression();
    if (!(keys instanceof AndBlock and))
      return false;
    for (final BooleanExpression block : and.getSubBlocks()) {
      if (block instanceof BinaryCondition condition) {
        if (MultiValue.isMultiValue(condition.getRight().execute((Result) null, context)))
          return false;
      } else if (block instanceof BetweenCondition between) {
        if (MultiValue.isMultiValue(between.getSecond().execute((Result) null, context))
            || MultiValue.isMultiValue(between.getThird().execute((Result) null, context)))
          return false;
      } else
        return false;
    }
    return true;
  }

  /** One pass over the entries an index step returns, restricted to the target buckets. */
  private final class IndexStepSource implements PhysicalOrderRidFetcher.Source {
    private final FetchFromIndexStep step;
    private final CommandContext     context;

    private IndexStepSource(final FetchFromIndexStep step, final CommandContext context) {
      this.step = step;
      this.context = context;
    }

    @Override
    public Object next() {
      Identifiable value;
      while ((value = step.nextIdentifiable(context)) != null)
        if (passesBucketFilter(value))
          return value;
      return null;
    }

    @Override
    public void close() {
      step.releaseCursors();
    }
  }

  private boolean passesBucketFilter(final Object value) {
    if (filterBucketIds == null)
      return true;
    if (!(value instanceof Identifiable identifiable))
      return false;
    final int bucketId = identifiable.getIdentity().getBucketId();
    if (bucketId < 0)
      return true;
    for (final int filterClusterId : filterBucketIds)
      if (filterClusterId == bucketId)
        return true;
    return false;
  }

  private static final int MAX_RECONCILE_ATTEMPTS   = 8;
  private static final int MAX_POINT_LOOKUP_ENTRIES = 256;

  /**
   * Loads the rows of a point lookup - an equality or an {@code IN} - against ONE committed state (#9369). A commit puts its
   * pages in the read cache one at a time and gives a freed record slot to the next record it creates, so entries read
   * before a commit and records loaded after it do not belong together: a row is lost when its record is gone, or served as
   * another key's when the slot was reused, and a row found in a later state can then collide with one served earlier from
   * an older one. The entries are read and every record loaded between two samples of the page manager's publication
   * sequence that are equal and even, which no commit can have interleaved with; otherwise the lookup starts again, and
   * after {@link #MAX_RECONCILE_ATTEMPTS} attempts runs under the publication lock, which keeps every commit out. Costs
   * two volatile reads when no commit overlaps.
   * <p>
   * The rows are held until all are loaded, so a lookup of more than {@link #MAX_POINT_LOOKUP_ENTRIES} entries is not
   * held: it returns null, leaving the entries read in {@link #pendingEntries} for the streaming path, which settles them
   * one by one (#9902, see {@link #pointLookupRow}): an answer that large is not one state, but every row of it is a record
   * that holds its key when it is loaded, none is served twice, and a record that holds its key the whole time is served.
   * <p>
   * The last-resort lock cannot deadlock: it is the page-manager lock, which is reentrant and which a committer takes last,
   * inside the file locks, only to publish pages. It also holds for a reader inside a transaction that holds file locks
   * (explicit {@code LOCK}, or writes): the committer that owns the page-manager lock took every file lock it needs before
   * taking it and acquires none under it, so it never waits for the locks this reader holds; and nothing the reader runs under the
   * lock waits on a committer.
   */
  @SuppressWarnings("unchecked")
  private ArrayDeque<Result> loadPointLookup(final CommandContext context, final FetchFromIndexStep indexStep, final int nRecords,
      final WorkGuard guard) {
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final PageManager pageManager = database.getPageManager();
    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      for (int attempt = 0; attempt < MAX_RECONCILE_ATTEMPTS; attempt++) {
        // a statement cancelled or past its deadline stops between attempts (#9893)
        guard.check();
        if ((pageManager.getPublicationSequence() & 1) != 0)
          // a commit is publishing: wait for it to let go of the lock instead of spending an attempt inside its window
          pageManager.executeInLock(() -> null);
        final ArrayDeque<Result> rows = readAndLoad(context, indexStep, nRecords, guard);
        if (rows == null)
          return null;
        final long baseline = indexStep.entriesSequence();
        if ((baseline & 1) == 0 && pageManager.getPublicationSequence() == baseline)
          return rows;
        restartLookup(database, indexStep);
      }
      LogManager.instance().log(this, Level.FINE, "Point lookup on index '%s' overlapped a commit %d times, reading it under the publication lock",
          null, indexStep.indexName, MAX_RECONCILE_ATTEMPTS);
      // and not once the lock is held: every commit would wait for a statement that is going to fail anyway
      guard.check();
      final ArrayDeque<Result> rows = (ArrayDeque<Result>) pageManager.executeInLock(() -> {
        restartLookup(database, indexStep);
        return readAndLoad(context, indexStep, nRecords, guard);
      });
      return rows;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /** The rows of every entry the index step returns, or null once there are more entries than are held. */
  private ArrayDeque<Result> readAndLoad(final CommandContext context, final FetchFromIndexStep indexStep, final int nRecords,
      final WorkGuard guard) {
    final List<Result> entries = new ArrayList<>();
    prevResult = indexStep.syncPull(context, nRecords);
    while (true) {
      while (prevResult.hasNext()) {
        entries.add(prevResult.next());
        if (entries.size() > MAX_POINT_LOOKUP_ENTRIES) {
          pendingEntries.addAll(entries);
          return null;
        }
      }
      prevResult = indexStep.syncPull(context, nRecords);
      if (!prevResult.hasNext())
        break;
    }
    final ArrayDeque<Result> rows = new ArrayDeque<>(entries.size());
    for (final Result entry : entries) {
      final Object value = entry.getProperty("rid");
      if (!passesBucketFilter(value))
        continue;
      if (value instanceof RID rid)
        readBucketIds.add(rid.getBucketId());
      // a record load each, I/O: checked on every one, or a termination waits for the whole chunk (#9893)
      guard.check();
      final Result row = toResult(value, context);
      if (row != null)
        rows.add(row);
    }
    return rows;
  }

  /** Starts the lookup over after a commit overlapped it, releasing what REPEATABLE_READ pinned of the torn state. */
  private void restartLookup(final DatabaseInternal database, final FetchFromIndexStep indexStep) {
    indexStep.reset();
    prevResult = null;
    prevExhausted = false;
    pendingEntries.clear();
    unpinRepeatableReadPages(database, indexStep);
  }

  /**
   * Under REPEATABLE_READ the pages this transaction pinned while a commit overlapped the lookup may be the very two states
   * that disagree. Only called once an overlap was detected, so a lookup that no commit touched keeps its snapshot; the
   * pinned pages of the files involved are read again from the committed state, which is the one the lookup is repeated on.
   */
  private void unpinRepeatableReadPages(final DatabaseInternal database, final FetchFromIndexStep indexStep) {
    if (!database.isTransactionActive() || database.getTransactionIsolationLevel() != Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ)
      return;
    final List<Integer> files = new ArrayList<>(indexStep.indexFileIds());
    if (filterBucketIds != null)
      files.addAll(filterBucketIds);
    files.addAll(readBucketIds);
    database.getTransaction().unpinFiles(files);
  }

  /**
   * Looks an index key up again and loads its records against one committed state, for an entry of a range scan whose record
   * could not be loaded (#9369): a range scan reads its entries as it goes, so only the entries whose record is gone are judged,
   * see {@link #pointLookupRow} for a point lookup. A unique key holds one entry and is looked up again, which finds the record
   * that replaced a deleted one; a non-unique key would have to be read whole, so only the entry itself is retried. The lookup
   * and the load are repeated until no commit published in between, and as a last resort run under the publication lock, which
   * keeps any commit out for the time of that one entry. Costs nothing unless an entry is dropped.
   *
   * @return the rows now found for the entry, empty when the key is gone or the entry was genuinely dangling
   */
  @SuppressWarnings("unchecked")
  private List<Result> reconcile(final FetchFromIndexStep indexStep, final Object key, final RID missing, final CommandContext context,
      final WorkGuard guard) {
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    final PageManager pageManager = database.getPageManager();
    for (int attempt = 0; attempt < MAX_RECONCILE_ATTEMPTS; attempt++) {
      guard.check();
      final long before = pageManager.getPublicationSequence();
      if ((before & 1) != 0) {
        // a commit is publishing: wait for it to let go of the lock
        pageManager.executeInLock(() -> null);
        continue;
      }
      final List<Result> found = loadEntryAgain(database, indexStep, key, missing, context, guard);
      if (pageManager.getPublicationSequence() == before)
        return found;
    }
    guard.check();
    return (List<Result>) pageManager.executeInLock(() -> loadEntryAgain(database, indexStep, key, missing, context, guard));
  }

  private List<Result> loadEntryAgain(final DatabaseInternal database, final FetchFromIndexStep indexStep, final Object key,
      final RID missing, final CommandContext context, final WorkGuard guard) {
    // The streaming path does not note the buckets it reads (a set insert per row): the entry that needs reconciling names one
    readBucketIds.add(missing.getBucketId());
    unpinRepeatableReadPages(database, indexStep);
    final List<Result> found = new ArrayList<>(1);
    final List<RID> fresh = indexStep.isIndexUnique() ? indexStep.lookupKeyAgain(key, guard) : null;
    for (final RID rid : fresh != null ? fresh : List.of(missing)) {
      if (!passesBucketFilter(rid))
        continue;
      final Result row = toResult(rid, context, false);
      if (row != null)
        found.add(row);
    }
    return found;
  }

  /**
   * The row of an index entry, loaded through the context's database, or null when there is none. Static: the workers
   * of a parallel load call it too.
   */
  private static Result toResult(final Object value, final CommandContext context) {
    return toResult(value, context, true);
  }

  private static Result toResult(final Object value, final CommandContext context, final boolean warnIfMissing) {
    if (value instanceof RID rid) {
      try {
        // A DatabaseRID carries its origin database, so asDocument() resolves directly. For bare RIDs, route through the query's command-context
        // database instead of rid.asDocument() — the latter falls back to the thread-local active database, which is ambiguous and can pick the wrong
        // schema when multiple databases are open on the same thread.
        return new ResultInternal(
            rid instanceof DatabaseRID ? rid.asDocument() : (Document) context.getDatabase().lookupByRID(rid, true));
      } catch (final RecordNotFoundException e) {
        if (warnIfMissing)
          LogManager.instance()
              .log(GetValueFromIndexEntryStep.class, Level.WARNING, "Record %s not found. Skip it from the result set", null, value);
        return null;
      }
    } else if (value instanceof Document document)
      return new ResultInternal(document);
    else if (value instanceof Result result)
      return result;
    return null;
  }

  @Override
  public void reset() {
    prevResult = null;
    consistentRows = null;
    consistentTried = false;
    pendingEntries.clear();
    prevExhausted = false;
    recovered.clear();
    streamKey = NO_KEY;
    servedOfKey = null;
    keyLostEntries = false;
    rereadsOfKey = 0;
    rereadCandidates = null;
    verifiedRows = 0;
    lostEntries = 0;
    keyRereads = 0;
    rereadEntries = 0;
    unsettledEntries = 0;
    servedOverflow = false;
    pendingLosses = 0;
    readBucketIds.clear();
    strategy = null;
    matchedEntries = 0;
    parallelDecided = false;
    parallelRounds = 0;
    releaseRuntimeSteps();
  }

  @Override
  public void close() {
    releaseRuntimeSteps();
    super.close();
  }

  private void releaseRuntimeSteps() {
    if (parallelRound != null) {
      parallelRound.close();
      parallelRound = null;
    }
    if (fetcher != null) {
      fetcher.close();
      fetcher = null;
    }
    if (scanStep != null) {
      scanStep.close();
      scanStep = null;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder(spaces).append("+ EXTRACT VALUE FROM INDEX ENTRY");

    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");

    if (filterBucketIds != null) {
      result.append("\n").append(spaces).append("  filtering buckets [");
      result.append(filterBucketIds.stream().map(x -> "" + x).collect(Collectors.joining(",")));
      result.append("]");
    }

    if (context.isProfiling() && (verifiedRows > 0 || keyRereads > 0 || unsettledEntries > 0)) {
      result.append("\n").append(spaces).append("  ").append(verifiedRows).append(" rows checked against their key after a commit, ")
          .append(lostEntries).append(" entries lost");
      if (keyRereads > 0)
        result.append(", key read again ").append(keyRereads).append(" times (").append(rereadEntries).append(" entries)");
      if (unsettledEntries > 0)
        result.append(", ").append(unsettledEntries).append(" lost entries not looked for again");
    }

    if (scanFallback != null) {
      result.append("\n").append(spaces).append("  in physical order, or by a full scan of ").append(scanFallback.typeName())
          .append(" when the range holds a large share of it");
      if (strategy != null) {
        result.append("\n").append(spaces).append("  served by ");
        switch (strategy) {
        case INDEX_ORDER -> result.append("index order (the share could not be estimated)");
        case PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED -> {
          result.append("physical order (").append(matchedEntries).append(" entries matched)");
          if (parallelRounds > 0)
            result.append(", loaded in parallel");
        }
        case SCAN -> result.append("full scan (more than ").append(scanThreshold).append(" entries matched)");
        }
        if (strategy == Strategy.SCAN && scanStep != null)
          result.append("\n").append(scanStep.prettyPrint(depth + 1, indent));
      }
    }

    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new GetValueFromIndexEntryStep(context, this.filterBucketIds, scanFallback == null ? null : scanFallback.copy());
  }
}
