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

import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.WhereClause;

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
 *
 * Created by luigidellaquila on 16/03/17.
 */
public class GetValueFromIndexEntryStep extends AbstractExecutionStep {
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

  private final List<Integer> filterBucketIds;
  private final ScanFallback  scanFallback;

  // runtime
  private ResultSet                   prevResult = null;
  private Strategy                    strategy;
  private PhysicalOrderRidFetcher     fetcher;
  private FetchFromTypeWithFilterStep scanStep;
  private long                        scanThreshold;

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

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();

    if (strategy == null)
      chooseStrategy(context, prevStep);

    return switch (strategy) {
      case SCAN -> scanStep.syncPull(context, nRecords);
      case PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED -> physicalOrderResultSet(context, nRecords);
      case INDEX_ORDER -> indexOrderResultSet(context, prevStep, nRecords);
    };
  }

  private ResultSet indexOrderResultSet(final CommandContext context, final ExecutionStepInternal prevStep, final int nRecords) {
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

        if (prevResult == null) {
          prevResult = prevStep.syncPull(context, nRecords);
          if (!prevResult.hasNext()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          while (!prevResult.hasNext()) {
            prevResult = prevStep.syncPull(context, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }
          final Result val = prevResult.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;

          try {
            final Object finalVal = val.getProperty("rid");
            if (!passesBucketFilter(finalVal))
              continue;

            nextItem = toResult(finalVal, context);
            if (nextItem != null)
              break;
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }
    };
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
      while ((entry = fetcher.next()) != null) {
        guard.checkPeriodically((int) ++rowCount);
        final Result result = entry instanceof RID rid ? load(rid, context) : toResult(entry, context);
        if (result != null)
          return result;
      }
      return null;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /** Loads a RID the fetcher serves in physical order, through the query's database. */
  private Result load(final RID rid, final CommandContext context) {
    try {
      return new ResultInternal((Document) context.getDatabase().lookupByRID(rid, true));
    } catch (final RecordNotFoundException e) {
      LogManager.instance().log(this, Level.WARNING, "Record %s not found. Skip it from the result set", null, rid);
      return null;
    }
  }

  /**
   * Reads the index entries alone, without loading a record, until either they run out or more of them match than
   * the scan threshold. See the class comment.
   */
  private void chooseStrategy(final CommandContext context, final ExecutionStepInternal prevStep) {
    if (scanFallback == null || !(prevStep instanceof FetchFromIndexStep indexStep) || filterBucketIds == null) {
      strategy = Strategy.INDEX_ORDER;
      return;
    }

    final long scanThreshold = PhysicalOrderRidFetcher.scanThreshold(context.getDatabase(), filterBucketIds);
    if (scanThreshold < 0) {
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
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
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

  private Result toResult(final Object value, final CommandContext context) {
    if (value instanceof RID rid) {
      try {
        // A DatabaseRID carries its origin database, so asDocument() resolves directly. For bare RIDs, route through the query's command-context
        // database instead of rid.asDocument() — the latter falls back to the thread-local active database, which is ambiguous and can pick the wrong
        // schema when multiple databases are open on the same thread.
        return new ResultInternal(
            rid instanceof DatabaseRID ? rid.asDocument() : (Document) context.getDatabase().lookupByRID(rid, true));
      } catch (final RecordNotFoundException e) {
        LogManager.instance().log(this, Level.WARNING, "Record %s not found. Skip it from the result set", null, value);
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
    strategy = null;
    releaseRuntimeSteps();
  }

  @Override
  public void close() {
    releaseRuntimeSteps();
    super.close();
  }

  private void releaseRuntimeSteps() {
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

    if (scanFallback != null) {
      result.append("\n").append(spaces).append("  in physical order, or by a full scan of ").append(scanFallback.typeName())
          .append(" when the range holds a large share of it");
      if (strategy != null) {
        result.append("\n").append(spaces).append("  served by ");
        switch (strategy) {
        case INDEX_ORDER -> result.append("index order (the share could not be estimated)");
        case PHYSICAL_ORDER, PHYSICAL_ORDER_CHUNKED -> result.append("physical order (").append(fetcher != null ? fetcher.getMatched() : 0)
            .append(" entries matched)");
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
