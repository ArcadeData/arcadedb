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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.query.sql.parser.PInteger;
import com.arcadedb.query.sql.parser.TraverseProjectionItem;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 26/10/16.
 */
public abstract class AbstractTraverseStep extends AbstractExecutionStep {
  protected final WhereClause                  whileClause;
  // Optional emit filter pushed down from an outer SELECT's WHERE. Non-matching vertices still drive expansion, they just don't appear in the results. Null
  // means "emit everything traversed", preserving the historical behavior.
  protected final WhereClause                  postFilter;
  protected final List<TraverseProjectionItem> projections;
  protected final PInteger                     maxDepth;

  // ArrayDeque, not ArrayList: entryPoints is used as a stack (addFirst/removeFirst in the hot loop). ArrayList would make both O(n), turning the traversal into
  // O(n^2) on the queue depth and was the main cause of slow TRAVERSE on deep/wide subgraphs.
  protected       Deque<Result> entryPoints = null;
  protected final List<Result>  results     = new ArrayList<>();

  // Visited set for traversal dedup. Graph traversal is inherently sparse (RIDs scatter across buckets with random offsets), so RidHashSet's primitive-packed
  // open-addressing hash wins over RidSet's bitmap on both memory and build time. See performance.RidDedupSetBenchmark.
  // EdgeIdentitySet keeps that fast path for record-backed identities and falls back to a plain, RID.equals()-honouring
  // set only for the record-less ones a LIGHTWEIGHT edge produces - RidHashSet alone keyed every one of them to the
  // same (edge type bucket, placeholder position) pair and collapsed a type's edges into one (issue #7480).
  final EdgeIdentitySet traversed;

  /**
   * What one record visited costs the heap: its slot in {@link #traversed}, two primitives in an open-addressing table
   * kept between a half and three quarters full.
   */
  static final int VISITED_ENTRY_BYTES = 24;

  // The visited set is the buffer a TRAVERSE holds for its whole life: one entry per record reached, released only when
  // the step closes. Its bytes are charged to the heap budget all queries share (issue #9792); the frontier is not counted
  // apart, as it never holds a record the visited set does not. It is not capped in number by
  // queryMaxHeapElementsAllowedPerOp: an entry is a couple of primitives, not the ~2KB element that cap is sized on, so a
  // traversal of a large graph would fail it with heap to spare.
  private OperationHeapLimit visitedLimit;

  public AbstractTraverseStep(final List<TraverseProjectionItem> projections, final WhereClause whileClause,
      final WhereClause postFilter,
      final PInteger maxDepth,
      final CommandContext context) {
    super(context);
    this.traversed = new EdgeIdentitySet();
    this.whileClause = whileClause;
    this.postFilter = postFilter;
    this.maxDepth = maxDepth;
    this.projections = projections.stream().map(TraverseProjectionItem::copy).collect(Collectors.toList());
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) {
    //TODO

    return new ResultSet() {
      int localFetched = 0;

      @Override
      public boolean hasNext() {
        if (localFetched >= nRecords) {
          return false;
        }
        if (results.isEmpty()) {
          fetchNextBlock(context, nRecords);
        }
        return !results.isEmpty();
      }

      @Override
      public Result next() {
        if (localFetched >= nRecords) {
          throw new NoSuchElementException();
        }
        if (results.isEmpty()) {
          fetchNextBlock(context, nRecords);
          if (results.isEmpty()) {
            throw new NoSuchElementException();
          }
        }
        localFetched++;
        final Result result = results.removeFirst();
        if (result.isElement())
          markTraversed(result.getElement().get().getIdentity());
        return result;
      }

    };
  }

  /**
   * Adds {@code rid} to the records visited, charging a new one to the heap budget: past it the query fails rather than
   * the visited set growing until the heap is exhausted.
   *
   * @return true if the record was not visited before
   */
  protected final boolean markTraversed(final RID rid) {
    if (!traversed.add(rid))
      return false;
    if (visitedLimit == null)
      visitedLimit = OperationHeapLimit.of(context, "visited records", "TRAVERSE");
    try {
      visitedLimit.charge(visitedEntryBytes());
    } catch (final RuntimeException e) {
      // THE QUERY FAILS, POSSIBLY BEFORE ITS CALLER HOLDS A RESULT SET TO CLOSE: GIVE THE HEAP BACK NOW
      visitedLimit.release();
      throw e;
    }
    return true;
  }

  /** What one more record visited costs the heap, across every structure the strategy keeps per visited record. */
  protected int visitedEntryBytes() {
    return VISITED_ENTRY_BYTES;
  }

  @Override
  public void close() {
    if (visitedLimit != null)
      visitedLimit.release();
    super.close();
  }

  private void fetchNextBlock(final CommandContext context, final int nRecords) {
    if (this.entryPoints == null)
      this.entryPoints = new ArrayDeque<>();

    // A WHILE clause that keeps expanding without emitting anything spends the whole traversal inside this one
    // call, so the command deadline is tested per expansion round rather than per returned row (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    while (this.results.isEmpty()) {
      guard.check();
      if (this.entryPoints.isEmpty())
        fetchNextEntryPoints(context, nRecords);

      if (this.entryPoints.isEmpty())
        return;

      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      fetchNextResults(context, nRecords);
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
  }

  /**
   * The entry point a row arriving from the previous step starts the walk from, or {@code null} when it cannot start one.
   * One answer for both strategies, because they differ in the order they visit nodes and in nothing else: the two private
   * copies this replaces had drifted apart, and the breadth-first one dropped a one-column row holding a bare RID
   * ({@code SELECT @rid ...}, a LINK property) where the depth-first one loaded the record, so the same query answered the
   * whole traversal under one strategy and nothing under the other (issue #9778).
   */
  protected static Result toTraverseResult(final Result item) {
    TraverseResult res = null;
    if (item instanceof TraverseResult result)
      res = result;
    else if (item.isElement() && item.getElement().get().getIdentity() != null) {
      res = new TraverseResult(item.getElement().get());
      res.depth = 0;
    } else if (item.getPropertyNames().size() == 1) {
      final Object val = item.getProperty(item.getPropertyNames().iterator().next());
      if (val instanceof Identifiable identifiable && loadEntryPoint(identifiable) instanceof Document document) {
        res = new TraverseResult(document);
        res.depth = 0;
        res.setMetadata("$depth", 0);
      }
    } else {
      res = new TraverseResult();
      for (final String key : item.getPropertyNames())
        res.setProperty(key, item.getProperty(key));

      for (final String md : item.getMetadataKeys())
        res.setMetadata(md, item.getMetadata(md));
    }

    return res;
  }

  /**
   * The record a one-column entry-point row points at, or {@code null} when there is none to start from: a LINK whose record
   * was deleted has nothing to walk, the same as a LINK left null, so it is skipped rather than failing the whole query.
   */
  private static Record loadEntryPoint(final Identifiable identifiable) {
    try {
      return identifiable.getRecord();
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  protected abstract void fetchNextEntryPoints(CommandContext context, int nRecords);

  protected abstract void fetchNextResults(CommandContext context, int nRecords);

  // whileClause/postFilter/projections/maxDepth are immutable configuration, evaluated dynamically against whatever
  // context a copy is bound to (same contract as FilterStep, ProjectionCalculationStep, ...); entryPoints/results/
  // traversed and each subclass's own dedup state are runtime-only and always start empty in a fresh instance, so
  // a template built from a row that was already pulled to exhaustion is still a safe copy source - see the
  // concrete subclasses' #copy for what actually gets reused.
  @Override
  public boolean canBeCached() {
    return true;
  }
}
