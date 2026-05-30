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

import com.arcadedb.query.sql.parser.PInteger;
import com.arcadedb.query.sql.parser.TraverseProjectionItem;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.utility.RidHashSet;

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
  final RidHashSet traversed;

  public AbstractTraverseStep(final List<TraverseProjectionItem> projections, final WhereClause whileClause,
      final WhereClause postFilter,
      final PInteger maxDepth,
      final CommandContext context) {
    super(context);
    this.traversed = new RidHashSet();
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
        if (result.isElement()) {
          traversed.add(result.getElement().get().getIdentity());
        }
        return result;
      }

    };
  }

  private void fetchNextBlock(final CommandContext context, final int nRecords) {
    if (this.entryPoints == null)
      this.entryPoints = new ArrayDeque<>();

    while (this.results.isEmpty()) {
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

  protected abstract void fetchNextEntryPoints(CommandContext context, int nRecords);

  protected abstract void fetchNextResults(CommandContext context, int nRecords);
}
