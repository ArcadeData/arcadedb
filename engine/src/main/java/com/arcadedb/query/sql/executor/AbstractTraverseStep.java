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

import com.arcadedb.database.RID;
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
  protected final List<TraverseProjectionItem> projections;
  protected final PInteger                     maxDepth;

  protected       List<Result> entryPoints = null;
  protected final List<Result> results     = new ArrayList<>();
  private         long         cost        = 0;

  final Set<RID> traversed = new RidSet();

  public AbstractTraverseStep(List<TraverseProjectionItem> projections, WhereClause whileClause, PInteger maxDepth, CommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.whileClause = whileClause;
    this.maxDepth = maxDepth;
    this.projections = projections.stream().map(TraverseProjectionItem::copy).collect(Collectors.toList());
  }

  @Override
  public ResultSet syncPull(CommandContext ctx, int nRecords) {
    //TODO

    return new ResultSet() {
      int localFetched = 0;

      @Override
      public boolean hasNext() {
        if (localFetched >= nRecords) {
          return false;
        }
        if (results.isEmpty()) {
          fetchNextBlock(ctx, nRecords);
        }
        return !results.isEmpty();
      }

      @Override
      public Result next() {
        if (localFetched >= nRecords) {
          throw new IllegalStateException();
        }
        if (results.isEmpty()) {
          fetchNextBlock(ctx, nRecords);
          if (results.isEmpty()) {
            throw new IllegalStateException();
          }
        }
        localFetched++;
        Result result = results.remove(0);
        if (result.isElement()) {
          traversed.add(result.getElement().get().getIdentity());
        }
        return result;
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private void fetchNextBlock(CommandContext ctx, int nRecords) {
    if (this.entryPoints == null) {
      this.entryPoints = new ArrayList<>();
    }

    while (this.results.isEmpty()) {
      if (this.entryPoints.isEmpty()) {
        fetchNextEntryPoints(ctx, nRecords);
      }
      if (this.entryPoints.isEmpty()) {
        return;
      }
      long begin = profilingEnabled ? System.nanoTime() : 0;
      fetchNextResults(ctx, nRecords);
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  protected abstract void fetchNextEntryPoints(CommandContext ctx, int nRecords);

  protected abstract void fetchNextResults(CommandContext ctx, int nRecords);

  protected boolean isFinished() {
    return entryPoints != null && entryPoints.isEmpty() && results.isEmpty();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
