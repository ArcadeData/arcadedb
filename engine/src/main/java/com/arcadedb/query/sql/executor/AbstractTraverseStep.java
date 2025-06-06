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
import java.util.stream.*;

/**
 * Created by luigidellaquila on 26/10/16.
 */
public abstract class AbstractTraverseStep extends AbstractExecutionStep {
  protected final WhereClause                  whileClause;
  protected final List<TraverseProjectionItem> projections;
  protected final PInteger                     maxDepth;

  protected       List<Result> entryPoints = null;
  protected final List<Result> results     = new ArrayList<>();

  final Set<RID> traversed = new RidSet();

  public AbstractTraverseStep(final List<TraverseProjectionItem> projections, final WhereClause whileClause,
      final PInteger maxDepth,
      final CommandContext context) {
    super(context);
    this.whileClause = whileClause;
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
      this.entryPoints = new ArrayList<>();

    while (this.results.isEmpty()) {
      if (this.entryPoints.isEmpty())
        fetchNextEntryPoints(context, nRecords);

      if (this.entryPoints.isEmpty())
        return;

      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      fetchNextResults(context, nRecords);
      if (context.isProfiling())
        cost += (System.nanoTime() - begin);
    }
  }

  protected abstract void fetchNextEntryPoints(CommandContext context, int nRecords);

  protected abstract void fetchNextResults(CommandContext context, int nRecords);
}
