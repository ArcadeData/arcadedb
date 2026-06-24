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

import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.FromClause;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Level;

/**
 * Created by luigidellaquila on 06/08/16.
 */
public class FetchFromIndexedFunctionStep extends AbstractExecutionStep {
  private final BinaryCondition functionCondition;
  private final FromClause      queryTarget;

  //runtime0
  Iterator<Record> fullResult = null;

  public FetchFromIndexedFunctionStep(final BinaryCondition functionCondition, final FromClause queryTarget,
      final CommandContext context) {
    super(context);
    this.functionCondition = functionCondition;
    this.queryTarget = queryTarget;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    init(context);

    return new ResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        return fullResult.hasNext();
      }

      @Override
      public Result next() {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          if (localCount >= nRecords) {
            throw new NoSuchElementException();
          }
          if (!fullResult.hasNext()) {
            throw new NoSuchElementException();
          }
          final ResultInternal result = new ResultInternal(fullResult.next().getRecord());
          localCount++;
          return result;
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

    };
  }

  private void init(final CommandContext context) {
    if (fullResult == null) {
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        fullResult = functionCondition.executeIndexedFunction(queryTarget, context).iterator();
      } finally {
        if (context.isProfiling()) {
          cost += System.nanoTime() - begin;
        }
      }
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    String result =
        ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEXED FUNCTION " + functionCondition.toString();
    if (context.isProfiling()) {
      result += " (" + getCostFormatted() + ")";
    }
    // Surface BM25 scoring metadata (similarity, k1/b, N, avgdl, per-term df/idf) for full-text SEARCH_INDEX conditions so it can
    // be inspected via EXPLAIN/PROFILE. This delegates down the indexed-function chain
    // (BinaryCondition -> Expression -> ... -> FunctionCall -> IndexableSQLFunction.getScoringExplain), mirroring
    // executeIndexedFunction; non-indexable or non-full-text functions return null. Best-effort: never let an explain failure
    // break the plan rendering.
    try {
      final Object scoring = functionCondition.getIndexedFunctionScoringExplain(queryTarget, context);
      if (scoring != null)
        result += "\n" + ExecutionStepInternal.getIndent(depth, indent) + "  SCORING " + scoring;
    } catch (final Exception ex) {
      // Scoring metadata is informational only, so never let it break plan rendering; log at FINE so the failure is still
      // discoverable when debugging instead of being silently swallowed.
      LogManager.instance().log(this, Level.FINE, "Failed to collect full-text scoring metadata for EXPLAIN/PROFILE", ex);
    }
    return result;
  }

  @Override
  public void reset() {
    this.fullResult = null;
  }
}
