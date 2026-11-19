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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.WhereClause;

/**
 * Ends a branch of an OR whose branches run one after the other and are deduplicated (issue #11618): sets {@code $score} of
 * each row to the best of the score the branch served it with and the score the whole WHERE gives it on the record, and
 * drops no row. The WHERE gives a record matching several arms the best score of those arms, the one {@code MergeByScoreStep}
 * serves it with, while the branch alone gives the score of its own arm. The branch's score is kept when it is higher: an arm
 * an index answered, as a full-text index answers CONTAINSTEXT ignoring case, can be false on the record, and then the
 * WHERE does not see the score that arm matched with.
 */
public class RescoreStep extends AbstractExecutionStep {
  private static final String      SCORE_VARIABLE = "$score";
  private final        WhereClause rescore;
  private final        boolean     served;

  /**
   * @param rescore the WHERE, in a form every row matches, that sets {@code $score} to the best score of the arms the record
   *                matches, or 0
   * @param served  whether the last step of the branch evaluated its scoring condition on the row, so that {@code $score}
   *                holds the score the branch served the row with. Otherwise it holds the score of another row
   */
  public RescoreStep(final WhereClause rescore, final boolean served, final CommandContext context) {
    super(context);
    this.rescore = rescore;
    this.served = served;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet source = checkForPrevious().syncPull(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public Result next() {
        final Result row = source.next();
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          final float branchScore = served && context.getVariable(SCORE_VARIABLE) instanceof Number score ? score.floatValue() : 0f;
          context.setVariable("current", row);
          rescore.matchesFilters(row, context);
          if (!(context.getVariable(SCORE_VARIABLE) instanceof Number score) || score.floatValue() < branchScore)
            context.setVariable(SCORE_VARIABLE, branchScore);
          return row;
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }

      @Override
      public void close() {
        source.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces).append("+ SET $score TO THE BEST OF ").append(served ? "THE BRANCH AND " : "").append("THE WHERE ");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");
    result.append("\n").append(spaces).append("  ").append(rescore);
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new RescoreStep(rescore.copy(), served, context);
  }
}
