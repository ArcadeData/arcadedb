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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.opencypher.executor.operators.VarLengthExpand;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Collections;
import java.util.List;

/**
 * Answers {@code MATCH (a)-[:T*0|1..k]->(b) [WHERE b <> a] RETURN count(DISTINCT b)} from the walks of a distinct-endpoint
 * expansion, counting the end vertices as they are reached (issue #10548). The rows the expansion would otherwise produce, one per
 * end vertex, the filter that drops the start, and the aggregation that counts them are never built: through a Graph Analytical
 * View the count is a bitmap of node ids.
 * <p>
 * The planner replaces the operators' rows with this step only when nothing reads them but that count (see
 * {@code CypherExecutionPlan#tryCountDistinctEndpoints}), so the row it answers is the one the aggregation would have.
 */
public final class DistinctEndpointCountStep extends AbstractExecutionStep {
  private final VarLengthExpand expand;
  private final boolean         excludeSource;
  private final boolean         countSources;
  private final String          countAlias;
  private       boolean         executed;

  /**
   * @param excludeSource the rows a {@code WHERE b <> a} keeps
   * @param countSources  count the expansion's start vertices that reach an end vertex, for a counted variable the plan starts
   *                      the walk from, rather than the end vertices
   */
  public DistinctEndpointCountStep(final VarLengthExpand expand, final boolean excludeSource, final boolean countSources,
      final String countAlias, final CommandContext context) {
    super(context);
    this.expand = expand;
    this.excludeSource = excludeSource;
    this.countSources = countSources;
    this.countAlias = countAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (executed)
      return new IteratorResultSet(Collections.<Result>emptyList().iterator());
    executed = true;

    final long begin = context.isProfiling() ? System.nanoTime() : 0L;
    final long count;
    try {
      count = expand.countDistinct(context, excludeSource, countSources);
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }
    if (context.isProfiling())
      rowCount = 1;

    final ResultInternal result = new ResultInternal();
    result.setProperty(countAlias, count);
    return new IteratorResultSet(List.of((Result) result).iterator());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append("  ".repeat(Math.max(0, depth * indent)));
    builder.append("+ DISTINCT ENDPOINT COUNT count(DISTINCT ")
        .append(countSources ? expand.getSourceVariable() : expand.getTargetVariable()).append(") AS ").append(countAlias);
    builder.append(" [").append(expand.getOperatorType()).append("(").append(expand.getSourceVariable()).append(")");
    if (excludeSource)
      builder.append(", ").append(expand.getTargetVariable()).append(" <> ").append(expand.getSourceVariable());
    builder.append("]");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    return builder.toString();
  }
}
