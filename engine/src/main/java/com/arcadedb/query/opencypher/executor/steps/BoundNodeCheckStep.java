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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.NoSuchElementException;

/**
 * The step a bare node pattern {@code (n)} over an already-bound variable plans to: the pattern adds no label and no
 * property, so the carried binding is the match, but only when it is a node. A binding an earlier OPTIONAL MATCH left
 * null (or one a WITH projected to a non-node value) cannot match, and the row is dropped, exactly as
 * {@link MatchNodeStep} does for a bound variable with constraints (issue #11761). Rows that pass are handed on as they
 * are, without a copy.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public class BoundNodeCheckStep extends AbstractExecutionStep {
  private final String variable;

  public BoundNodeCheckStep(final String variable, final CommandContext context) {
    super(context);
    this.variable = variable;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("BoundNodeCheckStep requires a previous step");

    final ResultSet prevResults = prev.syncPull(context, nRecords);

    return new ResultSet() {
      private Result nextResult = null;

      @Override
      public boolean hasNext() {
        while (nextResult == null && prevResults.hasNext()) {
          final Result candidate = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;
            if (candidate.getProperty(variable) instanceof Vertex)
              nextResult = candidate;
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
        return nextResult != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result result = nextResult;
        nextResult = null;
        return result;
      }

      @Override
      public void close() {
        prevResults.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder result = new StringBuilder();
    result.append("  ".repeat(Math.max(0, depth * indent))).append("+ CHECK BOUND NODE (").append(variable).append(")");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");
    return result.toString();
  }
}
