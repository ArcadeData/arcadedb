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
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.Statement;

import java.util.*;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class LetQueryStep extends AbstractExecutionStep {
  private final Identifier varName;
  private final Statement  query;

  public LetQueryStep(final Identifier varName, final Statement query, final CommandContext context, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.varName = varName;
    this.query = query;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("Cannot execute a local LET on a query without a target");

    return new ResultSet() {
      final ResultSet source = getPrev().syncPull(context, nRecords);

      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public Result next() {
        final ResultInternal result = (ResultInternal) source.next();
        if (result != null) {
          calculate(result, context);
        }
        return result;
      }

      private void calculate(final ResultInternal result, final CommandContext context) {
        final long beginTime = System.nanoTime();

        final BasicCommandContext subCtx = new BasicCommandContext();
        subCtx.setDatabase(context.getDatabase());
        subCtx.setParentWithoutOverridingChild(context);
        final InternalExecutionPlan subExecutionPlan = query.createExecutionPlan(subCtx, profilingEnabled);
        final List<Result> value = toList(new LocalResultSet(subExecutionPlan));
        result.setMetadata(varName.getStringValue(), value);
        context.setVariable(varName.getStringValue(), value);

        cost = System.nanoTime() - beginTime;
      }

      private List<Result> toList(final LocalResultSet oLocalResultSet) {
        final List<Result> result = new ArrayList<>();
        while (oLocalResultSet.hasNext()) {
          result.add(oLocalResultSet.next());
        }
        oLocalResultSet.close();
        return result;
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
    result.append(spaces).append("+ LET (for each record)\n").append(spaces).append("  ").append(varName).append(" = (").append(query).append(")");
    if (profilingEnabled)
      result.append(" (").append(getCostFormatted()).append(")");
    return result.toString();
  }
}
