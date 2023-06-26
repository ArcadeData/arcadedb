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

import java.util.*;

/**
 * Created by luigidellaquila on 22/07/16.
 */
public class SubQueryStep extends AbstractExecutionStep {
  private final InternalExecutionPlan subExecutionPlan;
  private final boolean               sameContextAsParent;

  /**
   * executes a sub-query
   *
   * @param subExecutionPlan the execution plan of the sub-query
   * @param context          the context of the current execution plan
   * @param subCtx           the context of the subquery execution plan
   */
  public SubQueryStep(final InternalExecutionPlan subExecutionPlan, final CommandContext context, final CommandContext subCtx, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.subExecutionPlan = subExecutionPlan;
    this.sameContextAsParent = (context == subCtx);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    ResultSet parentRs = subExecutionPlan.fetchNext(nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return parentRs.hasNext();
      }

      @Override
      public Result next() {
        Result item = parentRs.next();
        context.setVariable("current", item);
        return item;
      }

      @Override
      public void close() {
        parentRs.close();
      }
    };
  }

  @Override
  public List<ExecutionPlan> getSubExecutionPlans() {
    return List.of(subExecutionPlan);
  }

  @Override
  public boolean canBeCached() {
    return sameContextAsParent && subExecutionPlan.canBeCached();
  }

  @Override
  public ExecutionStep copy(CommandContext context) {
    return new SubQueryStep(subExecutionPlan.copy(context), context, context, profilingEnabled);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = ExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM SUBQUERY \n");
    builder.append(subExecutionPlan.prettyPrint(depth + 1, indent));
    return builder.toString();
  }
}
