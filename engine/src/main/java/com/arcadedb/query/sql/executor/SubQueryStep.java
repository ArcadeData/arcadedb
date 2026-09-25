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

import java.util.List;

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
  public SubQueryStep(final InternalExecutionPlan subExecutionPlan, final CommandContext context, final CommandContext subCtx) {
    super(context);
    this.subExecutionPlan = subExecutionPlan;
    this.sameContextAsParent = context == subCtx;
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

  /**
   * #5662: closes the sub-plan too. Only the OUTER plan's steps used to be closed, so anything the sub-plan held was
   * released only if the sub-plan happened to run to exhaustion - and a {@code DELETE ... WHERE} is built exactly this
   * way, with the index scan inside the sub-plan and the {@code LIMIT} outside it. The abandoned
   * {@link com.arcadedb.index.IndexCursor} then kept its compacted-series registration for the lifetime of the
   * database.
   */
  @Override
  public void close() {
    subExecutionPlan.close();
    super.close();
  }

  @Override
  public List<ExecutionPlan> getSubExecutionPlans() {
    return List.of(subExecutionPlan);
  }

  @Override
  public boolean canBeCached() {
    return subExecutionPlan.canBeCached();
  }

  /**
   * A correlated FROM-subquery (a target that reads {@code $parent}, e.g. {@code TRAVERSE ... FROM (SELECT
   * $parent.x)}) is planned with its own child context, not the enclosing plan's own - see
   * SelectExecutionPlanner#handleSubqueryAsTarget / TraverseExecutionPlanner, which build exactly the
   * {@code BasicCommandContext} + {@code setParent} pair reconstructed here. Reusing {@code context} for both ends,
   * as the {@code sameContextAsParent} branch does, would collapse that child relationship and resolve {@code
   * $parent} one level too shallow in the copy.
   */
  @Override
  public ExecutionStep copy(final CommandContext context) {
    if (sameContextAsParent)
      return new SubQueryStep(subExecutionPlan.copy(context), context, context);

    final BasicCommandContext subCtx = new BasicCommandContext();
    subCtx.setDatabase(context.getDatabase());
    subCtx.setParent(context);
    return new SubQueryStep(subExecutionPlan.copy(subCtx), context, subCtx);
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
