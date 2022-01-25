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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.parser.*;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class DeleteVertexExecutionPlanner {

  private final FromClause  fromClause;
  private final WhereClause whereClause;
  private final boolean     returnBefore;
  private final Limit       limit;

  public DeleteVertexExecutionPlanner(DeleteVertexStatement stm) {
    this.fromClause = stm.getFromClause() == null ? null : stm.getFromClause().copy();
    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.returnBefore = stm.isReturnBefore();
    this.limit = stm.getLimit();
  }

  public DeleteExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    DeleteExecutionPlan result = new DeleteExecutionPlan(ctx);

    if (handleIndexAsTarget(result, fromClause.getItem().getIndex(), whereClause, ctx)) {
      if (limit != null) {
        throw new CommandExecutionException("Cannot apply a LIMIT on a delete from index");
      }
      if (returnBefore) {
        throw new CommandExecutionException("Cannot apply a RETURN BEFORE on a delete from index");
      }

    } else {
      handleTarget(result, ctx, this.fromClause, this.whereClause, enableProfiling);
      handleLimit(result, ctx, this.limit, enableProfiling);
    }
    handleCastToVertex(result, ctx, enableProfiling);
    handleDelete(result, ctx, enableProfiling);
    handleReturn(result, ctx, this.returnBefore, enableProfiling);
    return result;
  }

  private boolean handleIndexAsTarget(DeleteExecutionPlan result, IndexIdentifier indexIdentifier, WhereClause whereClause,
      CommandContext ctx) {
    if (indexIdentifier == null) {
      return false;
    }
    throw new CommandExecutionException("DELETE VERTEX FROM INDEX is not supported");
  }

  private void handleDelete(DeleteExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new DeleteStep(ctx, profilingEnabled));
  }

//  private void handleUnsafe(DeleteExecutionPlan result, CommandContext ctx, boolean unsafe, boolean profilingEnabled) {
//    if (!unsafe) {
//      result.chain(new CheckSafeDeleteStep(ctx, profilingEnabled));
//    }
//  }

  private void handleReturn(DeleteExecutionPlan result, CommandContext ctx, boolean returnBefore, boolean profilingEnabled) {
    if (!returnBefore) {
      result.chain(new CountStep(ctx, profilingEnabled));
    }
  }

  private void handleLimit(UpdateExecutionPlan plan, CommandContext ctx, Limit limit, boolean profilingEnabled) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx, profilingEnabled));
    }
  }

  private void handleCastToVertex(DeleteExecutionPlan plan, CommandContext ctx, boolean profilingEnabled) {
    plan.chain(new CastToVertexStep(ctx, profilingEnabled));
  }

  private void handleTarget(UpdateExecutionPlan result, CommandContext ctx, FromClause target, WhereClause whereClause, boolean profilingEnabled) {
    SelectStatement sourceStatement = new SelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    SelectExecutionPlanner planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, profilingEnabled), ctx, ctx, profilingEnabled));
  }
}
