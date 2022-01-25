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
import com.arcadedb.index.RangeIndex;
import com.arcadedb.query.sql.parser.*;

import java.util.List;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class DeleteExecutionPlanner {

  private final FromClause  fromClause;
  private final WhereClause whereClause;
  private final boolean     returnBefore;
  private final Limit       limit;
  private final boolean     unsafe;

  public DeleteExecutionPlanner(DeleteStatement stm) {
    this.fromClause = stm.getFromClause() == null ? null : stm.getFromClause().copy();
    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.returnBefore = stm.isReturnBefore();
    this.limit = stm.getLimit();
    this.unsafe = stm.isUnsafe();
  }

  public DeleteExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    DeleteExecutionPlan result = new DeleteExecutionPlan(ctx);

    if (handleIndexAsTarget(result, fromClause.getItem().getIndex(), whereClause, ctx,enableProfiling)) {
      if (limit != null) {
        throw new CommandExecutionException("Cannot apply a LIMIT on a delete from index");
      }
      if (unsafe) {
        throw new CommandExecutionException("Cannot apply a UNSAFE on a delete from index");
      }
      if (returnBefore) {
        throw new CommandExecutionException("Cannot apply a RETURN BEFORE on a delete from index");
      }

      handleReturn(result, ctx, this.returnBefore, enableProfiling);
    } else {
      handleTarget(result, ctx, this.fromClause, this.whereClause, enableProfiling);
      handleUnsafe(result, ctx, this.unsafe, enableProfiling);
      handleLimit(result, ctx, this.limit, enableProfiling);
      handleDelete(result, ctx, enableProfiling);
      handleReturn(result, ctx, this.returnBefore, enableProfiling);
    }
    return result;
  }

  private boolean handleIndexAsTarget(DeleteExecutionPlan result, IndexIdentifier indexIdentifier, WhereClause whereClause,
      CommandContext ctx, boolean profilingEnabled) {
    if (indexIdentifier == null) {
      return false;
    }
    String indexName = indexIdentifier.getIndexName();
    RangeIndex index = (RangeIndex) ctx.getDatabase().getSchema().getIndexByName(indexName);
    if (index == null) {
      throw new CommandExecutionException("Index not found: " + indexName);
    }
    List<AndBlock> flattenedWhereClause = whereClause == null ? null : whereClause.flatten();

    switch (indexIdentifier.getType()) {
    case INDEX:
      BooleanExpression keyCondition;
      BooleanExpression ridCondition = null;
      if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
        //TODO
//        if (!index.supportsOrderedIterations()) {
          throw new CommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
//        }
      } else if (flattenedWhereClause.size() > 1) {
        throw new CommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
      } else {
        AndBlock andBlock = flattenedWhereClause.get(0);
        if (andBlock.getSubBlocks().size() == 1) {

          whereClause = null;//The WHERE clause won't be used anymore, the index does all the filtering
          flattenedWhereClause = null;
          keyCondition = getKeyCondition(andBlock);
          if (keyCondition == null) {
            throw new CommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
          }
        } else if (andBlock.getSubBlocks().size() == 2) {
          whereClause = null;//The WHERE clause won't be used anymore, the index does all the filtering
          flattenedWhereClause = null;
          keyCondition = getKeyCondition(andBlock);
          ridCondition = getRidCondition(andBlock);
          if (keyCondition == null || ridCondition == null) {
            throw new CommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
          }
        } else {
          throw new CommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
        }
      }
      result.chain(new DeleteFromIndexStep(index, keyCondition, null, ridCondition, ctx, profilingEnabled));
      if (ridCondition != null) {
        WhereClause where = new WhereClause(-1);
        where.setBaseExpression(ridCondition);
        result.chain(new FilterStep(where, ctx, profilingEnabled));
      }
      return true;
    case VALUES:
      result.chain(new FetchFromIndexValuesStep(index, true, ctx, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
      break;
    case VALUESASC:
//      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
//      }
//      result.chain(new FetchFromIndexValuesStep(index, true, ctx, profilingEnabled));
//      result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
//      break;
    case VALUESDESC:
//      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
//      }
//      result.chain(new FetchFromIndexValuesStep(index, false, ctx, profilingEnabled));
//      result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
//      break;
    }
    return false;
  }

  private void handleDelete(DeleteExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new DeleteStep(ctx, profilingEnabled));
  }

  private void handleUnsafe(DeleteExecutionPlan result, CommandContext ctx, boolean unsafe, boolean profilingEnabled) {
    if (!unsafe) {
      result.chain(new CheckSafeDeleteStep(ctx, profilingEnabled));
    }
  }

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

  private void handleTarget(UpdateExecutionPlan result, CommandContext ctx, FromClause target, WhereClause whereClause, boolean profilingEnabled) {
    SelectStatement sourceStatement = new SelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    SelectExecutionPlanner planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, profilingEnabled), ctx, ctx, profilingEnabled));
  }

  private BooleanExpression getKeyCondition(AndBlock andBlock) {
    for (BooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("key ")) {
        return exp;
      }
    }
    return null;
  }

  private BooleanExpression getRidCondition(AndBlock andBlock) {
    for (BooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("rid ")) {
        return exp;
      }
    }
    return null;
  }

}
