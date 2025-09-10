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
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.DeleteStatement;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.IndexIdentifier;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.*;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class DeleteExecutionPlanner {
  private final FromClause  fromClause;
  private final WhereClause whereClause;
  private final boolean     returnBefore;
  private final Limit       limit;
  private final boolean     unsafe;

  public DeleteExecutionPlanner(final DeleteStatement stm) {
    this.fromClause = stm.getFromClause() == null ? null : stm.getFromClause().copy();
    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.returnBefore = stm.isReturnBefore();
    this.limit = stm.getLimit();
    this.unsafe = stm.isUnsafe();
  }

  public DeleteExecutionPlan createExecutionPlan(final CommandContext context) {
    final DeleteExecutionPlan result = new DeleteExecutionPlan(context);

    if (handleIndexAsTarget(result, fromClause.getItem().getIndex(), whereClause, context)) {
      if (limit != null) {
        throw new CommandExecutionException("Cannot apply a LIMIT on a delete from index");
      }
      if (unsafe) {
        throw new CommandExecutionException("Cannot apply a UNSAFE on a delete from index");
      }
      if (returnBefore) {
        throw new CommandExecutionException("Cannot apply a RETURN BEFORE on a delete from index");
      }

      handleReturn(result, context, this.returnBefore);
    } else {
      handleTarget(result, context, this.fromClause, this.whereClause);
      handleUnsafe(result, context, this.unsafe);
      handleLimit(result, context, this.limit);
      handleDelete(result, context);
      handleReturn(result, context, this.returnBefore);
    }
    return result;
  }

  private boolean handleIndexAsTarget(final DeleteExecutionPlan result, final IndexIdentifier indexIdentifier,
      WhereClause whereClause,
      final CommandContext context) {
    if (indexIdentifier == null) {
      return false;
    }
    final String indexName = indexIdentifier.getIndexName();
    final RangeIndex index = (RangeIndex) context.getDatabase().getSchema().getIndexByName(indexName);
    if (index == null) {
      throw new CommandExecutionException("Index not found: " + indexName);
    }
    List<AndBlock> flattenedWhereClause = whereClause == null ? null : whereClause.flatten();

    switch (indexIdentifier.getType()) {
    case INDEX:
      final BooleanExpression keyCondition;
      BooleanExpression ridCondition = null;
      if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
        //TODO
//        if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
//        }
      } else if (flattenedWhereClause.size() > 1) {
        throw new CommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
      } else {
        final AndBlock andBlock = flattenedWhereClause.getFirst();
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
      result.chain(new DeleteFromIndexStep(index, keyCondition, null, ridCondition, context));
      if (ridCondition != null) {
        final WhereClause where = new WhereClause(-1);
        where.setBaseExpression(ridCondition);
        result.chain(new FilterStep(where, context));
      }
      return true;
    case VALUES:
      result.chain(new FetchFromIndexValuesStep(index, true, context));
      result.chain(new GetValueFromIndexEntryStep(context, null));
      break;
    case VALUESASC:
//      if (!index.supportsOrderedIterations()) {
      throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
//      }
//      result.chain(new FetchFromIndexValuesStep(index, true, context));
//      result.chain(new GetValueFromIndexEntryStep(context, null));
//      break;
    case VALUESDESC:
//      if (!index.supportsOrderedIterations()) {
      throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
//      }
//      result.chain(new FetchFromIndexValuesStep(index, false, context));
//      result.chain(new GetValueFromIndexEntryStep(context, null));
//      break;
    }
    return false;
  }

  private void handleDelete(final DeleteExecutionPlan result, final CommandContext context) {
    result.chain(new DeleteStep(context));
  }

  private void handleUnsafe(final DeleteExecutionPlan result, final CommandContext context, final boolean unsafe) {
    if (!unsafe)
      result.chain(new CheckSafeDeleteStep(context));
  }

  private void handleReturn(final DeleteExecutionPlan result, final CommandContext context, final boolean returnBefore) {
    if (!returnBefore)
      result.chain(new CountStep(context));
  }

  private void handleLimit(final UpdateExecutionPlan plan, final CommandContext context, final Limit limit) {
    if (limit != null)
      plan.chain(new LimitExecutionStep(limit, context));
  }

  private void handleTarget(final UpdateExecutionPlan result,
      final CommandContext context,
      final FromClause fromClause,
      final WhereClause whereClause) {
    final SelectStatement sourceStatement = new SelectStatement(-1);
    sourceStatement.setTarget(fromClause);
    sourceStatement.setWhereClause(whereClause);
    final SelectExecutionPlanner planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(
        new SubQueryStep(planner.createExecutionPlan(context, false), context, context));
  }

  private BooleanExpression getKeyCondition(final AndBlock andBlock) {
    for (final BooleanExpression exp : andBlock.getSubBlocks()) {
      final String str = exp.toString();
      if (str.length() < 5)
        continue;

      if (str.substring(0, 4).equalsIgnoreCase("key "))
        return exp;
    }
    return null;
  }

  private BooleanExpression getRidCondition(final AndBlock andBlock) {
    for (final BooleanExpression exp : andBlock.getSubBlocks()) {
      final String str = exp.toString();
      if (str.length() < 5)
        continue;

      if (str.substring(0, 4).equalsIgnoreCase("rid "))
        return exp;

    }
    return null;
  }
}
