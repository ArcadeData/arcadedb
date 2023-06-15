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
import com.arcadedb.query.sql.parser.DeleteEdgeStatement;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.*;
import java.util.stream.*;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class DeleteEdgeExecutionPlanner {
  protected final Identifier  className;
  protected final Identifier  targetClusterName;
  protected final List<Rid>   rids;
  private final   Expression  leftExpression;
  private final   Expression  rightExpression;
  private final   WhereClause whereClause;
  private final   Limit       limit;

  public DeleteEdgeExecutionPlanner(final DeleteEdgeStatement stm) {
    this.className = stm.getTypeName() == null ? null : stm.getTypeName().copy();
    this.targetClusterName = stm.getTargetBucketName() == null ? null : stm.getTargetBucketName().copy();
    if (stm.getRid() != null) {
      this.rids = new ArrayList<>();
      rids.add(stm.getRid().copy());
    } else {
      this.rids = stm.getRids() == null ? null : stm.getRids().stream().map(x -> x.copy()).collect(Collectors.toList());
    }

    this.leftExpression = stm.getLeftExpression() == null ? null : stm.getLeftExpression().copy();
    this.rightExpression = stm.getRightExpression() == null ? null : stm.getRightExpression().copy();

    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.limit = stm.getLimit() == null ? null : stm.getLimit().copy();
  }

  public DeleteExecutionPlan createExecutionPlan(final CommandContext context, final boolean enableProfiling) {
    final DeleteExecutionPlan result = new DeleteExecutionPlan(context);

    if (leftExpression != null || rightExpression != null) {
      if (leftExpression != null)
        handleGlobalLet(result, new Identifier("$__ARCADEDB_DELETE_EDGE_fromV"), leftExpression, context, enableProfiling);
      if (rightExpression != null)
        handleGlobalLet(result, new Identifier("$__ARCADEDB_DELETE_EDGE_toV"), rightExpression, context, enableProfiling);
      handleFetchFromTo(result, context,//
          leftExpression != null ? "$__ARCADEDB_DELETE_EDGE_fromV" : null,//
          rightExpression != null ? "$__ARCADEDB_DELETE_EDGE_toV" : null,//
          className, targetClusterName, enableProfiling);
      handleWhere(result, context, whereClause, enableProfiling);
    } else if (whereClause != null) {
      final FromClause fromClause = new FromClause(-1);
      final FromItem item = new FromItem(-1);
      if (className == null) {
        item.setIdentifier(new Identifier("E"));
      } else {
        item.setIdentifier(className);
      }
      fromClause.setItem(item);
      handleTarget(result, context, fromClause, this.whereClause, enableProfiling);
    } else {
      handleTargetClass(result, context, className, enableProfiling);
      handleTargetCluster(result, context, targetClusterName, enableProfiling);
      handleTargetRids(result, context, rids, enableProfiling);
    }

    handleLimit(result, context, this.limit, enableProfiling);
    handleCastToEdge(result, context, enableProfiling);
    handleDelete(result, context, enableProfiling);
    handleReturn(result, context, enableProfiling);
    return result;
  }

  private void handleWhere(final DeleteExecutionPlan result, final CommandContext context, final WhereClause whereClause, final boolean profilingEnabled) {
    if (whereClause != null)
      result.chain(new FilterStep(whereClause, context, profilingEnabled));
  }

  private void handleFetchFromTo(final DeleteExecutionPlan result, final CommandContext context, final String fromAlias, final String toAlias,
      final Identifier targetClass, final Identifier targetCluster, final boolean profilingEnabled) {
    if (fromAlias != null)
      result.chain(new FetchEdgesFromToVerticesStep(fromAlias, toAlias, targetClass, targetCluster, context, profilingEnabled));
    else if (toAlias != null)
      result.chain(new FetchEdgesToVerticesStep(toAlias, targetClass, targetCluster, context, profilingEnabled));
  }

  private void handleTargetRids(final DeleteExecutionPlan result, final CommandContext context, final List<Rid> rids, final boolean profilingEnabled) {
    if (rids != null) {
      result.chain(new FetchFromRidsStep(rids.stream().map(x -> x.toRecordId((Result) null, context)).collect(Collectors.toList()), context, profilingEnabled));
    }
  }

  private void handleTargetCluster(final DeleteExecutionPlan result, final CommandContext context, final Identifier targetClusterName,
      final boolean profilingEnabled) {
    if (targetClusterName != null) {
      final String name = targetClusterName.getStringValue();
      final int bucketId = context.getDatabase().getSchema().getBucketByName(name).getFileId();
      if (bucketId < 0)
        throw new CommandExecutionException("Cluster not found: " + name);

      result.chain(new FetchFromClusterExecutionStep(bucketId, context, profilingEnabled));
    }
  }

  private void handleTargetClass(final DeleteExecutionPlan result, final CommandContext context, final Identifier className, final boolean profilingEnabled) {
    if (className != null)
      result.chain(new FetchFromClassExecutionStep(className.getStringValue(), null, context, null, profilingEnabled));
  }

//  private boolean handleIndexAsTarget(DeleteExecutionPlan result, IndexIdentifier indexIdentifier, WhereClause whereClause,
//      CommandContext context, boolean profilingEnabled) {
//    if (indexIdentifier == null) {
//      return false;
//    }
//    throw new CommandExecutionException("DELETE VERTEX FROM INDEX is not supported");
//  }

  private void handleDelete(final DeleteExecutionPlan result, final CommandContext context, final boolean profilingEnabled) {
    result.chain(new DeleteStep(context, profilingEnabled));
  }

  private void handleReturn(final DeleteExecutionPlan result, final CommandContext context, final boolean profilingEnabled) {
    result.chain(new CountStep(context, profilingEnabled));
  }

  private void handleLimit(final UpdateExecutionPlan plan, final CommandContext context, final Limit limit, final boolean profilingEnabled) {
    if (limit != null)
      plan.chain(new LimitExecutionStep(limit, context, profilingEnabled));
  }

  private void handleCastToEdge(final DeleteExecutionPlan plan, final CommandContext context, final boolean profilingEnabled) {
    plan.chain(new CastToEdgeStep(context, profilingEnabled));
  }

  private void handleTarget(final UpdateExecutionPlan result, final CommandContext context, final FromClause target, final WhereClause whereClause,
      final boolean profilingEnabled) {
    final SelectStatement sourceStatement = new SelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    final SelectExecutionPlanner planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(context, profilingEnabled), context, context, profilingEnabled));
  }

  private void handleGlobalLet(final DeleteExecutionPlan result, final Identifier name, final Expression expression, final CommandContext context,
      final boolean profilingEnabled) {
    if (expression != null)
      result.chain(new GlobalLetExpressionStep(name, expression, context, profilingEnabled));
  }
}
