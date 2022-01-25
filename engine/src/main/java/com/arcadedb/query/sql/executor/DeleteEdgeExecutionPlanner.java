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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

  public DeleteEdgeExecutionPlanner(DeleteEdgeStatement stm) {

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

  public DeleteExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    DeleteExecutionPlan result = new DeleteExecutionPlan(ctx);

    if (leftExpression != null || rightExpression != null) {
      handleGlobalLet(result, new Identifier("$__ARCADEDB_DELETE_EDGE_fromV"), leftExpression, ctx, enableProfiling);
      handleGlobalLet(result, new Identifier("$__ARCADEDB_DELETE_EDGE_toV"), rightExpression, ctx, enableProfiling);
      handleFetchFromTo(result, ctx, "$__ARCADEDB_DELETE_EDGE_fromV", "$__ARCADEDB_DELETE_EDGE_toV", className, targetClusterName, enableProfiling);
      handleWhere(result, ctx, whereClause, enableProfiling);
    } else if (whereClause != null) {
      FromClause fromClause = new FromClause(-1);
      FromItem item = new FromItem(-1);
      if (className == null) {
        item.setIdentifier(new Identifier("E"));
      } else {
        item.setIdentifier(className);
      }
      fromClause.setItem(item);
      handleTarget(result, ctx, fromClause, this.whereClause, enableProfiling);
    } else {
      handleTargetClass(result, ctx, className, enableProfiling);
      handleTargetCluster(result, ctx, targetClusterName, enableProfiling);
      handleTargetRids(result, ctx, rids, enableProfiling);
    }

    handleLimit(result, ctx, this.limit, enableProfiling);

    handleCastToEdge(result, ctx, enableProfiling);

    handleDelete(result, ctx, enableProfiling);

    handleReturn(result, ctx, enableProfiling);
    return result;
  }

  private void handleWhere(DeleteExecutionPlan result, CommandContext ctx, WhereClause whereClause, boolean profilingEnabled) {
    if (whereClause != null) {
      result.chain(new FilterStep(whereClause, ctx, profilingEnabled));
    }
  }

  private void handleFetchFromTo(DeleteExecutionPlan result, CommandContext ctx, String fromAlias, String toAlias, Identifier targetClass,
      Identifier targetCluster, boolean profilingEnabled) {
    if (fromAlias != null && toAlias != null) {
      result.chain(new FetchEdgesFromToVerticesStep(fromAlias, toAlias, targetClass, targetCluster, ctx, profilingEnabled));
    } else if (toAlias != null) {
      result.chain(new FetchEdgesToVerticesStep(toAlias, targetClass, targetCluster, ctx, profilingEnabled));
    }
  }

  private void handleTargetRids(DeleteExecutionPlan result, CommandContext ctx, List<Rid> rids, boolean profilingEnabled) {
    if (rids != null) {
      result.chain(new FetchFromRidsStep(rids.stream().map(x -> x.toRecordId((Result) null, ctx)).collect(Collectors.toList()), ctx, profilingEnabled));
    }
  }

  private void handleTargetCluster(DeleteExecutionPlan result, CommandContext ctx, Identifier targetClusterName, boolean profilingEnabled) {
    if (targetClusterName != null) {
      String name = targetClusterName.getStringValue();
      int bucketId = ctx.getDatabase().getSchema().getBucketByName(name).getId();
      if (bucketId < 0) {
        throw new CommandExecutionException("Cluster not found: " + name);
      }
      result.chain(new FetchFromClusterExecutionStep(bucketId, ctx, profilingEnabled));
    }
  }

  private void handleTargetClass(DeleteExecutionPlan result, CommandContext ctx, Identifier className, boolean profilingEnabled) {
    if (className != null) {
      result.chain(new FetchFromClassExecutionStep(className.getStringValue(), null, ctx, null, profilingEnabled));
    }
  }

//  private boolean handleIndexAsTarget(DeleteExecutionPlan result, IndexIdentifier indexIdentifier, WhereClause whereClause,
//      CommandContext ctx, boolean profilingEnabled) {
//    if (indexIdentifier == null) {
//      return false;
//    }
//    throw new CommandExecutionException("DELETE VERTEX FROM INDEX is not supported");
//  }

  private void handleDelete(DeleteExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new DeleteStep(ctx, profilingEnabled));
  }

  private void handleReturn(DeleteExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new CountStep(ctx, profilingEnabled));
  }

  private void handleLimit(UpdateExecutionPlan plan, CommandContext ctx, Limit limit, boolean profilingEnabled) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx, profilingEnabled));
    }
  }

  private void handleCastToEdge(DeleteExecutionPlan plan, CommandContext ctx, boolean profilingEnabled) {
    plan.chain(new CastToEdgeStep(ctx, profilingEnabled));
  }

  private void handleTarget(UpdateExecutionPlan result, CommandContext ctx, FromClause target, WhereClause whereClause, boolean profilingEnabled) {
    SelectStatement sourceStatement = new SelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    SelectExecutionPlanner planner = new SelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, profilingEnabled), ctx, ctx, profilingEnabled));
  }

  private void handleGlobalLet(DeleteExecutionPlan result, Identifier name, Expression expression, CommandContext ctx, boolean profilingEnabled) {
    if (expression != null) {
      result.chain(new GlobalLetExpressionStep(name, expression, ctx, profilingEnabled));
    }
  }
}
