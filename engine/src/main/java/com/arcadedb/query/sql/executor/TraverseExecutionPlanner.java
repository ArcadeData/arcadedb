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

import com.arcadedb.database.Database;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.query.sql.parser.*;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class TraverseExecutionPlanner {
  private final List<TraverseProjectionItem> projections;
  private final FromClause                   target;
  private final WhereClause                  whileClause;
  private final TraverseStatement.Strategy   strategy;
  private final PInteger                     maxDepth;
  private final Skip                         skip;
  private final Limit                        limit;

  public TraverseExecutionPlanner(TraverseStatement statement) {
    //copying the content, so that it can be manipulated and optimized
    this.projections = statement.getProjections() == null ? null : statement.getProjections().stream().map(x -> x.copy()).collect(Collectors.toList());

    this.target = statement.getTarget();
    this.whileClause = statement.getWhileClause() == null ? null : statement.getWhileClause().copy();

    this.strategy = statement.getStrategy() == null ? TraverseStatement.Strategy.DEPTH_FIRST : statement.getStrategy();
    this.maxDepth = statement.getMaxDepth() == null ? null : statement.getMaxDepth().copy();

    this.skip = statement.getSkip();
    this.limit = statement.getLimit();
  }

  public InternalExecutionPlan createExecutionPlan(CommandContext ctx, boolean enableProfiling) {
    SelectExecutionPlan result = new SelectExecutionPlan(ctx);

    handleFetchFromTarger(result, ctx, enableProfiling);

    handleTraversal(result, ctx, enableProfiling);

    if (skip != null) {
      result.chain(new SkipExecutionStep(skip, ctx, enableProfiling));
    }
    if (limit != null) {
      result.chain(new LimitExecutionStep(limit, ctx, enableProfiling));
    }

    return result;
  }

  private void handleTraversal(SelectExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    switch (strategy) {
    case BREADTH_FIRST:
      result.chain(new BreadthFirstTraverseStep(this.projections, this.whileClause, maxDepth, ctx, profilingEnabled));
      break;
    case DEPTH_FIRST:
      result.chain(new DepthFirstTraverseStep(this.projections, this.whileClause, maxDepth, ctx, profilingEnabled));
      break;
    }
    //TODO
  }

  private void handleFetchFromTarger(SelectExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {

    FromItem target = this.target == null ? null : this.target.getItem();
    if (target == null) {
      handleNoTarget(result, ctx, profilingEnabled);
    } else if (target.getIdentifier() != null) {
      handleClassAsTarget(result, this.target, ctx, profilingEnabled);
    } else if (target.getBucket() != null) {
      handleClustersAsTarget(result, Collections.singletonList(target.getBucket()), ctx, profilingEnabled);
    } else if (target.getBucketList() != null) {
      handleClustersAsTarget(result, target.getBucketList().toListOfClusters(), ctx, profilingEnabled);
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(result, target.getStatement(), ctx, profilingEnabled);
    } else if (target.getFunctionCall() != null) {
      //        handleFunctionCallAsTarget(result, target.getFunctionCall(), ctx);//TODO
      throw new CommandExecutionException("function call as target is not supported yet");
    } else if (target.getInputParam() != null) {
      handleInputParamAsTarget(result, target.getInputParam(), ctx, profilingEnabled);
    } else if (target.getIndex() != null) {
      handleIndexAsTarget(result, target.getIndex(), ctx, profilingEnabled);
    } else if (target.getSchema() != null) {
      handleMetadataAsTarget(result, target.getSchema(), ctx, profilingEnabled);
    } else if (target.getRids() != null && target.getRids().size() > 0) {
      handleRidsAsTarget(result, target.getRids(), ctx, profilingEnabled);
    } else {
      throw new UnsupportedOperationException();
    }

  }

  private void handleInputParamAsTarget(SelectExecutionPlan result, InputParameter inputParam, CommandContext ctx, boolean profilingEnabled) {
    Object paramValue = inputParam.getValue(ctx.getInputParameters());
    if (paramValue == null) {
      result.chain(new EmptyStep(ctx, profilingEnabled));//nothing to return
    } else if (paramValue instanceof DocumentType) {
      FromClause from = new FromClause(-1);
      FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier(((DocumentType) paramValue).getName()));
      handleClassAsTarget(result, from, ctx, profilingEnabled);
    } else if (paramValue instanceof String) {
      //strings are treated as classes
      FromClause from = new FromClause(-1);
      FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier((String) paramValue));
      handleClassAsTarget(result, from, ctx, profilingEnabled);
    } else if (paramValue instanceof Identifiable) {
      RID orid = ((Identifiable) paramValue).getIdentity();

      Rid rid = new Rid(-1);
      PInteger bucket = new PInteger(-1);
      bucket.setValue(orid.getBucketId());
      PInteger position = new PInteger(-1);
      position.setValue(orid.getPosition());
      rid.setLegacy(true);
      rid.setBucket(bucket);
      rid.setPosition(position);

      handleRidsAsTarget(result, Collections.singletonList(rid), ctx, profilingEnabled);
    } else if (paramValue instanceof Iterable) {
      //try list of RIDs
      List<Rid> rids = new ArrayList<>();
      for (Object x : (Iterable) paramValue) {
        if (!(x instanceof Identifiable)) {
          throw new CommandExecutionException("Cannot use collection as target: " + paramValue);
        }
        RID orid = ((Identifiable) x).getIdentity();

        Rid rid = new Rid(-1);
        PInteger bucket = new PInteger(-1);
        bucket.setValue(orid.getBucketId());
        PInteger position = new PInteger(-1);
        position.setValue(orid.getPosition());
        rid.setBucket(bucket);
        rid.setPosition(position);

        rids.add(rid);
      }
      handleRidsAsTarget(result, rids, ctx, profilingEnabled);
    } else {
      throw new CommandExecutionException("Invalid target: " + paramValue);
    }
  }

  private void handleNoTarget(SelectExecutionPlan result, CommandContext ctx, boolean profilingEnabled) {
    result.chain(new EmptyDataGeneratorStep(1, ctx, profilingEnabled));
  }

  private void handleIndexAsTarget(SelectExecutionPlan result, IndexIdentifier indexIdentifier, CommandContext ctx, boolean profilingEnabled) {
    String indexName = indexIdentifier.getIndexName();
    RangeIndex index = (RangeIndex) ctx.getDatabase().getSchema().getIndexByName(indexName);
    if (index == null) {
      throw new CommandExecutionException("Index not found: " + indexName);
    }

    switch (indexIdentifier.getType()) {
    case INDEX:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
      }
      result.chain(new FetchFromIndexStep(index, null, null, ctx, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
      break;
    case VALUES:
    case VALUESASC:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, true, ctx, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
      break;
    case VALUESDESC:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, false, ctx, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(ctx, null, profilingEnabled));
      break;
    }
  }

  private void handleMetadataAsTarget(final SelectExecutionPlan plan, SchemaIdentifier metadata, CommandContext ctx, boolean profilingEnabled) {
    throw new UnsupportedOperationException();
//    final Database db = ctx.getDatabase();
//    String schemaRecordIdAsString = null;
//    if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_SCHEMA)) {
//      schemaRecordIdAsString = db.getStorage().getConfiguration().getSchemaRecordId();
//    } else if (metadata.getName().equalsIgnoreCase(OCommandExecutorSQLAbstract.METADATA_INDEXMGR)) {
//      schemaRecordIdAsString = db.getStorage().getConfiguration().getIndexMgrRecordId();
//    } else {
//      throw new UnsupportedOperationException("Invalid metadata: " + metadata.getName());
//    }
//    ORecordId schemaRid = new ORecordId(schemaRecordIdAsString);
//    plan.chain(new FetchFromRidsStep(Collections.singleton(schemaRid), ctx, profilingEnabled));

  }

  private void handleRidsAsTarget(SelectExecutionPlan plan, List<Rid> rids, CommandContext ctx, boolean profilingEnabled) {
    List<RID> actualRids = new ArrayList<>();
    for (Rid rid : rids) {
      actualRids.add(rid.toRecordId((Result) null, ctx));
    }
    plan.chain(new FetchFromRidsStep(actualRids, ctx, profilingEnabled));
  }

  private void handleClassAsTarget(SelectExecutionPlan plan, FromClause queryTarget, CommandContext ctx, boolean profilingEnabled) {
    Identifier identifier = queryTarget.getItem().getIdentifier();

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), null, ctx, orderByRidAsc, profilingEnabled);
    plan.chain(fetcher);
  }

  private void handleClustersAsTarget(SelectExecutionPlan plan, List<Bucket> clusters, CommandContext ctx, boolean profilingEnabled) {
    final Database db = ctx.getDatabase();
    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (clusters.size() == 1) {
      final Bucket bucket = clusters.get(0);
      java.lang.Integer bucketId = bucket.getBucketNumber();
      if (bucketId == null) {
        bucketId = db.getSchema().getBucketByName(bucket.getBucketName()).getId();
      }

      final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(bucketId, ctx, profilingEnabled);
      // TODO: THIS SEEMS A BUG
      if (Boolean.TRUE.equals(orderByRidAsc)) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
      } else if (Boolean.FALSE.equals(orderByRidAsc)) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
      }
      plan.chain(step);
    } else {
      int[] bucketIds = new int[clusters.size()];
      for (int i = 0; i < clusters.size(); i++) {
        final Bucket bucket = clusters.get(i);
        java.lang.Integer bucketId = bucket.getBucketNumber();
        if (bucketId == null) {
          bucketId = db.getSchema().getBucketByName(bucket.getBucketName()).getId();
        }
        bucketIds[i] = bucketId;
      }
      final FetchFromClustersExecutionStep step = new FetchFromClustersExecutionStep(bucketIds, ctx, orderByRidAsc, profilingEnabled);
      plan.chain(step);
    }
  }

  private void handleSubqueryAsTarget(SelectExecutionPlan plan, Statement subQuery, CommandContext ctx, boolean profilingEnabled) {
    BasicCommandContext subCtx = new BasicCommandContext();
    subCtx.setDatabase(ctx.getDatabase());
    subCtx.setParent(ctx);
    InternalExecutionPlan subExecutionPlan = subQuery.createExecutionPlan(subCtx, profilingEnabled);
    plan.chain(new SubQueryStep(subExecutionPlan, ctx, subCtx, profilingEnabled));
  }

}
