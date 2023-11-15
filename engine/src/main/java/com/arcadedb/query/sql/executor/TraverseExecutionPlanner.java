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
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.IndexIdentifier;
import com.arcadedb.query.sql.parser.InputParameter;
import com.arcadedb.query.sql.parser.Limit;
import com.arcadedb.query.sql.parser.PInteger;
import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.Skip;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.TraverseProjectionItem;
import com.arcadedb.query.sql.parser.TraverseStatement;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedDocumentType;

import java.util.*;
import java.util.stream.*;

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

  public TraverseExecutionPlanner(final TraverseStatement statement) {
    //copying the content, so that it can be manipulated and optimized
    this.projections = statement.getProjections() == null ?
        null :
        statement.getProjections().stream().map(x -> x.copy()).collect(Collectors.toList());

    this.target = statement.getTarget();
    this.whileClause = statement.getWhileClause() == null ? null : statement.getWhileClause().copy();

    this.strategy = statement.getStrategy() == null ? TraverseStatement.Strategy.DEPTH_FIRST : statement.getStrategy();
    this.maxDepth = statement.getMaxDepth() == null ? null : statement.getMaxDepth().copy();

    this.skip = statement.getSkip();
    this.limit = statement.getLimit();
  }

  public InternalExecutionPlan createExecutionPlan(final CommandContext context, final boolean enableProfiling) {
    final SelectExecutionPlan result = new SelectExecutionPlan(context);

    handleFetchFromTarget(result, context, enableProfiling);

    handleTraversal(result, context, enableProfiling);

    if (skip != null) {
      result.chain(new SkipExecutionStep(skip, context, enableProfiling));
    }
    if (limit != null) {
      result.chain(new LimitExecutionStep(limit, context, enableProfiling));
    }

    return result;
  }

  private void handleTraversal(final SelectExecutionPlan result, final CommandContext context, final boolean profilingEnabled) {
    switch (strategy) {
    case BREADTH_FIRST:
      result.chain(new BreadthFirstTraverseStep(this.projections, this.whileClause, maxDepth, context, profilingEnabled));
      break;
    case DEPTH_FIRST:
      result.chain(new DepthFirstTraverseStep(this.projections, this.whileClause, maxDepth, context, profilingEnabled));
      break;
    }
    //TODO
  }

  private void handleFetchFromTarget(final SelectExecutionPlan result, final CommandContext context,
      final boolean profilingEnabled) {

    final FromItem target = this.target == null ? null : this.target.getItem();
    if (target == null) {
      handleNoTarget(result, context, profilingEnabled);
    } else if (target.getIdentifier() != null) {
      handleClassAsTarget(result, this.target, context, profilingEnabled);
    } else if (target.getBucket() != null) {
      handleClustersAsTarget(result, Collections.singletonList(target.getBucket()), context, profilingEnabled);
    } else if (target.getBucketList() != null) {
      handleClustersAsTarget(result, target.getBucketList().toListOfClusters(), context, profilingEnabled);
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(result, target.getStatement(), context, profilingEnabled);
    } else if (target.getFunctionCall() != null) {
      //        handleFunctionCallAsTarget(result, target.getFunctionCall(), context);//TODO
      throw new CommandExecutionException("function call as target is not supported yet");
    } else if (target.getInputParam() != null) {
      handleInputParamAsTarget(result, target.getInputParam(), context, profilingEnabled);
    } else if (target.getIndex() != null) {
      handleIndexAsTarget(result, target.getIndex(), context, profilingEnabled);
    } else if (target.getRids() != null && target.getRids().size() > 0) {
      handleRidsAsTarget(result, target.getRids(), context, profilingEnabled);
    } else if (target.getResultSet() != null) {
      result.chain(new FetchFromResultsetStep(target.getResultSet(), context, profilingEnabled));
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private void handleInputParamAsTarget(final SelectExecutionPlan result, final InputParameter inputParam,
      final CommandContext context, final boolean profilingEnabled) {
    final Object paramValue = inputParam.getValue(context.getInputParameters());
    if (paramValue == null) {
      result.chain(new EmptyStep(context, profilingEnabled));//nothing to return
    } else if (paramValue instanceof EmbeddedDocumentType) {
      final FromClause from = new FromClause(-1);
      final FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier(((DocumentType) paramValue).getName()));
      handleClassAsTarget(result, from, context, profilingEnabled);
    } else if (paramValue instanceof String) {
      //strings are treated as classes
      final FromClause from = new FromClause(-1);
      final FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier((String) paramValue));
      handleClassAsTarget(result, from, context, profilingEnabled);
    } else if (paramValue instanceof Identifiable) {
      final RID orid = ((Identifiable) paramValue).getIdentity();

      final Rid rid = new Rid(-1);
      final PInteger bucket = new PInteger(-1);
      bucket.setValue(orid.getBucketId());
      final PInteger position = new PInteger(-1);
      position.setValue(orid.getPosition());
      rid.setLegacy(true);
      rid.setBucket(bucket);
      rid.setPosition(position);

      handleRidsAsTarget(result, Collections.singletonList(rid), context, profilingEnabled);
    } else if (paramValue instanceof Iterable) {
      //try list of RIDs
      final List<Rid> rids = new ArrayList<>();
      for (final Object x : (Iterable) paramValue) {
        if (!(x instanceof Identifiable)) {
          throw new CommandExecutionException("Cannot use collection as target: " + paramValue);
        }
        final RID orid = ((Identifiable) x).getIdentity();

        final Rid rid = new Rid(-1);
        final PInteger bucket = new PInteger(-1);
        bucket.setValue(orid.getBucketId());
        final PInteger position = new PInteger(-1);
        position.setValue(orid.getPosition());
        rid.setBucket(bucket);
        rid.setPosition(position);

        rids.add(rid);
      }
      handleRidsAsTarget(result, rids, context, profilingEnabled);
    } else {
      throw new CommandExecutionException("Invalid target: " + paramValue);
    }
  }

  private void handleNoTarget(final SelectExecutionPlan result, final CommandContext context, final boolean profilingEnabled) {
    result.chain(new EmptyDataGeneratorStep(1, context, profilingEnabled));
  }

  private void handleIndexAsTarget(final SelectExecutionPlan result, final IndexIdentifier indexIdentifier,
      final CommandContext context, final boolean profilingEnabled) {
    final String indexName = indexIdentifier.getIndexName();
    final RangeIndex index = (RangeIndex) context.getDatabase().getSchema().getIndexByName(indexName);
    if (index == null) {
      throw new CommandExecutionException("Index not found: " + indexName);
    }

    switch (indexIdentifier.getType()) {
    case INDEX:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
      }
      result.chain(new FetchFromIndexStep(index, null, null, context, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(context, null, profilingEnabled));
      break;
    case VALUES:
    case VALUESASC:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, true, context, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(context, null, profilingEnabled));
      break;
    case VALUESDESC:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, false, context, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(context, null, profilingEnabled));
      break;
    }
  }

  private void handleRidsAsTarget(final SelectExecutionPlan plan, final List<Rid> rids, final CommandContext context,
      final boolean profilingEnabled) {
    final List<RID> actualRids = new ArrayList<>();
    for (final Rid rid : rids) {
      actualRids.add(rid.toRecordId((Result) null, context));
    }
    plan.chain(new FetchFromRidsStep(actualRids, context, profilingEnabled));
  }

  private void handleClassAsTarget(final SelectExecutionPlan plan, final FromClause queryTarget, final CommandContext context,
      final boolean profilingEnabled) {
    final Identifier identifier = queryTarget.getItem().getIdentifier();

    final Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    final FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), null, context,
        orderByRidAsc, profilingEnabled);
    plan.chain(fetcher);
  }

  private void handleClustersAsTarget(final SelectExecutionPlan plan, final List<Bucket> clusters, final CommandContext context,
      final boolean profilingEnabled) {
    final Database db = context.getDatabase();
    if (clusters.size() == 1) {
      final Bucket bucket = clusters.get(0);
      Integer bucketId = bucket.getBucketNumber();
      if (bucketId == null)
        bucketId = db.getSchema().getBucketByName(bucket.getBucketName()).getFileId();

      final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(bucketId, context, profilingEnabled);
      plan.chain(step);
    } else {
      final int[] bucketIds = new int[clusters.size()];
      for (int i = 0; i < clusters.size(); i++) {
        final Bucket bucket = clusters.get(i);
        Integer bucketId = bucket.getBucketNumber();
        if (bucketId == null)
          bucketId = db.getSchema().getBucketByName(bucket.getBucketName()).getFileId();

        bucketIds[i] = bucketId;
      }
      final FetchFromClustersExecutionStep step = new FetchFromClustersExecutionStep(bucketIds, context, null, profilingEnabled);
      plan.chain(step);
    }
  }

  private void handleSubqueryAsTarget(final SelectExecutionPlan plan, final Statement subQuery, final CommandContext context,
      final boolean profilingEnabled) {
    final BasicCommandContext subCtx = new BasicCommandContext();
    subCtx.setDatabase(context.getDatabase());
    subCtx.setParent(context);
    final InternalExecutionPlan subExecutionPlan = subQuery.createExecutionPlan(subCtx, profilingEnabled);
    plan.chain(new SubQueryStep(subExecutionPlan, context, subCtx, profilingEnabled));
  }
}
