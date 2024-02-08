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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.parser.AggregateProjectionSplit;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.Bucket;
import com.arcadedb.query.sql.parser.ContainsAnyCondition;
import com.arcadedb.query.sql.parser.ContainsTextCondition;
import com.arcadedb.query.sql.parser.EqualsCompareOperator;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.FunctionCall;
import com.arcadedb.query.sql.parser.GeOperator;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.GtOperator;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.InCondition;
import com.arcadedb.query.sql.parser.IndexIdentifier;
import com.arcadedb.query.sql.parser.InputParameter;
import com.arcadedb.query.sql.parser.LeOperator;
import com.arcadedb.query.sql.parser.LetClause;
import com.arcadedb.query.sql.parser.LetItem;
import com.arcadedb.query.sql.parser.LtOperator;
import com.arcadedb.query.sql.parser.Node;
import com.arcadedb.query.sql.parser.OrBlock;
import com.arcadedb.query.sql.parser.OrderBy;
import com.arcadedb.query.sql.parser.OrderByItem;
import com.arcadedb.query.sql.parser.PInteger;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.RecordAttribute;
import com.arcadedb.query.sql.parser.Rid;
import com.arcadedb.query.sql.parser.SchemaIdentifier;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.SubQueryCollector;
import com.arcadedb.query.sql.parser.Timeout;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.utility.Pair;

import java.util.*;
import java.util.stream.*;

import static com.arcadedb.schema.Schema.INDEX_TYPE.FULL_TEXT;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class SelectExecutionPlanner {

  private static final String LOCAL_NODE_NAME = "local";
  QueryPlanningInfo info;
  final SelectStatement statement;

  public SelectExecutionPlanner(final SelectStatement oSelectStatement) {
    this.statement = oSelectStatement;
  }

  private void init(final CommandContext context) {
    //copying the content, so that it can be manipulated and optimized
    info = new QueryPlanningInfo();
    info.projection = this.statement.getProjection() == null ? null : this.statement.getProjection().copy();
    info.projection = translateDistinct(info.projection);
    info.distinct = info.projection != null && info.projection.isDistinct();
    if (info.projection != null) {
      info.projection.setDistinct(false);
    }

    info.target = this.statement.getTarget();
    info.whereClause = this.statement.getWhereClause() == null ? null : this.statement.getWhereClause().copy();
    info.perRecordLetClause = this.statement.getLetClause() == null ? null : this.statement.getLetClause().copy();
    info.groupBy = this.statement.getGroupBy() == null ? null : this.statement.getGroupBy().copy();
    info.orderBy = this.statement.getOrderBy() == null ? null : this.statement.getOrderBy().copy();
    info.unwind = this.statement.getUnwind() == null ? null : this.statement.getUnwind().copy();
    info.skip = this.statement.getSkip();
    info.limit = this.statement.getLimit();
    info.timeout = this.statement.getTimeout() == null ? null : this.statement.getTimeout().copy();
    if (info.timeout == null && context.getDatabase().getConfiguration().getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT) > 0) {
      info.timeout = new Timeout(-1);
      info.timeout.setValue(context.getDatabase().getConfiguration().getValueAsLong(GlobalConfiguration.COMMAND_TIMEOUT));
    }
  }

  public InternalExecutionPlan createExecutionPlan(final CommandContext context, final boolean enableProfiling) {
    final DatabaseInternal db = context.getDatabase();
    if (!enableProfiling && statement.executionPlanCanBeCached()) {
      final ExecutionPlan plan = db.getExecutionPlanCache().get(statement.getOriginalStatement(), context);
      if (plan != null)
        return (InternalExecutionPlan) plan;
    }

    final long planningStart = System.currentTimeMillis();

    init(context);
    final SelectExecutionPlan result = new SelectExecutionPlan(context);

    if (info.expand && info.distinct)
      throw new CommandExecutionException("Cannot execute a statement with DISTINCT expand(), please use a subquery");

    optimizeQuery(info, context);

    if (handleHardwiredOptimizations(result, context, enableProfiling))
      return result;

    handleGlobalLet(result, info, context, enableProfiling);

    info.buckets = calculateTargetBuckets(info, context);

    info.fetchExecutionPlan = new SelectExecutionPlan(context);

    handleFetchFromTarget(result, info, context, enableProfiling);

    if (info.globalLetPresent)
      // do the raw fetch remotely, then do the rest on the coordinator
      buildExecutionPlan(result, info);

    handleLet(result, info, context, enableProfiling);

    handleWhere(result, info, context, enableProfiling);

    // TODO optimization: in most cases the projections can be calculated on remote nodes
    buildExecutionPlan(result, info);

    handleProjectionsBlock(result, info, context, enableProfiling);

    if (info.timeout != null)
      result.chain(new AccumulatingTimeoutStep(info.timeout, context, enableProfiling));

    if (!enableProfiling && statement.executionPlanCanBeCached() && result.canBeCached()
        && db.getExecutionPlanCache().getLastInvalidation() < planningStart)
      db.getExecutionPlanCache().put(statement.getOriginalStatement(), result);

    return result;
  }

  public static void handleProjectionsBlock(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context, final boolean enableProfiling) {
    handleProjectionsBeforeOrderBy(result, info, context, enableProfiling);

    if (info.expand || info.unwind != null || info.groupBy != null) {
      handleProjections(result, info, context, enableProfiling);
      handleExpand(result, info, context, enableProfiling);
      handleUnwind(result, info, context, enableProfiling);
      handleOrderBy(result, info, context, enableProfiling);
      if (info.skip != null)
        result.chain(new SkipExecutionStep(info.skip, context, enableProfiling));

      if (info.limit != null)
        result.chain(new LimitExecutionStep(info.limit, context, enableProfiling));

    } else {
      handleOrderBy(result, info, context, enableProfiling);
      if (info.distinct || info.groupBy != null || info.aggregateProjection != null) {
        handleProjections(result, info, context, enableProfiling);
        handleDistinct(result, info, context, enableProfiling);
        if (info.skip != null)
          result.chain(new SkipExecutionStep(info.skip, context, enableProfiling));

        if (info.limit != null)
          result.chain(new LimitExecutionStep(info.limit, context, enableProfiling));
      } else {
        if (info.skip != null)
          result.chain(new SkipExecutionStep(info.skip, context, enableProfiling));

        if (info.limit != null)
          result.chain(new LimitExecutionStep(info.limit, context, enableProfiling));

        handleProjections(result, info, context, enableProfiling);
      }
    }
  }

  private void buildExecutionPlan(final SelectExecutionPlan result, final QueryPlanningInfo info) {
    if (info.fetchExecutionPlan == null)
      return;

    //everything is executed on local server
    for (final ExecutionStep step : info.fetchExecutionPlan.getSteps()) {
      result.chain((ExecutionStepInternal) step);
      info.fetchExecutionPlan = null;
      info.planCreated = true;
    }
  }

  /**
   * for backward compatibility, translate "distinct(foo)" to "DISTINCT foo". This method modifies the projection itself.
   *
   * @param projection the projection
   */
  protected static Projection translateDistinct(Projection projection) {
    if (projection != null && projection.getItems().size() == 1) {
      if (isDistinct(projection.getItems().get(0))) {
        projection = projection.copy();
        final ProjectionItem item = projection.getItems().get(0);
        final FunctionCall function = ((BaseExpression) item.getExpression().getMathExpression()).getIdentifier().getLevelZero()
            .getFunctionCall();
        final Expression exp = function.getParams().get(0);
        final ProjectionItem resultItem = new ProjectionItem(-1);
        resultItem.setAlias(item.getAlias());
        resultItem.setExpression(exp.copy());
        final Projection result = new Projection(-1);
        result.setItems(new ArrayList<>());
        result.setDistinct(true);
        result.getItems().add(resultItem);
        return result;
      }
    }
    return projection;
  }

  /**
   * checks if a projection is a distinct(expr). In new executor the distinct() function is not supported, so "distinct(expr)" is
   * translated to "DISTINCT expr"
   *
   * @param item the projection
   *
   * @return
   */
  private static boolean isDistinct(final ProjectionItem item) {
    if (item.getExpression() == null) {
      return false;
    }
    if (item.getExpression().getMathExpression() == null) {
      return false;
    }
    if (!(item.getExpression().getMathExpression() instanceof BaseExpression)) {
      return false;
    }
    final BaseExpression base = (BaseExpression) item.getExpression().getMathExpression();
    if (base.getIdentifier() == null) {
      return false;
    }
    if (base.getModifier() != null) {
      return false;
    }
    if (base.getIdentifier().getLevelZero() == null) {
      return false;
    }
    final FunctionCall function = base.getIdentifier().getLevelZero().getFunctionCall();
    if (function == null) {
      return false;
    }
    return function.getName().getStringValue().equalsIgnoreCase("distinct");
  }

  private boolean handleHardwiredOptimizations(final SelectExecutionPlan result, final CommandContext context,
      final boolean profilingEnabled) {
    return handleHardwiredCountOnIndex(result, info, context, profilingEnabled) || handleHardwiredCountOnClass(result, info,
        context, profilingEnabled);
  }

  private boolean handleHardwiredCountOnClass(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context, final boolean profilingEnabled) {
    final Identifier targetClass = info.target == null ? null : info.target.getItem().getIdentifier();
    if (targetClass == null)
      return false;

    if (info.distinct || info.expand)
      return false;

    if (info.preAggregateProjection != null)
      return false;

    if (!isCountStar(info))
      return false;

    if (!isMinimalQuery(info))
      return false;

    result.chain(new CountFromTypeStep(info.target.toString(), info.projection.getAllAliases().iterator().next(), context,
        profilingEnabled));
    return true;
  }

  private boolean handleHardwiredCountOnIndex(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context, final boolean profilingEnabled) {
    final IndexIdentifier targetIndex = info.target == null ? null : info.target.getItem().getIndex();
    if (targetIndex == null) {
      return false;
    }
    if (info.distinct || info.expand) {
      return false;
    }
    if (info.preAggregateProjection != null) {
      return false;
    }
    if (!isCountStar(info)) {
      return false;
    }
    if (!isMinimalQuery(info)) {
      return false;
    }
    result.chain(new CountFromIndexStep(targetIndex, info.projection.getAllAliases().iterator().next(), context, profilingEnabled));
    return true;
  }

  /**
   * returns true if the query is minimal, ie. no WHERE condition, no SKIP/LIMIT, no UNWIND, no GROUP/ORDER BY, no LET
   *
   * @return
   */
  private boolean isMinimalQuery(final QueryPlanningInfo info) {
    return info.projectionAfterOrderBy == null && info.globalLetClause == null && info.perRecordLetClause == null
        && info.whereClause == null && info.flattenedWhereClause == null && info.groupBy == null && info.orderBy == null
        && info.unwind == null && info.skip == null && info.limit == null;
  }

  private boolean isCountStar(final QueryPlanningInfo info) {
    if (info.aggregateProjection == null || info.projection == null || info.aggregateProjection.getItems().size() != 1
        || info.projection.getItems().size() != 1) {
      return false;
    }
    final ProjectionItem item = info.aggregateProjection.getItems().get(0);
    return item.getExpression().toString().equalsIgnoreCase("count(*)");
  }

  private static boolean isCountOnly(final QueryPlanningInfo info) {
    if (info.aggregateProjection == null || info.projection == null || info.aggregateProjection.getItems().size() != 1 ||
        info.projection.getItems().stream().filter(x -> !x.getProjectionAliasAsString().startsWith("_$$$ORDER_BY_ALIAS$$$_"))
            .count() != 1) {
      return false;
    }
    final ProjectionItem item = info.aggregateProjection.getItems().get(0);
    final Expression exp = item.getExpression();
    if (exp.getMathExpression() != null && exp.getMathExpression() instanceof BaseExpression) {
      final BaseExpression base = (BaseExpression) exp.getMathExpression();
      return base.isCount() && base.getModifier() == null;
    }
    return false;
  }

  private boolean isCount(final Projection aggregateProjection, final Projection projection) {
    if (aggregateProjection == null || projection == null || aggregateProjection.getItems().size() != 1
        || projection.getItems().size() != 1) {
      return false;
    }
    final ProjectionItem item = aggregateProjection.getItems().get(0);
    return item.getExpression().isCount();
  }

  public static void handleUnwind(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {
    if (info.unwind != null) {
      result.chain(new UnwindStep(info.unwind, context, profilingEnabled));
    }
  }

  private static void handleDistinct(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {
    if (info.distinct)
      result.chain(new DistinctExecutionStep(context, profilingEnabled));
  }

  private static void handleProjectionsBeforeOrderBy(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context, final boolean profilingEnabled) {
    if (info.orderBy != null) {
      handleProjections(result, info, context, profilingEnabled);
    }
  }

  private static void handleProjections(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context, final boolean profilingEnabled) {
    if (!info.projectionsCalculated && info.projection != null) {
      if (info.preAggregateProjection != null) {
        result.chain(new ProjectionCalculationStep(info.preAggregateProjection, context, profilingEnabled));
      }
      if (info.aggregateProjection != null) {
        long aggregationLimit = -1;
        if (info.orderBy == null && info.limit != null) {
          aggregationLimit = info.limit.getValue(context);
          if (info.skip != null && info.skip.getValue(context) > 0) {
            aggregationLimit += info.skip.getValue(context);
          }
        }

        result.chain(new AggregateProjectionCalculationStep(info.aggregateProjection, info.groupBy, aggregationLimit, context,
            info.timeout != null ? info.timeout.getVal().longValue() : -1, profilingEnabled));
        if (isCountOnly(info) && info.groupBy == null) {
          result.chain(new GuaranteeEmptyCountStep(info.aggregateProjection.getItems().get(0), context, profilingEnabled));
        }
      }
      result.chain(new ProjectionCalculationStep(info.projection, context, profilingEnabled));

      info.projectionsCalculated = true;
    }
  }

  protected static void optimizeQuery(final QueryPlanningInfo info, final CommandContext context) {
    splitLet(info, context);
    extractSubQueries(info);
    if (info.projection != null && info.projection.isExpand()) {
      info.expand = true;
      info.projection = info.projection.getExpandContent();
    }
    if (info.whereClause != null) {
      info.flattenedWhereClause = info.whereClause.flatten();
      //this helps index optimization
      info.flattenedWhereClause = moveFlattenedEqualitiesLeft(info.flattenedWhereClause);
    }

    splitProjectionsForGroupBy(info, context);
    addOrderByProjections(info);
  }

  /**
   * splits LET clauses in global (executed once) and local (executed once per record)
   */
  private static void splitLet(final QueryPlanningInfo info, final CommandContext context) {
    if (info.perRecordLetClause != null && info.perRecordLetClause.getItems() != null) {
      final Iterator<LetItem> iterator = info.perRecordLetClause.getItems().iterator();
      while (iterator.hasNext()) {
        final LetItem item = iterator.next();
        if (item.getExpression() != null && item.getExpression().isEarlyCalculated(context)) {
          iterator.remove();
          addGlobalLet(info, item.getVarName(), item.getExpression());
        } else if (item.getQuery() != null && !item.getQuery().refersToParent()) {
          iterator.remove();
          addGlobalLet(info, item.getVarName(), item.getQuery(), -1);
        }
      }
    }
  }

  /**
   * re-writes a list of flat AND conditions, moving left all the equality operations
   *
   * @param flattenedWhereClause
   *
   * @return
   */
  private static List<AndBlock> moveFlattenedEqualitiesLeft(final List<AndBlock> flattenedWhereClause) {
    if (flattenedWhereClause == null) {
      return null;
    }

    final List<AndBlock> result = new ArrayList<>();
    for (final AndBlock block : flattenedWhereClause) {
      final List<BooleanExpression> equalityExpressions = new ArrayList<>();
      final List<BooleanExpression> nonEqualityExpressions = new ArrayList<>();
      for (final BooleanExpression exp : block.getSubBlocks()) {
        if (exp instanceof BinaryCondition) {
          if (((BinaryCondition) exp).getOperator() instanceof EqualsCompareOperator) {
            equalityExpressions.add(exp);
          } else {
            nonEqualityExpressions.add(exp);
          }
        } else {
          nonEqualityExpressions.add(exp);
        }
      }
      final AndBlock newAnd = new AndBlock(equalityExpressions, nonEqualityExpressions);
      result.add(newAnd);
    }

    return result;
  }

  /**
   * creates additional projections for ORDER BY
   */
  private static void addOrderByProjections(final QueryPlanningInfo info) {
    if (info.orderApplied || info.expand || info.unwind != null || info.orderBy == null || info.orderBy.getItems().size() == 0
        || info.projection == null || info.projection.getItems() == null || (info.projection.getItems().size() == 1
        && info.projection.getItems().get(0).isAll())) {
      return;
    }

    final OrderBy newOrderBy = info.orderBy.copy();
    final List<ProjectionItem> additionalOrderByProjections = calculateAdditionalOrderByProjections(info.projection.getAllAliases(),
        newOrderBy);
    if (additionalOrderByProjections.size() > 0) {
      info.orderBy = newOrderBy;//the ORDER BY has changed
    }
    if (additionalOrderByProjections.size() > 0) {
      info.projectionAfterOrderBy = new Projection(-1);
      info.projectionAfterOrderBy.setItems(new ArrayList<>());
      for (final String alias : info.projection.getAllAliases()) {
        info.projectionAfterOrderBy.getItems().add(projectionFromAlias(new Identifier(alias)));
      }

      for (final ProjectionItem item : additionalOrderByProjections) {
        if (info.preAggregateProjection != null) {
          info.preAggregateProjection.getItems().add(item);
          info.aggregateProjection.getItems().add(projectionFromAlias(item.getAlias()));
          info.projection.getItems().add(projectionFromAlias(item.getAlias()));
        } else {
          info.projection.getItems().add(item);
        }
      }
    }
  }

  /**
   * given a list of aliases (present in the existing projections) calculates a list of additional projections to add to the
   * existing projections to allow ORDER BY calculation. The sorting clause will be modified with new replaced aliases
   *
   * @param allAliases existing aliases in the projection
   * @param orderBy    sorting clause
   *
   * @return a list of additional projections to add to the existing projections to allow ORDER BY calculation (empty if nothing has
   * to be added).
   */
  private static List<ProjectionItem> calculateAdditionalOrderByProjections(final List<String> allAliases, final OrderBy orderBy) {
    final List<ProjectionItem> result = new ArrayList<>();
    int nextAliasCount = 0;
    if (orderBy != null && orderBy.getItems() != null && !orderBy.getItems().isEmpty()) {
      for (final OrderByItem item : orderBy.getItems()) {
        if (!allAliases.contains(item.getName())) {
          final ProjectionItem newProj = new ProjectionItem(-1);
          if (item.getAlias() != null) {
            newProj.setExpression(new Expression(new Identifier(item.getAlias()), item.getModifier()));
          } else if (item.getRecordAttr() != null) {
            final RecordAttribute attr = new RecordAttribute(-1);
            attr.setName(item.getRecordAttr());
            newProj.setExpression(new Expression(attr, item.getModifier()));
          }
          final Identifier newAlias = new Identifier("_$$$ORDER_BY_ALIAS$$$_" + (nextAliasCount++));
          newProj.setAlias(newAlias);
          item.setAlias(newAlias.getStringValue());
          item.setModifier(null);
          result.add(newProj);
        }
      }
    }
    return result;
  }

  /**
   * splits projections in three parts (pre-aggregate, aggregate and final) to efficiently manage aggregations
   */
  private static void splitProjectionsForGroupBy(final QueryPlanningInfo info, final CommandContext context) {
    if (info.projection == null)
      return;

    final Projection preAggregate = new Projection(-1);
    preAggregate.setItems(new ArrayList<>());
    final Projection aggregate = new Projection(-1);
    aggregate.setItems(new ArrayList<>());
    final Projection postAggregate = new Projection(-1);
    postAggregate.setItems(new ArrayList<>());

    boolean isSplitted = false;

    //split for aggregate projections
    final AggregateProjectionSplit result = new AggregateProjectionSplit();
    for (final ProjectionItem item : info.projection.getItems()) {
      result.reset();
      if (isAggregate(item, context)) {
        isSplitted = true;
        final ProjectionItem post = item.splitForAggregation(result, context);
        Identifier postAlias = item.getProjectionAlias();
        postAlias = new Identifier(postAlias, true);
        post.setAlias(postAlias);
        postAggregate.getItems().add(post);
        aggregate.getItems().addAll(result.getAggregate());
        preAggregate.getItems().addAll(result.getPreAggregate());
      } else {
        preAggregate.getItems().add(item);
        //also push the alias forward in the chain
        final ProjectionItem aggItem = new ProjectionItem(-1);
        aggItem.setExpression(new Expression(item.getProjectionAlias()));
        aggregate.getItems().add(aggItem);
        postAggregate.getItems().add(aggItem);
      }
    }

    //bind split projections to the execution planner
    if (isSplitted) {
      info.preAggregateProjection = preAggregate;
      if (info.preAggregateProjection.getItems() == null || info.preAggregateProjection.getItems().size() == 0) {
        info.preAggregateProjection = null;
      }
      info.aggregateProjection = aggregate;
      if (info.aggregateProjection.getItems() == null || info.aggregateProjection.getItems().size() == 0) {
        info.aggregateProjection = null;
      }
      info.projection = postAggregate;

      addGroupByExpressionsToProjections(info, context);
    }
  }

  private static boolean isAggregate(final ProjectionItem item, final CommandContext context) {
    return item.isAggregate(context);
  }

  private static ProjectionItem projectionFromAlias(final Identifier oIdentifier) {
    final ProjectionItem result = new ProjectionItem(-1);
    result.setExpression(new Expression(oIdentifier));
    return result;
  }

  /**
   * if GROUP BY is performed on an expression that is not explicitly in the pre-aggregate projections, then that expression has to
   * be put in the pre-aggregate (only here, in subsequent steps it's removed)
   */
  private static void addGroupByExpressionsToProjections(final QueryPlanningInfo info, final CommandContext context) {
    if (info.groupBy == null || info.groupBy.getItems() == null || info.groupBy.getItems().size() == 0) {
      return;
    }
    final GroupBy newGroupBy = new GroupBy(-1);
    final int i = 0;
    for (final Expression exp : info.groupBy.getItems()) {
      if (exp.isAggregate(context)) {
        throw new CommandExecutionException("Cannot group by an aggregate function");
      }
      boolean found = false;
      if (info.preAggregateProjection != null) {
        for (final String alias : info.preAggregateProjection.getAllAliases()) {
          //if it's a simple identifier and it's the same as one of the projections in the query,
          //then the projection itself is used for GROUP BY without recalculating; in all the other cases, it is evaluated separately
          if (alias.equals(exp.getDefaultAlias().getStringValue()) && exp.isBaseIdentifier()) {
            found = true;
            newGroupBy.getItems().add(exp);
            break;
          }
        }
      }
      if (!found) {
        final ProjectionItem newItem = new ProjectionItem(-1);
        newItem.setExpression(exp);
        final Identifier groupByAlias = new Identifier("_$$$GROUP_BY_ALIAS$$$_" + i);
        newItem.setAlias(groupByAlias);
        if (info.preAggregateProjection == null) {
          info.preAggregateProjection = new Projection(-1);
        }
        if (info.preAggregateProjection.getItems() == null) {
          info.preAggregateProjection.setItems(new ArrayList<>());
        }
        info.preAggregateProjection.getItems().add(newItem);
        newGroupBy.getItems().add(new Expression(groupByAlias));
      }

      info.groupBy = newGroupBy;
    }

  }

  /**
   * translates subqueries to LET statements
   */
  private static void extractSubQueries(final QueryPlanningInfo info) {
    final SubQueryCollector collector = new SubQueryCollector();
    if (info.perRecordLetClause != null)
      info.perRecordLetClause.extractSubQueries(collector);

    int i = 0;
    int j = 0;
    for (final Map.Entry<Identifier, Statement> entry : collector.getSubQueries().entrySet()) {
      final Identifier alias = entry.getKey();
      final Statement query = entry.getValue();
      if (query.refersToParent()) {
        addRecordLevelLet(info, alias, query, -1);
      } else {
        addGlobalLet(info, alias, query, i++);
      }
    }
    collector.reset();

    if (info.whereClause != null) {
      info.whereClause.extractSubQueries(collector);
    }
    if (info.projection != null) {
      info.projection.extractSubQueries(collector);
    }
    if (info.orderBy != null) {
      info.orderBy.extractSubQueries(collector);
    }
    if (info.groupBy != null) {
      info.groupBy.extractSubQueries(collector);
    }

    for (final Map.Entry<Identifier, Statement> entry : collector.getSubQueries().entrySet()) {
      final Identifier alias = entry.getKey();
      final Statement query = entry.getValue();
      if (query.refersToParent()) {
        addRecordLevelLet(info, alias, query, -1);
      } else {
        addGlobalLet(info, alias, query, -1);
      }
    }
  }

  private static void addGlobalLet(final QueryPlanningInfo info, final Identifier alias, final Expression exp) {
    if (info.globalLetClause == null)
      info.globalLetClause = new LetClause(-1);

    final LetItem item = new LetItem(-1);
    item.setVarName(alias);
    item.setExpression(exp);
    info.globalLetClause.addItem(item);
  }

  private static void addGlobalLet(final QueryPlanningInfo info, final Identifier alias, final Statement stm, final int pos) {
    if (info.globalLetClause == null)
      info.globalLetClause = new LetClause(-1);

    final LetItem item = new LetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    if (pos > -1)
      info.globalLetClause.getItems().add(pos, item);
    else
      info.globalLetClause.getItems().add(item);
  }

  private static void addRecordLevelLet(final QueryPlanningInfo info, final Identifier alias, final Statement stm, final int pos) {
    if (info.perRecordLetClause == null)
      info.perRecordLetClause = new LetClause(-1);

    final LetItem item = new LetItem(-1);
    item.setVarName(alias);
    item.setQuery(stm);
    if (pos > -1)
      info.perRecordLetClause.getItems().add(pos, item);
    else
      info.perRecordLetClause.getItems().add(item);
  }

  private void handleFetchFromTarget(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {
    final FromItem target = info.target == null ? null : info.target.getItem();

    if (target == null) {
      handleNoTarget(info.fetchExecutionPlan, context, profilingEnabled);
    } else if (target.getIdentifier() != null && target.getModifier() != null) {

      final List<RID> rids = new ArrayList<>();
      final Collection<Identifiable> records = (Collection<Identifiable>) context.getVariablePath(target.toString());
      if (records != null && !records.isEmpty()) {
        for (Object o : records) {
          if (o instanceof Identifiable)
            rids.add(((Identifiable) o).getIdentity());
          else if (o instanceof Result && ((Result) o).isElement())
            rids.add(((Result) o).toElement().getIdentity());
        }
        info.fetchExecutionPlan.chain(new FetchFromRidsStep(rids, context, profilingEnabled));
      } else
        result.chain(new EmptyStep(context, profilingEnabled));//nothing to return
    } else if (target.getIdentifier() != null) {
      Set<String> filterClusters = info.buckets;

      final AndBlock ridRangeConditions = extractRidRanges(info.flattenedWhereClause, context);
      if (ridRangeConditions != null && !ridRangeConditions.isEmpty()) {
        info.ridRangeConditions = ridRangeConditions;
        filterClusters = filterClusters.stream()
            .filter(x -> clusterMatchesRidRange(x, ridRangeConditions, context.getDatabase(), context)).collect(Collectors.toSet());
      }
      handleClassAsTarget(info.fetchExecutionPlan, filterClusters, info, context, profilingEnabled);
    } else if (target.getBucket() != null) {
      handleClustersAsTarget(info.fetchExecutionPlan, info, Collections.singletonList(target.getBucket()), context,
          profilingEnabled);
    } else if (target.getBucketList() != null) {
      final List<Bucket> allClusters = target.getBucketList().toListOfClusters();
      final List<Bucket> clustersForShard = new ArrayList<>();
      for (final Bucket bucket : allClusters) {
        String name = bucket.getBucketName();
        if (name == null)
          name = context.getDatabase().getSchema().getBucketById(bucket.getBucketNumber()).getName();

        if (name != null && info.buckets.contains(name))
          clustersForShard.add(bucket);
      }
      handleClustersAsTarget(info.fetchExecutionPlan, info, clustersForShard, context, profilingEnabled);
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(info.fetchExecutionPlan, target.getStatement(), context, profilingEnabled);
    } else if (target.getFunctionCall() != null) {
      //        handleFunctionCallAsTarget(result, target.getFunctionCall(), context);//TODO
      throw new CommandExecutionException("function call as target is not supported yet");
    } else if (target.getInputParam() != null) {
      handleInputParamAsTarget(info.fetchExecutionPlan, info.buckets, info, target.getInputParam(), context, profilingEnabled);
    } else if (target.getInputParams() != null && !target.getInputParams().isEmpty()) {
      final List<InternalExecutionPlan> plans = new ArrayList<>();
      for (final InputParameter param : target.getInputParams()) {
        final SelectExecutionPlan subPlan = new SelectExecutionPlan(context);
        handleInputParamAsTarget(subPlan, info.buckets, info, param, context, profilingEnabled);
        plans.add(subPlan);
      }
      info.fetchExecutionPlan.chain(new ParallelExecStep(plans, context, profilingEnabled));
    } else if (target.getIndex() != null) {
      if (info.buckets.size() > 1)
        handleIndexAsTarget(info.fetchExecutionPlan, info, target.getIndex(), null, context, profilingEnabled);
    } else if (target.getSchema() != null) {
      handleSchemaAsTarget(info.fetchExecutionPlan, target.getSchema(), context, profilingEnabled);
    } else if (target.getRids() != null && !target.getRids().isEmpty()) {
      final Set<String> filterClusters = info.buckets;
      final List<Rid> rids = new ArrayList<>();
      for (final Rid rid : target.getRids()) {
        if (filterClusters == null || isFromClusters(rid, filterClusters, context.getDatabase()))
          rids.add(rid);
      }

      if (!rids.isEmpty())
        handleRidsAsTarget(info.fetchExecutionPlan, rids, context, profilingEnabled);
      else
        result.chain(new EmptyStep(context, profilingEnabled));//nothing to return

    } else if (target.getResultSet() != null) {
      result.chain(new FetchFromResultsetStep(target.getResultSet(), context, profilingEnabled));
    } else if (target.jjtGetNumChildren() == 1) {
      // FIX TO HANDLE FROM VARIABLES AS TARGET
      final Node child = target.jjtGetChild(0);

      if (child instanceof Identifier && ((Identifier) child).getStringValue().startsWith("$")) {
        final Object variable = context.getVariable(((Identifier) child).getStringValue());

        if (variable instanceof Iterable)
          info.fetchExecutionPlan.chain(new FetchFromRidsStep((Iterable<RID>) variable, context, profilingEnabled));
        else if (variable instanceof ResultSet) {
          final ResultSet resultSet = (ResultSet) variable;
          info.fetchExecutionPlan.chain(new FetchFromRidsStep(() -> new Iterator<>() {
            @Override
            public boolean hasNext() {
              return resultSet.hasNext();
            }

            @Override
            public RID next() {
              return resultSet.nextIfAvailable().getIdentity().get();
            }
          }, context, profilingEnabled));
        }

      } else
        throw new UnsupportedOperationException();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private boolean clusterMatchesRidRange(final String bucketName, final AndBlock ridRangeConditions, final Database database,
      final CommandContext context) {
    final int thisClusterId = database.getSchema().getBucketByName(bucketName).getFileId();
    for (final BooleanExpression ridRangeCondition : ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof BinaryCondition) {
        final BinaryCompareOperator operator = ((BinaryCondition) ridRangeCondition).getOperator();

        final Object obj;
        if (((BinaryCondition) ridRangeCondition).getRight().getRid() != null)
          obj = ((BinaryCondition) ridRangeCondition).getRight().getRid().toRecordId((Result) null, context);
        else
          obj = ((BinaryCondition) ridRangeCondition).getRight().execute((Result) null, context);

        final RID conditionRid = ((Identifiable) obj).getIdentity();

        if (conditionRid != null) {
          final int conditionClusterId = conditionRid.getBucketId();
          if (operator instanceof GtOperator || operator instanceof GeOperator) {
            if (thisClusterId < conditionClusterId) {
              return false;
            }
          } else if (operator instanceof LtOperator || operator instanceof LeOperator) {
            if (thisClusterId > conditionClusterId) {
              return false;
            }
          }
        }
      }
    }
    return true;
  }

  private AndBlock extractRidRanges(final List<AndBlock> flattenedWhereClause, final CommandContext context) {
    final AndBlock result = new AndBlock(-1);

    if (flattenedWhereClause == null || flattenedWhereClause.size() != 1)
      return result;

    //TODO optimization: merge multiple conditions
    for (final BooleanExpression booleanExpression : flattenedWhereClause.get(0).getSubBlocks()) {
      if (isRidRange(booleanExpression, context)) {
        result.getSubBlocks().add(booleanExpression.copy());
      }
    }
    return result;
  }

  private boolean isRidRange(final BooleanExpression booleanExpression, final CommandContext context) {
    if (booleanExpression instanceof BinaryCondition) {
      final BinaryCondition cond = ((BinaryCondition) booleanExpression);
      final BinaryCompareOperator operator = cond.getOperator();
      if (isRangeOperator(operator) && cond.getLeft().toString().equalsIgnoreCase("@rid")) {
        final Object obj;
        if (cond.getRight().getRid() != null) {
          obj = cond.getRight().getRid().toRecordId((Result) null, context);
        } else {
          obj = cond.getRight().execute((Result) null, context);
        }
        return obj instanceof Identifiable;
      }
    }
    return false;
  }

  private boolean isRangeOperator(final BinaryCompareOperator operator) {
    return operator instanceof LtOperator || operator instanceof LeOperator || operator instanceof GtOperator
        || operator instanceof GeOperator;
  }

  private void handleInputParamAsTarget(final SelectExecutionPlan result, final Set<String> filterClusters,
      final QueryPlanningInfo info, final InputParameter inputParam, final CommandContext context, final boolean profilingEnabled) {
    Object paramValue = inputParam.getValue(context.getInputParameters());

    if (paramValue instanceof String && RID.is(paramValue))
      paramValue = new RID(context.getDatabase(), (String) paramValue);

    if (paramValue == null) {
      result.chain(new EmptyStep(context, profilingEnabled));//nothing to return
    } else if (paramValue instanceof LocalDocumentType) {
      final FromClause from = new FromClause(-1);
      final FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier(((DocumentType) paramValue).getName()));
      handleClassAsTarget(result, filterClusters, from, info, context, profilingEnabled);
    } else if (paramValue instanceof String) {
      //strings are treated as classes
      final FromClause from = new FromClause(-1);
      final FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier((String) paramValue));
      handleClassAsTarget(result, filterClusters, from, info, context, profilingEnabled);
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

      if (filterClusters == null || isFromClusters(rid, filterClusters, context.getDatabase())) {
        handleRidsAsTarget(result, Collections.singletonList(rid), context, profilingEnabled);
      } else {
        result.chain(new EmptyStep(context, profilingEnabled));//nothing to return
      }

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
        if (filterClusters == null || isFromClusters(rid, filterClusters, context.getDatabase())) {
          rids.add(rid);
        }
      }
      if (rids.size() > 0) {
        handleRidsAsTarget(result, rids, context, profilingEnabled);
      } else {
        result.chain(new EmptyStep(context, profilingEnabled));//nothing to return
      }
    } else {
      throw new CommandExecutionException("Invalid target: " + paramValue);
    }
  }

  /**
   * checks if this RID is from one of these clusters
   *
   * @param rid
   * @param filterClusters
   * @param database
   *
   * @return
   */
  private boolean isFromClusters(final Rid rid, final Set<String> filterClusters, final Database database) {
    if (filterClusters == null) {
      throw new IllegalArgumentException();
    }
    final String bucketName = database.getSchema().getBucketById(rid.getBucket().getValue().intValue()).getName();
    return filterClusters.contains("*") || filterClusters.contains(bucketName);
  }

  private void handleNoTarget(final SelectExecutionPlan result, final CommandContext context, final boolean profilingEnabled) {
    result.chain(new EmptyDataGeneratorStep(1, context, profilingEnabled));
  }

  private void handleIndexAsTarget(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final IndexIdentifier indexIdentifier, final Set<String> filterClusters, final CommandContext context,
      final boolean profilingEnabled) {
    final String indexName = indexIdentifier.getIndexName();
    final RangeIndex index = (RangeIndex) context.getDatabase().getSchema().getIndexByName(indexName);
    if (index == null) {
      throw new CommandExecutionException("Index not found: " + indexName);
    }

    int[] filterClusterIds = null;
    if (filterClusters != null) {
      filterClusterIds = filterClusters.stream().map(name -> context.getDatabase().getSchema().getBucketByName(name).getFileId())
          .mapToInt(i -> i).toArray();
    }

    switch (indexIdentifier.getType()) {
    case INDEX:
      BooleanExpression keyCondition = null;
      BooleanExpression ridCondition = null;
      if (info.flattenedWhereClause == null || info.flattenedWhereClause.size() == 0) {
        if (!index.supportsOrderedIterations()) {
          throw new CommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
        }
      } else if (info.flattenedWhereClause.size() > 1) {
        throw new CommandExecutionException("Index queries with this kind of condition are not supported yet: " + info.whereClause);
      } else {
        final AndBlock andBlock = info.flattenedWhereClause.get(0);
        if (andBlock.getSubBlocks().size() == 1) {

          info.whereClause = null;//The WHERE clause won't be used anymore, the index does all the filtering
          info.flattenedWhereClause = null;
          keyCondition = getKeyCondition(andBlock);
          if (keyCondition == null) {
            throw new CommandExecutionException(
                "Index queries with this kind of condition are not supported yet: " + info.whereClause);
          }
        } else if (andBlock.getSubBlocks().size() == 2) {
          info.whereClause = null;//The WHERE clause won't be used anymore, the index does all the filtering
          info.flattenedWhereClause = null;
          keyCondition = getKeyCondition(andBlock);
          ridCondition = getRidCondition(andBlock);
          if (keyCondition == null || ridCondition == null) {
            throw new CommandExecutionException(
                "Index queries with this kind of condition are not supported yet: " + info.whereClause);
          }
        } else {
          throw new CommandExecutionException(
              "Index queries with this kind of condition are not supported yet: " + info.whereClause);
        }
      }
      result.chain(new FetchFromIndexStep(index, keyCondition, null, context, profilingEnabled));
      if (ridCondition != null) {
        final WhereClause where = new WhereClause(-1);
        where.setBaseExpression(ridCondition);
        result.chain(new FilterStep(where, context, profilingEnabled));
      }
      break;
    case VALUES:
    case VALUESASC:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, true, context, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(context, filterClusterIds, profilingEnabled));
      break;
    case VALUESDESC:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, false, context, profilingEnabled));
      result.chain(new GetValueFromIndexEntryStep(context, filterClusterIds, profilingEnabled));
      break;
    }
  }

  private BooleanExpression getKeyCondition(final AndBlock andBlock) {
    for (final BooleanExpression exp : andBlock.getSubBlocks()) {
      final String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("key ")) {
        return exp;
      }
    }
    return null;
  }

  private BooleanExpression getRidCondition(final AndBlock andBlock) {
    for (final BooleanExpression exp : andBlock.getSubBlocks()) {
      final String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("rid ")) {
        return exp;
      }
    }
    return null;
  }

  private void handleSchemaAsTarget(final SelectExecutionPlan plan, final SchemaIdentifier metadata, final CommandContext context,
      final boolean profilingEnabled) {
    if (metadata.getName().equalsIgnoreCase("types"))
      plan.chain(new FetchFromSchemaTypesStep(context, profilingEnabled));
    else if (metadata.getName().equalsIgnoreCase("indexes"))
      plan.chain(new FetchFromSchemaIndexesStep(context, profilingEnabled));
    else if (metadata.getName().equalsIgnoreCase("database"))
      plan.chain(new FetchFromSchemaDatabaseStep(context, profilingEnabled));
    else if (metadata.getName().equalsIgnoreCase("buckets"))
      plan.chain(new FetchFromSchemaBucketsStep(context, profilingEnabled));
    else
      throw new UnsupportedOperationException("Invalid metadata: " + metadata.getName());
  }

  private void handleRidsAsTarget(final SelectExecutionPlan plan, final List<Rid> rids, final CommandContext context,
      final boolean profilingEnabled) {
    final List<RID> actualRids = new ArrayList<>();
    for (final Rid rid : rids)
      actualRids.add(rid.toRecordId((Result) null, context));

    plan.chain(new FetchFromRidsStep(actualRids, context, profilingEnabled));
  }

  private static void handleExpand(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {
    if (info.expand) {
      result.chain(new ExpandStep(context, profilingEnabled));
    }
  }

  private void handleGlobalLet(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {
    if (info.globalLetClause != null) {
      final List<LetItem> items = info.globalLetClause.getItems();
      for (final LetItem item : items) {
        if (item.getExpression() != null) {
          result.chain(new GlobalLetExpressionStep(item.getVarName(), item.getExpression(), context, profilingEnabled));
        } else {
          result.chain(new GlobalLetQueryStep(item.getVarName(), item.getQuery(), context, profilingEnabled));
        }
        info.globalLetPresent = true;
      }
    }
  }

  private void handleLet(final SelectExecutionPlan plan, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {
    if (info.perRecordLetClause != null) {
      final List<LetItem> items = info.perRecordLetClause.getItems();
      if (info.planCreated) {
        for (final LetItem item : items) {
          if (item.getExpression() != null) {
            plan.chain(new LetExpressionStep(item.getVarName(), item.getExpression(), context, profilingEnabled));
          } else {
            plan.chain(new LetQueryStep(item.getVarName(), item.getQuery(), context, profilingEnabled));
          }
        }
      } else {

        boolean containsSubQuery = false;
        for (final LetItem item : items) {
          if (item.getExpression() != null) {
            info.fetchExecutionPlan.chain(
                new LetExpressionStep(item.getVarName().copy(), item.getExpression().copy(), context, profilingEnabled));
          } else {
            info.fetchExecutionPlan.chain(
                new LetQueryStep(item.getVarName().copy(), item.getQuery().copy(), context, profilingEnabled));
            containsSubQuery = true;
          }
        }

        if (containsSubQuery) {
          // RE-EXECUTE THE EXPRESSION IF THERE IS ANY SUB-QUERY. THIS IS A MUST BECAUSE THERE IS NO CONCEPT OF DEPENDENCY BETWEEN LETS
          for (final LetItem item : items) {
            if (item.getExpression() != null)
              info.fetchExecutionPlan.chain(
                  new LetExpressionStep(item.getVarName().copy(), item.getExpression().copy(), context, profilingEnabled));
          }
        }
      }
    }
  }

  private void handleWhere(final SelectExecutionPlan plan, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {
    if (info.whereClause != null) {
      if (info.planCreated) {
        plan.chain(new FilterStep(info.whereClause, context, profilingEnabled));
      } else {
        info.fetchExecutionPlan.chain(new FilterStep(info.whereClause, context, profilingEnabled));
      }
    }
  }

  public static void handleOrderBy(final SelectExecutionPlan plan, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {
    final int skipSize = info.skip == null ? 0 : info.skip.getValue(context);
    if (skipSize < 0) {
      throw new CommandExecutionException("Cannot execute a query with a negative SKIP");
    }
    final int limitSize = info.limit == null ? -1 : info.limit.getValue(context);
    Integer maxResults = null;
    if (limitSize >= 0) {
      maxResults = skipSize + limitSize;
    }
    if (info.expand || info.unwind != null) {
      maxResults = null;
    }
    if (!info.orderApplied && info.orderBy != null && info.orderBy.getItems() != null && info.orderBy.getItems().size() > 0) {
      plan.chain(new OrderByStep(info.orderBy, maxResults, context, info.timeout != null ? info.timeout.getVal().longValue() : -1,
          profilingEnabled));
      if (info.projectionAfterOrderBy != null) {
        plan.chain(new ProjectionCalculationStep(info.projectionAfterOrderBy, context, profilingEnabled));
      }
    }
  }

  /**
   * @param plan             the execution plan where to add the fetch step
   * @param filterClusters   clusters of interest (all the others have to be excluded from the result)
   * @param info
   * @param context
   * @param profilingEnabled
   */
  private void handleClassAsTarget(final SelectExecutionPlan plan, final Set<String> filterClusters, final QueryPlanningInfo info,
      final CommandContext context, final boolean profilingEnabled) {
    handleClassAsTarget(plan, filterClusters, info.target, info, context, profilingEnabled);
  }

  private void handleClassAsTarget(final SelectExecutionPlan plan, final Set<String> filterClusters, final FromClause from,
      final QueryPlanningInfo info, final CommandContext context, final boolean profilingEnabled) {
    final Identifier identifier = from.getItem().getIdentifier();
    if (handleClassAsTargetWithIndexedFunction(plan, filterClusters, identifier, info, context, profilingEnabled)) {
      plan.chain(new FilterByTypeStep(identifier, context, profilingEnabled));
      return;
    }

    if (handleClassAsTargetWithIndex(plan, identifier, filterClusters, info, context, profilingEnabled)) {
      plan.chain(new FilterByTypeStep(identifier, context, profilingEnabled));
      return;
    }

    if (info.orderBy != null && handleClassWithIndexForSortOnly(plan, identifier, filterClusters, info, context,
        profilingEnabled)) {
      plan.chain(new FilterByTypeStep(identifier, context, profilingEnabled));
      return;
    }

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (isOrderByRidAsc(info)) {
      orderByRidAsc = true;
    }
//    else if (isOrderByRidDesc(info)) {
//      orderByRidAsc = false;
//    }
    final FetchFromTypeExecutionStep fetcher = new FetchFromTypeExecutionStep(identifier.getStringValue(), filterClusters, info,
        context, orderByRidAsc, profilingEnabled);
    if (orderByRidAsc != null)
      info.orderApplied = true;

    plan.chain(fetcher);
  }

  private boolean handleClassAsTargetWithIndexedFunction(final SelectExecutionPlan plan, final Set<String> filterClusters,
      final Identifier queryTarget, final QueryPlanningInfo info, final CommandContext context, final boolean profilingEnabled) {
    if (queryTarget == null) {
      return false;
    }
    final DocumentType typez = context.getDatabase().getSchema().getType(queryTarget.getStringValue());
    if (typez == null) {
      throw new CommandExecutionException("Type not found: " + queryTarget);
    }
    if (info.flattenedWhereClause == null || info.flattenedWhereClause.size() == 0) {
      return false;
    }

    final List<InternalExecutionPlan> resultSubPlans = new ArrayList<>();

    boolean indexedFunctionsFound = false;

    for (AndBlock block : info.flattenedWhereClause) {
      List<BinaryCondition> indexedFunctionConditions = block.getIndexedFunctionConditions(typez, context);

      indexedFunctionConditions = filterIndexedFunctionsWithoutIndex(indexedFunctionConditions, info.target, context);

      if (indexedFunctionConditions == null || indexedFunctionConditions.size() == 0) {
        final List<IndexSearchDescriptor> bestIndexes = findBestIndexesFor(context, typez.getAllIndexes(true), block, typez);
        if (!bestIndexes.isEmpty()) {

          for (final IndexSearchDescriptor bestIndex : bestIndexes) {
            final FetchFromIndexStep step = new FetchFromIndexStep(bestIndex.idx, bestIndex.keyCondition,
                bestIndex.additionalRangeCondition, true, context, profilingEnabled);

            final SelectExecutionPlan subPlan = new SelectExecutionPlan(context);
            subPlan.chain(step);
            int[] filterClusterIds = null;
            if (filterClusters != null) {
              filterClusterIds = filterClusters.stream()
                  .map(name -> context.getDatabase().getSchema().getBucketByName(name).getFileId()).mapToInt(i -> i).toArray();
            }
            subPlan.chain(new GetValueFromIndexEntryStep(context, filterClusterIds, profilingEnabled));
            if (requiresMultipleIndexLookups(bestIndex.keyCondition)) {
              subPlan.chain(new DistinctExecutionStep(context, profilingEnabled));
            }
            if (!block.getSubBlocks().isEmpty()) {
              subPlan.chain(new FilterStep(createWhereFrom(block), context, profilingEnabled));
            }
            resultSubPlans.add(subPlan);
          }

        } else {
          final FetchFromTypeExecutionStep step = new FetchFromTypeExecutionStep(typez.getName(), filterClusters, context, true,
              profilingEnabled);
          final SelectExecutionPlan subPlan = new SelectExecutionPlan(context);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            subPlan.chain(new FilterStep(createWhereFrom(block), context, profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        }
      } else {
        BinaryCondition blockCandidateFunction = null;
        for (final BinaryCondition cond : indexedFunctionConditions) {
          if (!cond.allowsIndexedFunctionExecutionOnTarget(info.target, context)) {
            if (!cond.canExecuteIndexedFunctionWithoutIndex(info.target, context)) {
              throw new CommandExecutionException("Cannot execute " + block + " on " + queryTarget);
            }
          }
          if (blockCandidateFunction == null) {
            blockCandidateFunction = cond;
          } else {
            final boolean thisAllowsNoIndex = cond.canExecuteIndexedFunctionWithoutIndex(info.target, context);
            final boolean prevAllowsNoIndex = blockCandidateFunction.canExecuteIndexedFunctionWithoutIndex(info.target, context);
            if (!thisAllowsNoIndex && !prevAllowsNoIndex) {
              //none of the functions allow execution without index, so cannot choose one
              throw new CommandExecutionException(
                  "Cannot choose indexed function between " + cond + " and " + blockCandidateFunction
                      + ". Both require indexed execution");
            } else if (thisAllowsNoIndex && prevAllowsNoIndex) {
              //both can be calculated without index, choose the best one for index execution
              final long thisEstimate = cond.estimateIndexed(info.target, context);
              final long lastEstimate = blockCandidateFunction.estimateIndexed(info.target, context);
              if (thisEstimate > -1 && thisEstimate < lastEstimate) {
                blockCandidateFunction = cond;
              }
            } else if (prevAllowsNoIndex) {
              //choose current condition, because the other one can be calculated without index
              blockCandidateFunction = cond;
            }
          }
        }

        final FetchFromIndexedFunctionStep step = new FetchFromIndexedFunctionStep(blockCandidateFunction, info.target, context,
            profilingEnabled);
        if (!blockCandidateFunction.executeIndexedFunctionAfterIndexSearch(info.target, context)) {
          block = block.copy();
          block.getSubBlocks().remove(blockCandidateFunction);
        }
        if (info.flattenedWhereClause.size() == 1) {
          plan.chain(step);
          plan.chain(new FilterByClustersStep(filterClusters, context, profilingEnabled));
          if (!block.getSubBlocks().isEmpty()) {
            plan.chain(new FilterStep(createWhereFrom(block), context, profilingEnabled));
          }
        } else {
          final SelectExecutionPlan subPlan = new SelectExecutionPlan(context);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            subPlan.chain(new FilterStep(createWhereFrom(block), context, profilingEnabled));
          }
          resultSubPlans.add(subPlan);
        }
        indexedFunctionsFound = true;
      }
    }

    if (indexedFunctionsFound) {
      if (resultSubPlans.size() > 1) { //if resultSubPlans.size() == 1 the step was already chained (see above)
        plan.chain(new ParallelExecStep(resultSubPlans, context, profilingEnabled));
        plan.chain(new FilterByClustersStep(filterClusters, context, profilingEnabled));
        plan.chain(new DistinctExecutionStep(context, profilingEnabled));
      }
      //WHERE condition already applied
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    } else {
      return false;
    }
  }

  private List<BinaryCondition> filterIndexedFunctionsWithoutIndex(final List<BinaryCondition> indexedFunctionConditions,
      final FromClause fromClause, final CommandContext context) {
    if (indexedFunctionConditions == null) {
      return null;
    }
    final List<BinaryCondition> result = new ArrayList<>();
    for (final BinaryCondition cond : indexedFunctionConditions) {
      if (cond.allowsIndexedFunctionExecutionOnTarget(fromClause, context)) {
        result.add(cond);
      } else if (!cond.canExecuteIndexedFunctionWithoutIndex(fromClause, context)) {
        throw new CommandExecutionException("Cannot evaluate " + cond + ": no index defined");
      }
    }
    return result;
  }

  /**
   * tries to use an index for sorting only. Also adds the fetch step to the execution plan
   *
   * @param plan    current execution plan
   * @param info    the query planning information
   * @param context the current context
   *
   * @return true if it succeeded to use an index to sort, false otherwise.
   */

  private boolean handleClassWithIndexForSortOnly(final SelectExecutionPlan plan, final Identifier queryTarget,
      final Set<String> filterClusters, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {

    final DocumentType typez = context.getDatabase().getSchema().getType(queryTarget.getStringValue());
    if (typez == null) {
      throw new CommandExecutionException("Type not found: " + queryTarget.getStringValue());
    }

    for (final Index idx : typez.getAllIndexes(true).stream().filter(i -> i.supportsOrderedIterations())
        .collect(Collectors.toList())) {
      final List<String> indexFields = idx.getPropertyNames();
      if (indexFields.size() < info.orderBy.getItems().size()) {
        continue;
      }
      boolean indexFound = true;
      String orderType = null;
      for (int i = 0; i < info.orderBy.getItems().size(); i++) {
        final OrderByItem orderItem = info.orderBy.getItems().get(i);
        final String indexField = indexFields.get(i);
        if (i == 0) {
          orderType = orderItem.getType();
        } else {
          if (orderType == null || !orderType.equals(orderItem.getType())) {
            indexFound = false;
            break;//ASC/DESC interleaved, cannot be used with index.
          }
        }
        if (!indexField.equals(orderItem.getAlias())) {
          indexFound = false;
          break;
        }
      }
      if (indexFound && orderType != null) {
        plan.chain(new FetchFromIndexValuesStep((RangeIndex) idx, orderType.equals(OrderByItem.ASC), context, profilingEnabled));
        int[] filterClusterIds = null;
        if (filterClusters != null) {
          filterClusterIds = filterClusters.stream()
              .map(name -> context.getDatabase().getSchema().getBucketByName(name).getFileId()).mapToInt(i -> i).toArray();
        }
        plan.chain(new GetValueFromIndexEntryStep(context, filterClusterIds, profilingEnabled));
        info.orderApplied = true;
        return true;
      }
    }
    return false;
  }

  private boolean handleClassAsTargetWithIndex(final SelectExecutionPlan plan, final Identifier targetClass,
      final Set<String> filterClusters, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {

    final List<ExecutionStepInternal> result = handleClassAsTargetWithIndex(targetClass.getStringValue(), filterClusters, info,
        context, profilingEnabled);
    if (result != null) {
      result.forEach(plan::chain);
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    }
    //TODO
    final DocumentType typez = context.getDatabase().getSchema().getType(targetClass.getStringValue());
    if (typez == null)
      throw new CommandExecutionException("Cannot find type '" + targetClass + "'");

    if (!isEmptyNoSubclasses(typez) || typez.getSubTypes().isEmpty() || isDiamondHierarchy(typez))
      return false;

    final Collection<DocumentType> subTypes = typez.getSubTypes();

    final List<InternalExecutionPlan> subTypePlans = new ArrayList<>();
    for (final DocumentType subType : subTypes) {
      final List<ExecutionStepInternal> subSteps = handleClassAsTargetWithIndexRecursive(subType.getName(), filterClusters, info,
          context, profilingEnabled);
      if (subSteps == null || subSteps.isEmpty())
        return false;

      final SelectExecutionPlan subPlan = new SelectExecutionPlan(context);
      subSteps.forEach(x -> subPlan.chain(x));
      subTypePlans.add(subPlan);
    }
    if (!subTypePlans.isEmpty()) {
      plan.chain(new ParallelExecStep(subTypePlans, context, profilingEnabled));
      return true;
    }
    return false;
  }

  private boolean isEmptyNoSubclasses(final DocumentType typez) {
    final List<com.arcadedb.engine.Bucket> buckets = typez.getBuckets(false);
    for (final com.arcadedb.engine.Bucket bucket : buckets) {
      if (bucket.iterator().hasNext())
        return false;
    }
    return true;
  }

  /**
   * checks if a class is the top of a diamond hierarchy
   *
   * @param typez
   *
   * @return
   */
  private boolean isDiamondHierarchy(final DocumentType typez) {
    final Set<DocumentType> traversed = new HashSet<>();
    final List<DocumentType> stack = new ArrayList<>();
    stack.add(typez);
    while (!stack.isEmpty()) {
      final DocumentType current = stack.remove(0);
      traversed.add(current);
      for (final DocumentType sub : current.getSubTypes()) {
        if (traversed.contains(sub))
          return true;

        stack.add(sub);
        traversed.add(sub);
      }
    }
    return false;
  }

  private List<ExecutionStepInternal> handleClassAsTargetWithIndexRecursive(final String targetClass,
      final Set<String> filterClusters, final QueryPlanningInfo info, final CommandContext context,
      final boolean profilingEnabled) {
    List<ExecutionStepInternal> result = handleClassAsTargetWithIndex(targetClass, filterClusters, info, context, profilingEnabled);
    if (result == null) {
      result = new ArrayList<>();
      final DocumentType typez = context.getDatabase().getSchema().getType(targetClass);
      if (typez == null)
        throw new CommandExecutionException("Cannot find class " + targetClass);

//      if (typez.count(false) != 0 || typez.getSubclasses().size() == 0 || isDiamondHierarchy(typez)) {
//      return null;
//      }

      final Collection<DocumentType> subTypes = typez.getSubTypes();

      final List<InternalExecutionPlan> subTypePlans = new ArrayList<>();
      for (final DocumentType subType : subTypes) {
        final List<ExecutionStepInternal> subSteps = handleClassAsTargetWithIndexRecursive(subType.getName(), filterClusters, info,
            context, profilingEnabled);
        if (subSteps == null || subSteps.isEmpty()) {
          return null;
        }
        final SelectExecutionPlan subPlan = new SelectExecutionPlan(context);
        subSteps.forEach(subPlan::chain);
        subTypePlans.add(subPlan);
      }
      if (!subTypePlans.isEmpty()) {
        result.add(new ParallelExecStep(subTypePlans, context, profilingEnabled));
      }
    }
    return result.isEmpty() ? null : result;
  }

  private List<ExecutionStepInternal> handleClassAsTargetWithIndex(final String targetClass, final Set<String> filterClusters,
      final QueryPlanningInfo info, final CommandContext context, final boolean profilingEnabled) {
    if (info.flattenedWhereClause == null || info.flattenedWhereClause.size() == 0) {
      return null;
    }

    final DocumentType typez = context.getDatabase().getSchema().getType(targetClass);
    if (typez == null) {
      throw new CommandExecutionException("Cannot find type " + targetClass);
    }

    final Collection<TypeIndex> indexes = typez.getAllIndexes(true);

    if (indexes.isEmpty())
      return null;

    final List<IndexSearchDescriptor> indexSearchDescriptors = info.flattenedWhereClause.stream()
        .map(x -> findBestIndexFor(context, indexes, x, typez)).filter(Objects::nonNull).collect(Collectors.toList());

    if (indexSearchDescriptors.isEmpty())
      return null;

    // TODO
    //    if (indexSearchDescriptors.size() != info.flattenedWhereClause.size()) {
    //      return null; //some blocks could not be managed with an index
    //    }

//    // ARCADEDB: NOT APPLICABLE TO ORIENTDB
//    // IF THERE ARE ANY CHANGES IN TX, AVOID IT
//    for (IndexSearchDescriptor entry : indexSearchDescriptors) {
//      if (entry.idx instanceof TypeIndex) {
//        for (IndexInternal idx : ((TypeIndex) entry.idx).getIndexesOnBuckets()) {
//          if (context.getDatabase().getTransaction().getIndexChanges().getTotalEntriesByIndex(idx.getName()) > 0)
//            // CANNOT USE THE INDEX WITH MODIFIED PAGES IN TX BECAUSE THEY COULD CONTAIN NOT INDEXED ENTRIES
//            return null;
//        }
//      } else if (context.getDatabase().getTransaction().getIndexChanges().getTotalEntriesByIndex(entry.idx.getName()) > 0)
//        // CANNOT USE THE INDEX WITH MODIFIED PAGES IN TX BECAUSE THEY COULD CONTAIN NOT INDEXED ENTRIES
//        return null;
//    }

    final List<ExecutionStepInternal> result;

    final List<IndexSearchDescriptor> optimumIndexSearchDescriptors = commonFactor(indexSearchDescriptors);

    if (indexSearchDescriptors.size() == 1) {
      final IndexSearchDescriptor desc = indexSearchDescriptors.get(0);
      result = new ArrayList<>();
      final Boolean orderAsc = getOrderDirection(info);
      result.add(new FetchFromIndexStep(desc.idx, desc.keyCondition, desc.additionalRangeCondition, !Boolean.FALSE.equals(orderAsc),
          context, profilingEnabled));
      int[] filterClusterIds = null;
      if (filterClusters != null) {
        filterClusterIds = filterClusters.stream().map(name -> context.getDatabase().getSchema().getBucketByName(name).getFileId())
            .mapToInt(i -> i).toArray();
      }
      result.add(new GetValueFromIndexEntryStep(context, filterClusterIds, profilingEnabled));
      if (requiresMultipleIndexLookups(desc.keyCondition)) {
        result.add(new DistinctExecutionStep(context, profilingEnabled));
      }
      if (orderAsc != null && info.orderBy != null && fullySorted(info.orderBy, desc.keyCondition, desc.idx)) {
        info.orderApplied = true;
      }
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        result.add(new FilterStep(createWhereFrom(desc.remainingCondition), context, profilingEnabled));
      }
    } else {
      result = new ArrayList<>();
      result.add(createParallelIndexFetch(optimumIndexSearchDescriptors, filterClusters, context, profilingEnabled));
      if (optimumIndexSearchDescriptors.size() > 1) {
        result.add(new DistinctExecutionStep(context, profilingEnabled));
      }
    }
    return result;
  }

  private boolean fullySorted(final OrderBy orderBy, final AndBlock conditions, final Index idx) {
    if (!idx.supportsOrderedIterations())
      return false;

    final List<String> orderItems = new ArrayList<>();
    String order = null;

    for (final OrderByItem item : orderBy.getItems()) {
      if (order == null) {
        order = item.getType();
      } else if (!order.equals(item.getType())) {
        return false;
      }
      orderItems.add(item.getAlias());
    }

    final List<String> conditionItems = new ArrayList<>();

    for (int i = 0; i < conditions.getSubBlocks().size(); i++) {
      final BooleanExpression item = conditions.getSubBlocks().get(i);
      if (item instanceof BinaryCondition) {
        if (((BinaryCondition) item).getOperator() instanceof EqualsCompareOperator) {
          conditionItems.add(((BinaryCondition) item).getLeft().toString());
        } else if (i != conditions.getSubBlocks().size() - 1) {
          return false;
        }

      } else if (i != conditions.getSubBlocks().size() - 1) {
        return false;
      }
    }

    final List<String> orderedFields = new ArrayList<>();
    boolean overlapping = false;
    for (final String s : conditionItems) {
      if (orderItems.isEmpty()) {
        return true;//nothing to sort, the conditions completely overlap the ORDER BY
      }
      if (s.equals(orderItems.get(0))) {
        orderItems.remove(0);
        overlapping = true; //start overlapping
      } else if (overlapping) {
        return false; //overlapping, but next order item does not match...
      }
      orderedFields.add(s);
    }
    orderedFields.addAll(orderItems);

    final List<String> fields = idx.getPropertyNames();
    if (fields.size() < orderedFields.size()) {
      return false;
    }

    for (int i = 0; i < orderedFields.size(); i++) {
      final String orderFieldName = orderedFields.get(i);
      final String indexFieldName = fields.get(i);
      if (!orderFieldName.equals(indexFieldName)) {
        return false;
      }
    }

    return true;
  }

  /**
   * returns TRUE if all the order clauses are ASC, FALSE if all are DESC, null otherwise
   *
   * @return TRUE if all the order clauses are ASC, FALSE if all are DESC, null otherwise
   */
  private Boolean getOrderDirection(final QueryPlanningInfo info) {
    if (info.orderBy == null) {
      return null;
    }
    String result = null;
    for (final OrderByItem item : info.orderBy.getItems()) {
      if (result == null) {
        result = item.getType() == null ? OrderByItem.ASC : item.getType();
      } else {
        final String newType = item.getType() == null ? OrderByItem.ASC : item.getType();
        if (!newType.equals(result)) {
          return null;
        }
      }
    }
    return result == null || result.equals(OrderByItem.ASC);
  }

  private ExecutionStepInternal createParallelIndexFetch(final List<IndexSearchDescriptor> indexSearchDescriptors,
      final Set<String> filterClusters, final CommandContext context, final boolean profilingEnabled) {
    final List<InternalExecutionPlan> subPlans = new ArrayList<>();
    for (final IndexSearchDescriptor desc : indexSearchDescriptors) {
      final SelectExecutionPlan subPlan = new SelectExecutionPlan(context);
      subPlan.chain(new FetchFromIndexStep(desc.idx, desc.keyCondition, desc.additionalRangeCondition, context, profilingEnabled));
      int[] filterClusterIds = null;
      if (filterClusters != null) {
        filterClusterIds = filterClusters.stream().map(name -> context.getDatabase().getSchema().getBucketByName(name).getFileId())
            .mapToInt(i -> i).toArray();
      }
      subPlan.chain(new GetValueFromIndexEntryStep(context, filterClusterIds, profilingEnabled));
      if (requiresMultipleIndexLookups(desc.keyCondition)) {
        subPlan.chain(new DistinctExecutionStep(context, profilingEnabled));
      }
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        subPlan.chain(new FilterStep(createWhereFrom(desc.remainingCondition), context, profilingEnabled));
      }
      subPlans.add(subPlan);
    }
    return new ParallelExecStep(subPlans, context, profilingEnabled);
  }

  /**
   * checks whether the condition has CONTAINSANY or similar expressions, that require multiple index evaluations
   *
   * @param keyCondition
   *
   * @return
   */
  private boolean requiresMultipleIndexLookups(final AndBlock keyCondition) {
    for (final BooleanExpression oBooleanExpression : keyCondition.getSubBlocks()) {
      if (!(oBooleanExpression instanceof BinaryCondition)) {
        return true;
      }
    }
    return false;
  }

  private WhereClause createWhereFrom(final BooleanExpression remainingCondition) {
    final WhereClause result = new WhereClause(-1);
    result.setBaseExpression(remainingCondition);
    return result;
  }

  /**
   * given a flat AND block and a set of indexes, returns the best index to be used to process it, with the complete description on
   * how to use it
   *
   * @param context
   * @param indexes
   * @param block
   *
   * @return
   */
  private List<IndexSearchDescriptor> findBestIndexesFor(final CommandContext context, final Collection<TypeIndex> indexes,
      final AndBlock block, final DocumentType typez) {
    final Iterator<IndexSearchDescriptor> it = indexes.stream()
        //.filter(index -> index.getInternal().canBeUsedInEqualityOperators())
        .map(index -> buildIndexSearchDescriptor(context, index, block)).filter(Objects::nonNull)
        .filter(x -> x.keyCondition != null).filter(x -> x.keyCondition.getSubBlocks().size() > 0)
        .sorted(Comparator.comparing(x -> x.cost(context))).iterator();

    final List<IndexSearchDescriptor> list = new ArrayList<>();

    while (it.hasNext())
      list.add(it.next());

    return list;
  }

  /**
   * given a flat AND block and a set of indexes, returns the best index to be used to process it,
   * with the complete description on how to use it
   *
   * @param context
   * @param indexes
   * @param block
   *
   * @return
   */
  private IndexSearchDescriptor findBestIndexFor(final CommandContext context, final Collection<TypeIndex> indexes,
      final AndBlock block, final DocumentType clazz) {
    // get all valid index descriptors
    List<IndexSearchDescriptor> descriptors = indexes.stream().map(index -> buildIndexSearchDescriptor(context, index, block))
        .filter(Objects::nonNull).filter(x -> x.keyCondition != null).filter(x -> x.keyCondition.getSubBlocks().size() > 0)
        .collect(Collectors.toList());

    final List<IndexSearchDescriptor> fullTextIndexDescriptors = indexes.stream().filter(idx -> idx.getType().equals(FULL_TEXT))
        .map(idx -> buildIndexSearchDescriptorForFulltext(context, idx, block, clazz)).filter(Objects::nonNull)
        .filter(x -> x.keyCondition != null).filter(x -> x.keyCondition.getSubBlocks().size() > 0).collect(Collectors.toList());

    descriptors.addAll(fullTextIndexDescriptors);

    // remove the redundant descriptors (eg. if I have one on [a] and one on [a, b], the first one
    // is redundant, just discard it)
    descriptors = removePrefixIndexes(descriptors);

    // sort by cost
    final List<Pair<Integer, IndexSearchDescriptor>> sortedDescriptors = descriptors.stream()
        .map(x -> (Pair<Integer, IndexSearchDescriptor>) new Pair(x.cost(context), x)).sorted().collect(Collectors.toList());

    // get only the descriptors with the lowest cost
    if (sortedDescriptors.isEmpty()) {
      descriptors = Collections.emptyList();
    } else {
      descriptors = sortedDescriptors.stream().filter(x -> x.getFirst().equals(sortedDescriptors.get(0).getFirst()))
          .map(x -> x.getSecond()).collect(Collectors.toList());
    }

    // sort remaining by the number of indexed fields
    descriptors = descriptors.stream().sorted(Comparator.comparingInt(x -> x.keyCondition.getSubBlocks().size()))
        .collect(Collectors.toList());

    // get the one that has more indexed fields
    return descriptors.isEmpty() ? null : descriptors.get(descriptors.size() - 1);
  }

  /**
   * returns true if the first argument is a prefix for the second argument, eg. if the first
   * argument is [a] and the second argument is [a, b]
   *
   * @param item
   * @param desc
   *
   * @return
   */
  private boolean isPrefixOf(final IndexSearchDescriptor item, final IndexSearchDescriptor desc) {
    final List<BooleanExpression> left = item.keyCondition.getSubBlocks();
    final List<BooleanExpression> right = desc.keyCondition.getSubBlocks();
    if (left.size() > right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); i++) {
      if (!left.get(i).equals(right.get(i))) {
        return false;
      }
    }
    return true;
  }

  private boolean isPrefixOfAny(final IndexSearchDescriptor desc, final List<IndexSearchDescriptor> result) {
    for (final IndexSearchDescriptor item : result) {
      if (isPrefixOf(desc, item)) {
        return true;
      }
    }
    return false;
  }

  /**
   * finds prefix conditions for a given condition, eg. if the condition is on [a,b] and in the list
   * there is another condition on [a] or on [a,b], then that condition is returned.
   *
   * @param desc
   * @param descriptors
   *
   * @return
   */
  private List<IndexSearchDescriptor> findPrefixes(final IndexSearchDescriptor desc,
      final List<IndexSearchDescriptor> descriptors) {
    final List<IndexSearchDescriptor> result = new ArrayList<>();
    for (final IndexSearchDescriptor item : descriptors) {
      if (isPrefixOf(item, desc)) {
        result.add(item);
      }
    }
    return result;
  }

  private List<IndexSearchDescriptor> removePrefixIndexes(final List<IndexSearchDescriptor> descriptors) {
    final List<IndexSearchDescriptor> result = new ArrayList<>();
    for (final IndexSearchDescriptor desc : descriptors) {
      if (result.isEmpty()) {
        result.add(desc);
      } else {
        final List<IndexSearchDescriptor> prefixes = findPrefixes(desc, result);
        if (prefixes.isEmpty()) {
          if (!isPrefixOfAny(desc, result)) {
            result.add(desc);
          }
        } else {
          result.removeAll(prefixes);
          result.add(desc);
        }
      }
    }
    return result;
  }

  /**
   * given a full text index and a flat AND block, returns a descriptor on how to process it with an
   * index (index, index key and additional filters to apply after index fetch
   *
   * @param context
   * @param index
   * @param block
   * @param clazz
   *
   * @return
   */
  private IndexSearchDescriptor buildIndexSearchDescriptorForFulltext(final CommandContext context, final Index index,
      final AndBlock block, final DocumentType clazz) {
    final List<String> indexFields = index.getPropertyNames();
    final BinaryCondition keyCondition = new BinaryCondition(-1);
    final Identifier key = new Identifier("key");
    keyCondition.setLeft(new Expression(key));
    boolean found = false;

    final AndBlock blockCopy = block.copy();
    Iterator<BooleanExpression> blockIterator;

    final AndBlock indexKeyValue = new AndBlock(-1);
    final IndexSearchDescriptor result = new IndexSearchDescriptor();
    result.idx = (RangeIndex) index;
    result.keyCondition = indexKeyValue;
    for (final String indexField : indexFields) {
      blockIterator = blockCopy.getSubBlocks().iterator();
      final boolean breakHere = false;
      boolean indexFieldFound = false;
      while (blockIterator.hasNext()) {
        final BooleanExpression singleExp = blockIterator.next();
        if (singleExp instanceof ContainsTextCondition) {
          final Expression left = ((ContainsTextCondition) singleExp).getLeft();
          if (left.isBaseIdentifier()) {
            final String fieldName = left.getDefaultAlias().getStringValue();
            if (indexField.equals(fieldName)) {
              found = true;
              indexFieldFound = true;
              final ContainsTextCondition condition = new ContainsTextCondition(-1);
              condition.setLeft(left);
              condition.setRight(((ContainsTextCondition) singleExp).getRight().copy());
              indexKeyValue.getSubBlocks().add(condition);
              blockIterator.remove();
              break;
            }
          }
        }
      }
      if (breakHere || !indexFieldFound) {
        break;
      }
    }

    if (result.keyCondition.getSubBlocks().size() < index.getPropertyNames().size() && !index.supportsOrderedIterations()) {
      // hash indexes do not support partial key match
      return null;
    }

    if (found) {
      result.remainingCondition = blockCopy;
      return result;
    }
    return null;
  }

  /**
   * given an index and a flat AND block, returns a descriptor on how to process it with an index (index, index key and additional
   * filters to apply after index fetch
   *
   * @param index
   * @param block
   *
   * @return
   */
  private IndexSearchDescriptor buildIndexSearchDescriptor(final CommandContext context, final Index index, final AndBlock block) {
    final List<String> indexFields = index.getPropertyNames();
    final BinaryCondition keyCondition = new BinaryCondition(-1);
    final Identifier key = new Identifier("key");
    keyCondition.setLeft(new Expression(key));
    final boolean allowsRange = allowsRangeQueries(index);
    boolean found = false;

    final AndBlock blockCopy = block.copy();
    Iterator<BooleanExpression> blockIterator;

    final AndBlock indexKeyValue = new AndBlock(-1);
    final IndexSearchDescriptor result = new IndexSearchDescriptor();
    result.idx = (RangeIndex) index;
    result.keyCondition = indexKeyValue;
    for (final String indexField : indexFields) {
      blockIterator = blockCopy.getSubBlocks().iterator();
      boolean breakHere = false;
      boolean indexFieldFound = false;
      while (blockIterator.hasNext()) {
        final BooleanExpression singleExp = blockIterator.next();
        if (singleExp instanceof BinaryCondition) {
          final Expression left = ((BinaryCondition) singleExp).getLeft();
          if (left.isBaseIdentifier()) {
            final String fieldName = left.getDefaultAlias().getStringValue();
            if (indexField.equals(fieldName)) {
              final BinaryCompareOperator operator = ((BinaryCondition) singleExp).getOperator();
              if (!((BinaryCondition) singleExp).getRight().isEarlyCalculated(context)) {
                continue; //this cannot be used because the value depends on single record
              }
              if (operator instanceof EqualsCompareOperator) {
                found = true;
                indexFieldFound = true;
                final BinaryCondition condition = new BinaryCondition(-1);
                condition.setLeft(left);
                condition.setOperator(operator);
                condition.setRight(((BinaryCondition) singleExp).getRight().copy());
                indexKeyValue.getSubBlocks().add(condition);
                blockIterator.remove();
                break;
              } else if (allowsRange && operator.isRangeOperator()) {
                found = true;
                indexFieldFound = true;
                breakHere = true;//this is last element, no other fields can be added to the key because this is a range condition
                final BinaryCondition condition = new BinaryCondition(-1);
                condition.setLeft(left);
                condition.setOperator(operator);
                condition.setRight(((BinaryCondition) singleExp).getRight().copy());
                indexKeyValue.getSubBlocks().add(condition);
                blockIterator.remove();
                //look for the opposite condition, on the same field, for range queries (the other side of the range)
                while (blockIterator.hasNext()) {
                  final BooleanExpression next = blockIterator.next();
                  if (createsRangeWith((BinaryCondition) singleExp, next)) {
                    result.additionalRangeCondition = (BinaryCondition) next;
                    blockIterator.remove();
                    break;
                  }
                }
                break;
              }
            }
          }
        } else if (singleExp instanceof ContainsAnyCondition) {
          final Expression left = ((ContainsAnyCondition) singleExp).getLeft();
          if (left.isBaseIdentifier()) {
            final String fieldName = left.getDefaultAlias().getStringValue();
            if (indexField.equals(fieldName)) {
              if (!((ContainsAnyCondition) singleExp).getRight().isEarlyCalculated(context)) {
                continue; //this cannot be used because the value depends on single record
              }
              found = true;
              indexFieldFound = true;
              final ContainsAnyCondition condition = new ContainsAnyCondition(-1);
              condition.setLeft(left);
              condition.setRight(((ContainsAnyCondition) singleExp).getRight().copy());
              indexKeyValue.getSubBlocks().add(condition);
              blockIterator.remove();
              break;
            }
          }
        } else if (singleExp instanceof InCondition) {
          final Expression left = ((InCondition) singleExp).getLeft();
          if (left.isBaseIdentifier()) {
            final String fieldName = left.getDefaultAlias().getStringValue();
            if (indexField.equals(fieldName)) {
              if (((InCondition) singleExp).getRightMathExpression() != null) {

                if (!((InCondition) singleExp).getRightMathExpression().isEarlyCalculated(context)) {
                  continue; //this cannot be used because the value depends on single record
                }
                found = true;
                indexFieldFound = true;
                final InCondition condition = new InCondition(-1);
                condition.setLeft(left);
                condition.setRightMathExpression(((InCondition) singleExp).getRightMathExpression().copy());
                indexKeyValue.getSubBlocks().add(condition);
                blockIterator.remove();
                break;
              } else if (((InCondition) singleExp).getRightParam() != null) {
                found = true;
                indexFieldFound = true;
                final InCondition condition = new InCondition(-1);
                condition.setLeft(left);
                condition.setRightParam(((InCondition) singleExp).getRightParam().copy());
                indexKeyValue.getSubBlocks().add(condition);
                blockIterator.remove();
                break;
              }
            }
          }
        }
      }
      if (breakHere || !indexFieldFound) {
        break;
      }
    }

    if (found) {
      result.remainingCondition = blockCopy;
      return result;
    }
    return null;
  }

  private boolean createsRangeWith(final BinaryCondition left, final BooleanExpression next) {
    if (!(next instanceof BinaryCondition))
      return false;

    final BinaryCondition right = (BinaryCondition) next;
    if (!left.getLeft().equals(right.getLeft()))
      return false;

    final BinaryCompareOperator leftOperator = left.getOperator();
    final BinaryCompareOperator rightOperator = right.getOperator();
    if (leftOperator instanceof GeOperator || leftOperator instanceof GtOperator)
      return rightOperator instanceof LeOperator || rightOperator instanceof LtOperator;

    if (leftOperator instanceof LeOperator || leftOperator instanceof LtOperator)
      return rightOperator instanceof GeOperator || rightOperator instanceof GtOperator;
    return false;
  }

  private boolean allowsRangeQueries(final Index index) {
    return index.supportsOrderedIterations();
  }

  /**
   * aggregates multiple index conditions that refer to the same key search
   *
   * @param indexSearchDescriptors
   *
   * @return
   */
  private List<IndexSearchDescriptor> commonFactor(final List<IndexSearchDescriptor> indexSearchDescriptors) {
    //index, key condition, additional filter (to aggregate in OR)
    final Map<RangeIndex, Map<IndexCondPair, OrBlock>> aggregation = new HashMap<>();
    for (final IndexSearchDescriptor item : indexSearchDescriptors) {
      final Map<IndexCondPair, OrBlock> filtersForIndex = aggregation.computeIfAbsent(item.idx, k -> new HashMap<>());
      final IndexCondPair extendedCond = new IndexCondPair(item.keyCondition, item.additionalRangeCondition);

      OrBlock existingAdditionalConditions = filtersForIndex.get(extendedCond);
      if (existingAdditionalConditions == null) {
        existingAdditionalConditions = new OrBlock(-1);
        filtersForIndex.put(extendedCond, existingAdditionalConditions);
      }
      existingAdditionalConditions.getSubBlocks().add(item.remainingCondition);
    }
    final List<IndexSearchDescriptor> result = new ArrayList<>();
    for (final Map.Entry<RangeIndex, Map<IndexCondPair, OrBlock>> item : aggregation.entrySet()) {
      for (final Map.Entry<IndexCondPair, OrBlock> filters : item.getValue().entrySet()) {
        result.add(new IndexSearchDescriptor(item.getKey(), filters.getKey().mainCondition, filters.getKey().additionalRange,
            filters.getValue()));
      }
    }
    return result;
  }

  private void handleClustersAsTarget(final SelectExecutionPlan plan, final QueryPlanningInfo info, final List<Bucket> buckets,
      final CommandContext context, final boolean profilingEnabled) {
    final Database db = context.getDatabase();

    DocumentType candidateClass = null;
    boolean tryByIndex = true;
    final Set<String> bucketNames = new HashSet<>();

    for (final Bucket parserBucket : buckets) {
      String name = parserBucket.getBucketName();
      Integer bucketId = parserBucket.getBucketNumber();
      if (name == null && bucketId != null)
        name = db.getSchema().getBucketById(bucketId).getName();

      if (bucketId == null) {
        final com.arcadedb.engine.Bucket bucket = db.getSchema().getBucketByName(name);
        if (bucket != null)
          bucketId = bucket.getFileId();
      }

      if (name != null) {
        bucketNames.add(name);
        final DocumentType typez = bucketId != null ? db.getSchema().getTypeByBucketId(bucketId) : null;
        if (typez == null) {
          tryByIndex = false;
          break;
        }
        if (candidateClass == null) {
          candidateClass = typez;
        } else if (!candidateClass.equals(typez)) {
          candidateClass = null;
          tryByIndex = false;
          break;
        }
      } else {
        tryByIndex = false;
        break;
      }

    }

    if (tryByIndex && candidateClass != null) {
      final Identifier typez = new Identifier(candidateClass.getName());
      if (handleClassAsTargetWithIndexedFunction(plan, bucketNames, typez, info, context, profilingEnabled))
        return;

      if (handleClassAsTargetWithIndex(plan, typez, bucketNames, info, context, profilingEnabled))
        return;

      if (info.orderBy != null && handleClassWithIndexForSortOnly(plan, typez, bucketNames, info, context, profilingEnabled))
        return;
    }

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (isOrderByRidAsc(info))
      orderByRidAsc = true;
//    else if (isOrderByRidDesc(info))
//      orderByRidAsc = false;
    if (orderByRidAsc != null)
      info.orderApplied = true;

    if (buckets.size() == 1) {
      final Bucket parserBucket = buckets.get(0);

      Integer bucketId = parserBucket.getBucketNumber();
      if (bucketId == null) {
        final com.arcadedb.engine.Bucket bucket = db.getSchema().getBucketByName(parserBucket.getBucketName());
        if (bucket != null)
          bucketId = bucket.getFileId();
      }

      if (bucketId == null)
        throw new CommandExecutionException("Bucket '" + parserBucket + "' does not exist");

      final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(bucketId, context, profilingEnabled);
      if (Boolean.TRUE.equals(orderByRidAsc))
        step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
      else if (Boolean.FALSE.equals(orderByRidAsc))
        step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
      plan.chain(step);
    } else {
      final int[] bucketIds = new int[buckets.size()];
      for (int i = 0; i < buckets.size(); i++) {
        final Bucket parserBucket = buckets.get(i);

        Integer bucketId = parserBucket.getBucketNumber();
        if (bucketId == null) {
          final com.arcadedb.engine.Bucket bucket = db.getSchema().getBucketByName(parserBucket.getBucketName());
          if (bucket != null)
            bucketId = bucket.getFileId();
        }

        if (bucketId == null) {
          throw new CommandExecutionException("Bucket '" + parserBucket + "' does not exist");
        }
        bucketIds[i] = bucketId;
      }
      final FetchFromClustersExecutionStep step = new FetchFromClustersExecutionStep(bucketIds, context, orderByRidAsc,
          profilingEnabled);
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

  private boolean isOrderByRidDesc(final QueryPlanningInfo info) {
    return false;
    //TODO buckets do not support reverse iteration, so order by rid desc cannot be optimised!
//    if (!hasTargetWithSortedRids(info)) {
//      return false;
//    }
//
//    if (info.orderBy == null) {
//      return false;
//    }
//    if (info.orderBy.getItems().size() == 1) {
//      OrderByItem item = info.orderBy.getItems().get(0);
//      String recordAttr = item.getRecordAttr();
//      return recordAttr != null && recordAttr.equalsIgnoreCase("@rid") && OrderByItem.DESC.equals(item.getType());
//    }
//    return false;
  }

  private boolean isOrderByRidAsc(final QueryPlanningInfo info) {
    if (!hasTargetWithSortedRids(info))
      return false;

    if (info.orderBy == null)
      return false;

    if (info.orderBy.getItems().size() == 1) {
      final OrderByItem item = info.orderBy.getItems().get(0);
      final String recordAttr = item.getRecordAttr();
      return recordAttr != null && recordAttr.equalsIgnoreCase("@rid") && (item.getType() == null || OrderByItem.ASC.equals(
          item.getType()));
    }
    return false;
  }

  private boolean hasTargetWithSortedRids(final QueryPlanningInfo info) {
    if (info.target == null)
      return false;

    if (info.target.getItem() == null)
      return false;

    if (info.target.getItem().getIdentifier() != null)
      return true;
    else if (info.target.getItem().getBucket() != null)
      return true;
    else
      return info.target.getItem().getBucketList() != null;
  }

  /**
   * Tries to calculate which clusters will be impacted by this query
   *
   * @return a set of bucket names this query will fetch from
   */
  private Set<String> calculateTargetBuckets(final QueryPlanningInfo info, final CommandContext context) {
    if (info.target == null)
      return Collections.emptySet();

    final FromItem item = info.target.getItem();
    if (item.getResultSet() != null)
      return Collections.emptySet();

    final Set<String> result = new HashSet<>();
    final Database db = context.getDatabase();

    if (item.getIdentifier() != null) {
      if (item.getIdentifier().getStringValue().startsWith("$")) {
        // RESOLVE VARIABLE
        final Object value = context.getVariable(item.toString());
        if (value != null) {
          item.setValue(value);
          item.setIdentifier(null);
        }
      }
    }

    if (item.getRids() != null && item.getRids().size() > 0) {
      if (item.getRids().size() == 1) {
        final PInteger bucket = item.getRids().get(0).getBucket();
        result.add(db.getSchema().getBucketById(bucket.getValue().intValue()).getName());
      } else {
        for (final Rid rid : item.getRids()) {
          final PInteger bucket = rid.getBucket();
          result.add(db.getSchema().getBucketById(bucket.getValue().intValue()).getName());
        }
      }
      return result;
    } else if (item.getInputParams() != null && item.getInputParams().size() > 0) {
      return null;
    } else if (item.getBucket() != null) {
      String name = item.getBucket().getBucketName();
      if (name == null) {
        name = db.getSchema().getBucketById(item.getBucket().getBucketNumber()).getName();
      }
      if (name != null) {
        result.add(name);
        return result;
      } else {
        return null;
      }
    } else if (item.getBucketList() != null) {
      for (final Bucket bucket : item.getBucketList().toListOfClusters()) {
        String name = bucket.getBucketName();
        if (name == null) {
          name = db.getSchema().getBucketById(bucket.getBucketNumber()).getName();
        }
        if (name != null) {
          result.add(name);
        }
      }
      return result;
    } else if (item.getIndex() != null) {
      final String indexName = item.getIndex().getIndexName();
      final Index idx = db.getSchema().getIndexByName(indexName);
      if (idx == null)
        throw new CommandExecutionException("Index '" + indexName + "' does not exist");

      if (idx instanceof TypeIndex) {
        for (final Index subIdx : ((TypeIndex) idx).getSubIndexes())
          result.add(db.getSchema().getBucketById(subIdx.getAssociatedBucketId()).getName());
      } else
        result.add(db.getSchema().getBucketById(idx.getAssociatedBucketId()).getName());

      if (result.isEmpty()) {
        return null;
      }
      return result;
    } else if (item.getInputParam() != null) {
      return null;
    } else if (item.getIdentifier() != null) {

      if (item.getIdentifier().getStringValue().startsWith("$")) {
        // RESOLVE VARIABLE
        final Object value = context.getVariable(item.getIdentifier().getStringValue());
        if (value != null) {
          if (value instanceof RID)
            item.getRids().add(new Rid((RID) value));
        }
      } else {
        final String className = item.getIdentifier().getStringValue();
        final DocumentType typez = db.getSchema().getType(className);
        final int[] bucketIds = typez.getBuckets(true).stream().mapToInt(com.arcadedb.engine.Bucket::getFileId).toArray();
        for (final int bucketId : bucketIds) {
          final String bucketName = db.getSchema().getBucketById(bucketId).getName();
          if (bucketName != null) {
            result.add(bucketName);
          }
        }
      }
      return result;
    }

    return null;
  }
}
