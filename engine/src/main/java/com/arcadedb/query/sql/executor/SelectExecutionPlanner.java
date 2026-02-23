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
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.parser.AggregateProjectionSplit;
import com.arcadedb.query.sql.parser.AndBlock;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.BinaryCompareOperator;
import com.arcadedb.query.sql.parser.BinaryCondition;
import com.arcadedb.query.sql.parser.BetweenCondition;
import com.arcadedb.query.sql.parser.BooleanExpression;
import com.arcadedb.query.sql.parser.Bucket;
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
import com.arcadedb.query.sql.parser.IndexIdentifier;
import com.arcadedb.query.sql.parser.InputParameter;
import com.arcadedb.query.sql.parser.IsNullCondition;
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
import com.arcadedb.query.sql.parser.SuffixIdentifier;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.SubQueryCollector;
import com.arcadedb.query.sql.parser.Timeout;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.MultiColumnAggregationRequest;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.function.sql.time.SQLFunctionTimeBucket;
import com.arcadedb.query.sql.parser.BaseIdentifier;
import com.arcadedb.query.sql.parser.LevelZeroIdentifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Schema.INDEX_TYPE.FULL_TEXT;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class SelectExecutionPlanner {

  private static final String            LOCAL_NODE_NAME = "local";
  private final        SelectStatement   statement;
  private              QueryPlanningInfo info;

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

  public InternalExecutionPlan createExecutionPlan(final CommandContext context, final boolean useCache) {
    final DatabaseInternal db = context.getDatabase();
    if (useCache && !context.isProfiling() && statement.executionPlanCanBeCached()) {
      final ExecutionPlan plan = db.getExecutionPlanCache().get(statement.getOriginalStatement(), context);
      if (plan != null)
        return (InternalExecutionPlan) plan;
    }

    final long planningStart = System.currentTimeMillis();

    init(context);

    // Try to get the limit value for the execution plan
    // If it depends on runtime context (e.g., LET variables), use 0 for now
    int limitValue = 0;
    try {
      if (statement.getLimit() != null)
        limitValue = statement.getLimit().getValue(context);
    } catch (final Exception e) {
      // Limit value depends on runtime context, use 0 for now
      limitValue = 0;
    }

    final SelectExecutionPlan selectExecutionPlan = new SelectExecutionPlan(context, limitValue);

    if (info.expand && info.distinct)
      throw new CommandExecutionException("Cannot execute a statement with DISTINCT expand(), please use a subquery");

    optimizeQuery(info, context);

    if (handleHardwiredOptimizations(selectExecutionPlan, context))
      return selectExecutionPlan;

    handleGlobalLet(selectExecutionPlan, info, context);

    info.buckets = calculateTargetBuckets(info, context);

    // Try to get the limit value for the fetch execution plan
    // If it depends on runtime context (e.g., LET variables), use 0 for now
    int fetchLimitValue = 0;
    try {
      if (statement.getLimit() != null)
        fetchLimitValue = statement.getLimit().getValue(context);
    } catch (final Exception e) {
      // Limit value depends on runtime context, use 0 for now
      fetchLimitValue = 0;
    }

    info.fetchExecutionPlan = new SelectExecutionPlan(context, fetchLimitValue);

    handleFetchFromTarget(selectExecutionPlan, info, context);

    if (info.globalLetPresent)
      // do the raw fetch remotely, then do the rest on the coordinator
      buildExecutionPlan(selectExecutionPlan, info);

    handleLet(selectExecutionPlan, info, context);

    handleWhere(selectExecutionPlan, info, context);

    buildExecutionPlan(selectExecutionPlan, info);

    handleProjectionsBlock(selectExecutionPlan, info, context);

    if (info.timeout != null)
      selectExecutionPlan.chain(new AccumulatingTimeoutStep(info.timeout, context));

    if (useCache && !context.isProfiling() && statement.executionPlanCanBeCached() && selectExecutionPlan.canBeCached()
        && db.getExecutionPlanCache().getLastInvalidation() < planningStart)
      db.getExecutionPlanCache().put(statement.getOriginalStatement(), selectExecutionPlan);

    return selectExecutionPlan;
  }

  public static void handleProjectionsBlock(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context) {
    handleProjectionsBeforeOrderBy(result, info, context);

    if (info.expand || info.unwind != null || info.groupBy != null) {
      handleProjections(result, info, context);
      handleExpand(result, info, context);
      handleUnwind(result, info, context);
      // Apply final projection after UNWIND to remove temporarily added fields
      if (info.projectionAfterUnwind != null)
        result.chain(new ProjectionCalculationStep(info.projectionAfterUnwind, context));

      handleOrderBy(result, info, context);
      if (info.skip != null)
        result.chain(new SkipExecutionStep(info.skip, context));

      if (info.limit != null)
        result.chain(new LimitExecutionStep(info.limit, context));

    } else {
      handleOrderBy(result, info, context);
      if (info.distinct || info.groupBy != null || info.aggregateProjection != null) {
        handleProjections(result, info, context);
        handleDistinct(result, info, context);
        if (info.skip != null)
          result.chain(new SkipExecutionStep(info.skip, context));

        if (info.limit != null)
          result.chain(new LimitExecutionStep(info.limit, context));
      } else {
        if (info.skip != null)
          result.chain(new SkipExecutionStep(info.skip, context));

        if (info.limit != null)
          result.chain(new LimitExecutionStep(info.limit, context));

        handleProjections(result, info, context);
      }
    }
  }

  private void buildExecutionPlan(final SelectExecutionPlan result, final QueryPlanningInfo info) {
    if (info.fetchExecutionPlan == null)
      return;

    for (final ExecutionStep step : info.fetchExecutionPlan.getSteps()) {
      result.chain((ExecutionStepInternal) step);
    }
    info.fetchExecutionPlan = null;
    info.planCreated = true;
  }

  /**
   * for backward compatibility, translate "distinct(foo)" to "DISTINCT foo". This method modifies the projection itself.
   *
   * @param projection the projection
   */
  protected static Projection translateDistinct(Projection projection) {
    if (projection != null && projection.getItems().size() == 1) {
      if (isDistinct(projection.getItems().getFirst())) {
        projection = projection.copy();
        final ProjectionItem item = projection.getItems().getFirst();
        final FunctionCall function = ((BaseExpression) item.getExpression().getMathExpression()).getIdentifier().getLevelZero()
            .getFunctionCall();
        final Expression exp = function.getParams().getFirst();
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
    if (!(item.getExpression().getMathExpression() instanceof final BaseExpression base)) {
      return false;
    }
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

  private boolean handleHardwiredOptimizations(final SelectExecutionPlan result, final CommandContext context) {
    return handleHardwiredCountOnIndex(result, info, context) || handleHardwiredCountOnType(result, info, context)
        || handleHardwiredMaxMinOnIndex(result, info, context);
  }

  private boolean handleHardwiredCountOnType(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context) {
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

    result.chain(new CountFromTypeStep(info.target.toString(), info.projection.getAllAliases().getFirst(), context));
    return true;
  }

  private boolean handleHardwiredCountOnIndex(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context) {
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
    result.chain(new CountFromIndexStep(targetIndex, info.projection.getAllAliases().getFirst(), context));
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
    final ProjectionItem item = info.aggregateProjection.getItems().getFirst();
    return item.getExpression().toString().equalsIgnoreCase("count(*)");
  }

  private static boolean isCountOnly(final QueryPlanningInfo info) {
    if (info.aggregateProjection == null || info.projection == null || info.aggregateProjection.getItems().size() != 1 ||
        info.projection.getItems().stream().filter(x -> !x.getProjectionAliasAsString().startsWith("_$$$ORDER_BY_ALIAS$$$_"))
            .count() != 1) {
      return false;
    }
    final ProjectionItem item = info.aggregateProjection.getItems().getFirst();
    final Expression exp = item.getExpression();
    if (exp.getMathExpression() != null && exp.getMathExpression() instanceof final BaseExpression base) {
      return base.isCount() && base.getModifier() == null;
    }
    return false;
  }

  private boolean isCount(final Projection aggregateProjection, final Projection projection) {
    if (aggregateProjection == null || projection == null || aggregateProjection.getItems().size() != 1
        || projection.getItems().size() != 1) {
      return false;
    }
    final ProjectionItem item = aggregateProjection.getItems().getFirst();
    return item.getExpression().isCount();
  }

  /**
   * Handles optimization for MAX(indexed_field) or MIN(indexed_field) queries.
   * Instead of scanning all records, uses the index to directly get the max/min value.
   * <p>
   * For MAX: iterates index in descending order and returns first non-null key.
   * For MIN: iterates index in ascending order and returns first non-null key.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/3304">Issue #3304</a>
   */
  private boolean handleHardwiredMaxMinOnIndex(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context) {
    // Must have a type target
    Identifier targetClass = info.target == null ? null : info.target.getItem().getIdentifier();
    if (targetClass == null && info.target != null) {
      // Check if the target is specified via a positional/named parameter (e.g., SELECT max(p) FROM ?)
      final InputParameter inputParam = info.target.getItem().getInputParam();
      if (inputParam != null) {
        final Object paramValue = inputParam.getValue(context.getInputParameters());
        if (paramValue instanceof String string)
          targetClass = new Identifier(string);
      }
    }
    if (targetClass == null)
      return false;

    if (info.distinct || info.expand)
      return false;

    // Must be a minimal query (no WHERE, GROUP BY, etc.)
    if (!isMinimalQuery(info))
      return false;

    // Check for MAX or MIN aggregate function on a single property
    final MaxMinInfo maxMinInfo = getMaxMinInfo(info);
    if (maxMinInfo == null)
      return false;

    // Get the document type
    final String typeName = targetClass.getStringValue();
    final DocumentType documentType = context.getDatabase().getSchema().getType(typeName);
    if (documentType == null)
      return false;

    // Find an index on the property
    final RangeIndex index = findIndexForProperty(documentType, maxMinInfo.propertyName);
    if (index == null)
      return false;

    // Create the optimized execution step
    result.chain(new MaxMinFromIndexStep(index, info.projection.getAllAliases().getFirst(), maxMinInfo.isMax, context));
    return true;
  }

  /**
   * Information about a MAX/MIN aggregate function.
   *
   * @param isMax true for MAX, false for MIN
   */
    private record MaxMinInfo(String propertyName, boolean isMax) {
  }

  /**
   * Extracts MAX/MIN function info from the query if it's a simple MAX(field) or MIN(field).
   */
  private MaxMinInfo getMaxMinInfo(final QueryPlanningInfo info) {
    if (info.aggregateProjection == null || info.projection == null || info.aggregateProjection.getItems().size() != 1
        || info.projection.getItems().size() != 1)
      return null;

    // preAggregateProjection contains the field expression (e.g., "value AS _$$$OALIAS$$$_1")
    if (info.preAggregateProjection == null || info.preAggregateProjection.getItems().size() != 1)
      return null;

    final ProjectionItem aggregateItem = info.aggregateProjection.getItems().getFirst();
    final Expression exp = aggregateItem.getExpression();

    if (exp.getMathExpression() == null || !(exp.getMathExpression() instanceof BaseExpression base))
      return null;

    if (base.getModifier() != null)
      return null;

    if (base.getIdentifier() == null || base.getIdentifier().getLevelZero() == null)
      return null;

    final FunctionCall functionCall = base.getIdentifier().getLevelZero().getFunctionCall();
    if (functionCall == null)
      return null;

    final String functionName = functionCall.getName().getStringValue().toLowerCase();
    final boolean isMax;
    if (functionName.equals("max"))
      isMax = true;
    else if (functionName.equals("min"))
      isMax = false;
    else
      return null;

    // Get the property name from the pre-aggregate projection
    final ProjectionItem preAggItem = info.preAggregateProjection.getItems().getFirst();
    final Expression preAggExp = preAggItem.getExpression();

    if (preAggExp.getMathExpression() == null || !(preAggExp.getMathExpression() instanceof BaseExpression preAggBase))
      return null;

    if (preAggBase.getIdentifier() == null)
      return null;

    // For simple properties like "value", the identifier is in suffix, not levelZero
    final SuffixIdentifier suffix = preAggBase.getIdentifier().suffix;
    if (suffix == null || suffix.identifier == null)
      return null;

    return new MaxMinInfo(suffix.identifier.getStringValue(), isMax);
  }

  /**
   * Finds a RangeIndex (LSM_TREE) on the specified property.
   */
  private RangeIndex findIndexForProperty(final DocumentType type, final String propertyName) {
    final Collection<TypeIndex> indexes = type.getAllIndexes(true);
    for (final TypeIndex index : indexes) {
      // Must be a single-property index on the exact property
      final List<String> propNames = index.getPropertyNames();
      if (propNames.size() == 1 && propNames.getFirst().equals(propertyName)) {
        // Must support ordered iterations (RangeIndex like LSM_TREE)
        if (index.supportsOrderedIterations())
          return index;
      }
    }
    return null;
  }

  public static void handleUnwind(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context) {
    if (info.unwind != null) {
      result.chain(new UnwindStep(info.unwind, context));
    }
  }

  private static void handleDistinct(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context) {
    if (info.distinct)
      result.chain(new DistinctExecutionStep(context));
  }

  private static void handleProjectionsBeforeOrderBy(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context) {
    if (info.orderBy != null) {
      handleProjections(result, info, context);
    }
  }

  private static void handleProjections(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final CommandContext context) {
    if (!info.projectionsCalculated && info.projection != null) {
      if (info.preAggregateProjection != null) {
        result.chain(new ProjectionCalculationStep(info.preAggregateProjection, context));
      }
      if (info.aggregateProjection != null) {
        long aggregationLimit = -1;
        if (info.orderBy == null && info.limit != null) {
          try {
            aggregationLimit = info.limit.getValue(context);
            if (info.skip != null && info.skip.getValue(context) > 0) {
              aggregationLimit += info.skip.getValue(context);
            }
          } catch (final Exception e) {
            // Limit/skip value depends on runtime context (e.g., LET variables), use -1 for now
            // The actual LIMIT/SKIP steps will handle it at execution time
            aggregationLimit = -1;
          }
        }

        result.chain(new AggregateProjectionCalculationStep(info.aggregateProjection, info.groupBy, aggregationLimit, context,
            info.timeout != null ? info.timeout.getVal().longValue() : -1));
        if (isCountOnly(info) && info.groupBy == null) {
          result.chain(new GuaranteeEmptyCountStep(info.aggregateProjection.getItems().getFirst(), context));
        }
      }
      result.chain(new ProjectionCalculationStep(info.projection, context));

      info.projectionsCalculated = true;
    }
  }

  protected static void optimizeQuery(final QueryPlanningInfo info, final CommandContext context) {
    splitLet(info, context);
    rewriteIndexChainsAsSubqueries(info, context);
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
    addUnwindProjections(info);
  }

  private static void rewriteIndexChainsAsSubqueries(QueryPlanningInfo info, CommandContext context) {
    if (context == null ||
        context.getDatabase() == null) {
      return;
    }

    if (info.whereClause != null &&
        info.target != null &&
        info.target.getItem().getIdentifier() != null) {

      String typeName = info.target.getItem().getIdentifier().getStringValue();
      if (typeName.startsWith("$")) {
        typeName = (String) context.getVariable(typeName);
        info.target.getItem().setIdentifier(new Identifier(typeName));
      }

      Schema schema = context.getDatabase().getSchema();
      DocumentType type = schema.getType(typeName);

      info.whereClause.getBaseExpression().rewriteIndexChainsAsSubqueries(context, type);
    }
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
      AndBlock newBlock = block.copy();
      for (final BooleanExpression exp : newBlock.getSubBlocks()) {
        if (exp instanceof BinaryCondition condition) {
          if (condition.getOperator() instanceof EqualsCompareOperator) {
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
        && info.projection.getItems().getFirst().isAll())) {
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
   * Adds UNWIND fields to projections if they are not already present.
   * This ensures the UNWIND step can access the field values even when projections would otherwise exclude them.
   */
  private static void addUnwindProjections(final QueryPlanningInfo info) {
    if (info.unwind == null || info.unwind.getItems() == null || info.unwind.getItems().isEmpty()
        || info.projection == null || info.projection.getItems() == null || (info.projection.getItems().size() == 1
        && info.projection.getItems().getFirst().isAll())) {
      return;
    }

    final List<String> allAliases = info.projection.getAllAliases();
    final List<ProjectionItem> additionalUnwindProjections = new ArrayList<>();

    for (final Identifier unwindItem : info.unwind.getItems()) {
      final String unwindFieldName = unwindItem.getStringValue();
      if (!allAliases.contains(unwindFieldName)) {
        final ProjectionItem newProj = new ProjectionItem(-1);
        newProj.setExpression(new Expression(unwindItem));
        // Keep the original field name so UNWIND can find it
        newProj.setAlias(unwindItem);
        additionalUnwindProjections.add(newProj);
      }
    }

    if (!additionalUnwindProjections.isEmpty()) {
      // Save the original projection to restore after UNWIND
      info.projectionAfterUnwind = new Projection(-1);
      info.projectionAfterUnwind.setItems(new ArrayList<>());
      for (final String alias : allAliases) {
        info.projectionAfterUnwind.getItems().add(projectionFromAlias(new Identifier(alias)));
      }

      // Add the UNWIND fields to the current projection
      for (final ProjectionItem item : additionalUnwindProjections) {
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
    } else
      handleGroupByNoSplit(info, context);
  }

  private static void handleGroupByNoSplit(final QueryPlanningInfo info, final CommandContext context) {
    if (info.groupBy == null || info.groupBy.getItems() == null || info.groupBy.getItems().size() == 0)
      return;

    for (Expression exp : info.groupBy.getItems()) {
      if (exp.isAggregate(context))
        throw new CommandExecutionException("Cannot group by an aggregate function");

      final ProjectionItem newItem = new ProjectionItem(-1);
      newItem.setExpression(exp);
      if (info.aggregateProjection == null)
        info.aggregateProjection = new Projection(-1);

      if (info.aggregateProjection.getItems() == null)
        info.aggregateProjection.setItems(new ArrayList<>());

      info.aggregateProjection.getItems().add(newItem);
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
      if (query.refersToParent())
        addRecordLevelLet(info, alias, query, -1);
      else
        addGlobalLet(info, alias, query, i++);

    }
    collector.reset();

    if (info.whereClause != null)
      info.whereClause.extractSubQueries(collector);

    if (info.projection != null)
      info.projection.extractSubQueries(collector);

    if (info.orderBy != null)
      info.orderBy.extractSubQueries(collector);

    if (info.groupBy != null)
      info.groupBy.extractSubQueries(collector);

    for (final Map.Entry<Identifier, Statement> entry : collector.getSubQueries().entrySet()) {
      final Identifier alias = entry.getKey();
      final Statement query = entry.getValue();
      if (query.refersToParent())
        addRecordLevelLet(info, alias, query, -1);
      else
        addGlobalLet(info, alias, query, -1);
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

  private void handleFetchFromTarget(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context) {

    final FromItem target = info.target == null ? null : info.target.getItem();

    if (target == null) {
      handleNoTarget(info.fetchExecutionPlan, context);
    } else if (target.getIdentifier() != null && target.getModifier() != null) {

      final List<RID> rids = new ArrayList<>();
      final String targetStr = target.toString();
      final Object variableValue = context.getVariablePath(targetStr);
      if (variableValue != null) {
        // Handle single Result object (e.g., from $parent.$current)
        if (variableValue instanceof Result resultVal) {
          if (resultVal.isElement()) {
            rids.add(resultVal.toElement().getIdentity());
          } else if (resultVal.getIdentity().isPresent()) {
            rids.add(resultVal.getIdentity().get());
          }
        }
        // Handle single Identifiable object
        else if (variableValue instanceof Identifiable identifiable) {
          rids.add(identifiable.getIdentity());
        }
        // Handle single RID object
        else if (variableValue instanceof RID rid) {
          rids.add(rid);
        }
        // Handle collection of records
        else if (variableValue instanceof Collection<?> records) {
          for (Object o : records) {
            if (o instanceof Identifiable identifiable)
              rids.add(identifiable.getIdentity());
            else if (o instanceof Result result1 && result1.isElement())
              rids.add(result1.toElement().getIdentity());
            else if (o instanceof RID rid)
              rids.add(rid);
          }
        }
      }
      if (!rids.isEmpty()) {
        info.fetchExecutionPlan.chain(new FetchFromRidsStep(rids, context));
      } else
        result.chain(new EmptyStep(context));//nothing to return
    } else if (target.getIdentifier() != null) {
      final String identifierValue = target.getIdentifier().getStringValue();

      // Check if identifier is a variable reference
      if (identifierValue.startsWith("$")) {
        final Object variableValue = context.getVariable(identifierValue);
        if (variableValue != null) {
          // Handle variable containing a RID string (e.g., '#1:143')
          if (variableValue instanceof String strValue && strValue.startsWith("#")) {
            final RID rid = new RID(context.getDatabase(), strValue);
            info.fetchExecutionPlan.chain(new FetchFromRidsStep(List.of(rid), context));
            return;
          }
          // Handle variable containing a RID object directly
          else if (variableValue instanceof RID rid) {
            info.fetchExecutionPlan.chain(new FetchFromRidsStep(List.of(rid), context));
            return;
          }
          // Handle variable containing an Identifiable
          else if (variableValue instanceof Identifiable identifiable) {
            info.fetchExecutionPlan.chain(new FetchFromRidsStep(List.of(identifiable.getIdentity()), context));
            return;
          }
          // Handle variable containing a collection of RIDs or Identifiables
          else if (variableValue instanceof Iterable<?> iterable) {
            final List<RID> rids = new ArrayList<>();
            for (final Object item : iterable) {
              if (item instanceof RID rid)
                rids.add(rid);
              else if (item instanceof Identifiable identifiable)
                rids.add(identifiable.getIdentity());
              else if (item instanceof Result resultItem && resultItem.getIdentity().isPresent())
                rids.add(resultItem.getIdentity().get());
              else if (item instanceof String strItem && strItem.startsWith("#"))
                rids.add(new RID(context.getDatabase(), strItem));
            }
            if (!rids.isEmpty()) {
              info.fetchExecutionPlan.chain(new FetchFromRidsStep(rids, context));
              return;
            }
          }
          // Handle variable containing a type name string (no '#' prefix)
          else if (variableValue instanceof String typeName) {
            target.setIdentifier(new Identifier(typeName));
          }
        }
      }

      Set<String> filterBuckets = info.buckets;

      final AndBlock ridRangeConditions = extractRidRanges(info.flattenedWhereClause, context);
      if (ridRangeConditions != null && !ridRangeConditions.isEmpty()) {
        info.ridRangeConditions = ridRangeConditions;
        filterBuckets = filterBuckets.stream()
            .filter(x -> clusterMatchesRidRange(x, ridRangeConditions, context.getDatabase(), context)).collect(Collectors.toSet());
      }
      handleTypeAsTarget(info.fetchExecutionPlan, filterBuckets, info, context);
    } else if (target.getBucket() != null) {
      handleBucketsAsTarget(info.fetchExecutionPlan, info, List.of(target.getBucket()), context);
    } else if (target.getBucketList() != null) {
      final List<Bucket> allBuckets = target.getBucketList().toListOfClusters();
      final List<Bucket> buckets = new ArrayList<>();
      for (final Bucket bucket : allBuckets) {
        String name = bucket.getBucketName();
        if (name == null)
          name = context.getDatabase().getSchema().getBucketById(bucket.getBucketNumber()).getName();

        if (name != null && info.buckets.contains(name))
          buckets.add(bucket);
      }
      handleBucketsAsTarget(info.fetchExecutionPlan, info, buckets, context);
    } else if (target.getStatement() != null) {
      handleSubqueryAsTarget(info.fetchExecutionPlan, target.getStatement(), context);
    } else if (target.getFunctionCall() != null) {
      //        handleFunctionCallAsTarget(result, target.getFunctionCall(), context);//TODO
      throw new CommandExecutionException("function call as target is not supported yet");
    } else if (target.getInputParam() != null) {
      handleInputParamAsTarget(info.fetchExecutionPlan, info.buckets, info, target.getInputParam(), context);
    } else if (target.getInputParams() != null && !target.getInputParams().isEmpty()) {
      final List<InternalExecutionPlan> plans = new ArrayList<>();
      for (final InputParameter param : target.getInputParams()) {
        final SelectExecutionPlan subPlan = new SelectExecutionPlan(context,
            statement.getLimit() != null ? statement.getLimit().getValue(context) : 0);
        handleInputParamAsTarget(subPlan, info.buckets, info, param, context);
        plans.add(subPlan);
      }
      info.fetchExecutionPlan.chain(new ParallelExecStep(plans, context));
    } else if (target.getIndex() != null) {
      handleIndexAsTarget(info.fetchExecutionPlan, info, target.getIndex(), null, context);
    } else if (target.getSchema() != null) {
      handleSchemaAsTarget(info.fetchExecutionPlan, target.getSchema(), context);
    } else if (target.getRids() != null && !target.getRids().isEmpty()) {
      final Set<String> filterClusters = info.buckets;
      final List<Rid> rids = new ArrayList<>();
      for (final Rid rid : target.getRids()) {
        if (filterClusters == null || isFromClusters(rid, filterClusters, context.getDatabase()))
          rids.add(rid);
      }

      if (!rids.isEmpty())
        handleRidsAsTarget(info.fetchExecutionPlan, rids, context);
      else
        result.chain(new EmptyStep(context));//nothing to return

    } else if (target.getResultSet() != null) {
      result.chain(new FetchFromResultsetStep(target.getResultSet(), context));
    } else if (target.jjtGetNumChildren() == 1) {
      // FIX TO HANDLE FROM VARIABLES AS TARGET
      final Node child = target.jjtGetChild(0);

      if (child instanceof Identifier identifier && identifier.getStringValue().startsWith("$")) {
        final Object variable = context.getVariable(identifier.getStringValue());

        if (variable instanceof Iterable)
          info.fetchExecutionPlan.chain(new FetchFromRidsStep((Iterable<RID>) variable, context));
        else if (variable instanceof ResultSet resultSet) {
          info.fetchExecutionPlan.chain(new FetchFromRidsStep(() -> new Iterator<>() {
            @Override
            public boolean hasNext() {
              return resultSet.hasNext();
            }

            @Override
            public RID next() {
              return resultSet.nextIfAvailable().getIdentity().get();
            }
          }, context));
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
      if (ridRangeCondition instanceof BinaryCondition condition) {
        final BinaryCompareOperator operator = condition.getOperator();

        final Object obj;
        if (condition.getRight().getRid() != null)
          obj = condition.getRight().getRid().toRecordId((Result) null, context);
        else
          obj = condition.getRight().execute((Result) null, context);

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
    for (final BooleanExpression booleanExpression : flattenedWhereClause.getFirst().getSubBlocks()) {
      if (isRidRange(booleanExpression, context)) {
        result.getSubBlocks().add(booleanExpression.copy());
      }
    }
    return result;
  }

  private boolean isRidRange(final BooleanExpression booleanExpression, final CommandContext context) {
    if (booleanExpression instanceof BinaryCondition condition) {
      final BinaryCondition cond = condition;
      final BinaryCompareOperator operator = cond.getOperator();
      if (isRangeOperator(operator) && cond.getLeft().toString().equalsIgnoreCase(RID_PROPERTY)) {
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
      final QueryPlanningInfo info, final InputParameter inputParam, final CommandContext context) {
    Object paramValue = inputParam.getValue(context.getInputParameters());

    if (paramValue instanceof String string && RID.is(paramValue))
      paramValue = new RID(context.getDatabase(), string);

    if (paramValue == null) {
      result.chain(new EmptyStep(context));//nothing to return
    } else if (paramValue instanceof LocalDocumentType type) {
      final FromClause from = new FromClause(-1);
      final FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier(type.getName()));
      handleTypeAsTarget(result, filterClusters, from, info, context);
    } else if (paramValue instanceof String string) {
      //strings are treated as classes
      final FromClause from = new FromClause(-1);
      final FromItem item = new FromItem(-1);
      from.setItem(item);
      item.setIdentifier(new Identifier(string));
      handleTypeAsTarget(result, filterClusters, from, info, context);
    } else if (paramValue instanceof Identifiable identifiable) {
      final RID orid = identifiable.getIdentity();

      final Rid rid = new Rid(-1);
      final PInteger bucket = new PInteger(-1);
      bucket.setValue(orid.getBucketId());
      final PInteger position = new PInteger(-1);
      position.setValue(orid.getPosition());
      rid.setLegacy(true);
      rid.setBucket(bucket);
      rid.setPosition(position);

      if (filterClusters == null || isFromClusters(rid, filterClusters, context.getDatabase())) {
        handleRidsAsTarget(result, List.of(rid), context);
      } else {
        result.chain(new EmptyStep(context));//nothing to return
      }

    } else if (paramValue instanceof Iterable iterable) {
      //try list of RIDs
      final List<Rid> rids = new ArrayList<>();
      for (final Object x : iterable) {
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
        handleRidsAsTarget(result, rids, context);
      } else {
        result.chain(new EmptyStep(context));//nothing to return
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

  private void handleNoTarget(final SelectExecutionPlan result, final CommandContext context) {
    result.chain(new EmptyDataGeneratorStep(1, context));
  }

  private void handleIndexAsTarget(final SelectExecutionPlan result, final QueryPlanningInfo info,
      final IndexIdentifier indexIdentifier, final Set<String> filterClusters, final CommandContext context) {
    final String indexName = indexIdentifier.getIndexName();
    final RangeIndex index = (RangeIndex) context.getDatabase().getSchema().getIndexByName(indexName);
    if (index == null) {
      throw new CommandExecutionException("Index not found: " + indexName);
    }

    List<Integer> filterClusterIds = null;
    if (filterClusters != null)
      filterClusterIds = filterClusters.stream().map(name -> context.getDatabase().getSchema().getBucketByName(name).getFileId())
          .mapToInt(i -> i).boxed().toList();

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
        final AndBlock andBlock = info.flattenedWhereClause.getFirst();
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
      result.chain(new FetchFromIndexStep(index, keyCondition, null, context));
      if (ridCondition != null) {
        final WhereClause where = new WhereClause(-1);
        where.setBaseExpression(ridCondition);
        result.chain(new FilterStep(where, context));
      }
      break;
    case VALUES:
    case VALUESASC:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, true, context));
      result.chain(new GetValueFromIndexEntryStep(context, filterClusterIds));
      break;
    case VALUESDESC:
      if (!index.supportsOrderedIterations()) {
        throw new CommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, false, context));
      result.chain(new GetValueFromIndexEntryStep(context, filterClusterIds));
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

  private void handleSchemaAsTarget(final SelectExecutionPlan plan, final SchemaIdentifier metadata, final CommandContext context) {
    switch (metadata.getName().toLowerCase()) {
    case "types" -> plan.chain(new FetchFromSchemaTypesStep(context));
    case "indexes" -> plan.chain(new FetchFromSchemaIndexesStep(context));
    case "database" -> plan.chain(new FetchFromSchemaDatabaseStep(context));
    case "buckets" -> plan.chain(new FetchFromSchemaBucketsStep(context));
    case "materializedviews" -> plan.chain(new FetchFromSchemaMaterializedViewsStep(context));
    case "continuousaggregates" -> plan.chain(new FetchFromSchemaContinuousAggregatesStep(context));
    case "stats" -> plan.chain(new FetchFromSchemaStatsStep(context));
    case "dictionary" -> plan.chain(new FetchFromSchemaDictionaryStep(context));
    default -> {
      final String name = metadata.getName().toLowerCase();
      if (name.startsWith("bucket:"))
        plan.chain(new FetchFromSchemaBucketDetailStep(metadata.getName().substring("bucket:".length()), context));
      else if (name.startsWith("index:"))
        plan.chain(new FetchFromSchemaIndexDetailStep(metadata.getName().substring("index:".length()), context));
      else
        throw new UnsupportedOperationException("Invalid metadata: " + metadata.getName());
    }
    }
  }

  private void handleRidsAsTarget(final SelectExecutionPlan plan, final List<Rid> rids, final CommandContext context) {
    final List<RID> actualRids = new ArrayList<>();
    for (final Rid rid : rids)
      actualRids.add(rid.toRecordId((Result) null, context));

    plan.chain(new FetchFromRidsStep(actualRids, context));
  }

  private static void handleExpand(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context) {
    if (info.expand) {
      result.chain(new ExpandStep(context));
    }
  }

  private void handleGlobalLet(final SelectExecutionPlan result, final QueryPlanningInfo info, final CommandContext context) {
    if (info.globalLetClause != null) {
      List<LetItem> items = info.globalLetClause.getItems();
      items = sortLet(items, this.statement.getLetClause());
      List<String> scriptVars = new ArrayList<>();
      for (LetItem item : items) {
        context.declareScriptVariable(item.getVarName().getStringValue());
        if (item.getExpression() != null) {
          result.chain(new GlobalLetExpressionStep(item.getVarName(), item.getExpression(), context));
        } else {
          result.chain(new GlobalLetQueryStep(item.getVarName(), item.getQuery(), context, scriptVars));
        }
        scriptVars.add(item.getVarName().getStringValue());
        info.globalLetPresent = true;
      }
    }
  }

  private void handleLet(final SelectExecutionPlan plan, final QueryPlanningInfo info, final CommandContext context) {
    if (info.perRecordLetClause != null) {
      List<LetItem> items = info.perRecordLetClause.getItems();
      items = sortLet(items, this.statement.getLetClause());
      if (!plan.steps.isEmpty()) {
        for (final LetItem item : items) {
          if (item.getExpression() != null)
            plan.chain(new LetExpressionStep(item.getVarName(), item.getExpression(), context));
          else
            plan.chain(new LetQueryStep(item.getVarName(), item.getQuery(), context));
        }
      } else {

        boolean containsSubQuery = false;
        for (final LetItem item : items) {
          if (item.getExpression() != null) {
            info.fetchExecutionPlan.chain(
                new LetExpressionStep(item.getVarName().copy(), item.getExpression().copy(), context));
          } else {
            info.fetchExecutionPlan.chain(
                new LetQueryStep(item.getVarName().copy(), item.getQuery().copy(), context));
            containsSubQuery = true;
          }
        }

        if (containsSubQuery) {
          // RE-EXECUTE THE EXPRESSION IF THERE IS ANY SUB-QUERY. THIS IS A MUST BECAUSE THERE IS NO CONCEPT OF DEPENDENCY BETWEEN LETS
          for (final LetItem item : items) {
            if (item.getExpression() != null)
              info.fetchExecutionPlan.chain(
                  new LetExpressionStep(item.getVarName().copy(), item.getExpression().copy(), context));
          }
        }
      }
    }
  }

  private List<LetItem> sortLet(final List<LetItem> items, final LetClause letClause) {
    if (letClause == null)
      return items;

    final List<LetItem> i = new ArrayList<>(items);
    final List<LetItem> result = new ArrayList<>();
    for (LetItem item : letClause.getItems()) {
      final String var = item.getVarName().getStringValue();
      final Iterator<LetItem> iterator = i.iterator();
      while (iterator.hasNext()) {
        final LetItem x = iterator.next();
        if (x.getVarName().getStringValue().equals(var)) {
          iterator.remove();
          result.add(x);
          break;
        }
      }
    }

    result.addAll(i);

    return result;
  }

  private void handleWhere(final SelectExecutionPlan plan, final QueryPlanningInfo info, final CommandContext context) {
    if (info.whereClause != null) {
      if (info.planCreated)
        plan.chain(new FilterStep(info.whereClause, context));
      else
        info.fetchExecutionPlan.chain(new FilterStep(info.whereClause.copy(), context));
    }
  }

  public static void handleOrderBy(final SelectExecutionPlan plan, final QueryPlanningInfo info, final CommandContext context) {
    int skipSize = 0;
    int limitSize = -1;

    try {
      skipSize = info.skip == null ? 0 : info.skip.getValue(context);
      if (skipSize < 0)
        throw new CommandExecutionException("Cannot execute a query with a negative SKIP");

      limitSize = info.limit == null ? -1 : info.limit.getValue(context);
    } catch (final Exception e) {
      // Limit/skip value depends on runtime context (e.g., LET variables)
      // Use default values for planning, actual values will be used at execution time
      skipSize = 0;
      limitSize = -1;
    }

    Integer maxResults = null;
    if (limitSize >= 0)
      maxResults = skipSize + limitSize;

    if (info.expand || info.unwind != null)
      maxResults = null;

    if (!info.orderApplied && info.orderBy != null && info.orderBy.getItems() != null && !info.orderBy.getItems().isEmpty()) {
      plan.chain(new OrderByStep(info.orderBy, maxResults, context, info.timeout != null ? info.timeout.getVal().longValue() : -1));
      if (info.projectionAfterOrderBy != null) {
        plan.chain(new ProjectionCalculationStep(info.projectionAfterOrderBy, context));
      }
    }
  }

  /**
   * @param plan           the execution plan where to add the fetch step
   * @param filterClusters clusters of interest (all the others have to be excluded from the result)
   * @param info
   * @param context
   */
  private void handleTypeAsTarget(final SelectExecutionPlan plan, final Set<String> filterClusters, final QueryPlanningInfo info,
      final CommandContext context) {
    handleTypeAsTarget(plan, filterClusters, info.target, info, context);
  }

  private void handleTypeAsTarget(final SelectExecutionPlan plan, final Set<String> filterClusters, final FromClause from,
      final QueryPlanningInfo info, final CommandContext context) {
    final Identifier identifier = from.getItem().getIdentifier();

    // Check if this is a TimeSeries type — use the engine for range queries
    final DocumentType docType = context.getDatabase().getSchema().getType(identifier.getStringValue());
    if (docType instanceof LocalTimeSeriesType tsType && tsType.getEngine() != null) {
      // Extract time range from WHERE clause (if available)
      long fromTs = Long.MIN_VALUE;
      long toTs = Long.MAX_VALUE;

      if (info.flattenedWhereClause != null) {
        for (final AndBlock andBlock : info.flattenedWhereClause) {
          for (final BooleanExpression expr : andBlock.getSubBlocks()) {
            final long[] range = extractTimeRange(expr, tsType.getTimestampColumn(), context);
            if (range != null) {
              // Tighten bounds: take the most restrictive range
              if (range[0] != Long.MIN_VALUE)
                fromTs = Math.max(fromTs, range[0]);
              if (range[1] != Long.MAX_VALUE)
                toTs = Math.min(toTs, range[1]);
            }
          }
        }
      }

      // Extract tag filter from WHERE clause
      final TagFilter tagFilter = extractTagFilter(info.flattenedWhereClause, tsType.getTsColumns(),
          tsType.getTimestampColumn(), context);

      // Try push-down aggregation before falling back to full row fetch
      if (tryTimeSeriesAggregationPushDown(plan, tsType, fromTs, toTs, info, context))
        return;

      plan.chain(new FetchFromTimeSeriesStep(tsType, fromTs, toTs, tagFilter, context));
      return;
    }

    if (handleTypeAsTargetWithIndexedFunction(plan, filterClusters, identifier, info, context)) {
      plan.chain(new FilterByTypeStep(identifier, context));
      return;
    }

    if (handleTypeAsTargetWithIndex(plan, identifier, filterClusters, info, context)) {
      plan.chain(new FilterByTypeStep(identifier, context));
      return;
    }

    if (info.orderBy != null && handleClassWithIndexForSortOnly(plan, identifier, filterClusters, info, context)) {
      plan.chain(new FilterByTypeStep(identifier, context));
      return;
    }

    Boolean orderByRidAsc = null; // null: no order. true: asc, false:desc
    if (isOrderByRidAsc(info))
      orderByRidAsc = true;
    else if (isOrderByRidDesc(info))
      orderByRidAsc = false;

    final FetchFromTypeExecutionStep fetcher = new FetchFromTypeExecutionStep(identifier.getStringValue(), filterClusters, info,
        context, orderByRidAsc);
    if (orderByRidAsc != null)
      info.orderApplied = true;

    plan.chain(fetcher);
  }

  /**
   * Extracts a time range from a BETWEEN or comparison expression on the timestamp column.
   * Returns [fromTs, toTs] or null if not a matching expression.
   * Supports: BETWEEN, >, >=, <, <=, = operators.
   */
  private long[] extractTimeRange(final BooleanExpression expr, final String timestampColumn, final CommandContext context) {
    if (expr instanceof BetweenCondition between) {
      final String fieldName = between.getFirst() != null ? between.getFirst().toString().trim() : null;
      if (timestampColumn.equals(fieldName)) {
        final Object fromVal = between.getSecond().execute((Identifiable) null, context);
        final Object toVal = between.getThird().execute((Identifiable) null, context);
        return new long[] { toEpochMs(fromVal), toEpochMs(toVal) };
      }
    } else if (expr instanceof BinaryCondition binary) {
      // Check if one side is the timestamp column and the other is a value
      final String leftStr = binary.left != null ? binary.left.toString().trim() : null;
      final String rightStr = binary.right != null ? binary.right.toString().trim() : null;
      final boolean leftIsTs = timestampColumn.equals(leftStr);
      final boolean rightIsTs = timestampColumn.equals(rightStr);

      if (leftIsTs || rightIsTs) {
        final Expression valueExpr = leftIsTs ? binary.right : binary.left;
        final Object rawVal = valueExpr.execute((Identifiable) null, context);
        final long val = toEpochMs(rawVal);
        if (val == Long.MIN_VALUE)
          return null;

        final BinaryCompareOperator op = binary.operator;
        // When field is on the right side, invert the operator semantics
        if (leftIsTs) {
          if (op instanceof GtOperator)
            return new long[] { val + 1, Long.MAX_VALUE };
          if (op instanceof GeOperator)
            return new long[] { val, Long.MAX_VALUE };
          if (op instanceof LtOperator)
            return new long[] { Long.MIN_VALUE, val - 1 };
          if (op instanceof LeOperator)
            return new long[] { Long.MIN_VALUE, val };
          if (op instanceof EqualsCompareOperator)
            return new long[] { val, val };
        } else {
          // timestamp is on the right: "value > ts" means "ts < value"
          if (op instanceof GtOperator)
            return new long[] { Long.MIN_VALUE, val - 1 };
          if (op instanceof GeOperator)
            return new long[] { Long.MIN_VALUE, val };
          if (op instanceof LtOperator)
            return new long[] { val + 1, Long.MAX_VALUE };
          if (op instanceof LeOperator)
            return new long[] { val, Long.MAX_VALUE };
          if (op instanceof EqualsCompareOperator)
            return new long[] { val, val };
        }
      }
    }
    return null;
  }

  private static long toEpochMs(final Object value) {
    if (value instanceof Long l)
      return l;
    if (value instanceof java.util.Date d)
      return d.getTime();
    if (value instanceof Number n)
      return n.longValue();
    if (value instanceof String s) {
      try {
        return java.time.Instant.parse(s).toEpochMilli();
      } catch (final Exception e) {
        // Try parsing as ISO date without time (assumes UTC)
        try {
          return java.time.LocalDate.parse(s).atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        } catch (final Exception e2) {
          throw new CommandExecutionException("Cannot parse timestamp: '" + s + "'", e);
        }
      }
    }
    return Long.MIN_VALUE;
  }

  /**
   * Extracts a TagFilter from the flattened WHERE clause by matching equality predicates on TAG columns.
   * Only simple equality conditions (column = 'value') on TAG columns are extracted.
   */
  private static TagFilter extractTagFilter(final List<AndBlock> flattenedWhere, final List<ColumnDefinition> columns,
      final String timestampColumn, final CommandContext context) {
    if (flattenedWhere == null)
      return null;

    TagFilter filter = null;
    for (final AndBlock andBlock : flattenedWhere) {
      for (final BooleanExpression expr : andBlock.getSubBlocks()) {
        if (!(expr instanceof BinaryCondition binary))
          continue;
        if (!(binary.operator instanceof EqualsCompareOperator))
          continue;
        final String leftStr = binary.left != null ? binary.left.toString().trim() : null;
        final String rightStr = binary.right != null ? binary.right.toString().trim() : null;
        if (leftStr == null || rightStr == null)
          continue;
        // Skip timestamp predicates — already handled by time range extraction
        if (timestampColumn.equals(leftStr) || timestampColumn.equals(rightStr))
          continue;

        // Determine which side is the column name and which is the value
        for (int i = 0; i < columns.size(); i++) {
          final ColumnDefinition col = columns.get(i);
          if (col.getRole() != ColumnDefinition.ColumnRole.TAG)
            continue;
          final boolean leftIsCol = col.getName().equals(leftStr);
          final boolean rightIsCol = col.getName().equals(rightStr);
          if (!leftIsCol && !rightIsCol)
            continue;
          final Expression valueExpr = leftIsCol ? binary.right : binary.left;
          final Object value = valueExpr.execute((Identifiable) null, context);
          if (value == null)
            continue;
          // Column index for TagFilter is the non-timestamp column index
          int nonTsIdx = -1;
          for (int j = 0; j <= i; j++)
            if (columns.get(j).getRole() != ColumnDefinition.ColumnRole.TIMESTAMP)
              nonTsIdx++;
          filter = filter == null ? TagFilter.eq(nonTsIdx, value.toString()) : filter.and(nonTsIdx, value.toString());
          break;
        }
      }
    }
    return filter;
  }

  /**
   * Returns true if the WHERE clause contains conditions that are NOT consumed by time-series
   * push-down (i.e., not time-range predicates and not tag equality filters).
   */
  private static boolean hasNonPushDownConditions(final List<AndBlock> flattenedWhere,
      final List<ColumnDefinition> columns, final String timestampColumn) {
    for (final AndBlock andBlock : flattenedWhere) {
      for (final BooleanExpression expr : andBlock.getSubBlocks()) {
        if (expr instanceof BetweenCondition between) {
          final String fieldName = between.getFirst() != null ? between.getFirst().toString().trim() : null;
          if (timestampColumn.equals(fieldName))
            continue; // consumed by time-range extraction
          return true; // BETWEEN on a non-timestamp field — not consumed
        }
        if (!(expr instanceof BinaryCondition binary))
          return true; // unknown condition type — not consumed
        final String leftStr = binary.left != null ? binary.left.toString().trim() : null;
        final String rightStr = binary.right != null ? binary.right.toString().trim() : null;
        // Time range predicate on timestamp column
        if (timestampColumn.equals(leftStr) || timestampColumn.equals(rightStr))
          continue;
        // Tag equality predicate
        if (binary.operator instanceof EqualsCompareOperator) {
          boolean isTagPredicate = false;
          for (final ColumnDefinition col : columns)
            if (col.getRole() == ColumnDefinition.ColumnRole.TAG && (col.getName().equals(leftStr) || col.getName().equals(rightStr))) {
              isTagPredicate = true;
              break;
            }
          if (isTagPredicate)
            continue;
        }
        return true; // anything else is not consumed by push-down
      }
    }
    return false;
  }

  /**
   * Attempts to push down aggregation into the TimeSeries engine.
   * Eligible queries have: ts.timeBucket GROUP BY, simple aggregate functions (avg, max, min, sum, count),
   * no DISTINCT, no HAVING, no UNWIND, no LET.
   */
  private boolean tryTimeSeriesAggregationPushDown(final SelectExecutionPlan plan, final LocalTimeSeriesType tsType,
      final long fromTs, final long toTs, final QueryPlanningInfo info, final CommandContext context) {
    // Must have aggregate projection (set by splitProjectionsForGroupBy)
    if (info.aggregateProjection == null)
      return false;

    // No DISTINCT
    if (info.distinct)
      return false;

    // Must have exactly one GROUP BY
    if (info.groupBy == null || info.groupBy.getItems() == null || info.groupBy.getItems().size() != 1)
      return false;

    // No unsupported clauses
    if (info.unwind != null || info.perRecordLetClause != null || info.globalLetPresent)
      return false;

    // The original projection from the statement (before splitting)
    final Projection originalProjection = statement.getProjection();
    if (originalProjection == null || originalProjection.getItems() == null)
      return false;

    // Find the timeBucket item and aggregate items
    String timeBucketAlias = null;
    String intervalStr = null;
    final List<MultiColumnAggregationRequest> requests = new ArrayList<>();
    final Map<String, String> requestAliasToOutputAlias = new HashMap<>();
    final List<ColumnDefinition> columns = tsType.getTsColumns();

    for (final ProjectionItem item : originalProjection.getItems()) {
      final FunctionCall funcCall = extractFunctionCall(item.expression);
      if (funcCall == null)
        return false; // not a simple function call — bail out

      final String funcName = funcCall.getName().getStringValue();

      if ("ts.timeBucket".equalsIgnoreCase(funcName)) {
        // This is the time bucket function
        if (timeBucketAlias != null)
          return false; // duplicate timeBucket
        timeBucketAlias = item.getProjectionAliasAsString();
        // Extract interval from first parameter
        if (funcCall.getParams().size() < 2)
          return false;
        final Object intervalVal = funcCall.getParams().get(0).execute((Identifiable) null, context);
        if (!(intervalVal instanceof String))
          return false;
        intervalStr = (String) intervalVal;
      } else {
        // Must be an aggregate function
        final String aggFuncName = funcName.toLowerCase();
        final AggregationType aggType = switch (aggFuncName) {
          case "avg" -> AggregationType.AVG;
          case "max" -> AggregationType.MAX;
          case "min" -> AggregationType.MIN;
          case "sum" -> AggregationType.SUM;
          case "count" -> AggregationType.COUNT;
          default -> null;
        };
        if (aggType == null)
          return false; // unsupported aggregate

        // For COUNT(*), columnIndex doesn't matter
        int columnIndex = 0;
        if (aggType != AggregationType.COUNT) {
          // Extract field name from first parameter
          if (funcCall.getParams().isEmpty())
            return false;
          final String fieldName = funcCall.getParams().get(0).toString().trim();
          columnIndex = findColumnIndex(columns, fieldName);
          if (columnIndex < 0)
            return false; // field not found in timeseries columns
        }

        final String alias = item.getProjectionAliasAsString();
        requests.add(new MultiColumnAggregationRequest(columnIndex, aggType, alias));
        requestAliasToOutputAlias.put(alias, alias);
      }
    }

    // Must have found both timeBucket and at least one aggregate
    if (timeBucketAlias == null || intervalStr == null || requests.isEmpty())
      return false;

    // Verify GROUP BY references the timeBucket alias
    final String groupByStr = info.groupBy.getItems().get(0).toString().trim();
    if (!groupByStr.equals(timeBucketAlias))
      return false;

    // Parse interval
    final long bucketIntervalMs;
    try {
      bucketIntervalMs = SQLFunctionTimeBucket.parseInterval(intervalStr);
    } catch (final IllegalArgumentException e) {
      return false;
    }

    // Extract tag filter from WHERE clause for push-down
    final TagFilter tagFilter = extractTagFilter(info.flattenedWhereClause, columns, tsType.getTimestampColumn(), context);

    // Verify all WHERE conditions are consumed by push-down (time-range or tag equality).
    // If any field-value predicate remains (e.g., WHERE value > 100), bail out to avoid
    // silently dropping it — the standard filter step will handle it instead.
    if (info.flattenedWhereClause != null && hasNonPushDownConditions(info.flattenedWhereClause, columns, tsType.getTimestampColumn()))
      return false;

    // Chain the push-down step
    plan.chain(new AggregateFromTimeSeriesStep(tsType, fromTs, toTs, requests, bucketIntervalMs,
        timeBucketAlias, requestAliasToOutputAlias, tagFilter, context));

    // Null out the aggregate projections so handleProjections doesn't add duplicate steps
    info.preAggregateProjection = null;
    info.aggregateProjection = null;
    info.groupBy = null;
    info.projectionsCalculated = true;
    // The time range and tag filters are consumed by the push-down step
    info.whereClause = null;
    info.flattenedWhereClause = null;

    return true;
  }

  /**
   * Extracts a FunctionCall from an Expression if it's a simple function call.
   * Returns null if the expression is not a simple function call.
   */
  private static FunctionCall extractFunctionCall(final Expression expr) {
    if (expr == null || expr.mathExpression == null)
      return null;
    if (!(expr.mathExpression instanceof BaseExpression base))
      return null;
    if (base.identifier == null)
      return null;
    if (base.identifier.levelZero != null && base.identifier.levelZero.functionCall != null)
      return base.identifier.levelZero.functionCall;
    return null;
  }

  /**
   * Finds the index of a column by name in the timeseries column definitions.
   * Returns -1 if not found.
   */
  private static int findColumnIndex(final List<ColumnDefinition> columns, final String fieldName) {
    for (int i = 0; i < columns.size(); i++)
      if (columns.get(i).getName().equals(fieldName))
        return i;
    return -1;
  }

  private boolean handleTypeAsTargetWithIndexedFunction(final SelectExecutionPlan plan, final Set<String> filterClusters,
      final Identifier queryTarget, final QueryPlanningInfo info, final CommandContext context) {
    if (queryTarget == null)
      return false;

    final DocumentType typez = context.getDatabase().getSchema().getType(queryTarget.getStringValue());
    if (typez == null)
      throw new CommandExecutionException("Type not found: " + queryTarget);

    if (info.flattenedWhereClause == null || info.flattenedWhereClause.isEmpty())
      return false;

    final List<InternalExecutionPlan> resultSubPlans = new ArrayList<>();

    boolean indexedFunctionsFound = false;

    for (AndBlock block : info.flattenedWhereClause) {
      List<BinaryCondition> indexedFunctionConditions = block.getIndexedFunctionConditions(typez, context);

      indexedFunctionConditions = filterIndexedFunctionsWithoutIndex(indexedFunctionConditions, info.target, context);

      if (indexedFunctionConditions == null || indexedFunctionConditions.isEmpty()) {
        IndexSearchDescriptor bestIndex = findBestIndexFor(context, typez.getAllIndexes(true), block, typez);
        if (bestIndex != null) {

          final FetchFromIndexStep step = new FetchFromIndexStep(bestIndex.index, bestIndex.keyCondition,
              bestIndex.additionalRangeCondition, true, context);

          SelectExecutionPlan subPlan = new SelectExecutionPlan(context,
              statement.getLimit() != null ? statement.getLimit().getValue(context) : 0);
          subPlan.chain(step);
          List<Integer> filterClusterIds = null;
          if (filterClusters != null)
            filterClusterIds = classClustersFiltered(context.getDatabase(), typez, filterClusters);
          else
            filterClusterIds = typez.getBucketIds(true);

          subPlan.chain(new GetValueFromIndexEntryStep(context, filterClusterIds));
          if (bestIndex.requiresDistinctStep()) {
            subPlan.chain(new DistinctExecutionStep(context));
          }
          // Use remaining condition (not covered by index) instead of the whole block, to avoid
          // re-applying index conditions (e.g. CONTAINSTEXT) as plain string filters that use
          // a different evaluation than the full-text index semantics.
          final BooleanExpression remaining = bestIndex.getRemainingCondition();
          if (remaining != null && !remaining.isEmpty()) {
            if ((info.perRecordLetClause != null && refersToLet(Collections.singletonList(remaining)))) {
              handleLet(subPlan, info, context);
            }
            subPlan.chain(new FilterStep(createWhereFrom(remaining), context));
          }
          resultSubPlans.add(subPlan);
        } else {
          FetchFromTypeExecutionStep step = new FetchFromTypeExecutionStep(typez.getName(), filterClusters, context, true);
          SelectExecutionPlan subPlan = new SelectExecutionPlan(context,
              statement.getLimit() != null ? statement.getLimit().getValue(context) : 0);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            if ((info.perRecordLetClause != null && refersToLet(block.getSubBlocks()))) {
              handleLet(subPlan, info, context);
            }
            subPlan.chain(new FilterStep(createWhereFrom(block), context));
          }
          resultSubPlans.add(subPlan);
        }
      } else {
        BinaryCondition blockCandidateFunction = null;
        for (BinaryCondition cond : indexedFunctionConditions) {
          if (!cond.allowsIndexedFunctionExecutionOnTarget(info.target, context)) {
            if (!cond.canExecuteIndexedFunctionWithoutIndex(info.target, context)) {
              throw new CommandExecutionException("Cannot execute " + block + " on " + queryTarget);
            }
          }
          if (blockCandidateFunction == null) {
            blockCandidateFunction = cond;
          } else {
            boolean thisAllowsNoIndex = cond.canExecuteIndexedFunctionWithoutIndex(info.target, context);
            boolean prevAllowsNoIndex = blockCandidateFunction.canExecuteIndexedFunctionWithoutIndex(info.target, context);
            if (!thisAllowsNoIndex && !prevAllowsNoIndex) {
              // none of the functions allow execution without index, so cannot choose one
              throw new CommandExecutionException(
                  "Cannot choose indexed function between " + cond + " and " + blockCandidateFunction
                      + ". Both require indexed execution");
            } else if (thisAllowsNoIndex && prevAllowsNoIndex) {
              // both can be calculated without index, choose the best one for index execution
              final long thisEstimate = cond.estimateIndexed(info.target, context);
              final long lastEstimate = blockCandidateFunction.estimateIndexed(info.target, context);
              if (thisEstimate > -1 && thisEstimate < lastEstimate) {
                blockCandidateFunction = cond;
              }
            } else if (prevAllowsNoIndex) {
              // choose current condition, because the other one can be calculated without index
              blockCandidateFunction = cond;
            }
          }
        }

        FetchFromIndexedFunctionStep step = new FetchFromIndexedFunctionStep(blockCandidateFunction, info.target, context);
        if (!blockCandidateFunction.executeIndexedFunctionAfterIndexSearch(info.target, context)) {
          block = block.copy();
          block.getSubBlocks().remove(blockCandidateFunction);
        }
        if (info.flattenedWhereClause.size() == 1) {
          plan.chain(step);
          plan.chain(new FilterByClustersStep(filterClusters, context));
          if (!block.getSubBlocks().isEmpty()) {
            if ((info.perRecordLetClause != null && refersToLet(block.getSubBlocks()))) {
              handleLet(plan, info, context);
            }
            plan.chain(new FilterStep(createWhereFrom(block), context));
          }
        } else {
          SelectExecutionPlan subPlan = new SelectExecutionPlan(context,
              statement.getLimit() != null ? statement.getLimit().getValue(context) : 0);
          subPlan.chain(step);
          if (!block.getSubBlocks().isEmpty()) {
            subPlan.chain(new FilterStep(createWhereFrom(block), context));
          }
          resultSubPlans.add(subPlan);
        }
        indexedFunctionsFound = true;
      }
    }

    if (resultSubPlans.size() > 1) { // if resultSubPlans.size() == 1 the step was already chained (see above)
      plan.chain(new ParallelExecStep(resultSubPlans, context));
      plan.chain(new FilterByClustersStep(filterClusters, context));
      plan.chain(new DistinctExecutionStep(context));
      indexedFunctionsFound = true;
    }

    if (indexedFunctionsFound) {
      // WHERE condition already applied
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    } else {
      return false;
    }
  }

  private boolean refersToLet(final List<BooleanExpression> subBlocks) {
    if (subBlocks == null)
      return false;

    for (BooleanExpression exp : subBlocks) {
      // Check if expression contains a variable reference (starts with or contains "$")
      // An expression like "#X:Y IN $brain" doesn't start with "$" but contains a variable reference
      if (exp.toString().contains("$"))
        return true;
    }
    return false;
  }

  private List<Integer> classClustersFiltered(final Database db, final DocumentType clazz, final Set<String> filterClusters) {
    final List<Integer> ids = clazz.getBucketIds(true);
    final List<Integer> filtered = new ArrayList<>();
    for (Integer id : ids) {
      if (filterClusters.contains(db.getSchema().getBucketById(id).getName()))
        filtered.add(id);
    }

    return filtered;
  }

  private List<BinaryCondition> filterIndexedFunctionsWithoutIndex(final List<BinaryCondition> indexedFunctionConditions,
      final FromClause fromClause, final CommandContext context) {
    if (indexedFunctionConditions == null)
      return null;

    final List<BinaryCondition> result = new ArrayList<>();
    for (final BinaryCondition cond : indexedFunctionConditions) {
      if (cond.allowsIndexedFunctionExecutionOnTarget(fromClause, context))
        result.add(cond);
      else if (!cond.canExecuteIndexedFunctionWithoutIndex(fromClause, context))
        throw new CommandExecutionException("Cannot evaluate " + cond + ": no index defined");
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
      final Set<String> filterClusters, final QueryPlanningInfo info, final CommandContext context) {

    final DocumentType typez = context.getDatabase().getSchema().getType(queryTarget.getStringValue());
    if (typez == null)
      throw new CommandExecutionException("Type not found: " + queryTarget.getStringValue());

    for (final Index idx : typez.getAllIndexes(true).stream().filter(TypeIndex::supportsOrderedIterations).toList()) {
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
        final boolean isAsc = orderType.equals(OrderByItem.ASC);

        List<Integer> filterClusterIds = null;
        if (filterClusters != null)
          filterClusterIds = filterClusters.stream()
              .map(name -> context.getDatabase().getSchema().getBucketByName(name).getFileId()).mapToInt(i -> i).boxed().toList();

        // Check if the index has NULL_STRATEGY.INDEX - if so, NULLs are already in the index
        if (idx.getNullStrategy() == LSMTreeIndexAbstract.NULL_STRATEGY.INDEX) {
          // NULLs are indexed, just use the index directly
          plan.chain(new FetchFromIndexValuesStep((RangeIndex) idx, isAsc, context));
          plan.chain(new GetValueFromIndexEntryStep(context, filterClusterIds));
        } else {
          // NULLs are NOT in the index (SKIP or ERROR strategy)
          // We need to fetch records with NULL values separately and merge them with index results
          // NULL values should come first for ASC, last for DESC

          // Create sub-plan for fetching from index (sorted values)
          final SelectExecutionPlan indexPlan = new SelectExecutionPlan(context, 0);
          indexPlan.chain(new FetchFromIndexValuesStep((RangeIndex) idx, isAsc, context));
          indexPlan.chain(new GetValueFromIndexEntryStep(context, filterClusterIds));

          // Create sub-plan for fetching records where the indexed property IS NULL
          final SelectExecutionPlan nullPlan = new SelectExecutionPlan(context, 0);
          nullPlan.chain(new FetchFromTypeExecutionStep(queryTarget.getStringValue(), filterClusters, context, true));

          // Create IS NULL filter for the first indexed property
          final String propertyName = indexFields.getFirst();
          final IsNullCondition isNullCondition = new IsNullCondition(-1);
          final Expression expr = new Expression(new Identifier(propertyName));
          isNullCondition.setExpression(expr);
          final WhereClause nullWhereClause = new WhereClause(-1);
          nullWhereClause.setBaseExpression(isNullCondition);
          nullPlan.chain(new FilterStep(nullWhereClause, context));

          // Combine: for ASC, NULL records come first; for DESC, NULL records come last
          final List<InternalExecutionPlan> subPlans = new ArrayList<>();
          if (isAsc) {
            subPlans.add(nullPlan);
            subPlans.add(indexPlan);
          } else {
            subPlans.add(indexPlan);
            subPlans.add(nullPlan);
          }
          plan.chain(new ParallelExecStep(subPlans, context));
        }

        info.orderApplied = true;
        return true;
      }
    }
    return false;
  }

  private boolean handleTypeAsTargetWithIndex(final SelectExecutionPlan plan, final Identifier targetType,
      final Set<String> filterBuckets, final QueryPlanningInfo info, final CommandContext context) {

    final List<ExecutionStepInternal> result = handleTypeAsTargetWithIndex(targetType.getStringValue(), filterBuckets, info,
        context);

    if (result != null) {
      result.forEach(plan::chain);
      info.whereClause = null;
      info.flattenedWhereClause = null;
      return true;
    }

    final DocumentType typez = context.getDatabase().getSchema().getType(targetType.getStringValue());
    if (typez == null)
      throw new CommandExecutionException("Cannot find type '" + targetType + "'");

    if (context.getDatabase().countType(typez.getName(), false) > 0 || typez.getSubTypes().isEmpty() || isDiamondHierarchy(typez))
      return false;

    final Collection<DocumentType> subTypes = typez.getSubTypes();

    final List<InternalExecutionPlan> subTypePlans = new ArrayList<>();
    for (final DocumentType subType : subTypes) {
      final List<ExecutionStepInternal> subSteps = handleTypeAsTargetWithIndexRecursive(subType.getName(), filterBuckets, info,
          context);
      if (subSteps == null || subSteps.isEmpty())
        return false;

      final SelectExecutionPlan subPlan = new SelectExecutionPlan(context,
          statement.getLimit() != null ? statement.getLimit().getValue(context) : 0);
      subSteps.forEach(x -> subPlan.chain(x));
      subTypePlans.add(subPlan);
    }
    if (!subTypePlans.isEmpty()) {
      plan.chain(new ParallelExecStep(subTypePlans, context));
      return true;
    }
    return false;
  }

  /**
   * Checks if a class is the top of a diamond hierarchy.
   */
  private boolean isDiamondHierarchy(final DocumentType typez) {
    final Set<DocumentType> traversed = new HashSet<>();
    final List<DocumentType> stack = new ArrayList<>();
    stack.add(typez);
    while (!stack.isEmpty()) {
      final DocumentType current = stack.removeFirst();
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

  private List<ExecutionStepInternal> handleTypeAsTargetWithIndexRecursive(final String targetType, final Set<String> filterBuckets,
      final QueryPlanningInfo info, final CommandContext context) {
    List<ExecutionStepInternal> result = handleTypeAsTargetWithIndex(targetType, filterBuckets, info, context);
    if (result == null) {
      result = new ArrayList<>();
      final DocumentType typez = context.getDatabase().getSchema().getType(targetType);
      if (typez == null)
        throw new CommandExecutionException("Cannot find class " + targetType);

      if (context.getDatabase().countType(typez.getName(), false) != 0 || typez.getSubTypes().size() == 0 || isDiamondHierarchy(
          typez))
        return null;

      final Collection<DocumentType> subTypes = typez.getSubTypes();

      final List<InternalExecutionPlan> subTypePlans = new ArrayList<>();
      for (final DocumentType subType : subTypes) {
        final List<ExecutionStepInternal> subSteps = handleTypeAsTargetWithIndexRecursive(subType.getName(), filterBuckets, info,
            context);
        if (subSteps == null || subSteps.isEmpty()) {
          return null;
        }
        final SelectExecutionPlan subPlan = new SelectExecutionPlan(context,
            statement.getLimit() != null ? statement.getLimit().getValue(context) : 0);
        subSteps.forEach(subPlan::chain);
        subTypePlans.add(subPlan);
      }
      if (!subTypePlans.isEmpty()) {
        result.add(new ParallelExecStep(subTypePlans, context));
      }
    }
    return result.isEmpty() ? null : result;
  }

  private List<ExecutionStepInternal> handleTypeAsTargetWithIndex(final String targetType, final Set<String> filterBuckets,
      final QueryPlanningInfo info, final CommandContext context) {
    if (info.flattenedWhereClause == null || info.flattenedWhereClause.isEmpty())
      return null;

    final DocumentType typez = context.getDatabase().getSchema().getType(targetType);
    if (typez == null)
      throw new CommandExecutionException("Cannot find type " + targetType);

    final Collection<TypeIndex> indexes = typez.getAllIndexes(true);

    if (indexes.isEmpty())
      return null;

    List<IndexSearchDescriptor> indexSearchDescriptors =
        info.flattenedWhereClause.stream()
            .map(x -> findBestIndexFor(context, indexes, x, typez))
            .collect(Collectors.toList());

    // Fix for issue #2695: If any OR branch cannot be optimized with an index,
    // we must fall back to a full scan. Otherwise, those branches would be silently
    // ignored, causing incomplete query results (e.g., DELETE only deleting some records).
    if (indexSearchDescriptors.contains(null))
      return null; // some blocks could not be managed with an index, fall back to full scan

    List<IndexSearchDescriptor> optimumIndexSearchDescriptors =
        commonFactor(indexSearchDescriptors);

    return executionStepFromIndexes(filterBuckets, typez, info, context, optimumIndexSearchDescriptors);
  }

  private List<ExecutionStepInternal> executionStepFromIndexes(final Set<String> filterClusters, final DocumentType clazz,
      final QueryPlanningInfo info, final CommandContext context, List<IndexSearchDescriptor> optimumIndexSearchDescriptors) {
    List<ExecutionStepInternal> result;
    if (optimumIndexSearchDescriptors.size() == 1) {
      final IndexSearchDescriptor desc = optimumIndexSearchDescriptors.getFirst();
      result = new ArrayList<>();

      final Boolean orderAsc = getOrderDirection(info);
      result.add(
          new FetchFromIndexStep((RangeIndex) desc.getIndex(), desc.keyCondition, desc.additionalRangeCondition,
              !Boolean.FALSE.equals(orderAsc),
              context));

      List<Integer> filterClusterIds = null;
      if (filterClusters != null) {
        filterClusterIds = classClustersFiltered(context.getDatabase(), clazz, filterClusters);
      } else {
        filterClusterIds = clazz.getBucketIds(true);
      }
      result.add(new GetValueFromIndexEntryStep(context, filterClusterIds));
      if (desc.requiresDistinctStep()) {
        result.add(new DistinctExecutionStep(context));
      }

      if (orderAsc != null && info.orderBy != null && fullySorted(info.orderBy, (AndBlock) desc.keyCondition, desc.getIndex()))
        info.orderApplied = true;

        if (desc.getRemainingCondition() != null && !desc.getRemainingCondition().isEmpty()) {
        if ((info.perRecordLetClause != null
            && refersToLet(Collections.singletonList(desc.getRemainingCondition())))) {
          SelectExecutionPlan stubPlan = new SelectExecutionPlan(context,
              statement.getLimit() != null ? statement.getLimit().getValue(context) : 0);
          handleLet(stubPlan, info, context);
          for (ExecutionStep step : stubPlan.getSteps()) {
            result.add((ExecutionStepInternal) step);
          }
        }
        result.add(
            new FilterStep(createWhereFrom(desc.getRemainingCondition()), context));
      }
    } else {
      result = new ArrayList<>();
      result.add(createParallelIndexFetch(optimumIndexSearchDescriptors, filterClusters, context));
      if (optimumIndexSearchDescriptors.size() > 1) {
        result.add(new DistinctExecutionStep(context));
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
      if (item instanceof BinaryCondition condition) {
        if (condition.getOperator() instanceof EqualsCompareOperator) {
          conditionItems.add(condition.getLeft().toString());
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
      if (s.equals(orderItems.getFirst())) {
        orderItems.removeFirst();
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
    if (info.orderBy == null)
      return null;

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
      final Set<String> filterClusters, final CommandContext context) {
    final List<InternalExecutionPlan> subPlans = new ArrayList<>();
    for (final IndexSearchDescriptor desc : indexSearchDescriptors) {
      final SelectExecutionPlan subPlan = new SelectExecutionPlan(context,
          statement.getLimit() != null ? statement.getLimit().getValue(context) : 0);
      subPlan.chain(new FetchFromIndexStep(desc.index, desc.keyCondition, desc.additionalRangeCondition, context));
      List<Integer> filterClusterIds = null;
      if (filterClusters != null)
        filterClusterIds = filterClusters.stream().map(name -> context.getDatabase().getSchema().getBucketByName(name).getFileId())
            .mapToInt(i -> i).boxed().toList();

      subPlan.chain(new GetValueFromIndexEntryStep(context, filterClusterIds));
      if (requiresMultipleIndexLookups((AndBlock) desc.keyCondition)) {
        subPlan.chain(new DistinctExecutionStep(context));
      }
      if (desc.remainingCondition != null && !desc.remainingCondition.isEmpty()) {
        subPlan.chain(new FilterStep(createWhereFrom(desc.remainingCondition), context));
      }
      subPlans.add(subPlan);
    }
    return new ParallelExecStep(subPlans, context);
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
  private IndexSearchDescriptor findBestIndexesFor(final CommandContext context, final Collection<TypeIndex> indexes,
      final AndBlock block, final DocumentType typez) {
    // get all valid index descriptors
    List<IndexSearchDescriptor> descriptors = indexes.stream()
        .map(index -> buildIndexSearchDescriptor(context, index, block, typez)).filter(Objects::nonNull)
        .filter(x -> x.keyCondition != null).filter(x -> x.getSubBlocks().size() > 0)
        .sorted(Comparator.comparing(x -> x.cost(context))).collect(Collectors.toList());

    descriptors = removeGenericIndexes(descriptors, typez);

    // remove the redundant descriptors (eg. if I have one on [a] and one on [a, b], the first one
    // is redundant, just discard it)
    descriptors = removePrefixIndexes(descriptors);

    // sort by cost
    List<Pair<Integer, IndexSearchDescriptor>> sortedDescriptors = descriptors.stream()
        .map(x -> (Pair<Integer, IndexSearchDescriptor>) new Pair(x.cost(context), x)).sorted().toList();

    // get only the descriptors with the lowest cost
    if (sortedDescriptors.isEmpty()) {
      descriptors = Collections.emptyList();
    } else {
      descriptors = sortedDescriptors.stream().filter(x -> x.getFirst().equals(sortedDescriptors.getFirst().getFirst()))
          .map(Pair::getSecond).collect(Collectors.toList());
    }

    // sort remaining by the number of indexed fields
    descriptors = descriptors.stream().sorted(Comparator.comparingInt(x -> x.blockCount())).collect(Collectors.toList());

    // get the one that has more indexed fields
    return descriptors.isEmpty() ? null : descriptors.getLast();
  }

  /**
   * If between the index candidates there are for the same property target class index and super
   * class index prefer the target class.
   */
  private List<IndexSearchDescriptor> removeGenericIndexes(final List<IndexSearchDescriptor> descriptors,
      final DocumentType clazz) {
    List<IndexSearchDescriptor> results = new ArrayList<>();
    for (IndexSearchDescriptor desc : descriptors) {
      IndexSearchDescriptor matching = null;
      for (IndexSearchDescriptor result : results) {
        if (desc.isSameCondition(result)) {
          matching = result;
          break;
        }
      }
      if (matching != null) {
        if (clazz.getName().equals(desc.getIndex().getTypeName())) {
          results.remove(matching);
          results.add(desc);
        }
      } else {
        results.add(desc);
      }
    }
    return results;
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
    List<IndexSearchDescriptor> descriptors = indexes.stream()
        .map(index -> buildIndexSearchDescriptor(context, index, block, clazz))
        .filter(Objects::nonNull)
        .filter(x -> x.keyCondition != null)
        .filter(x -> !x.getSubBlocks().isEmpty())
        .collect(Collectors.toList());

    final List<IndexSearchDescriptor> fullTextIndexDescriptors = indexes.stream()
        .filter(idx -> idx.getType().equals(FULL_TEXT))
        .map(idx -> buildIndexSearchDescriptorForFulltext(context, idx, block, clazz))
        .filter(Objects::nonNull)
        .filter(x -> x.keyCondition != null)
        .filter(x -> !x.getSubBlocks().isEmpty())
        .toList();

    descriptors.addAll(fullTextIndexDescriptors);

    // remove the redundant descriptors (eg. if I have one on [a] and one on [a, b], the first one
    // is redundant, just discard it)
    //descriptors = removePrefixIndexes(descriptors);

    if (descriptors.isEmpty())
      return null;

    // First, prefer indexes that cover more conditions (more subBlocks)
    // This ensures composite indexes are preferred over single-property indexes
    final int maxSubBlocks = descriptors.stream()
        .mapToInt(x -> x.getSubBlocks().size())
        .max()
        .orElse(0);

    // Filter to keep only indexes with the maximum number of covered conditions
    descriptors = descriptors.stream()
        .filter(x -> x.getSubBlocks().size() == maxSubBlocks)
        .toList();

    // Prefer FULL_TEXT indexes when they cover CONTAINSTEXT conditions, because CONTAINSTEXT
    // has different semantics via full-text index (case-insensitive, tokenized) vs post-filter
    // (case-sensitive String.contains). If a standard index wins, the CONTAINSTEXT remaining
    // condition would lose full-text semantics. (Issue #3483 follow-up)
    if (descriptors.size() > 1) {
      final List<IndexSearchDescriptor> fullTextDescriptors = descriptors.stream()
          .filter(d -> d.index.getType().equals(FULL_TEXT))
          .toList();
      if (!fullTextDescriptors.isEmpty() && fullTextDescriptors.size() < descriptors.size())
        descriptors = fullTextDescriptors;
    }

    // If there are multiple indexes covering the same number of conditions,
    // select based on cost (lower is better)
    if (descriptors.size() > 1) {
      final List<Pair<Integer, IndexSearchDescriptor>> sortedDescriptors = descriptors.stream()
          .map(x -> (Pair<Integer, IndexSearchDescriptor>) new Pair(x.cost(context), x))
          .sorted()
          .toList();

      // get only the descriptors with the lowest cost
      final int lowestCost = sortedDescriptors.getFirst().getFirst();
      descriptors = sortedDescriptors.stream()
          .filter(x -> x.getFirst().equals(lowestCost))
          .map(x -> x.getSecond())
          .toList();
    }

    // Return the first descriptor (all remaining have same subBlocks size and cost)
    return descriptors.getFirst();
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
    final List<BooleanExpression> left = item.getSubBlocks();
    final List<BooleanExpression> right = desc.getSubBlocks();
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
    result.index = (RangeIndex) index;
    result.keyCondition = indexKeyValue;
    for (final String indexField : indexFields) {
      blockIterator = blockCopy.getSubBlocks().iterator();
      final boolean breakHere = false;
      boolean indexFieldFound = false;
      while (blockIterator.hasNext()) {
        final BooleanExpression singleExp = blockIterator.next();
        if (singleExp instanceof ContainsTextCondition textCondition) {
          final Expression left = textCondition.getLeft();
          // Get field name from expression - handle both simple identifiers (e.g. txt) and dotted
          // path expressions (e.g. lst.txt) used with BY ITEM nested property indexes
          final String fieldName = left.getDefaultAlias().getStringValue();
          // Strip modifiers to get base field name
          String baseFieldName = indexField;
          if (indexField.endsWith(" by key")) {
            baseFieldName = indexField.substring(0, indexField.length() - 7);
          } else if (indexField.endsWith(" by value")) {
            baseFieldName = indexField.substring(0, indexField.length() - 9);
          } else if (indexField.endsWith(" by item")) {
            baseFieldName = indexField.substring(0, indexField.length() - 8);
          }
          if (baseFieldName.equals(fieldName)) {
            found = true;
            indexFieldFound = true;
            final ContainsTextCondition condition = new ContainsTextCondition(-1);
            condition.setLeft(left);
            condition.setRight(textCondition.getRight().copy());
            indexKeyValue.getSubBlocks().add(condition);
            blockIterator.remove();
            break;
          }
        }
      }
      if (breakHere || !indexFieldFound) {
        break;
      }
    }

    if (result.getSubBlocks().size() < index.getPropertyNames().size() && !index.supportsOrderedIterations())
      // hash indexes do not support partial key match
      return null;

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
  private IndexSearchDescriptor buildIndexSearchDescriptor(final CommandContext context, final Index index, final AndBlock block,
      final DocumentType clazz) {
    final List<String> indexFields = index.getPropertyNames();
    boolean found = false;

    AndBlock blockCopy = block.copy();
    Iterator<BooleanExpression> blockIterator;

    AndBlock indexKeyValue = new AndBlock(-1);
    BinaryCondition additionalRangeCondition = null;

    for (String indexField : indexFields) {
      // Strip modifiers to get base field name
      String baseFieldName = indexField;
      if (indexField.endsWith(" by key")) {
        baseFieldName = indexField.substring(0, indexField.length() - 7);
      } else if (indexField.endsWith(" by value")) {
        baseFieldName = indexField.substring(0, indexField.length() - 9);
      } else if (indexField.endsWith(" by item")) {
        baseFieldName = indexField.substring(0, indexField.length() - 8);
      }

      final boolean supportNull = index.getNullStrategy() == LSMTreeIndexAbstract.NULL_STRATEGY.INDEX;
      final IndexSearchInfo info = new IndexSearchInfo(baseFieldName, allowsRangeQueries(index), isMap(clazz, baseFieldName),
          isIndexByKey(index, baseFieldName), isIndexByValue(index, baseFieldName), isIndexByItem(index, baseFieldName), supportNull,
          context);
      blockIterator = blockCopy.getSubBlocks().iterator();
      boolean indexFieldFound = false;
      boolean rangeOp = false;
      while (blockIterator.hasNext()) {
        BooleanExpression singleExp = blockIterator.next();
        if (singleExp.isIndexAware(info)) {
          indexFieldFound = true;
          indexKeyValue.getSubBlocks().add(singleExp.copy());
          blockIterator.remove();
          if (singleExp instanceof BinaryCondition condition && condition.getOperator().isRangeOperator()) {
            rangeOp = true;
          }
          if (rangeOp && info.allowsRange()) {
            // look for the opposite condition, on the same field, for range queries (the other
            // side of the range)
            while (blockIterator.hasNext()) {
              BooleanExpression next = blockIterator.next();
              if (next.createRangeWith(singleExp)) {
                additionalRangeCondition = (BinaryCondition) next;
                blockIterator.remove();
                break;
              }
            }
          }
          break;
        }
      }

      if (indexFieldFound) {
        found = true;
      }
      if (!indexFieldFound || rangeOp) {
        break;
      }
    }

    if (indexKeyValue.getSubBlocks().size() < index.getPropertyNames().size()) {
      if (!index.supportsOrderedIterations()) {
        return null;
      }
    }

    if (found)
      return new IndexSearchDescriptor((RangeIndex) index, indexKeyValue, additionalRangeCondition, blockCopy);

    return null;
  }

  private boolean createsRangeWith(final BinaryCondition left, final BooleanExpression next) {
    if (!(next instanceof final BinaryCondition right))
      return false;

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

  private boolean isMap(final DocumentType clazz, final String indexField) {
    final Property prop = clazz.getPropertyIfExists(indexField);
    if (prop == null)
      return false;
    return prop.getType() == Type.MAP;
  }

  private boolean isIndexByKey(final Index index, final String field) {
    for (String o : index.getPropertyNames()) {
      if (o.equalsIgnoreCase(field + " by key"))
        return true;
    }
    return false;
  }

  private boolean isIndexByValue(final Index index, final String field) {
    for (String o : index.getPropertyNames()) {
      if (o.equalsIgnoreCase(field + " by value"))
        return true;
    }
    return false;
  }

  private boolean isIndexByItem(final Index index, final String field) {
    for (String o : index.getPropertyNames()) {
      if (o.equalsIgnoreCase(field + " by item"))
        return true;
    }
    return false;
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
      final Map<IndexCondPair, OrBlock> filtersForIndex = aggregation.computeIfAbsent(item.index, k -> new HashMap<>());
      final IndexCondPair extendedCond = new IndexCondPair((AndBlock) item.keyCondition, item.additionalRangeCondition);

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
        result.add(new IndexSearchDescriptor(item.getKey(), filters.getKey().mainCondition(), filters.getKey().additionalRange(),
            filters.getValue()));
      }
    }
    return result;
  }

  private void handleBucketsAsTarget(final SelectExecutionPlan plan, final QueryPlanningInfo info, final List<Bucket> buckets,
      final CommandContext context) {
    final Database db = context.getDatabase();

    DocumentType candidateClass = null;
    boolean tryByIndex = true;
    final Set<String> bucketNames = new HashSet<>();

    for (final Bucket parserBucket : buckets) {
      String name = resolveBucketName(parserBucket, context);
      Integer bucketId = parserBucket.getBucketNumber();
      if (name == null && bucketId != null)
        name = db.getSchema().getBucketById(bucketId).getName();

      if (bucketId == null && name != null) {
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
      if (handleTypeAsTargetWithIndexedFunction(plan, bucketNames, typez, info, context))
        return;

      if (handleTypeAsTargetWithIndex(plan, typez, bucketNames, info, context))
        return;

      if (info.orderBy != null && handleClassWithIndexForSortOnly(plan, typez, bucketNames, info, context))
        return;
    }

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (isOrderByRidAsc(info))
      orderByRidAsc = true;
    else if (isOrderByRidDesc(info))
      orderByRidAsc = false;

    if (orderByRidAsc != null)
      info.orderApplied = true;

    if (buckets.size() == 1) {
      final Bucket parserBucket = buckets.getFirst();

      Integer bucketId = parserBucket.getBucketNumber();
      if (bucketId == null) {
        final String resolvedName = resolveBucketName(parserBucket, context);
        if (resolvedName != null) {
          final com.arcadedb.engine.Bucket bucket = db.getSchema().getBucketByName(resolvedName);
          if (bucket != null)
            bucketId = bucket.getFileId();
        }
      }

      if (bucketId == null)
        throw new CommandExecutionException("Bucket '" + parserBucket + "' does not exist");

      final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(bucketId, context);
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
          final String resolvedName = resolveBucketName(parserBucket, context);
          if (resolvedName != null) {
            final com.arcadedb.engine.Bucket bucket = db.getSchema().getBucketByName(resolvedName);
            if (bucket != null)
              bucketId = bucket.getFileId();
          }
        }

        if (bucketId == null) {
          throw new CommandExecutionException("Bucket '" + parserBucket + "' does not exist");
        }
        bucketIds[i] = bucketId;
      }
      final FetchFromClustersExecutionStep step = new FetchFromClustersExecutionStep(bucketIds, context, orderByRidAsc);
      plan.chain(step);
    }
  }

  /**
   * Resolves the bucket name from a Bucket object, handling parameterized bucket names.
   * If the bucket has an inputParam, it resolves the parameter value from the context.
   *
   * @param parserBucket the bucket object to resolve
   * @param context      the command context containing input parameters
   * @return the resolved bucket name, or null if it cannot be resolved
   */
  private String resolveBucketName(final Bucket parserBucket, final CommandContext context) {
    // First check for literal bucket name
    if (parserBucket.getBucketName() != null) {
      return parserBucket.getBucketName();
    }

    // Check for input parameter
    if (parserBucket.getInputParam() != null) {
      final Object paramValue = parserBucket.getInputParam().getValue(context.getInputParameters());
      if (paramValue != null) {
        return paramValue.toString();
      }
    }

    return null;
  }

  private void handleSubqueryAsTarget(final SelectExecutionPlan plan, final Statement subQuery, final CommandContext context) {
    final BasicCommandContext subCtx = new BasicCommandContext();
    subCtx.setDatabase(context.getDatabase());
    subCtx.setParent(context);
    final InternalExecutionPlan subExecutionPlan = subQuery.createExecutionPlan(subCtx);
    plan.chain(new SubQueryStep(subExecutionPlan, context, subCtx));
  }

  private boolean isOrderByRidDesc(final QueryPlanningInfo info) {
    if (!hasTargetWithSortedRids(info))
      return false;

    if (info.orderBy == null)
      return false;

    if (info.orderBy.getItems().size() == 1) {
      OrderByItem item = info.orderBy.getItems().getFirst();
      String recordAttr = item.getRecordAttr();
      return recordAttr != null && recordAttr.equalsIgnoreCase(RID_PROPERTY) && OrderByItem.DESC.equals(item.getType());
    }
    return false;
  }

  private boolean isOrderByRidAsc(final QueryPlanningInfo info) {
    if (!hasTargetWithSortedRids(info))
      return false;

    if (info.orderBy == null)
      return false;

    if (info.orderBy.getItems().size() == 1) {
      final OrderByItem item = info.orderBy.getItems().getFirst();
      final String recordAttr = item.getRecordAttr();
      return recordAttr != null && recordAttr.equalsIgnoreCase(RID_PROPERTY) && (item.getType() == null || OrderByItem.ASC.equals(
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

    final Set<String> buckets = new HashSet<>();
    final Database db = context.getDatabase();

    if (item.getIdentifier() != null) {
      if (item.getIdentifier().getStringValue().startsWith("$")) {
        // RESOLVE VARIABLE - use getVariablePath to support nested paths like $parent.$current.@rid
        final Object value = context.getVariablePath(item.toString());
        if (value != null) {
          // Handle RID string (e.g., '#1:143') - Issue #2350
          if (value instanceof String strValue && strValue.startsWith("#")) {
            final RID rid = new RID(db, strValue);
            if (item.getRids() == null) {
              item.setRids(new ArrayList<>());
            }
            item.getRids().add(new Rid(rid));
            item.setIdentifier(null);
          } else if (value instanceof RID rid) {
            // Handle RID object directly
            if (item.getRids() == null) {
              item.setRids(new ArrayList<>());
            }
            item.getRids().add(new Rid(rid));
            item.setIdentifier(null);
          } else if (value instanceof Identifiable || value instanceof ResultSet) {
            // Handle Identifiable or ResultSet through setValue
            item.setValue(value);
            item.setIdentifier(null);
          } else if (value instanceof String typeName) {
            // Handle type name string - resolve as type
            item.setIdentifier(new Identifier(typeName));
          }
          // For other types, keep the identifier as-is and let handleFetchFromTarget deal with it
        }
      }
    }

    if (item.getRids() != null && !item.getRids().isEmpty()) {
      if (item.getRids().size() == 1) {
        final PInteger bucket = item.getRids().getFirst().getBucket();
        buckets.add(db.getSchema().getBucketById(bucket.getValue().intValue()).getName());
      } else {
        for (final Rid rid : item.getRids()) {
          final PInteger bucket = rid.getBucket();
          buckets.add(db.getSchema().getBucketById(bucket.getValue().intValue()).getName());
        }
      }
      return buckets;
    } else if (item.getInputParams() != null && item.getInputParams().size() > 0) {
      return null;
    } else if (item.getBucket() != null) {
      // If bucket has an input parameter, defer resolution to runtime
      if (item.getBucket().getInputParam() != null) {
        return null;
      }
      String name = item.getBucket().getBucketName();
      if (name == null && item.getBucket().getBucketNumber() != null) {
        name = db.getSchema().getBucketById(item.getBucket().getBucketNumber()).getName();
      }
      if (name != null) {
        buckets.add(name);
        return buckets;
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
          buckets.add(name);
        }
      }
      return buckets;
    } else if (item.getIndex() != null) {
      final String indexName = item.getIndex().getIndexName();
      final Index idx = db.getSchema().getIndexByName(indexName);
      if (idx == null)
        throw new CommandExecutionException("Index '" + indexName + "' does not exist");

      if (idx instanceof TypeIndex index) {
        for (final Index subIdx : index.getSubIndexes())
          buckets.add(db.getSchema().getBucketById(subIdx.getAssociatedBucketId()).getName());
      } else
        buckets.add(db.getSchema().getBucketById(idx.getAssociatedBucketId()).getName());

      if (buckets.isEmpty()) {
        return null;
      }
      return buckets;
    } else if (item.getInputParam() != null) {
      return null;
    } else if (item.getIdentifier() != null) {

      if (item.getIdentifier().getStringValue().startsWith("$")) {
        // RESOLVE VARIABLE
        final Object value = context.getVariable(item.getIdentifier().getStringValue());
        if (value != null) {
          if (value instanceof RID iD)
            item.getRids().add(new Rid(iD));
        }
      } else {
        final String className = item.getIdentifier().getStringValue();
        final DocumentType typez = db.getSchema().getType(className);
        final int[] bucketIds = typez.getBuckets(true).stream().mapToInt(com.arcadedb.engine.Bucket::getFileId).toArray();
        for (final int bucketId : bucketIds) {
          final String bucketName = db.getSchema().getBucketById(bucketId).getName();
          if (bucketName != null) {
            buckets.add(bucketName);
          }
        }
      }
      return buckets;
    }

    return null;
  }
}
