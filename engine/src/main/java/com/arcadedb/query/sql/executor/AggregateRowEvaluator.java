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
import com.arcadedb.function.HeapBufferingFunction;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.function.sql.math.SQLFunctionCount;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FunctionCall;
import com.arcadedb.query.sql.parser.GroupBy;
import com.arcadedb.query.sql.parser.NestedProjection;
import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.ProjectionItem;
import com.arcadedb.query.sql.parser.SimpleNode;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.query.sql.parser.SubQueryCollector;
import com.arcadedb.query.sql.parser.SuffixIdentifier;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Evaluates, row after row, what the aggregation of a GROUP BY needs of a row: the key of its group, the non-aggregate
 * projections of a new group, and the arguments of the aggregates (issue #9496).
 * <p>
 * The planner splits an aggregate across two projections: {@code sum(a * b)} becomes {@code a * b AS _$$$OALIAS$$$_1}
 * computed on every row, then {@code sum(_$$$OALIAS$$$_1)} reading it back. Run as written, that made a row of nine
 * properties for every input row of TPC-H Q1, only for every aggregate to look its argument up in it by name, and the
 * aggregate states were looked up by name in their group too. Here the projection before the aggregation is evaluated
 * into an array, and everything that reads one of its columns by name - an aggregate argument, a GROUP BY key, a
 * non-aggregate projection - is bound to its index once, when the evaluator is built. The row of that projection is
 * made only when an expression the evaluator cannot bind needs it, and then exactly as the projection makes it, so
 * every expression sees what it always saw.
 * <p>
 * The record's properties are read through a {@link PropertyCachingResult} when nothing the expressions do can hand
 * the row out: one walk of the record header per row, one decoding per value.
 * <p>
 * The groups keep their aggregate states in an array, in the order of the aggregate projection, and the values of
 * their non-aggregate projections in another: the row of a group is made once, when the aggregation is over.
 * <p>
 * An evaluator holds per-row state: one per thread, on its own copies of the expressions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AggregateRowEvaluator {
  /** The key of every row when there is no GROUP BY: one group. */
  static final  GroupByKey NO_GROUP_BY = new GroupByKey(new Object[0]);
  private static final int NO_SLOT     = -1;
  // THE ARGUMENT OF count(*): THE ROW, WHICH count() ONLY TELLS FROM NULL
  private static final int STAR_SLOT   = -2;

  private final Database database;

  // THE PROJECTION BEFORE THE AGGREGATION, OR NULL WHEN THE ROWS ARE AGGREGATED AS THEY COME
  private final Projection           preProjection;
  // WHETHER ITS ITEMS ARE EVALUATED INTO preValues, ITS ROW MADE ONLY ON DEMAND
  private final boolean              fused;
  private final ProjectionItem[]     preItems;
  private final Object[]             preValues;
  // WHETHER $current IS SET TO THE ROW WHILE THE PROJECTION IS EVALUATED, AS ITS OWN STEP DOES (THE SEQUENTIAL EXECUTION)
  private final boolean              setsCurrent;
  // THE VIEW OF THE ROW THE EXPRESSIONS READ THE RECORD THROUGH, OR NULL WHEN AN EXPRESSION COULD HAND THE ROW OUT
  private final PropertyCachingResult cachingView;
  // WHETHER THE CONDITIONS OF A PARALLEL WORKER CAN READ THE RECORD THROUGH THE SAME VIEW
  private final boolean              conditionsOnView;

  // THE AGGREGATE PROJECTION: ITS NON-AGGREGATE ITEMS, THEN ITS AGGREGATES
  private final ProjectionItem[] valueItems;
  final         String[]         valueAliases;
  private final int[]            valueSlots;
  private final ProjectionItem[] aggregateItems;
  final         String[]         aggregateAliases;
  // THE COLUMN EACH SINGLE-ARGUMENT AGGREGATE READS, STAR_SLOT FOR count(*), NO_SLOT WHEN apply() EVALUATES IT ON THE ROW
  private final int[]            argumentSlots;

  private final Expression[] keys;
  private final int[]        keySlots;
  private final GroupByKey   probe;

  // PER ROW
  private Result row;
  private Result view;
  private Result input;

  /**
   * @param preProjection the projection between the source and the aggregation, or null
   * @param setsCurrent   whether {@code $current} is set to the row while the projection is evaluated, as the projection
   *                      step does; a parallel worker sets it for the whole row already
   * @param conditions    the conditions a parallel worker checks on the row before it is aggregated, or null
   */
  AggregateRowEvaluator(final Projection preProjection, final Projection projection, final GroupBy groupBy, final boolean setsCurrent,
      final WhereClause[] conditions, final CommandContext context) {
    this.database = context.getDatabase();
    this.preProjection = preProjection;
    this.setsCurrent = setsCurrent;

    fused = preProjection != null && !preProjection.isExpand() && !preProjection.returnsRecordAsIs() && hasOnlyPlainItems(preProjection);
    final Map<String, Integer> slotByAlias = new HashMap<>();
    if (fused) {
      preItems = preProjection.getItems().toArray(new ProjectionItem[0]);
      preValues = new Object[preItems.length];
      // A LATER ITEM OF THE SAME ALIAS WINS, AS IN THE ROW OF THE PROJECTION
      for (int i = 0; i < preItems.length; i++)
        slotByAlias.put(preItems[i].getProjectionAliasAsString(), i);
    } else {
      preItems = null;
      preValues = null;
    }

    final List<ProjectionItem> values = new ArrayList<>();
    final List<ProjectionItem> aggregates = new ArrayList<>();
    for (final ProjectionItem item : projection.getItems())
      (item.isAggregate(context) ? aggregates : values).add(item);

    valueItems = values.toArray(new ProjectionItem[0]);
    valueAliases = new String[valueItems.length];
    valueSlots = new int[valueItems.length];
    for (int i = 0; i < valueItems.length; i++) {
      final ProjectionItem item = valueItems[i];
      valueAliases[i] = item.getProjectionAliasAsString();
      valueSlots[i] = item.isAll() || item.nestedProjection != null ? NO_SLOT : slotOf(item.getExpression(), slotByAlias);
    }

    boolean aggregatesSafe = true;
    aggregateItems = aggregates.toArray(new ProjectionItem[0]);
    aggregateAliases = new String[aggregateItems.length];
    argumentSlots = new int[aggregateItems.length];
    for (int i = 0; i < aggregateItems.length; i++) {
      final ProjectionItem item = aggregateItems[i];
      aggregateAliases[i] = item.getProjectionAliasAsString();
      argumentSlots[i] = NO_SLOT;
      final AggregationContext prototype = item.getAggregationContext(context);
      if (!(prototype instanceof FunctionAggregationContext function) || !isBuiltIn(function.getFunction())) {
        aggregatesSafe = false;
        continue;
      }
      final List<Expression> params = function.getParams();
      for (final Expression param : params)
        aggregatesSafe &= isSafeOnView(param);
      if (params.size() == 1) {
        final Expression param = params.getFirst();
        if ("*".equals(param.toString()))
          argumentSlots[i] = function.getFunction() instanceof SQLFunctionCount && !function.isDistinct() ? STAR_SLOT : NO_SLOT;
        else
          argumentSlots[i] = slotOf(param, slotByAlias);
      }
    }

    if (groupBy == null || groupBy.getItems() == null || groupBy.getItems().isEmpty()) {
      keys = new Expression[0];
      keySlots = new int[0];
      probe = NO_GROUP_BY;
    } else {
      keys = groupBy.getItems().toArray(new Expression[0]);
      keySlots = new int[keys.length];
      for (int i = 0; i < keys.length; i++)
        keySlots[i] = slotOf(keys[i], slotByAlias);
      probe = new GroupByKey(new Object[keys.length]);
    }

    // THE VIEW CACHES THE PROPERTIES OF THE RECORD THE EXPRESSIONS EVALUATED ON THE ROW READ: THE ITEMS OF THE PROJECTION
    // WHEN IT IS FUSED, ALL THE EXPRESSIONS OF THE AGGREGATION WHEN THERE IS NO PROJECTION. A PROJECTION THAT IS NOT FUSED
    // MAKES ITS ROW OF THE ROW ITSELF, AND MAY ANSWER IT AS IT IS
    final boolean viewSafe;
    if (fused)
      viewSafe = isSafeOnView(preProjection);
    else if (preProjection == null) {
      boolean safe = aggregatesSafe && isSafeOnView(groupBy);
      for (final ProjectionItem item : valueItems)
        safe &= isSafeOnView(item);
      viewSafe = safe;
    } else
      viewSafe = false;
    cachingView = viewSafe ? new PropertyCachingResult(database) : null;

    boolean conditionsSafe = cachingView != null && conditions != null;
    if (conditionsSafe)
      for (final WhereClause condition : conditions)
        conditionsSafe &= isSafeOnView(condition);
    conditionsOnView = conditionsSafe;
  }

  /**
   * Starts a row: answers what a parallel worker checks its conditions on - the caching view of the row when they can
   * read through it, else the row - and readies {@link #evaluate}.
   */
  Result begin(final Result row) {
    this.row = row;
    this.view = cachingView != null ? cachingView.of(row) : row;
    return conditionsOnView ? view : row;
  }

  /**
   * Evaluates what the aggregation needs of the row passed to {@link #begin}, and answers the key of its group. The key
   * is reused for the next row: {@link #newGroup} keeps a copy of it.
   */
  GroupByKey evaluate(final CommandContext context) {
    input = null;
    if (preProjection != null) {
      Object oldCurrent = null;
      if (setsCurrent) {
        oldCurrent = context.getVariable("current");
        context.setVariable("current", row);
      }
      if (fused)
        for (int i = 0; i < preItems.length; i++)
          preValues[i] = preItems[i].execute(view, context);
      else
        input = preProjection.calculateSingle(context, view);
      if (setsCurrent)
        context.setVariable("current", oldCurrent);
    } else
      input = view;

    if (keys.length == 0)
      return NO_GROUP_BY;

    final Object[] keyValues = probe.keyValues;
    for (int i = 0; i < keys.length; i++)
      keyValues[i] = keySlots[i] >= 0 ? slotValue(keySlots[i]) : keys[i].execute(input(context), context);
    probe.rehash();
    return probe;
  }

  /** A new group for the row just evaluated, whose key is {@code key}: a copy of it, the key being reused for the next row. */
  Group newGroup(final GroupByKey key, final long firstSeen, final CommandContext context, final OperationHeapLimit heapLimit) {
    final Object[] values = new Object[valueItems.length];
    for (int i = 0; i < values.length; i++)
      values[i] = valueSlots[i] >= 0 ?
          valueItems[i].convert(slotValue(valueSlots[i])) :
          valueItems[i].execute(input(context), context);

    final AggregationContext[] states = new AggregationContext[aggregateItems.length];
    for (int i = 0; i < states.length; i++)
      states[i] = HeapBufferingFunction.adopt(aggregateItems[i].getAggregationContext(context), heapLimit);

    return new Group(key, values, states, firstSeen);
  }

  /** Feeds the row just evaluated to the aggregates of {@code group}. */
  void accumulate(final Group group, final CommandContext context) {
    final AggregationContext[] states = group.aggregates;
    for (int i = 0; i < states.length; i++) {
      final int slot = argumentSlots[i];
      if (slot >= 0)
        ((FunctionAggregationContext) states[i]).applyValue(slotValue(slot), null, context);
      else if (slot == STAR_SLOT)
        ((FunctionAggregationContext) states[i]).applyValue(Boolean.TRUE, null, context);
      else
        states[i].apply(input(context), context);
    }
  }

  /** The row of a group whose aggregation is over: its non-aggregate values, and the result of every aggregate. */
  ResultInternal toResult(final Group group) {
    final ResultInternal result = new ResultInternal(database);
    for (int i = 0; i < valueAliases.length; i++)
      result.setProperty(valueAliases[i], group.columnValues[i]);
    for (int i = 0; i < aggregateAliases.length; i++)
      result.setTemporaryProperty(aggregateAliases[i], group.aggregates[i].getFinalValue());
    return result;
  }

  /** The row the expressions that are not bound to a column evaluate on: the row of the projection, made now if fused. */
  private Result input(final CommandContext context) {
    if (input == null)
      input = preProjection.calculateSingle(context, row, preValues);
    return input;
  }

  /** The value of column {@code slot} of the projection as its row would answer it. */
  private Object slotValue(final int slot) {
    final Object value = preValues[slot];
    // THE MEASURES AND KEYS OF A GROUP BY: VALUES THE ROW KEEPS AND ANSWERS AS THEY ARE
    if (value instanceof Number || value instanceof String)
      return value;
    return ResultInternal.toPropertyValue(ResultInternal.toStoredValue(database, value));
  }

  /**
   * The column of the fused projection {@code expression} reads, when it is nothing but the name of one: what
   * {@code SuffixIdentifier} answers for such a name on the row of the projection, where the column is always set.
   */
  private int slotOf(final Expression expression, final Map<String, Integer> slotByAlias) {
    if (!fused || expression == null || !expression.isBaseIdentifier())
      return NO_SLOT;
    final String name = expression.getDefaultAlias().getStringValue();
    // A VARIABLE OR THE LET OF A LIFTED SUB-QUERY IS ASKED TO THE CONTEXT FIRST, AND A $N TO THE PARAMETERS
    if (name.startsWith("$") || SubQueryCollector.isGeneratedAlias(name))
      return NO_SLOT;
    return slotByAlias.getOrDefault(name, NO_SLOT);
  }

  private static boolean hasOnlyPlainItems(final Projection projection) {
    for (final ProjectionItem item : projection.getItems())
      if (item.exclude || item.isAll())
        return false;
    return true;
  }

  /**
   * Whether an aggregate is one of the engine's SQL aggregates, none of which reads or keeps the row it gets besides its
   * arguments: it can be fed without the row, and fed the caching view.
   */
  private static boolean isBuiltIn(final SQLFunction function) {
    return function instanceof SQLAggregatedFunction && function.getName() != null && DefaultSQLFunctionFactory.getInstance()
        .isBuiltIn(function.getName().toLowerCase(Locale.ENGLISH));
  }

  /**
   * Whether evaluating {@code fragment} on the caching view can never hand the view out, so the view can serve the next
   * row: no {@code *}, which answers the row itself, no function call, which gets the row, no nested projection and no
   * sub-query.
   * <p>
   * THE CONTRACT OF {@link PropertyCachingResult}: a node that can answer, or keep, the row it is evaluated on must be
   * refused here. Every other node reads values off the row or computes on values; {@code @this} answers the record, not
   * the row, and {@code $current} is the row the scan set, not the view. A new node type that can answer the row it gets
   * belongs in this list.
   */
  private static boolean isSafeOnView(final Object fragment) {
    return SqlAstInspector.allNodesMatch(fragment, AggregateRowEvaluator::isSafeNodeOnView);
  }

  private static boolean isSafeNodeOnView(final SimpleNode node) {
    return !(node instanceof FunctionCall) && !(node instanceof Statement) && !(node instanceof NestedProjection) && !(
        node instanceof SuffixIdentifier suffix && suffix.star);
  }

  /**
   * A group: its own key in the map of the groups - one object less to reach per row than a key pointing to a group -
   * the values of its non-aggregate projections, its aggregate states, and where in the sequential scan it was first seen.
   */
  static final class Group extends GroupByKey {
    Object[]                   columnValues;
    final AggregationContext[] aggregates;
    long                       firstSeen;

    Group(final GroupByKey key, final Object[] columnValues, final AggregationContext[] aggregates, final long firstSeen) {
      super(key.keyValues.clone(), key);
      this.columnValues = columnValues;
      this.aggregates = aggregates;
      this.firstSeen = firstSeen;
    }

    /** Folds {@code other}, the same group fed other rows, into this one: the aggregates merge, the earliest first row wins. */
    void merge(final Group other) {
      for (int i = 0; i < aggregates.length; i++)
        aggregates[i].merge(other.aggregates[i]);
      if (other.firstSeen < firstSeen) {
        columnValues = other.columnValues;
        firstSeen = other.firstSeen;
      }
    }
  }

  /**
   * The key of a group: the values of the GROUP BY expressions. Numbers meet in a canonical form, so numerically equal
   * keys of different types - {@code Integer(1)} and {@code Long(1)}, {@code BigDecimal("1")} and {@code BigDecimal("1.0")}
   * - are one group (issue #4516).
   * <p>
   * A single key that is an integer - the canonical form of every integral number - is also kept as a {@code long}, and
   * compared as one: a lookup in a map of many groups does not have to reach the boxed key of the group it lands on.
   */
  static class GroupByKey {
    final   Object[] keyValues;
    private int      hashCode;
    private boolean  longKeyed;
    private long     longKey;

    GroupByKey(final Object[] keyValues) {
      this.keyValues = keyValues;
      rehash();
    }

    /** A key of {@code keyValues}, already normalized, hashed like {@code key}. */
    private GroupByKey(final Object[] keyValues, final GroupByKey key) {
      this.keyValues = keyValues;
      this.hashCode = key.hashCode;
      this.longKeyed = key.longKeyed;
      this.longKey = key.longKey;
    }

    /** Normalizes the values just written into {@link #keyValues}, and hashes them. */
    void rehash() {
      for (int i = 0; i < keyValues.length; i++)
        keyValues[i] = Type.normalizeForKey(keyValues[i]);
      hashCode = Arrays.hashCode(keyValues);
      longKeyed = keyValues.length == 1 && keyValues[0] instanceof Long;
      longKey = longKeyed ? (Long) keyValues[0] : 0L;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj)
        return true;
      if (!(obj instanceof GroupByKey other) || hashCode != other.hashCode)
        return false;
      if (longKeyed || other.longKeyed)
        // A Long NEVER EQUALS THE CANONICAL FORM OF ANY OTHER VALUE
        return longKeyed && other.longKeyed && longKey == other.longKey;
      return Arrays.equals(keyValues, other.keyValues);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
