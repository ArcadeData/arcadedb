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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.executor.operators.InListValues;
import com.arcadedb.query.opencypher.optimizer.statistics.GraphStatisticsCache;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What it costs to start a hop from the node an index pins instead of from the node written first (issue #13267): the number of
 * records the walk back reads for one row of the clauses before the MATCH, to be weighed against the scan of the first node's label,
 * which reads every vertex of it for every row.
 * <p>
 * The walk back is one index seek per key, and for each vertex found all its incoming relationships of the hop's types, whichever
 * label their other end has. So the cost of a key is {@code 1 + sum over its vertices of (1 + in-degree)}. Where the keys are known
 * when the statement is planned - a literal, a parameter, an {@code IN}-list, or a variable an {@code UNWIND} of a literal or a
 * parameter binds - the index and the vertices are asked: a few keys are probed, spread over the list, and the rest read as the
 * mean of those (a hub is seen as one, a key with many entries is counted). Where they are not, the cost is estimated from the
 * entries per key of the index, read from both ends of its key order, and the average in-degree of the type; an estimate that
 * cannot be made says {@code -1}, and the written order stays.
 */
final class IndexedTargetCost {
  /** The keys probed against the index for one pin, spread over its values. */
  private static final int PROBED_KEYS       = 32;
  /** The vertices of one key whose in-degree is read, the others reading as their mean. */
  private static final int PROBED_VERTICES   = 32;
  /** The entries of one key kept to choose the vertices whose in-degree is read from. */
  private static final int KEPT_ENTRIES      = 4096;
  /** The most entries one probe asks the index for. */
  private static final int MAX_PROBED_ENTRIES = 100_000;
  /** The entries read from each end of an index to estimate its entries per key. */
  private static final int KEY_SAMPLE_ENTRIES = 256;

  private static final Result NO_ROW = new ResultInternal(Collections.emptyMap());

  /** The values a property of the target is pinned to: one row's keys ({@code allKeys}), or one key per row of an UNWIND. */
  private record Pin(List<Expression> elements, boolean fromIn) {
  }

  private final DatabaseInternal  database;
  private final CommandContext    context;
  private final CypherStatement   statement;
  private final ExpressionEvaluator evaluator = new ExpressionEvaluator(new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance()));

  IndexedTargetCost(final DatabaseInternal database, final CommandContext context, final CypherStatement statement) {
    this.database = database;
    this.context = context;
    this.statement = statement;
  }

  /**
   * The records the walk back reads for one row, or -1 when no index of the target's label can seek its pins, or the cost cannot
   * be known.
   *
   * @param edgeTypes   the types of the hop, whose incoming relationships the walk reads
   * @param labelCount  the vertices of the target's label, for the average in-degree
   * @param limit       the cost of the scan the walk is weighed against: counting stops there
   */
  double rowsPerRow(final NodePattern target, final String targetVar, final BooleanExpression where, final Set<String> boundVariables,
      final List<String> edgeTypes, final long labelCount, final long limit) {
    if (!target.hasLabels() || target.getLabels().size() != 1 || !database.getSchema().existsType(target.getLabels().getFirst()))
      return -1D;
    final Map<String, Pin> pins = collectPins(target, targetVar, where, boundVariables);
    if (pins.isEmpty())
      return -1D;

    double best = -1D;
    for (final TypeIndex index : database.getSchema().getType(target.getLabels().getFirst()).getAllIndexes(true)) {
      final List<String> keys = index.isReadyForQueries() ? index.getPropertyNamesIfExactKeyLookup() : null;
      if (keys == null)
        continue;
      final Pin[] pinned = new Pin[keys.size()];
      boolean covered = true;
      for (int i = 0; i < pinned.length && covered; i++)
        covered = (pinned[i] = pins.get(keys.get(i))) != null;
      if (!covered)
        continue;
      double cost = -1D;
      try {
        cost = keys.size() == 1 ? probedCost(index, pinned[0], edgeTypes, limit) : -1D;
        if (cost < 0D)
          cost = estimatedCost(index, pinned, edgeTypes, labelCount);
      } catch (final IndexException | RecordNotFoundException e) {
        // dropped, rebuilt or emptied since it was checked: no estimate from this index
        cost = -1D;
      }
      if (cost >= 0D && (best < 0D || cost < best))
        best = cost;
    }
    return best;
  }

  // ---- the pins ----

  private Map<String, Pin> collectPins(final NodePattern target, final String targetVar, final BooleanExpression where,
      final Set<String> boundVariables) {
    final Map<String, Pin> pins = new HashMap<>();
    if (target.hasProperties() && target.getPropertiesParameterName() == null)
      for (final Map.Entry<String, Object> entry : target.getProperties().entrySet()) {
        final Expression value = EdgeIndexSeedPlanner.asExpression(entry.getValue());
        if (value != null && readsOnly(value, boundVariables))
          keepFewer(pins, entry.getKey(), new Pin(List.of(value), false));
      }
    if (targetVar == null || where == null)
      return pins;

    final Map<String, Object> rowIndependent = new LinkedHashMap<>();
    EdgeIndexSeedPlanner.collectWherePredicates(targetVar, where, rowIndependent);
    for (final Map.Entry<String, Object> entry : rowIndependent.entrySet())
      keepFewer(pins, entry.getKey(), entry.getValue() instanceof InListValues inList ? new Pin(inList.getValues(), true)
          : new Pin(List.of((Expression) entry.getValue()), false));

    for (final BooleanExpression conjunct : CountPushDownPredicates.conjuncts(where)) {
      if (conjunct instanceof ComparisonExpression comparison && comparison.getOperator() == ComparisonExpression.Operator.EQUALS) {
        if (comparison.getLeft() instanceof PropertyAccessExpression left && targetVar.equals(left.getVariableName())
            && readsOnly(comparison.getRight(), boundVariables))
          keepFewer(pins, left.getPropertyName(), new Pin(List.of(comparison.getRight()), false));
        else if (comparison.getRight() instanceof PropertyAccessExpression right && targetVar.equals(right.getVariableName())
            && readsOnly(comparison.getLeft(), boundVariables))
          keepFewer(pins, right.getPropertyName(), new Pin(List.of(comparison.getLeft()), false));
      } else if (conjunct instanceof InExpression in && in.getExpression() instanceof PropertyAccessExpression listed
          && targetVar.equals(listed.getVariableName()) && in.getList() != null && !in.getList().isEmpty()
          && in.getList().stream().allMatch(value -> readsOnly(value, boundVariables)))
        keepFewer(pins, listed.getPropertyName(), new Pin(unwrapped(in.getList()), true));
    }
    return pins;
  }

  private static boolean readsOnly(final Expression value, final Set<String> boundVariables) {
    if (RowIndependentValues.isSeekable(value))
      return true;
    if (!RowIndependentValues.isDeterministic(value))
      return false;
    final CypherReferencedVariables referenced = CypherReferencedVariables.of(value);
    return referenced.isComplete() && boundVariables.containsAll(referenced.getNames());
  }

  private static List<Expression> unwrapped(final List<Expression> list) {
    return list.size() == 1 && list.getFirst() instanceof ListExpression written ? written.getElements() : list;
  }

  private static void keepFewer(final Map<String, Pin> pins, final String property, final Pin pin) {
    final Pin existing = pins.get(property);
    if (existing == null || pin.elements().size() < existing.elements().size())
      pins.put(property, pin);
  }

  // ---- keys known at planning time ----

  /** The keys of a pin as the index holds them, and whether one row seeks them all ({@code true}) or one of them ({@code false}). */
  private record Keys(List<Object> values, boolean allOfThemPerRow) {
  }

  /** The keys the pin names in this execution, or null when they depend on something planning cannot read. */
  private Keys resolve(final Pin pin) {
    try {
      if (pin.fromIn()) {
        final List<Object> elements = new ArrayList<>();
        final List<Expression> list = pin.elements();
        for (final Expression element : list) {
          if (!RowIndependentValues.isRowIndependent(element))
            return null;
          final Object value = evaluator.evaluate(element, NO_ROW, context);
          if (list.size() == 1 && !(element instanceof LiteralExpression) && value instanceof Collection<?> collection)
            elements.addAll(collection);
          else if (value instanceof Collection<?> || value instanceof Map<?, ?> || value != null && value.getClass().isArray())
            return null;
          else
            elements.add(value);
        }
        return new Keys(distinctKeys(elements), true);
      }
      final Expression element = pin.elements().getFirst();
      if (RowIndependentValues.isRowIndependent(element)) {
        final Object value = evaluator.evaluate(element, NO_ROW, context);
        if (value instanceof Collection<?> || value instanceof Map<?, ?> || value != null && value.getClass().isArray())
          return null;
        return new Keys(distinctKeys(Collections.singletonList(value)), true);
      }
      if (element instanceof VariableExpression variable)
        for (final UnwindClause unwind : statement.getUnwindClauses())
          if (variable.getVariableName().equals(unwind.getVariable()) && RowIndependentValues.isRowIndependent(unwind.getListExpression())) {
            final Object list = evaluator.evaluate(unwind.getListExpression(), NO_ROW, context);
            if (list instanceof Collection<?> collection)
              return new Keys(distinctKeys(collection), false);
          }
    } catch (final RuntimeException e) {
      // a value only a row can answer, or one the evaluator refuses: the estimate stands in
    }
    return null;
  }

  private static List<Object> distinctKeys(final Collection<?> values) {
    final Set<Object> distinct = new LinkedHashSet<>();
    for (final Object value : values)
      if (value != null && !(value instanceof Collection<?>) && !(value instanceof Map<?, ?>) && !value.getClass().isArray())
        distinct.add(TemporalUtil.toIndexKey(value));
    return new ArrayList<>(distinct);
  }

  /** The cost of the keys a pin names, from the index and the vertices it finds; -1 when the keys are not known. */
  private double probedCost(final TypeIndex index, final Pin pin, final List<String> edgeTypes, final long limit) {
    final Keys keys = resolve(pin);
    if (keys == null)
      return -1D;
    final List<Object> values = keys.values();
    if (values.isEmpty())
      return 0D;

    final int probed = Math.min(PROBED_KEYS, values.size());
    double sum = 0D;
    for (int i = 0; i < probed; i++)
      sum += costOfKey(index, values.get((int) ((long) i * values.size() / probed)), edgeTypes, limit);
    final double mean = sum / probed;
    return keys.allOfThemPerRow() ? mean * values.size() : mean;
  }

  /**
   * One seek and the walk back from the vertices it finds: the entries are counted up to the limit, and the in-degree is read for
   * a few of the first {@value #KEPT_ENTRIES}, spread over them, so a hub among many entries is seen as a share of them.
   */
  private double costOfKey(final TypeIndex index, final Object key, final List<String> edgeTypes, final long limit) {
    long entries = 0L;
    final List<Identifiable> kept = new ArrayList<>();
    // the lookup is asked for no more entries than the plan it is weighed against reads: a key of 100,000 entries is not collected
    // to find out it is worse than a scan of 3 vertices
    final IndexCursor cursor = index.get(new Object[] { key }, (int) Math.min(Math.max(limit, 1L), MAX_PROBED_ENTRIES));
    try {
      while (cursor.hasNext() && entries < limit) {
        final Identifiable found = cursor.next();
        ++entries;
        if (kept.size() < KEPT_ENTRIES)
          kept.add(found);
      }
    } finally {
      cursor.close();
    }
    final String[] types = edgeTypes.toArray(new String[0]);
    final int probed = Math.min(PROBED_VERTICES, kept.size());
    long inEdges = 0L;
    int loaded = 0;
    for (int i = 0; i < probed; i++)
      if (kept.get((int) ((long) i * kept.size() / probed)).getRecord(false) instanceof Vertex vertex) {
        inEdges += vertex.countEdges(Vertex.DIRECTION.IN, types);
        ++loaded;
      }
    final double meanInDegree = loaded == 0 ? 0D : (double) inEdges / loaded;
    return 1D + entries * (1D + meanInDegree);
  }

  // ---- keys only a row knows ----

  /** The cost of one seek of the index from its entries per key and the average in-degree of the hop's types; -1 when unknown. */
  private double estimatedCost(final TypeIndex index, final Pin[] pinned, final List<String> edgeTypes, final long labelCount) {
    long seeks = 1L;
    for (final Pin pin : pinned) {
      final long count = keysOf(pin);
      // MatchNodeStep seeks a list of keys on an index of one column only
      if (count < 0L || count > 1L && pinned.length > 1)
        return -1D;
      seeks *= count;
    }
    final long perKey;
    if (index.isUnique())
      perKey = 1L;
    else {
      if (!index.supportsOrderedIterations())
        return -1D;
      perKey = entriesPerKey(index);
      if (perKey < 0L)
        return -1D;
    }
    long edges = 0L;
    for (final String type : edgeTypes)
      edges += database.countType(type, true);
    final double averageInDegree = labelCount > 0L ? (double) edges / labelCount : 0D;
    return seeks * (1D + perKey * (1D + averageInDegree));
  }

  /** The keys of a pin when only the plan can count them: its elements, -1 for one parameter or variable that stands for a list. */
  private static long keysOf(final Pin pin) {
    final List<Expression> list = pin.elements();
    return pin.fromIn() && list.size() == 1 && !(list.getFirst() instanceof LiteralExpression) ? -1L : list.size();
  }

  /**
   * The entries per distinct key of an ordered, NOTUNIQUE index, from its first and its last {@value #KEY_SAMPLE_ENTRIES} entries in
   * key order, the larger of the two. A sample that one key fills only says the key holds at least that many entries: -1, no
   * estimate. A key that is large in the middle of the key order is the limit of the sample. The answer is kept for as long as the
   * index holds the same number of entries.
   */
  private long entriesPerKey(final TypeIndex index) {
    final GraphStatisticsCache cache = database.getGraphStatisticsCache();
    final long total = index.countEntries();
    if (cache != null) {
      final Long cached = cache.getEntriesPerKey(index.getName(), total);
      if (cached != null)
        return cached;
    }
    final long ascending = sampleEntriesPerKey(index, true);
    final long descending = ascending < 0L ? -1L : sampleEntriesPerKey(index, false);
    final long perKey = ascending < 0L || descending < 0L ? -1L : Math.max(ascending, descending);
    if (cache != null)
      cache.putEntriesPerKey(index.getName(), perKey, total);
    return perKey;
  }

  private static long sampleEntriesPerKey(final TypeIndex index, final boolean ascending) {
    final Set<List<Object>> keys = new HashSet<>();
    long entries = 0L;
    final IndexCursor cursor = index.iterator(ascending);
    try {
      while (entries < KEY_SAMPLE_ENTRIES && cursor.hasNext()) {
        cursor.next();
        keys.add(Arrays.asList(cursor.getKeys()));
        ++entries;
      }
    } finally {
      cursor.close();
    }
    if (keys.isEmpty())
      return 1L;
    return entries == KEY_SAMPLE_ENTRIES && keys.size() == 1 ? -1L : Math.max(1L, (entries + keys.size() - 1L) / keys.size());
  }
}
