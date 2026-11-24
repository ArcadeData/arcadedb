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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.ComparisonExpression;
import com.arcadedb.query.opencypher.ast.CypherReferencedVariables;
import com.arcadedb.query.opencypher.ast.CypherStatement;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.InExpression;
import com.arcadedb.query.opencypher.ast.ListExpression;
import com.arcadedb.query.opencypher.ast.LiteralExpression;
import com.arcadedb.query.opencypher.ast.MapExpression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.PropertyAccessExpression;
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.opencypher.ast.UnwindClause;
import com.arcadedb.query.opencypher.ast.VariableExpression;
import com.arcadedb.query.opencypher.ast.WithClause;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

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
  /** The direction the walk back follows from the target: set by {@link #rowsPerRow}, which an instance serves once. */
  private Vertex.DIRECTION        walk;

  IndexedTargetCost(final DatabaseInternal database, final CommandContext context, final CypherStatement statement) {
    this.database = database;
    this.context = context;
    this.statement = statement;
  }

  /**
   * The records the walk back reads for one row, or -1 when no index of the target's label can seek its pins, or the cost cannot
   * be known.
   *
   * @param edgeTypes   the types of the hop, whose relationships of the vertices found the walk reads
   * @param written     the direction the hop is written in: the walk from the target follows the opposite one
   * @param labelCount  the vertices of the target's label, for the average degree
   * @param limit       the cost of the scan the walk is weighed against: counting stops there
   */
  double rowsPerRow(final NodePattern target, final String targetVar, final BooleanExpression where, final Set<String> boundVariables,
      final List<String> edgeTypes, final Direction written, final long labelCount, final long limit) {
    walk = written == Direction.BOTH ? Vertex.DIRECTION.BOTH : written == Direction.OUT ? Vertex.DIRECTION.IN : Vertex.DIRECTION.OUT;
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
      } catch (final CommandExecutionException | IllegalArgumentException | ArithmeticException | ClassCastException e) {
        // a key expression the evaluator refuses: the statement fails or answers when it runs, as it would without this estimate, and
        // the written order stays. Only the kind is logged: the message may carry a value
        LogManager.instance().log(this, Level.FINE, "No cost for the walk back from an index of '%s': %s", null,
            target.getLabels().getFirst(), e.getClass().getSimpleName());
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

    // top-level conjuncts only, as the node step reads them: an OR of two equalities is a scan of the type there, not two seeks
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

  /**
   * The keys of a pin as the index holds them, and whether one row seeks them all ({@code true}) or one of them ({@code false}).
   * {@code occurrences} is the number of rows that hold each key (null: one each).
   */
  private record Keys(List<Object> values, int[] occurrences, boolean allOfThemPerRow) {
    double weight(final int position) {
      return occurrences == null ? 1D : occurrences[position];
    }
  }

  /**
   * The keys the pin names in this execution, or null when they depend on something planning cannot read (a row, or a variable that is
   * not bound by one UNWIND of a literal or a parameter) or hold a list or a map, which the node step does not seek. An expression
   * the evaluator refuses is not caught here: {@link #rowsPerRow} keeps the written order for it.
   */
  private Keys resolve(final Pin pin) {
    if (pin.fromIn()) {
      final List<Object> elements = new ArrayList<>();
      final List<Expression> list = pin.elements();
      for (final Expression element : list) {
        if (!RowIndependentValues.isRowIndependent(element))
          return null;
        final Object value = evaluator.evaluate(element, NO_ROW, context);
        if (list.size() == 1 && !(element instanceof LiteralExpression) && value instanceof Collection<?> collection)
          elements.addAll(collection);
        else
          elements.add(value);
      }
      return isScalars(elements) ? new Keys(distinctKeys(elements), null, true) : null;
    }
    final Expression element = pin.elements().getFirst();
    if (RowIndependentValues.isRowIndependent(element)) {
      final Object value = evaluator.evaluate(element, NO_ROW, context);
      return isScalars(Collections.singletonList(value)) ? new Keys(distinctKeys(Collections.singletonList(value)), null, true) : null;
    }
    if (element instanceof VariableExpression variable) {
      final Expression unwound = unwoundList(variable.getVariableName());
      if (unwound != null && evaluator.evaluate(unwound, NO_ROW, context) instanceof Collection<?> rows && isScalars(rows)) {
        final Map<Object, Integer> occurrences = new LinkedHashMap<>();
        for (final Object row : rows)
          if (row != null)
            occurrences.merge(TemporalUtil.toIndexKey(row), 1, Integer::sum);
        return new Keys(new ArrayList<>(occurrences.keySet()), occurrences.values().stream().mapToInt(Integer::intValue).toArray(), false);
      }
    }
    return null;
  }

  /**
   * The list a variable is bound to when one UNWIND of a literal or a parameter binds it and no WITH gives the name another value;
   * null otherwise, where the name may stand for something else than that list.
   */
  private Expression unwoundList(final String name) {
    Expression list = null;
    for (final UnwindClause unwind : statement.getUnwindClauses())
      if (name.equals(unwind.getVariable())) {
        if (list != null)
          return null;
        list = unwind.getListExpression();
      }
    if (list == null || !RowIndependentValues.isRowIndependent(list))
      return null;
    for (final WithClause with : statement.getWithClauses())
      for (final ReturnClause.ReturnItem item : with.getItems())
        if (name.equals(item.getAlias()) && !(item.getExpression() instanceof VariableExpression same && name.equals(same.getVariableName())))
          return null;
    return list;
  }

  /** Whether no value is a list, a map or an array: none of those is a key the node step seeks. */
  private static boolean isScalars(final Collection<?> values) {
    for (final Object value : values)
      if (value instanceof Collection<?> || value instanceof Map<?, ?> || value != null && value.getClass().isArray())
        return false;
    return true;
  }

  private static List<Object> distinctKeys(final Collection<?> values) {
    final Set<Object> distinct = new LinkedHashSet<>();
    for (final Object value : values)
      if (value != null && !(value instanceof Collection<?>) && !(value instanceof Map<?, ?>) && !value.getClass().isArray())
        distinct.add(TemporalUtil.toIndexKey(value));
    return new ArrayList<>(distinct);
  }

  /**
   * The cost of the keys a pin names, from the index and the vertices it finds; -1 when the keys are not known. For one key per row
   * (an UNWIND) it is the mean over the rows, each key weighted by the rows that hold it; for the keys of one row (an IN-list) the sum.
   */
  private double probedCost(final TypeIndex index, final Pin pin, final List<String> edgeTypes, final long limit) {
    final Keys keys = resolve(pin);
    if (keys == null)
      return -1D;
    final List<Object> values = keys.values();
    if (values.isEmpty())
      return 0D;

    final int probed = Math.min(PROBED_KEYS, values.size());
    final int[] chosen = new int[probed];
    double weights = 0D;
    for (int i = 0; i < probed; i++) {
      chosen[i] = (int) ((long) i * values.size() / probed);
      weights += keys.weight(chosen[i]);
    }
    double sum = 0D;
    for (int i = 0; i < probed; i++) {
      sum += keys.weight(chosen[i]) * costOfKey(index, values.get(chosen[i]), edgeTypes, limit);
      // what the keys read so far is a lower bound of the mean: once it fills the scan the rest cannot bring it under
      if (sum / weights >= limit)
        break;
    }
    final double mean = sum / weights;
    return keys.allOfThemPerRow() ? mean * values.size() : mean;
  }

  /** How many times the cost of the scan a key or a vertex that fills it is counted as. */
  private static final long OVER_LIMIT = 64L;

  /** The reads that stand for "far more than the scan": {@value #OVER_LIMIT} times it, at most {@value #MAX_PROBED_ENTRIES}. */
  private static long overLimit(final long limit) {
    return Math.min(Math.max(limit, 1L) * OVER_LIMIT, MAX_PROBED_ENTRIES);
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
    // the walk back from a vertex reads as many entries as it has incoming relationships, which is not bounded for a hub: they are
    // counted, with no record read, up to OVER_LIMIT times the cost of the scan it is weighed against, and a key that already fills
    // that cost is not looked into at all. Both stand for "far more than the scan reads" in the mean over the keys of a list.
    // A probe stopped by the hard cap before it filled the scan says only that the key is large: it is over the scan too
    final long ceiling = overLimit(limit);
    final double over = 1D + Math.max(ceiling, limit);
    if (entries >= limit || entries >= MAX_PROBED_ENTRIES)
      return over;
    final String[] types = edgeTypes.toArray(new String[0]);
    final int probed = Math.min(PROBED_VERTICES, kept.size());
    long inEdges = 0L;
    int loaded = 0;
    for (int i = 0; i < probed; i++)
      if (kept.get((int) ((long) i * kept.size() / probed)).getRecord(false) instanceof Vertex vertex) {
        long degree = 0L;
        for (final RID ignored : vertex.getConnectedVertexRIDs(walk, types))
          if (++degree >= ceiling)
            break;
        // a vertex stopped by the hard cap before it filled the scan is a hub of unknown size: over the scan as well
        if (degree >= ceiling && ceiling < limit)
          return over;
        inEdges += degree;
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
    // the average over the target label: an undirected hop walks both lists of a vertex
    final double averageDegree = labelCount > 0L ? (double) edges * (walk == Vertex.DIRECTION.BOTH ? 2D : 1D) / labelCount : 0D;
    return seeks * (1D + perKey * (1D + averageDegree));
  }

  /**
   * The keys of a pin when only the plan can count them: its elements, -1 for one parameter or variable that stands for a list and
   * for a list that holds a list or a map (which the node step does not seek: it scans the type).
   */
  private static long keysOf(final Pin pin) {
    final List<Expression> list = pin.elements();
    if (pin.fromIn() && list.size() == 1 && !(list.getFirst() instanceof LiteralExpression))
      return -1L;
    for (final Expression element : list)
      if (element instanceof ListExpression || element instanceof MapExpression)
        return -1L;
    return list.size();
  }

  /**
   * The entries of the largest key at either end of an ordered, NOTUNIQUE index: the longest run of equal keys in its first and in its
   * last {@value #KEY_SAMPLE_ENTRIES} entries in key order, the larger of the two. A run that fills the sample says only that the key
   * holds at least that many entries: -1, no estimate. The largest run, not the mean, because the plan weighs the seek of one key:
   * a key that became large after an earlier statement was planned is seen as long as it sorts at an end, with nothing kept between
   * statements to go stale. A key that is large in the middle of the key order is the limit of the sample.
   */
  private static long entriesPerKey(final TypeIndex index) {
    final long ascending = largestRunAtAnEnd(index, true);
    final long descending = ascending < 0L ? -1L : largestRunAtAnEnd(index, false);
    return ascending < 0L || descending < 0L ? -1L : Math.max(ascending, descending);
  }

  private static long largestRunAtAnEnd(final TypeIndex index, final boolean ascending) {
    long entries = 0L;
    long run = 0L;
    long largest = 1L;
    List<Object> previous = null;
    final IndexCursor cursor = index.iterator(ascending);
    try {
      while (entries < KEY_SAMPLE_ENTRIES && cursor.hasNext()) {
        cursor.next();
        final List<Object> key = Arrays.asList(cursor.getKeys());
        run = key.equals(previous) ? run + 1L : 1L;
        previous = key;
        largest = Math.max(largest, run);
        ++entries;
      }
    } finally {
      cursor.close();
    }
    return largest >= KEY_SAMPLE_ENTRIES ? -1L : largest;
  }
}
