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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.ConsistentKeyLookup;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.EdgeIndexSeedPlanner;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.executor.operators.InListValues;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Leading (seed) execution step that drives a single-hop relationship pattern from the edge type's own
 * index, instead of scanning every vertex and expanding every edge.
 * <p>
 * It serves the shape {@code MATCH (a)-[t:TYPE]->(b) WHERE t.k1 = v1 AND t.k2 IN [v2, v3] ...} where the
 * endpoints carry no selective constraint and the edge type has an index whose key - or a leading prefix
 * of it - is covered by equality or {@code IN}-list predicates on the edge. Mirrors the SQL
 * {@code FETCH FROM INDEX} path (issue #740): OpenCypher's planner otherwise only ever uses an index to
 * anchor a <em>vertex</em> scan, never an edge.
 * <p>
 * Each key column carries either one value expression (an equality) or an {@link InListValues} (an
 * {@code IN}-list, or an {@code OR} of equalities on the same property). The seek keys are the cartesian
 * product of the resolved column values: a key covering every index column is an exact lookup, a shorter
 * one walks the contiguous range of the ordered index sharing that prefix - the same two shapes
 * {@code NodeIndexSeek} answers for a vertex anchor. Several seek keys are chained one after the other,
 * with result RIDs de-duplicated so set membership is honoured whatever the list repeats.
 * <p>
 * The MATCH clause's {@code WHERE} filter is still applied above this step, so any predicate not part of
 * the index key (or any looser interpretation) is re-validated - this step is only ever a selective
 * prefilter, never the final word on membership.
 * <p>
 * The one constraint the {@code WHERE} filter above cannot re-validate is the one the <em>pattern</em>
 * carries rather than the predicate: writing the same variable at both endpoints, as in
 * {@code (a)-[t:TYPE]->(a)}, asks for self-loops only. Binding both endpoints under one name would let the
 * second {@code setProperty} silently overwrite the first and pass a non-self-loop edge off as a match
 * (issue #7008), so this step enforces {@code out == in} itself for that shape and keeps the index seek.
 * <p>
 * Since issue #11481 the step serves every spelling of the same lookup, not only the one above: a union of types is one seek per
 * type, one after the other ({@link EdgeIndexSeedPlanner#planSeeks}); the relationship's inline property map, which no filter above
 * the step re-applies, is checked on each edge here; and so are the labels and inline properties of the endpoints, which let the
 * seek answer {@code (n:Entity)-[e:T]->(m:Entity)} instead of a scan of every Entity and an expansion of every edge.
 */
public class MatchEdgeByIndexStep extends AbstractExecutionStep {
  /** One seek per relationship type of the pattern, walked in order. */
  private final List<EdgeIndexSeedPlanner.TypeSeek> seeks;
  private final Direction direction;
  private final String    sourceVariable;
  private final String    relationshipVariable; // may be null when the edge is anonymous
  private final String    targetVariable;
  // True when the pattern repeats one variable at both endpoints - (a)-[t:TYPE]->(a) - which constrains the
  // hop to self-loops. Anonymous endpoints get distinct generated names, so only an explicit repeat sets it.
  private final boolean   selfLoopOnly;
  /** What the pattern asks of the vertices bound to the source and the target, checked on each edge found */
  private final EdgeIndexSeedPlanner.EndpointFilter sourceFilter;
  private final EdgeIndexSeedPlanner.EndpointFilter targetFilter;
  /** The relationship's inline property map, which no filter above re-applies; null when it has none */
  private final Map<String, Object>                 relationshipProperties;

  private final ExpressionEvaluator evaluator;

  // Held at the step level (not local to the syncPull() ResultSet) so close() can release it, matching the
  // AutoCloseable contract on IndexCursor and this codebase's convention for any step holding one (see
  // FetchFromIndexStep). A prefix seek opens a live range cursor over the ordered index, so unlike the
  // full-key lookup (which returns an already-drained collection) it can hold LSM resources across pulls.
  private IndexCursor cursor;

  // Read-only empty row used to evaluate the (row-independent) key value expressions - literals,
  // parameters and functions such as date('...'). Mirrors MatchNodeStep.EMPTY_RESULT.
  private static final Result EMPTY_RESULT = new ResultInternal(Collections.emptyMap());

  /**
   * @param seeks                  one per relationship type, see {@link EdgeIndexSeedPlanner#planSeeks}
   * @param sourceFilter           what the source endpoint asks of its vertex, {@link EdgeIndexSeedPlanner.EndpointFilter#NONE} for nothing
   * @param targetFilter           the same for the target endpoint
   * @param relationshipProperties the relationship's inline property map, null when it has none
   */
  public MatchEdgeByIndexStep(final List<EdgeIndexSeedPlanner.TypeSeek> seeks, final Direction direction,
      final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final EdgeIndexSeedPlanner.EndpointFilter sourceFilter, final EdgeIndexSeedPlanner.EndpointFilter targetFilter,
      final Map<String, Object> relationshipProperties, final CommandContext context) {
    super(context);
    for (final EdgeIndexSeedPlanner.TypeSeek seek : seeks)
      if (seek.propertyNames().length != seek.keyValues().length)
        throw new IllegalArgumentException("One key value per covered index column is required");
    this.seeks = seeks;
    this.direction = direction;
    this.sourceVariable = sourceVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.selfLoopOnly = sourceVariable != null && sourceVariable.equals(targetVariable);
    this.sourceFilter = sourceFilter != null ? sourceFilter : EdgeIndexSeedPlanner.EndpointFilter.NONE;
    this.targetFilter = targetFilter != null ? targetFilter : EdgeIndexSeedPlanner.EndpointFilter.NONE;
    this.relationshipProperties = relationshipProperties == null || relationshipProperties.isEmpty() ? null : relationshipProperties;
    this.evaluator = new ExpressionEvaluator(new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance()));
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private boolean            initialized = false;
      private final List<Result> buffer      = new ArrayList<>();
      private int                bufferIndex = 0;
      private boolean            finished    = false;

      /** The seek of {@link #seeks} being walked, -1 before the first. */
      private int                           typeIndex = -1;
      private EdgeIndexSeedPlanner.TypeSeek seek;
      private Index          index;
      private boolean        wholeKey;
      private List<Object[]> seekKeys  = Collections.emptyList();
      private int            seekIndex = 0;
      /** RIDs already emitted, kept only when more than one seek key could yield the same edge. */
      private Set<RID>       seen;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      /** Moves to the next relationship type's seek, or returns false when every one has been walked. */
      private boolean nextTypeSeek() {
        while (++typeIndex < seeks.size()) {
          seek = seeks.get(typeIndex);
          index = context.getDatabase().getSchema().getIndexByName(seek.indexName());
          if (index == null)
            // The planner picked an index that the schema no longer offers. Returning an empty result set
            // here would silently drop rows the query must return, so fail instead (as NodeIndexSeek does).
            throw new CommandExecutionException(
                "Index '" + seek.indexName() + "' on type '" + seek.edgeType() + "' is no longer available: re-plan the query");

          seekKeys = resolveSeekKeys(seek.keyValues());
          seekIndex = 0;
          wholeKey = !seekKeys.isEmpty() && seekKeys.getFirst().length == index.getPropertyNames().size();
          // An edge has one type, so the seeks of two types never return the same one: the set spans one type's keys
          seen = seekKeys.size() > 1 ? new HashSet<>() : null;
          if (!seekKeys.isEmpty())
            return true;
        }
        return false;
      }

      /** Opens the cursor for the next seek key, or returns false when every key of every type has been walked. */
      private boolean openNextCursor() {
        releaseCursor();
        if (seekIndex >= seekKeys.size() && !nextTypeSeek())
          return false;

        if (wholeKey && seekIndex == 0) {
          // every key of the seek is read with its records against ONE committed state (#9397)
          cursor = ConsistentKeyLookup.lookupCursor(context.getDatabase(), index, seekKeys);
          if (cursor != null) {
            seekIndex = seekKeys.size();
            return true;
          }
        }
        final Object[] key = seekKeys.get(seekIndex++);
        // A key covering every index column identifies at most one entry per RID; a shorter one matches a
        // contiguous range of the ordered index, which only the range cursor can walk.
        cursor = wholeKey ? index.get(key) : ((RangeIndex) index).range(true, key, true, key, true);
        return true;
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        if (!initialized) {
          initialized = true;
          if (!openNextCursor()) {
            finished = true;
            return;
          }
        }

        while (buffer.size() < n) {
          guard.check();

          if (!cursor.hasNext()) {
            if (!openNextCursor()) {
              finished = true;
              return;
            }
            continue;
          }

          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            final Identifiable identifiable = cursor.next();

            // Set semantics over several seek keys: an edge reached through two of them is one answer.
            if (seen != null && !seen.add(identifiable.getIdentity()))
              continue;

            final Edge edge;
            try {
              final var record = identifiable.getRecord();
              if (!(record instanceof Edge))
                continue;
              edge = (Edge) record;

              // A repeated endpoint variable constrains the hop to self-loops. This reads the two RIDs the
              // edge carries rather than resolving the vertex records, so a rejected edge costs no extra
              // load - but getOut()/getIn() still lazy-load the EDGE's own content, so the comparison has to
              // sit under the same dangling-entry guard as the resolution above.
              if (selfLoopOnly && !edge.getOut().equals(edge.getIn()))
                continue;
            } catch (final RecordNotFoundException e) {
              // Dangling index entry pointing at a removed edge: skip it, like every other RID resolver.
              continue;
            }

            // An index inherited from a parent edge type spans the whole hierarchy, so its cursor also
            // carries the parent's own edges and every sibling's. A relationship pattern matches a type and
            // its subtypes, never its ancestors, so those are not answers (issue #7021).
            //
            // Unconditional, unlike the equivalent in NodeIndexSeek/MatchNodeStep, which skip the check
            // entirely for an index the queried type owns: those pay a bucket-id lookup per row to learn the
            // type, while the record here is already loaded (the Edge cast above needs it), so the check is
            // one instanceOf on a type reference already in hand and a flag to skip it would cost more to
            // carry than to ignore.
            if (!edge.getType().instanceOf(seek.edgeType()))
              continue;

            // The inline map is a filter only this step applies: the WHERE above does not hold it (issue #11481). It also
            // decides what the index key conversion let through, a '5' found by the key 5
            if (relationshipProperties != null && !InlineProperties.matches(edge, relationshipProperties, EMPTY_RESULT, context))
              continue;

            final ResultInternal result = new ResultInternal();
            if (relationshipVariable != null && !relationshipVariable.isEmpty())
              result.setProperty(relationshipVariable, edge);

            // Bind the endpoints from the edge, honouring the written direction. For OUT the source is
            // the edge's OUT vertex; for IN the pattern is written (a)<-[t]-(b), so the source (a) is the
            // edge's IN vertex.
            try {
              final Vertex source = direction == Direction.IN ? edge.getInVertex() : edge.getOutVertex();
              final Vertex target = direction == Direction.IN ? edge.getOutVertex() : edge.getInVertex();
              // The endpoints' labels and inline properties, which the seek answers instead of a scan of the label (#11481)
              if (!sourceFilter.isEmpty() && !sourceFilter.matches(source, EMPTY_RESULT, context))
                continue;
              if (!targetFilter.isEmpty() && !targetFilter.matches(target, EMPTY_RESULT, context))
                continue;
              result.setProperty(sourceVariable, source);
              result.setProperty(targetVariable, target);
            } catch (final RecordNotFoundException e) {
              // Ghost endpoint (dangling edge left by a rollback/HA resync): skip the row.
              continue;
            }

            buffer.add(result);
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }

      @Override
      public void close() {
        MatchEdgeByIndexStep.this.close();
      }
    };
  }

  @Override
  public void close() {
    releaseCursor();
    super.close();
  }

  /**
   * Releases whatever the current seek opened, honouring the {@code IndexCursor} contract regardless of how the
   * current or a future index implementation backs it.
   */
  private void releaseCursor() {
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
  }

  /**
   * Resolves the key values at execution time (parameters change per execution) and expands them into the seek
   * keys: one per combination of the per-column values. A column whose values all resolve to {@code null} - an
   * unbound parameter, an empty list - truncates the key there, degrading the seek to the shorter prefix the
   * {@code WHERE} filter above re-validates; a leading column with no value yields no seek at all, as no edge
   * can equal {@code null}.
   */
  private List<Object[]> resolveSeekKeys(final Object[] keyValues) {
    final List<List<Object>> columns = new ArrayList<>(keyValues.length);
    for (final Object keyValue : keyValues) {
      final List<Object> values = resolveColumnValues(keyValue);
      if (values.isEmpty())
        break;
      columns.add(values);
    }
    if (columns.isEmpty())
      return Collections.emptyList();

    List<Object[]> keys = new ArrayList<>();
    keys.add(new Object[0]);
    for (final List<Object> values : columns) {
      final List<Object[]> expanded = new ArrayList<>(keys.size() * values.size());
      for (final Object[] prefix : keys)
        for (final Object value : values) {
          final Object[] key = new Object[prefix.length + 1];
          System.arraycopy(prefix, 0, key, 0, prefix.length);
          key[prefix.length] = value;
          expanded.add(key);
        }
      keys = expanded;
    }
    return keys;
  }

  private List<Object> resolveColumnValues(final Object keyValue) {
    final Set<Object> values = new LinkedHashSet<>();
    if (keyValue instanceof InListValues inList) {
      for (final Expression element : inList.getValues()) {
        final Object resolved = coerceForIndexKey(evaluator.evaluate(element, EMPTY_RESULT, context));
        // A single parameter may stand for the whole list (t.k IN $ids), so a multi-value element expands.
        if (resolved instanceof Collection<?> collection) {
          for (final Object v : collection)
            addValue(values, coerceForIndexKey(v));
        } else if (resolved != null && resolved.getClass().isArray()) {
          for (final Object v : MultiValue.getMultiValueAsList(resolved))
            addValue(values, coerceForIndexKey(v));
        } else
          addValue(values, resolved);
      }
    } else
      addValue(values, coerceForIndexKey(evaluator.evaluate((Expression) keyValue, EMPTY_RESULT, context)));
    return new ArrayList<>(values);
  }

  private static void addValue(final Set<Object> values, final Object value) {
    if (value != null)
      values.add(value);
  }

  /**
   * Unwraps an OpenCypher temporal wrapper to its underlying {@code java.time} value so the LSM index
   * key coercion (which accepts {@code java.time} types and strings, but not the Cypher wrappers) can
   * match the stored key. Any other value is passed through unchanged.
   */
  private static Object coerceForIndexKey(final Object value) {
    if (value instanceof CypherDate d)
      return d.getValue();
    if (value instanceof CypherDateTime dt)
      return dt.getValue();
    if (value instanceof CypherLocalDateTime ldt)
      return ldt.getValue();
    if (value instanceof CypherLocalTime lt)
      return lt.getValue();
    if (value instanceof CypherTime t)
      return t.getValue();
    return value;
  }

  /**
   * {@code MATCH EDGE BY INDEX (a)-[t:T]->(b) [index: ...] [key: ...]}, with one index and key per relationship type of a union. Shared
   * with the cost-based plan's {@code EdgeIndexSeek}, which runs this step.
   */
  public String describe(final Schema schema) {
    return describe(schema, seeks, direction, sourceVariable, relationshipVariable, targetVariable, sourceFilter, targetFilter);
  }

  /** The description of a seek with these parts, for an operator that has no step at hand to ask. */
  public static String describe(final Schema schema, final List<EdgeIndexSeedPlanner.TypeSeek> seeks, final Direction direction,
      final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final EdgeIndexSeedPlanner.EndpointFilter sourceFilter, final EdgeIndexSeedPlanner.EndpointFilter targetFilter) {
    final StringBuilder builder = new StringBuilder("MATCH EDGE BY INDEX ");
    builder.append("(").append(sourceVariable).append(labelsOf(sourceFilter)).append(")");
    final String arrowRel = relationshipVariable != null && !relationshipVariable.isEmpty()
        && !relationshipVariable.startsWith("  ") ? relationshipVariable : "";
    final StringBuilder types = new StringBuilder();
    for (final EdgeIndexSeedPlanner.TypeSeek seek : seeks)
      types.append(types.isEmpty() ? ":" : "|").append(seek.edgeType());
    if (direction == Direction.IN)
      builder.append("<-[").append(arrowRel).append(types).append("]-");
    else
      builder.append("-[").append(arrowRel).append(types).append("]->");
    builder.append("(").append(targetVariable).append(labelsOf(targetFilter)).append(")");
    for (final EdgeIndexSeedPlanner.TypeSeek seek : seeks) {
      // The whole index key, then every column the seek resolves, so a prefix seek or an IN-list on a composite
      // index is recognizable at a glance. A dropped index falls back to the covered columns.
      final Index index = schema.existsIndex(seek.indexName()) ? schema.getIndexByName(seek.indexName()) : null;
      final List<String> indexKey = index != null ? index.getPropertyNames() : List.of(seek.propertyNames());
      builder.append(" [index: ").append(seek.indexName()).append(" on ").append(String.join(",", indexKey)).append("]");
      builder.append(" [key: ");
      for (int i = 0; i < seek.propertyNames().length; i++) {
        if (i > 0)
          builder.append(", ");
        builder.append(seek.propertyNames()[i]);
        if (seek.keyValues()[i] instanceof InListValues inList) {
          builder.append(" IN [");
          for (int j = 0; j < inList.getValues().size(); j++) {
            if (j > 0)
              builder.append(", ");
            builder.append(inList.getValues().get(j).getText());
          }
          builder.append("]");
        } else
          builder.append(" = ").append(((Expression) seek.keyValues()[i]).getText());
      }
      builder.append("]");
    }
    return builder.toString();
  }

  private static String labelsOf(final EdgeIndexSeedPlanner.EndpointFilter filter) {
    return filter.labels().isEmpty() ? "" : ":" + String.join(filter.disjunction() ? "|" : ":", filter.labels());
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append("  ".repeat(Math.max(0, depth * indent)));
    builder.append("+ ").append(describe(context.getDatabase().getSchema()));
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
