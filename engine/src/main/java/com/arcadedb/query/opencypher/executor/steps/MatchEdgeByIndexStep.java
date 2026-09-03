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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.temporal.CypherDate;
import com.arcadedb.query.opencypher.temporal.CypherDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalDateTime;
import com.arcadedb.query.opencypher.temporal.CypherLocalTime;
import com.arcadedb.query.opencypher.temporal.CypherTime;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Leading (seed) execution step that drives a single-hop relationship pattern from the edge type's own
 * index, instead of scanning every vertex and expanding every edge.
 * <p>
 * It serves the shape {@code MATCH (a)-[t:TYPE]->(b) WHERE t.k1 = v1 AND t.k2 = v2 ...} where the
 * endpoints carry no selective constraint and the edge type has an index whose key is fully covered by
 * the equality predicates. Mirrors the SQL {@code FETCH FROM INDEX} path (issue #740): OpenCypher's
 * planner otherwise only ever uses an index to anchor a <em>vertex</em> scan, never an edge.
 * <p>
 * The lookup is an equality seek on the full index key, so it returns exactly the edges carrying those
 * key values. The MATCH clause's {@code WHERE} filter is still applied above this step, so any predicate
 * not part of the index key (or any looser interpretation) is re-validated - this step is only ever a
 * selective prefilter, never the final word on membership.
 * <p>
 * The one constraint the {@code WHERE} filter above cannot re-validate is the one the <em>pattern</em>
 * carries rather than the predicate: writing the same variable at both endpoints, as in
 * {@code (a)-[t:TYPE]->(a)}, asks for self-loops only. Binding both endpoints under one name would let the
 * second {@code setProperty} silently overwrite the first and pass a non-self-loop edge off as a match
 * (issue #7008), so this step enforces {@code out == in} itself for that shape and keeps the index seek.
 */
public class MatchEdgeByIndexStep extends AbstractExecutionStep {
  private final String       edgeType;
  private final String[]     propertyNames;
  private final Expression[] valueExpressions;
  private final String       indexName;
  private final Direction    direction;
  private final String       sourceVariable;
  private final String       relationshipVariable; // may be null when the edge is anonymous
  private final String       targetVariable;
  // True when the pattern repeats one variable at both endpoints - (a)-[t:TYPE]->(a) - which constrains the
  // hop to self-loops. Anonymous endpoints get distinct generated names, so only an explicit repeat sets it.
  private final boolean      selfLoopOnly;

  private final ExpressionEvaluator evaluator;

  // Held at the step level (not local to the syncPull() ResultSet) so close() can release it, matching the
  // AutoCloseable contract on IndexCursor and this codebase's convention for any step holding one (see
  // FetchFromIndexStep). openCursor() always resolves through a full-key equality lookupByKey(), which today
  // returns an already fully-drained IndexCursorCollection - so this is a robustness/consistency measure, not
  // a fix for a reproducible leak: nothing here can hold a live LSM series cursor open across pulls.
  private IndexCursor cursor;

  // Read-only empty row used to evaluate the (row-independent) key value expressions - literals,
  // parameters and functions such as date('...'). Mirrors MatchNodeStep.EMPTY_RESULT.
  private static final Result EMPTY_RESULT = new ResultInternal(Collections.emptyMap());

  public MatchEdgeByIndexStep(final String edgeType, final String[] propertyNames,
      final Expression[] valueExpressions, final String indexName, final Direction direction,
      final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final CommandContext context) {
    super(context);
    this.edgeType = edgeType;
    this.propertyNames = propertyNames;
    this.valueExpressions = valueExpressions;
    this.indexName = indexName;
    this.direction = direction;
    this.sourceVariable = sourceVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.selfLoopOnly = sourceVariable != null && sourceVariable.equals(targetVariable);
    this.evaluator = new ExpressionEvaluator(new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance()));
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    // A key that cannot be resolved (should not happen for literal/parameter keys) yields no rows.
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private boolean            initialized = false;
      private final List<Result> buffer = new ArrayList<>();
      private int                bufferIndex = 0;
      private boolean            finished = false;

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

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        if (!initialized) {
          initialized = true;
          cursor = openCursor();
          if (cursor == null) {
            finished = true;
            return;
          }
        }

        while (buffer.size() < n && cursor.hasNext()) {
          guard.check();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            final Identifiable identifiable = cursor.next();
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
            if (!edge.getType().instanceOf(edgeType))
              continue;

            final ResultInternal result = new ResultInternal();
            if (relationshipVariable != null && !relationshipVariable.isEmpty())
              result.setProperty(relationshipVariable, edge);

            // Bind the endpoints from the edge, honouring the written direction. For OUT the source is
            // the edge's OUT vertex; for IN the pattern is written (a)<-[t]-(b), so the source (a) is the
            // edge's IN vertex.
            try {
              if (direction == Direction.IN) {
                result.setProperty(sourceVariable, edge.getInVertex());
                result.setProperty(targetVariable, edge.getOutVertex());
              } else {
                result.setProperty(sourceVariable, edge.getOutVertex());
                result.setProperty(targetVariable, edge.getInVertex());
              }
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

        if (!cursor.hasNext())
          finished = true;
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
   * Releases whatever {@link #openCursor()} returned, honouring the {@code IndexCursor} contract regardless of
   * how the current or a future {@code lookupByKey()} implementation backs it.
   */
  private void releaseCursor() {
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
  }

  private IndexCursor openCursor() {
    final Object[] keyValues = new Object[propertyNames.length];
    for (int i = 0; i < propertyNames.length; i++) {
      final Object resolved = coerceForIndexKey(evaluator.evaluate(valueExpressions[i], EMPTY_RESULT, context));
      if (resolved == null)
        return null; // an unresolved key cannot seek; caller-side WHERE filter still runs on the fallback
      keyValues[i] = resolved;
    }
    return context.getDatabase().lookupByKey(edgeType, propertyNames, keyValues);
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

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append("  ".repeat(Math.max(0, depth * indent)));
    builder.append("+ MATCH EDGE BY INDEX ");
    builder.append("(").append(sourceVariable).append(")");
    final String arrowRel = relationshipVariable != null && !relationshipVariable.isEmpty()
        && !relationshipVariable.startsWith("  ") ? relationshipVariable : "";
    if (direction == Direction.IN)
      builder.append("<-[").append(arrowRel).append(":").append(edgeType).append("]-");
    else
      builder.append("-[").append(arrowRel).append(":").append(edgeType).append("]->");
    builder.append("(").append(targetVariable).append(")");
    builder.append(" [index: ").append(indexName).append(" on ").append(String.join(",", propertyNames)).append("]");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
