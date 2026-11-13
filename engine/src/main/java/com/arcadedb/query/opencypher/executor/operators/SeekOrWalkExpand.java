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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.GraphEngine;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.executor.RowIndependentValues;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * A hop from a bound vertex to a far end keyed by an IN-list on every column of a UNIQUE index, answered per input row by
 * whichever of two plans costs less for that row (issue #11715):
 * <ul>
 *   <li><b>walk</b>: the relationships of the bound vertex, as {@link ExpandAll} reads them, the far end then filtered on
 *   its key above this operator;</li>
 *   <li><b>seek</b>: the far end sought once per key ({@link NodeIndexSeek}, at most one vertex each, sought once per
 *   execution whatever the number of input rows), then each pair read off the shorter of its two edge lists
 *   ({@link ExpandInto}, issue #11699).</li>
 * </ul>
 * The walk costs the degree of the bound vertex, the seek a probe per key. No edge list keeps a count, so the entries of
 * the bound vertex's list are stepped, without a record read, up to {@link #WALKED_ENTRIES_PER_KEY} per key: a list that
 * ends first is walked, a longer one is answered by the seek. The count is taken before either plan emits a row, so a row
 * is never produced twice nor by both plans, and the two plans give the same rows: the far end's key predicate is
 * evaluated above this operator on both, and the seek only narrows the vertices it is evaluated on.
 * <p>
 * The seek's own key handling applies on the seek side: a duplicate key finds its vertex once, a null key finds nothing,
 * and a right side that is not a list is refused as the {@code IN} refuses it. A list of no key seeks nothing.
 * <p>
 * Cost: per input row, O(min(degree, keys * {@link #WALKED_ENTRIES_PER_KEY})) entry steps, then the cheaper of the walk
 * and the seek
 */
public class SeekOrWalkExpand extends AbstractPhysicalOperator {
  /**
   * Relationships of the bound vertex walked per key of the list before the far end is sought instead. A walked
   * relationship loads its far vertex and evaluates its key; a key sought costs an index probe and the read of the pair,
   * which is worth a handful of walked relationships.
   */
  public static final int WALKED_ENTRIES_PER_KEY = 16;

  private final String           nearVariable;
  private final String           farVariable;
  private final Vertex.DIRECTION nearDirection;
  private final String[]         edgeTypes;
  private final InListValues     farKeys;
  private final NodeIndexSeek    farSeek;
  private final ExpandAll        walk;
  private final ExpandInto       seek;
  private final String           edgeTrackingVar;

  /**
   * @param walk            the hop from {@code nearVariable}, with no child: it is handed one input row at a time
   * @param seek            the hop between the two bound ends, with no child: it is handed the pairs of one input row at a
   *                        time
   * @param edgeTrackingVar the binding both hops keep an anonymous relationship under for the uniqueness checks, or null
   */
  public SeekOrWalkExpand(final PhysicalOperator child, final String nearVariable, final String farVariable,
      final Vertex.DIRECTION nearDirection, final String[] edgeTypes, final InListValues farKeys, final NodeIndexSeek farSeek,
      final ExpandAll walk, final ExpandInto seek, final String edgeTrackingVar, final double estimatedCost,
      final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    this.nearVariable = nearVariable;
    this.farVariable = farVariable;
    this.nearDirection = nearDirection;
    this.edgeTypes = edgeTypes;
    this.farKeys = farKeys;
    this.farSeek = farSeek;
    this.walk = walk;
    this.seek = seek;
    this.edgeTrackingVar = edgeTrackingVar;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final ResultSet inputResults = child.execute(context, nRecords);
    final GraphEngine engine = ((DatabaseInternal) context.getDatabase()).getGraphEngine();
    // A hop reaching the incoming side of a unidirectional type is stored on the far end only: the pair reads it there,
    // where the walk would ask the query's lookup (issue #8625)
    final boolean alwaysSeek = IncomingEdgeLookup.isIncomingSideMissing(context.getDatabase().getSchema(), nearDirection,
        edgeTypes);

    return new ResultSet() {
      private ResultSet current;
      // -1 until the first bound vertex with a relationship asks: the list is not evaluated for an input with none
      private long      walkLimit = -1;
      private RowBuffer farRows;
      private boolean   finished;

      @Override
      public boolean hasNext() {
        while (true) {
          if (current != null) {
            if (current.hasNext())
              return true;
            current.close();
            current = null;
          }
          if (finished)
            return false;
          if (!inputResults.hasNext()) {
            finished = true;
            release();
            return false;
          }
          guard.check();
          current = expansionOf(inputResults.next());
        }
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return current.next();
      }

      private ResultSet expansionOf(final Result row) {
        if (row.getProperty(nearVariable) instanceof VertexInternal near && (alwaysSeek || !walks(near)))
          return seek.expand(context, nRecords, new IteratorResultSet(pairsOf(row)));
        // An unbound end (OPTIONAL MATCH) is the walk's to skip, as it always was
        return walk.expand(context, nRecords, new IteratorResultSet(List.of(row).iterator()));
      }

      private boolean walks(final VertexInternal near) {
        if (walkLimit < 0) {
          if (!engine.hasMoreEdgeEntriesThan(near, nearDirection, 0))
            return true;
          walkLimit = keyCount(farKeys, context) * WALKED_ENTRIES_PER_KEY;
        }
        return !engine.hasMoreEdgeEntriesThan(near, nearDirection, walkLimit);
      }

      /** The input row with each far vertex the keys find, the far vertices sought on the first call only. */
      private Iterator<Result> pairsOf(final Result row) {
        if (farRows == null) {
          farRows = new RowBuffer(null, OperationHeapLimit.of(context, "SeekOrWalkExpand"), 0);
          try (final ResultSet sought = farSeek.execute(context, -1)) {
            while (sought.hasNext())
              farRows.add(sought.next());
          } catch (final RuntimeException e) {
            // Over a limit: the query fails, and the rows buffered so far give their heap back now
            farRows.clear();
            throw e;
          }
        }
        return new Iterator<>() {
          private int index = 0;

          @Override
          public boolean hasNext() {
            return index < farRows.size();
          }

          @Override
          public Result next() {
            if (!hasNext())
              throw new NoSuchElementException();
            final ResultInternal pair = ResultInternal.copyBindings(row, 1);
            pair.setProperty(farVariable, farRows.get(index++).getProperty(farVariable));
            return pair;
          }
        };
      }

      private void release() {
        if (farRows != null)
          farRows.clear();
      }

      @Override
      public void close() {
        finished = true;
        if (current != null) {
          current.close();
          current = null;
        }
        inputResults.close();
        release();
      }
    };
  }

  /**
   * The keys the list holds, an upper bound of the vertices the seek can find: an element that is the whole right side
   * ({@code IN $ids}) counts its items, any other one is one key, a null is none.
   */
  private static long keyCount(final InListValues keys, final CommandContext context) {
    long count = 0;
    for (final Expression element : keys.getValues()) {
      final Object value = RowIndependentValues.resolve(element, context);
      if (value == null)
        continue;
      if (!keys.isWholeList(element) || keys.isScalar(element))
        ++count;
      else if (value instanceof Collection<?> collection)
        count += collection.size();
      else if (MultiValue.isSequenceArray(value))
        count += MultiValue.getSize(value);
      else
        // not a list: the seek refuses it as the IN does
        ++count;
    }
    return count;
  }

  @Override
  public String getOperatorType() {
    return "SeekOrWalkExpand";
  }

  @Override
  public String explain(final int depth) {
    final String indent = getIndent(depth);
    final StringBuilder sb = new StringBuilder();
    sb.append(indent).append("+ SeekOrWalkExpand(").append(nearVariable).append(") [walk ").append(nearVariable)
        .append(" while it has at most ").append(WALKED_ENTRIES_PER_KEY).append(" relationships per key, else seek ")
        .append(farVariable).append(" per key").append(", cost=").append(String.format(Locale.US, "%.2f", estimatedCost))
        .append(", rows=").append(estimatedCardinality).append("]\n");
    sb.append(walk.explain(depth + 1));
    sb.append(seek.explain(depth + 1));
    sb.append(farSeek.explain(depth + 2));
    if (child != null)
      sb.append(child.explain(depth + 1));
    return sb.toString();
  }

  public String getEdgeTrackingVar() {
    return edgeTrackingVar;
  }

  @Override
  public List<PhysicalOperator> getInputs() {
    return List.of(child, farSeek);
  }
}
