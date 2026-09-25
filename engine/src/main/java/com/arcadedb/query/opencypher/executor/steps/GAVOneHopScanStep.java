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

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.GAVVertex;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.executor.SelfLoops;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

/**
 * Produces the rows of a one-hop pattern {@code (a:A)-[:T]->(b:B) WHERE pred(a, b)} straight from a Graph Analytical
 * View, for an aggregation to consume (issue #8335).
 * <p>
 * The plan it replaces scans the label through the OLTP records and expands every source through the view, building
 * for every edge a result row that copies the source row and carries a freshly loaded source vertex: the view speeds
 * the traversal up, and the rows around it eat the gain. Here both ends come from the view: the sources are the view's
 * nodes carrying the source label, and every endpoint is a {@link GAVVertex}, so {@code a.city} and {@code b.age} are
 * read from the view's columns rather than decoded from a record, once per edge. Each edge becomes one two-slot row
 * with no map behind it, the source-only part of the WHERE clause is decided once per source, and the target label is
 * a bucket lookup in a table.
 * <p>
 * The planner builds it only when the answer is provably the one the plan it replaces would give: the view is not
 * stale, covers every vertex the labels can match, and the transaction has no pending change the view cannot see.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GAVOneHopScanStep extends AbstractExecutionStep {
  private final GraphTraversalProvider provider;
  private final String                 sourceVariable;
  private final String                 sourceLabel;
  private final String                 targetVariable;
  private final String                 targetLabel;
  private final Vertex.DIRECTION       direction;
  private final String[]               edgeTypes;
  private final BooleanExpression      sourceFilter;
  private final BooleanExpression      edgeFilter;
  private final boolean[]              sourceBuckets;
  private final boolean[]              targetBuckets;
  private final Set<String>            variables;

  // runtime
  private WorkGuard guard;
  private int       upperBound;
  private int       nextSourceId;
  private GAVVertex source;
  private int[]     neighbors;
  private int       neighborIndex;
  private Result    nextRow;

  /**
   * @param sourceVariable the variable bound to the node the edges are walked from, or null when anonymous
   * @param sourceLabel    the label of that node
   * @param targetVariable the variable bound to the node at the other end, or null when anonymous
   * @param targetLabel    the label of that node, or null for any
   * @param direction      OUT or BOTH, from the source
   * @param edgeTypes      the edge types to walk, empty or null for all
   * @param sourceFilter   the part of the WHERE clause that reads the source only, or null
   * @param edgeFilter     the rest of the WHERE clause, or null
   * @param sourceBuckets  the ids of the buckets a source can live in (the source label and its sub-types)
   * @param targetBuckets  the ids of the buckets a target can live in, or null for any
   */
  public GAVOneHopScanStep(final GraphTraversalProvider provider, final String sourceVariable, final String sourceLabel,
      final String targetVariable, final String targetLabel, final Vertex.DIRECTION direction, final String[] edgeTypes,
      final BooleanExpression sourceFilter, final BooleanExpression edgeFilter, final int[] sourceBuckets,
      final int[] targetBuckets, final CommandContext context) {
    super(context);
    this.provider = provider;
    this.sourceVariable = sourceVariable;
    this.sourceLabel = sourceLabel;
    this.targetVariable = targetVariable;
    this.targetLabel = targetLabel;
    this.direction = direction;
    this.edgeTypes = edgeTypes;
    this.sourceFilter = sourceFilter;
    this.edgeFilter = edgeFilter;
    this.sourceBuckets = toTable(sourceBuckets);
    this.targetBuckets = targetBuckets != null ? toTable(targetBuckets) : null;

    final Set<String> names = new LinkedHashSet<>(2);
    if (sourceVariable != null)
      names.add(sourceVariable);
    if (targetVariable != null)
      names.add(targetVariable);
    this.variables = Collections.unmodifiableSet(names);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    // Like the operator wrapper it stands in for, one result set streams every row: the aggregation steps above pull
    // once and drain it, whatever batch size they ask for. The cursor lives on the step, so a second pull finds it
    // exhausted rather than starting over.
    if (guard == null) {
      guard = WorkGuard.forCommandDeadline(context);
      upperBound = provider.getNodeIdUpperBound();
    }

    return new ResultSet() {
      @Override
      public boolean hasNext() {
        if (nextRow != null)
          return true;
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          nextRow = fetchNext(context);
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
        return nextRow != null;
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Result row = nextRow;
        nextRow = null;
        if (context.isProfiling())
          ++rowCount;
        return row;
      }
    };
  }

  private Result fetchNext(final CommandContext context) {
    final Database database = context.getDatabase();
    while (true) {
      if (neighbors != null && neighborIndex < neighbors.length) {
        final int targetId = neighbors[neighborIndex++];
        guard.checkPeriodically(neighborIndex);
        final RID targetRid = provider.getRID(targetId);
        if (targetRid == null || !inBuckets(targetBuckets, targetRid.getBucketId()))
          continue;

        final Row row = new Row(source, new GAVVertex(targetRid, targetId, provider, database));
        if (edgeFilter != null && !Boolean.TRUE.equals(edgeFilter.evaluateTernary(row, context)))
          continue;
        return row;
      }

      if (!advanceSource(context, database))
        return null;
    }
  }

  private boolean advanceSource(final CommandContext context, final Database database) {
    neighbors = null;
    while (nextSourceId < upperBound) {
      final int nodeId = nextSourceId++;
      guard.checkPeriodically(nodeId);
      if (!provider.isNodeLive(nodeId))
        continue;
      final RID rid = provider.getRID(nodeId);
      if (rid == null || !inBuckets(sourceBuckets, rid.getBucketId()))
        continue;

      source = new GAVVertex(rid, nodeId, provider, database);
      if (sourceFilter != null && !Boolean.TRUE.equals(sourceFilter.evaluateTernary(new Row(source, null), context)))
        continue;

      int[] adjacent = provider.getNeighborIds(nodeId, direction, edgeTypes);
      // An undirected hop yields each relationship once; a self-loop sits in both the outgoing and the incoming
      // list, so half of those entries are the same relationship seen twice (as GAVExpandAll does)
      if (direction == Vertex.DIRECTION.BOTH)
        adjacent = SelfLoops.deduplicate(adjacent, nodeId);
      if (adjacent.length == 0)
        continue;
      neighbors = adjacent;
      neighborIndex = 0;
      return true;
    }
    return false;
  }

  private static boolean inBuckets(final boolean[] table, final int bucketId) {
    return table == null || (bucketId >= 0 && bucketId < table.length && table[bucketId]);
  }

  private static boolean[] toTable(final int[] bucketIds) {
    int max = -1;
    for (final int id : bucketIds)
      max = Math.max(max, id);
    final boolean[] table = new boolean[max + 1];
    for (final int id : bucketIds)
      if (id >= 0)
        table[id] = true;
    return table;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder("  ".repeat(Math.max(0, depth * indent)));
    builder.append("+ GAV ONE-HOP SCAN (").append(sourceVariable != null ? sourceVariable : "").append(":").append(sourceLabel)
        .append(")-[");
    if (edgeTypes != null && edgeTypes.length > 0)
      builder.append(":").append(String.join("|", edgeTypes));
    builder.append("]-").append(direction == Vertex.DIRECTION.OUT ? ">" : "").append("(")
        .append(targetVariable != null ? targetVariable : "");
    if (targetLabel != null)
      builder.append(":").append(targetLabel);
    builder.append(") [provider=").append(provider.getName()).append("]");
    if (sourceFilter != null)
      builder.append(" [source filter: ").append(sourceFilter.getText()).append("]");
    if (edgeFilter != null)
      builder.append(" [filter: ").append(edgeFilter.getText()).append("]");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }

  /**
   * The row of one edge: the two endpoints and nothing else, read by the aggregation's expressions and then dropped.
   * Two fields instead of the map a {@code ResultInternal} carries.
   */
  private final class Row implements Result {
    private final Vertex sourceVertex;
    private final Vertex targetVertex;

    private Row(final Vertex sourceVertex, final Vertex targetVertex) {
      this.sourceVertex = sourceVertex;
      this.targetVertex = targetVertex;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProperty(final String name) {
      if (name == null)
        return null;
      if (name.equals(sourceVariable))
        return (T) sourceVertex;
      if (name.equals(targetVariable))
        return (T) targetVertex;
      return null;
    }

    @Override
    public <T> T getProperty(final String name, final Object defaultValue) {
      final T value = getProperty(name);
      return value != null || hasProperty(name) ? value : (T) defaultValue;
    }

    @Override
    public Record getElementProperty(final String name) {
      return getProperty(name);
    }

    @Override
    public Set<String> getPropertyNames() {
      return variables;
    }

    @Override
    public boolean hasProperty(final String name) {
      return variables.contains(name);
    }

    @Override
    public Optional<RID> getIdentity() {
      return Optional.empty();
    }

    @Override
    public boolean isElement() {
      return false;
    }

    @Override
    public Optional<Document> getElement() {
      return Optional.empty();
    }

    @Override
    public Document toElement() {
      throw new UnsupportedOperationException("A pattern row is not a document");
    }

    @Override
    public Optional<Record> getRecord() {
      return Optional.empty();
    }

    @Override
    public boolean isProjection() {
      return true;
    }

    @Override
    public Object getMetadata(final String key) {
      return null;
    }

    @Override
    public Set<String> getMetadataKeys() {
      return Collections.emptySet();
    }

    @Override
    public Database getDatabase() {
      return context.getDatabase();
    }

    @Override
    public Map<String, Object> toMap() {
      final Map<String, Object> map = new HashMap<>(4);
      if (sourceVariable != null)
        map.put(sourceVariable, sourceVertex);
      if (targetVariable != null)
        map.put(targetVariable, targetVertex);
      return map;
    }
  }
}
