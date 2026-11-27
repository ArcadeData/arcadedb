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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.LogicalExpression;
import com.arcadedb.query.opencypher.executor.EdgeIndexSeedPlanner;
import com.arcadedb.query.opencypher.executor.operators.ParallelSafeExpressions;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ParallelRecordScan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Leading (seed) execution step that drives a single-hop relationship pattern from a scan of the buckets of its relationship
 * types, instead of a scan of every vertex and a walk of every edge list (issue #11874).
 * <p>
 * It serves {@code MATCH (a)-[e:K]->(b) WHERE e.w = 3} when no index of K answers the predicate ({@link MatchEdgeByIndexStep}
 * serves it when one does). The walk also finds the light edges of K, which have no record in its buckets, so the planner takes
 * this step only under a predicate a relationship with no property never satisfies, or for a statement that only aggregates
 * properties of the relationship with aggregates that skip a null, {@code RETURN count(e.w)}, to which no light edge adds anything
 * (see {@link com.arcadedb.query.opencypher.executor.EdgeTypeScanPlanner}).
 * <p>
 * Each edge is checked against the relationship's inline property map and against the conjuncts of the {@code WHERE} clause that
 * read only the relationship, which the clause's filter leaves to this step, before its endpoints are loaded. The endpoints are
 * then checked against the labels and inline properties of their node patterns. An edge whose endpoint record is missing is
 * skipped, as the walk skips it. An undirected pattern binds each edge once per orientation, as the walk from each of its two
 * vertices does, and a self-loop once.
 * <p>
 * A directed pattern over one edge type scans in the workers of a parallel scan when a predicate of its own filters the rows,
 * and hands its rows to an aggregation that consumes them in the workers ({@link ParallelRowSource}), as the label scan does.
 */
public class MatchEdgeByTypeScanStep extends AbstractExecutionStep implements ParallelRowSource {
  private static final Result EMPTY_RESULT = new ResultInternal(Collections.emptyMap());

  /** The edge types to scan, polymorphically; none is a sub-type of another. */
  private final List<String>                        edgeTypes;
  private final Direction                           direction;
  private final String                              sourceVariable;
  private final String                              relationshipVariable; // null when the relationship is anonymous
  private final String                              targetVariable;
  // The pattern repeats one variable at both endpoints, (a)-[e:K]->(a): self-loops only
  private final boolean                             selfLoopOnly;
  private final EdgeIndexSeedPlanner.EndpointFilter sourceFilter;
  private final EdgeIndexSeedPlanner.EndpointFilter targetFilter;
  /** The relationship's inline property map, null when it has none. */
  private final Map<String, Object>                 relationshipProperties;
  /** The WHERE conjuncts that read only the relationship, null when there is none. */
  private final BooleanExpression                   relationshipFilter;

  private boolean started;
  private boolean servedInParallel;

  /**
   * @param edgeTypes              the edge types to scan, none a sub-type of another
   * @param direction              the written direction; {@link Direction#BOTH} binds each edge once per orientation
   * @param relationshipVariable   the variable bound to the edge, null when nothing reads it
   * @param sourceFilter           what the source endpoint asks of its vertex
   * @param targetFilter           what the target endpoint asks of its vertex
   * @param relationshipProperties the relationship's inline property map, null when it has none
   * @param relationshipFilter     the WHERE conjuncts that read only the relationship, null for none
   */
  public MatchEdgeByTypeScanStep(final List<String> edgeTypes, final Direction direction, final String sourceVariable,
      final String relationshipVariable, final String targetVariable, final EdgeIndexSeedPlanner.EndpointFilter sourceFilter,
      final EdgeIndexSeedPlanner.EndpointFilter targetFilter, final Map<String, Object> relationshipProperties,
      final BooleanExpression relationshipFilter, final CommandContext context) {
    super(context);
    this.edgeTypes = edgeTypes;
    this.direction = direction;
    this.sourceVariable = sourceVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.selfLoopOnly = sourceVariable.equals(targetVariable);
    this.sourceFilter = sourceFilter;
    this.targetFilter = targetFilter;
    this.relationshipProperties = relationshipProperties == null || relationshipProperties.isEmpty() ? null : relationshipProperties;
    this.relationshipFilter = relationshipFilter;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private final List<Result> buffer      = new ArrayList<>();
      private       int          bufferIndex = 0;
      private       boolean      finished    = false;
      private       boolean      initialized = false;
      private       EdgeRows     rows;
      private       ResultSet    parallelRows;
      private       int          typeIndex   = -1;
      private       Iterator<Record> records;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore(nRecords > 0 ? nRecords : 100);
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
          started = true;
          requireReadable(context);
          // A predicate of its own (the WHERE conjuncts it took, the inline map) filters the rows in the workers, which hand them
          // over in the sequential order
          final ParallelRecordScan scan = relationshipFilter != null || relationshipProperties != null ?
              planScan(context, List.of()) : null;
          if (scan != null)
            parallelRows = scan.pull(context);
          else
            rows = new EdgeRows(null);
        }

        if (parallelRows != null) {
          while (buffer.size() < n && parallelRows.hasNext()) {
            guard.check();
            buffer.add(parallelRows.next());
          }
          if (!parallelRows.hasNext())
            finished = true;
          return;
        }

        while (buffer.size() < n) {
          guard.check();
          if (records == null || !records.hasNext()) {
            if (++typeIndex >= edgeTypes.size()) {
              finished = true;
              rows.forgetRejected();
              return;
            }
            records = context.getDatabase().iterateType(edgeTypes.get(typeIndex), true);
            continue;
          }

          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            final int before = buffer.size();
            rows.emit(records.next().asEdge(), context, buffer);
            if (context.isProfiling())
              rowCount += buffer.size() - before;
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }

      @Override
      public void close() {
        // The workers of a parallel scan stop when its result set is closed
        if (parallelRows != null) {
          parallelRows.close();
          parallelRows = null;
        }
        if (rows != null)
          rows.forgetRejected();
        finished = true;
        MatchEdgeByTypeScanStep.this.close();
      }
    };
  }

  /**
   * The variable the expressions run in the workers may read: the relationship, or the source endpoint when the relationship is
   * anonymous. A row also binds the endpoints, so an expression reading only that variable evaluates on it as on the sequential
   * row. Null when the scan cannot run in parallel: an undirected pattern makes two rows of one edge, and several types are
   * several scans.
   */
  @Override
  public String parallelVariable() {
    if (direction == Direction.BOTH || edgeTypes.size() != 1)
      return null;
    return relationshipVariable != null ? relationshipVariable : sourceVariable;
  }

  @Override
  public ParallelRecordScan planParallelRows(final CommandContext context, final List<BooleanExpression> filters) {
    if (started)
      return null;
    requireReadable(context);
    return planScan(context, filters);
  }

  /**
   * Refuses the user who may not read a type the scan reads, or the label of an endpoint it binds, as the walk of the buckets of
   * those types does and as the edge index seek does (issue #12663), once before the first edge is read.
   * <p>
   * The scan loads the endpoints of the edges its predicate keeps only, so left to itself it would refuse a user who may not read
   * the label of an endpoint only when an edge holds the value asked for, and tell that user whether one does. The walk it stands
   * for opens the buckets of the source's label, and of every type under it, whatever the value is. The plan is not always built
   * for the user who runs it (the cache of Cypher plans belongs to the database), so the check is the step's to make, per
   * execution, on the thread of the query: the workers of a parallel scan start after it.
   *
   * @throws SecurityException when the current user may not read one of the types
   */
  private void requireReadable(final CommandContext context) {
    final DatabaseInternal database = context.getDatabase();
    final Schema schema = database.getSchema();
    for (int i = 0; i < edgeTypes.size(); i++)
      SecurityHelper.checkReadOnTypeAndSubTypes(database, schema.getTypeOrNull(edgeTypes.get(i)));
    for (final String label : sourceFilter.labels())
      SecurityHelper.checkReadOnTypeAndSubTypes(database, schema.getTypeOrNull(label));
    for (final String label : targetFilter.labels())
      SecurityHelper.checkReadOnTypeAndSubTypes(database, schema.getTypeOrNull(label));
  }

  /**
   * The parallel scan of the one edge type, its rows filtered by this step and by {@code extraFilters}, or {@code null} when this
   * execution stays sequential: a predicate or an inline value the workers cannot evaluate, or the parallel scan declines
   * (disabled, in a transaction, too small).
   */
  private ParallelRecordScan planScan(final CommandContext context, final List<BooleanExpression> extraFilters) {
    final String variable = parallelVariable();
    if (variable == null || !(context.getDatabase().getSchema().getTypeOrNull(edgeTypes.getFirst()) instanceof EdgeType))
      return null;
    if (relationshipFilter != null && !ParallelSafeExpressions.isParallelSafe(relationshipFilter, relationshipVariable))
      return null;
    if (!parallelSafeValues(relationshipProperties) || !parallelSafeValues(sourceFilter.properties())
        || !parallelSafeValues(targetFilter.properties()))
      return null;
    BooleanExpression extra = null;
    for (final BooleanExpression filter : extraFilters) {
      if (!ParallelSafeExpressions.isParallelSafe(filter, variable))
        return null;
      extra = extra == null ? filter : new LogicalExpression(LogicalExpression.Operator.AND, extra, filter);
    }

    final BooleanExpression rowFilter = extra;
    final ParallelRecordScan scan = ParallelRecordScan.plan(context, edgeTypes.getFirst(), () -> new EdgeRows(rowFilter));
    servedInParallel = scan != null;
    return scan;
  }

  /** Whether the workers can resolve every value of an inline property map: a literal, a parameter or a safe expression. */
  private static boolean parallelSafeValues(final Map<String, Object> properties) {
    if (properties != null)
      for (final Object value : properties.values())
        if (value instanceof Expression expression && !ParallelSafeExpressions.isParallelSafe(expression, ""))
          return false;
    return true;
  }

  /**
   * Turns a scanned edge into the rows it binds. One per thread: the sequential scan has one, every unit of a parallel scan its
   * own, so the row its predicate rejected last is reused for the next edge without sharing it (issue #10818).
   */
  private final class EdgeRows implements ParallelRecordScan.RowMapper {
    /** Filters of the steps downstream a parallel scan took over, evaluated on the whole row; null for none. */
    private final BooleanExpression downstreamFilter;
    private       ResultInternal    rejected;

    private EdgeRows(final BooleanExpression downstreamFilter) {
      this.downstreamFilter = downstreamFilter;
    }

    /** The row of a directed pattern's edge, or {@code null} when it does not match: a parallel scan unit's mapper. */
    @Override
    public Result map(final Record record, final CommandContext workerContext) {
      final Edge edge = record.asEdge();
      final ResultInternal row = edgeRow(edge, workerContext);
      if (row == null || !bindEndpoints(row, edge, direction == Direction.IN, workerContext))
        return null;
      return downstreamFilter == null || downstreamFilter.evaluate(row, workerContext) ? row : null;
    }

    /** Adds the rows of {@code edge} to {@code out}: one for a directed pattern, one per orientation for an undirected one. */
    void emit(final Edge edge, final CommandContext context, final List<Result> out) {
      final ResultInternal row = edgeRow(edge, context);
      if (row == null)
        return;
      if (direction != Direction.BOTH) {
        if (bindEndpoints(row, edge, direction == Direction.IN, context))
          out.add(row);
        return;
      }
      // Walked from both of its vertices, an edge is found twice, a self-loop once (the walk de-duplicates it per vertex)
      final ResultInternal reversed = edge.getOut().equals(edge.getIn()) ? null : edgeOnlyRow(edge);
      if (bindEndpoints(row, edge, false, context))
        out.add(row);
      if (reversed != null && bindEndpoints(reversed, edge, true, context))
        out.add(reversed);
    }

    /** A row binding the edge, if it passes the relationship's inline map and predicate, or null. */
    private ResultInternal edgeRow(final Edge edge, final CommandContext context) {
      if (relationshipProperties != null && !InlineProperties.matches(edge, relationshipProperties, EMPTY_RESULT, context))
        return null;
      // Reads the two RIDs the edge holds, without loading the vertices
      if (selfLoopOnly && !edge.getOut().equals(edge.getIn()))
        return null;
      if (relationshipVariable == null)
        return new ResultInternal();

      ResultInternal row = rejected;
      rejected = null;
      if (row == null || !row.rebindSingleProperty(relationshipVariable, edge)) {
        row = new ResultInternal();
        row.setProperty(relationshipVariable, edge);
      }
      if (relationshipFilter == null || relationshipFilter.evaluate(row, context))
        return row;
      rejected = row;
      return null;
    }

    /**
     * Binds the endpoints of {@code edge} the written way, or the reverse one, if they pass their node patterns. False also when
     * an endpoint record is missing (a dangling edge left by a rollback or an HA resync), which the walk skips too.
     */
    private boolean bindEndpoints(final ResultInternal row, final Edge edge, final boolean reversed, final CommandContext context) {
      try {
        final Vertex source = reversed ? edge.getInVertex() : edge.getOutVertex();
        if (!sourceFilter.isEmpty() && !sourceFilter.matches(source, EMPTY_RESULT, context))
          return false;
        final Vertex target = reversed ? edge.getOutVertex() : edge.getInVertex();
        if (!targetFilter.isEmpty() && !targetFilter.matches(target, EMPTY_RESULT, context))
          return false;
        row.setProperty(sourceVariable, source);
        row.setProperty(targetVariable, target);
        return true;
      } catch (final RecordNotFoundException e) {
        return false;
      }
    }

    private ResultInternal edgeOnlyRow(final Edge edge) {
      final ResultInternal row = new ResultInternal();
      if (relationshipVariable != null)
        row.setProperty(relationshipVariable, edge);
      return row;
    }

    /** A scan that is over does not keep the last edge it rejected reachable. */
    void forgetRejected() {
      rejected = null;
    }
  }

  /**
   * {@code MATCH EDGE BY TYPE SCAN (a)-[e:K|L]->(b) [filter: ...]}. Shared with the cost-based plan's {@code EdgeTypeScan}, which
   * runs this step.
   */
  public static String describe(final List<String> edgeTypes, final Direction direction, final String sourceVariable,
      final String relationshipVariable, final String targetVariable, final EdgeIndexSeedPlanner.EndpointFilter sourceFilter,
      final EdgeIndexSeedPlanner.EndpointFilter targetFilter, final BooleanExpression relationshipFilter) {
    final StringBuilder builder = new StringBuilder("MATCH EDGE BY TYPE SCAN ");
    builder.append("(").append(sourceVariable).append(MatchEdgeByIndexStep.labelsOf(sourceFilter)).append(")");
    final String arrowRel = relationshipVariable != null && !relationshipVariable.startsWith("  ") ? relationshipVariable : "";
    final String types = ":" + String.join("|", edgeTypes);
    if (direction == Direction.IN)
      builder.append("<-[").append(arrowRel).append(types).append("]-");
    else if (direction == Direction.OUT)
      builder.append("-[").append(arrowRel).append(types).append("]->");
    else
      builder.append("-[").append(arrowRel).append(types).append("]-");
    builder.append("(").append(targetVariable).append(MatchEdgeByIndexStep.labelsOf(targetFilter)).append(")");
    if (relationshipFilter != null)
      builder.append(" [filter: ").append(relationshipFilter.getText()).append("]");
    return builder.toString();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append("  ".repeat(Math.max(0, depth * indent))).append("+ ");
    builder.append(describe(edgeTypes, direction, sourceVariable, relationshipVariable, targetVariable, sourceFilter, targetFilter,
        relationshipFilter));
    if (servedInParallel)
      builder.append(" [parallel]");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
