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

import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.traversal.GAVDistinctEndpointTraverser;
import com.arcadedb.query.opencypher.traversal.GraphTraverser;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.opencypher.traversal.VariableLengthPathTraverser;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Lazy physical operator for an ordinary variable-length MATCH relationship. It deliberately
 * delegates path enumeration to the same depth-first traverser as the traditional execution step,
 * keeping path modes, inline edge predicates, zero-hop behavior, and edge identity semantics in one
 * implementation while allowing the cost-based planner to choose the anchor and surrounding joins.
 */
public class VarLengthExpand extends AbstractPhysicalOperator {
  public record ExpansionVariables(String source, String relationship, String target, String path) {
  }

  public record TraversalSpec(RelationshipPattern pattern, Direction direction, boolean reverseResultPath,
      PathMode pathMode) {
  }

  private final String sourceVariable;
  private final String relationshipVariable;
  private final String targetVariable;
  private final String pathVariable;
  private final RelationshipPattern pattern;
  private final Direction direction;
  private final boolean reverseResultPath;
  private final PathMode pathMode;
  private String targetLabel;
  private Set<String> sameClausePrecedingRelVars;
  private String edgeTrackingVar;
  private boolean distinctEndpoints;

  public VarLengthExpand(final PhysicalOperator child, final ExpansionVariables variables,
      final TraversalSpec traversal, final double estimatedCost, final long estimatedCardinality) {
    super(child, estimatedCost, estimatedCardinality);
    final RelationshipPattern pattern = traversal != null ? traversal.pattern() : null;
    if (pattern == null || !pattern.isVariableLength())
      throw new IllegalArgumentException("VarLengthExpand requires a variable-length relationship pattern");

    if (variables == null)
      throw new IllegalArgumentException("VarLengthExpand requires variable bindings");

    this.sourceVariable = variables.source();
    this.relationshipVariable = variables.relationship();
    this.targetVariable = variables.target();
    this.pathVariable = variables.path();
    this.pattern = pattern;
    this.direction = traversal.direction();
    this.reverseResultPath = traversal.reverseResultPath();
    this.pathMode = traversal.pathMode();
  }

  public void setSameClausePrecedingRelVars(final Set<String> sameClausePrecedingRelVars) {
    this.sameClausePrecedingRelVars = sameClausePrecedingRelVars;
  }

  public void setEdgeTrackingVar(final String edgeTrackingVar) {
    this.edgeTrackingVar = edgeTrackingVar;
  }

  /**
   * Reports each end vertex once per start vertex, found with a visited set, instead of one row per path. The optimizer sets it when
   * nothing downstream can tell two paths between the same pair apart (see
   * {@link com.arcadedb.query.opencypher.executor.VariableLengthDistinctEndpoints}).
   */
  public void setDistinctEndpoints(final boolean distinctEndpoints) {
    this.distinctEndpoints = distinctEndpoints;
  }

  public void setTargetLabel(final String targetLabel) {
    this.targetLabel = targetLabel;
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  public String getSourceVariable() {
    return sourceVariable;
  }

  /**
   * {@code count(DISTINCT v)} over every row this operator would produce, for {@code v} its target or its source, counted while
   * the walks run instead of from rows built one per end vertex (issue #10548). Only for an operator in distinct-endpoint mode
   * feeding nothing but that count, which the planner checks. Through a view the vertices are counted by node id, and a start
   * vertex the view does not map is walked through the records, as {@link #execute} does. A source is counted when its walk
   * reaches at least one end vertex, the walk stopping there.
   *
   * @param excludeSource leave each row's start out of its own walk, for a {@code WHERE target <> source}
   * @param countSources  count the sources that reach an end vertex rather than the end vertices
   */
  public long countDistinct(final CommandContext context, final boolean excludeSource, final boolean countSources) {
    if (!distinctEndpoints)
      throw new IllegalStateException("Only a distinct-endpoint expansion counts its end vertices itself");
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final GAVDistinctEndpointTraverser viewTraverser = createViewTraverser(context);
    final DistinctVertices counted = new DistinctVertices(viewTraverser);
    try (final ResultSet inputResults = child.execute(context, 100)) {
      while (inputResults.hasNext()) {
        guard.check();
        final Result input = inputResults.next();
        if (!(input.getProperty(sourceVariable) instanceof Vertex source))
          continue;
        if (countSources) {
          if (counted.contains(source))
            continue;
          final int fromView = viewTraverser != null ? viewTraverser.reachesAny(source, excludeSource, targetLabel) : -1;
          if (fromView == 1 || (fromView < 0 && nextEnd(createTraverser(input, context).traverseDistinctEndpoints(source), source,
              excludeSource) != null))
            counted.add(source);
          continue;
        }
        final long fromView = viewTraverser != null ? viewTraverser.countInto(source, excludeSource, targetLabel, counted.ids) : -1L;
        if (fromView >= 0) {
          counted.count += fromView;
          continue;
        }
        final Iterator<Vertex> endpoints = createTraverser(input, context).traverseDistinctEndpoints(source);
        for (Vertex target = nextEnd(endpoints, source, excludeSource); target != null; target = nextEnd(endpoints, source,
            excludeSource))
          counted.add(target);
      }
    }
    return counted.count;
  }

  /** The next end vertex of a walk through the records that the row would keep, or null when there is none. */
  private Vertex nextEnd(final Iterator<Vertex> endpoints, final Vertex source, final boolean excludeSource) {
    while (endpoints.hasNext()) {
      final Vertex target = endpoints.next();
      if (excludeSource && target.getIdentity().equals(source.getIdentity()))
        continue;
      if (targetLabel != null && !target.getType().instanceOf(targetLabel))
        continue;
      return target;
    }
    return null;
  }

  /** The vertices counted so far: by node id where the view maps them, so a vertex met both ways is counted once, by RID otherwise. */
  private static final class DistinctVertices {
    private final GAVDistinctEndpointTraverser view;
    private final BitSet                       ids = new BitSet();
    private       Set<RID>                     rids;
    private       long                         count;

    DistinctVertices(final GAVDistinctEndpointTraverser view) {
      this.view = view;
    }

    boolean contains(final Vertex vertex) {
      final int nodeId = view != null ? view.nodeIdOf(vertex) : -1;
      return nodeId >= 0 ? ids.get(nodeId) : rids != null && rids.contains(vertex.getIdentity());
    }

    void add(final Vertex vertex) {
      final int nodeId = view != null ? view.nodeIdOf(vertex) : -1;
      if (nodeId >= 0) {
        if (!ids.get(nodeId)) {
          ids.set(nodeId);
          ++count;
        }
        return;
      }
      if (rids == null)
        rids = new HashSet<>();
      if (rids.add(vertex.getIdentity()))
        ++count;
    }
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final ResultSet inputResults = child.execute(context, nRecords);
    final int batchSize = nRecords > 0 ? nRecords : 100;
    final GAVDistinctEndpointTraverser viewTraverser = distinctEndpoints ? createViewTraverser(context) : null;

    return new ResultSet() {
      private Result currentInput;
      private Vertex boundTarget;
      private Iterator<TraversalPath> currentPaths;
      private Iterator<Vertex> currentEndpoints;
      private EdgeIdentitySet usedEdges;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex;
      private boolean finished;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore();
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      private void fetchMore() {
        buffer.clear();
        bufferIndex = 0;

        while (buffer.size() < batchSize) {
          guard.check();
          if (currentEndpoints != null && currentEndpoints.hasNext()) {
            final Vertex traversedTarget = currentEndpoints.next();
            if (targetLabel != null && !traversedTarget.getType().instanceOf(targetLabel))
              continue;
            if (boundTarget != null && !traversedTarget.getIdentity().equals(boundTarget.getIdentity()))
              continue;
            final ResultInternal result = VarLengthExpand.copy(currentInput);
            result.setProperty(targetVariable, traversedTarget);
            buffer.add(result);
            continue;
          }
          if (currentPaths != null && currentPaths.hasNext()) {
            final TraversalPath traversedPath = currentPaths.next();
            final Vertex traversedTarget = traversedPath.getEndVertex();
            if (targetLabel != null && !traversedTarget.getType().instanceOf(targetLabel))
              continue;
            if (boundTarget != null && !traversedTarget.getIdentity().equals(boundTarget.getIdentity()))
              continue;
            if (RelationshipBindings.overlaps(usedEdges, traversedPath))
              continue;

            final TraversalPath resultPath = reverseResultPath ? traversedPath.reversed() : traversedPath;
            final ResultInternal result = VarLengthExpand.copy(currentInput);
            if (pathVariable != null && !pathVariable.isEmpty())
              result.setProperty(pathVariable, extendPath(currentInput.getProperty(pathVariable), resultPath));

            final List<Edge> edges = new ArrayList<>(resultPath.getEdges());
            if (relationshipVariable != null && !relationshipVariable.isEmpty())
              result.setProperty(relationshipVariable, edges);
            else if (edgeTrackingVar != null)
              result.setProperty(edgeTrackingVar, edges);
            result.setProperty(targetVariable, traversedTarget);
            buffer.add(result);
            continue;
          }

          if (!inputResults.hasNext()) {
            finished = true;
            break;
          }

          currentInput = inputResults.next();
          final Object source = currentInput.getProperty(sourceVariable);
          final Object target = currentInput.getProperty(targetVariable);
          boundTarget = target instanceof Vertex vertex ? vertex : null;
          usedEdges = RelationshipBindings.collectEdgeIdentities(currentInput, sameClausePrecedingRelVars);
          currentEndpoints = null;
          if (!(source instanceof Vertex vertex))
            currentPaths = null;
          else if (distinctEndpoints && !GraphTraverser.isBoundRelationshipList(currentInput, relationshipVariable)) {
            currentPaths = null;
            // A start vertex the view does not map (created after its last build) walks the records
            final Iterator<Vertex> fromView = viewTraverser != null ? viewTraverser.traverse(vertex) : null;
            currentEndpoints = fromView != null ? fromView : createTraverser(currentInput, context).traverseDistinctEndpoints(vertex);
          } else if (GraphTraverser.isBoundRelationshipList(currentInput, relationshipVariable)) {
            // A relationship list an earlier clause bound to this variable is the whole path (issue #9828)
            final TraversalPath bound = createTraverser(currentInput, context).matchBoundPath(vertex,
                currentInput.getProperty(relationshipVariable), reverseResultPath, relationshipVariable);
            currentPaths = bound != null ? List.of(bound).iterator() : null;
          } else
            currentPaths = createTraverser(currentInput, context).traversePaths(vertex);
        }
      }

      @Override
      public void close() {
        inputResults.close();
      }
    };
  }

  /**
   * The walk of the distinct end vertices through a Graph Analytical View, for one execution, or null to walk the records: see
   * {@link GAVVarLengthExpand}.
   */
  protected GAVDistinctEndpointTraverser createViewTraverser(final CommandContext context) {
    return null;
  }

  protected Direction getDirection() {
    return direction;
  }

  protected RelationshipPattern getPattern() {
    return pattern;
  }

  protected PathMode getPathMode() {
    return pathMode;
  }

  private VariableLengthPathTraverser createTraverser(final Result row, final CommandContext context) {
    final String[] types = pattern.hasTypes() ? pattern.getTypes().toArray(new String[0]) : null;
    final Map<String, Object> properties = InlineProperties.resolveAll(pattern.getProperties(), row, context);
    final VariableLengthPathTraverser traverser = new VariableLengthPathTraverser(
        direction, types, properties, pattern.getEffectiveMinHops(), pattern.getEffectiveMaxHops(),
        true, false, pathMode != null ? pathMode : PathMode.TRAIL);
    final Predicate<Edge> edgePredicate = pattern.buildInlineWherePredicate(row, context);
    traverser.withEdgePredicate(edgePredicate);
    traverser.withContext(context);
    // Relationships no one reads are followed without loading their records (issue #8537)
    traverser.withEdgesUnread((relationshipVariable == null || relationshipVariable.isEmpty())
        && (pathVariable == null || pathVariable.isEmpty()) && (properties == null || properties.isEmpty()) && edgePredicate == null);
    return traverser;
  }

  private static ResultInternal copy(final Result input) {
    return ResultInternal.copyBindings(input, 2);
  }

  private static TraversalPath extendPath(final Object existing, final TraversalPath extension) {
    if (!(existing instanceof TraversalPath path))
      return extension;
    if (sameVertex(path.getEndVertex(), extension.getStartVertex()))
      return new TraversalPath(path, extension);
    if (sameVertex(extension.getEndVertex(), path.getStartVertex()))
      return new TraversalPath(extension, path);
    return extension;
  }

  private static boolean sameVertex(final Vertex left, final Vertex right) {
    return left != null && right != null && left.getIdentity().equals(right.getIdentity());
  }

  /** What {@link #explain} prints after the walk's mode: the view a {@link GAVVarLengthExpand} reads. */
  protected String explainProvider() {
    return "";
  }

  @Override
  public String getOperatorType() {
    return "VarLengthExpand";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getIndent(depth)).append("+ ").append(getOperatorType()).append("(").append(sourceVariable).append(")");
    sb.append(direction == Direction.IN ? "<-[" : "-[");
    if (relationshipVariable != null)
      sb.append(relationshipVariable);
    if (pattern.hasTypes())
      sb.append(":").append(String.join("|", pattern.getTypes()));
    sb.append("*");
    if (pattern.getMinHops() != null)
      sb.append(pattern.getMinHops());
    sb.append("..");
    if (pattern.getMaxHops() != null)
      sb.append(pattern.getMaxHops());
    sb.append(direction == Direction.OUT ? "]->" : "]-");
    sb.append("(").append(targetVariable);
    if (targetLabel != null)
      sb.append(":").append(targetLabel);
    sb.append(distinctEndpoints ? ") [DISTINCT ENDPOINTS" : ") [DFS").append(explainProvider()).append(", cost=")
        .append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality).append("]\n");
    if (child != null)
      sb.append(child.explain(depth + 1));
    return sb.toString();
  }
}
