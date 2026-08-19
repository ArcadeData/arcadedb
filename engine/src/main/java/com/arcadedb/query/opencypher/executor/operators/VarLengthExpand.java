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

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeIdentitySet;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.opencypher.traversal.VariableLengthPathTraverser;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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

  public void setTargetLabel(final String targetLabel) {
    this.targetLabel = targetLabel;
  }

  public String getTargetVariable() {
    return targetVariable;
  }

  @Override
  public ResultSet execute(final CommandContext context, final int nRecords) {
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);
    final ResultSet inputResults = child.execute(context, nRecords);
    final int batchSize = nRecords > 0 ? nRecords : 100;

    return new ResultSet() {
      private Result currentInput;
      private Vertex boundTarget;
      private Iterator<TraversalPath> currentPaths;
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
          currentPaths = source instanceof Vertex vertex ? createTraverser(currentInput, context).traversePaths(vertex) : null;
        }
      }

      @Override
      public void close() {
        inputResults.close();
      }
    };
  }

  private VariableLengthPathTraverser createTraverser(final Result row, final CommandContext context) {
    final String[] types = pattern.hasTypes() ? pattern.getTypes().toArray(new String[0]) : null;
    final Map<String, Object> properties = InlineProperties.resolveAll(pattern.getProperties(), row, context);
    final VariableLengthPathTraverser traverser = new VariableLengthPathTraverser(
        direction, types, properties, pattern.getEffectiveMinHops(), pattern.getEffectiveMaxHops(),
        true, false, pathMode != null ? pathMode : PathMode.TRAIL);
    traverser.withEdgePredicate(pattern.buildInlineWherePredicate(row, context));
    return traverser;
  }

  private static ResultInternal copy(final Result input) {
    final ResultInternal result = new ResultInternal();
    for (final String property : input.getPropertyNames())
      result.setProperty(property, input.getProperty(property));
    return result;
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

  @Override
  public String getOperatorType() {
    return "VarLengthExpand";
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getIndent(depth)).append("+ VarLengthExpand(").append(sourceVariable).append(")");
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
    sb.append(") [DFS, cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality).append("]\n");
    if (child != null)
      sb.append(child.explain(depth + 1));
    return sb.toString();
  }
}
