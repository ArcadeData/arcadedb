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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.query.opencypher.traversal.VariableLengthPathTraverser;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import com.arcadedb.graph.Edge;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for variable-length path patterns.
 * Handles patterns like (a)-[*1..3]->(b).
 * <p>
 * Uses specialized traversers (BFS/DFS) to efficiently find paths
 * within the specified hop range.
 */
public class ExpandPathStep extends AbstractExecutionStep {
  private final String sourceVariable;
  private final String pathVariable;
  private final String relationshipVariable;
  private final String targetVariable;
  private final RelationshipPattern pattern;
  private final NodePattern targetNodePattern;
  private final boolean useBFS;
  private final PathMode pathMode;
  private final Set<String> previousStepVariables;
  private final Direction directionOverride;
  private final boolean reverseResultPath;
  /**
   * Created only when the target node pattern carries Cypher 25 dynamic {@code $(expression)}
   * labels: every other pattern shape resolves its labels statically and must not pay for it.
   */
  private final ExpressionEvaluator dynamicLabelEvaluator;

  /**
   * Creates an expand path step.
   *
   * @param sourceVariable       variable name for source vertex
   * @param pathVariable         variable name for the path (can be null)
   * @param relationshipVariable variable name for the relationship list (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern with variable-length specification
   * @param useBFS               true for BFS (shortest paths), false for DFS (all paths)
   * @param context              command context
   */
  public ExpandPathStep(final String sourceVariable, final String pathVariable, final String relationshipVariable,
      final String targetVariable, final RelationshipPattern pattern, final boolean useBFS,
      final NodePattern targetNodePattern, final CommandContext context) {
    this(sourceVariable, pathVariable, relationshipVariable, targetVariable, pattern, useBFS,
        targetNodePattern, null, null, context);
  }

  public ExpandPathStep(final String sourceVariable, final String pathVariable, final String relationshipVariable,
      final String targetVariable, final RelationshipPattern pattern, final boolean useBFS,
      final NodePattern targetNodePattern, final PathMode pathMode, final CommandContext context) {
    this(sourceVariable, pathVariable, relationshipVariable, targetVariable, pattern, useBFS,
        targetNodePattern, pathMode, null, context);
  }

  /**
   * Creates an expand path step with previous-step variable scoping for relationship uniqueness.
   *
   * @param sourceVariable        variable name for source vertex
   * @param pathVariable          variable name for the path (can be null)
   * @param relationshipVariable  variable name for the relationship list (can be null)
   * @param targetVariable        variable name for target vertex
   * @param pattern               relationship pattern with variable-length specification
   * @param useBFS                true for BFS (shortest paths), false for DFS (all paths)
   * @param targetNodePattern     target node pattern for label/property filtering
   * @param pathMode              path mode override (DIFFERENT_RELATIONSHIPS, etc.)
   * @param previousStepVariables snapshot of variables bound by previous MATCH clauses (or WITH); these
   *                              are excluded from edge-uniqueness conflict checking, since Cypher's
   *                              relationship uniqueness only applies within a single MATCH clause
   */
  public ExpandPathStep(final String sourceVariable, final String pathVariable, final String relationshipVariable,
      final String targetVariable, final RelationshipPattern pattern, final boolean useBFS,
      final NodePattern targetNodePattern, final PathMode pathMode, final Set<String> previousStepVariables,
      final CommandContext context) {
    this(sourceVariable, pathVariable, relationshipVariable, targetVariable, pattern, useBFS,
        targetNodePattern, pathMode, previousStepVariables, null, false, context);
  }

  public ExpandPathStep(final String sourceVariable, final String pathVariable, final String relationshipVariable,
      final String targetVariable, final RelationshipPattern pattern, final boolean useBFS,
      final NodePattern targetNodePattern, final PathMode pathMode, final Set<String> previousStepVariables,
      final Direction directionOverride, final boolean reverseResultPath, final CommandContext context) {
    super(context);

    if (!pattern.isVariableLength())
      throw new IllegalArgumentException("ExpandPathStep requires a variable-length relationship pattern");

    this.sourceVariable = sourceVariable;
    this.pathVariable = pathVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.pattern = pattern;
    this.targetNodePattern = targetNodePattern;
    this.useBFS = useBFS;
    this.pathMode = pathMode;
    this.previousStepVariables = previousStepVariables;
    this.directionOverride = directionOverride;
    this.reverseResultPath = reverseResultPath;
    this.dynamicLabelEvaluator = targetNodePattern != null && targetNodePattern.hasDynamicLabels() ?
        new ExpressionEvaluator(new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance())) : null;
  }

  /**
   * Creates an expand path step with BFS (default).
   *
   * @param sourceVariable       variable name for source vertex
   * @param pathVariable         variable name for the path (can be null)
   * @param relationshipVariable variable name for the relationship list (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern with variable-length specification
   * @param context              command context
   */
  public ExpandPathStep(final String sourceVariable, final String pathVariable, final String relationshipVariable,
      final String targetVariable, final RelationshipPattern pattern, final CommandContext context) {
    this(sourceVariable, pathVariable, relationshipVariable, targetVariable, pattern, true, null, context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("ExpandPathStep requires a previous step");

    final boolean hasPathVar = pathVariable != null && !pathVariable.isEmpty();
    final boolean hasRelVar = relationshipVariable != null && !relationshipVariable.isEmpty();

    return new ResultSet() {
      private ResultSet prevResults = null;
      private Result lastResult = null;
      private Iterator<TraversalPath> currentPaths = null;
      private Vertex boundTarget = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        // Fetch more results
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        while (buffer.size() < n) {
          // Always use path traversal for correct Cypher edge-based relationship uniqueness
          if (currentPaths != null && currentPaths.hasNext()) {
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (context.isProfiling())
                rowCount++;

              final TraversalPath traversedPath = currentPaths.next();
              final Vertex targetVertex = traversedPath.getEndVertex();
              final TraversalPath path = reverseResultPath ? traversedPath.reversed() : traversedPath;

              // Filter by target node label if specified
              if (targetNodePattern != null && (targetNodePattern.hasLabels() || targetNodePattern.hasDynamicLabels())) {
                if (!matchesTargetLabel(targetVertex, lastResult))
                  continue;
              }

              // Filter by target node properties if specified
              if (targetNodePattern != null && targetNodePattern.hasProperties()) {
                if (!matchesTargetProperties(targetVertex, lastResult))
                  continue;
              }

              // If the target variable is already bound from a previous step,
              // only accept paths that end at the bound vertex
              if (boundTarget != null) {
                if (!targetVertex.getIdentity().equals(boundTarget.getIdentity()))
                  continue;
              }

              // Relationship uniqueness: check if any edge in this path is
              // already used by a relationship variable in the current result
              if (hasEdgeConflict(lastResult, path))
                continue;

              final ResultInternal result = new ResultInternal();

              // Copy all properties from previous result
              for (final String prop : lastResult.getPropertyNames()) {
                result.setProperty(prop, lastResult.getProperty(prop));
              }

              // Add path binding - extend existing path if present (multi-segment VLP)
              if (hasPathVar) {
                final Object existingPath = lastResult.getProperty(pathVariable);
                if (existingPath instanceof TraversalPath)
                  result.setProperty(pathVariable, new TraversalPath((TraversalPath) existingPath, path));
                else
                  result.setProperty(pathVariable, path);
              }

              // Add relationship variable as list of edges
              if (hasRelVar)
                result.setProperty(relationshipVariable, new ArrayList<>(path.getEdges()));

              // Add target vertex binding
              result.setProperty(targetVariable, targetVertex);

              buffer.add(result);
            } finally {
              if (context.isProfiling())
                cost += System.nanoTime() - begin;
            }
          } else {
            // Get next source vertex from previous step
            if (prevResults == null) {
              prevResults = prev.syncPull(context, nRecords);
            }

            if (!prevResults.hasNext()) {
              finished = true;
              break;
            }

            lastResult = prevResults.next();
            final Object sourceObj = lastResult.getProperty(sourceVariable);

            // Check if target variable is already bound (e.g., from a previous MATCH)
            final Object targetObj = lastResult.getProperty(targetVariable);
            boundTarget = targetObj instanceof Vertex vertex ? vertex : null;

            if (sourceObj instanceof Vertex) {
              final Vertex sourceVertex = (Vertex) sourceObj;
              currentPaths = createTraverser(lastResult).traversePaths(sourceVertex);
            } else {
              currentPaths = null;
            }
          }
        }
      }

      @Override
      public void close() {
        ExpandPathStep.this.close();
      }
    };
  }

  /**
   * Creates a traverser for this pattern.
   */
  private VariableLengthPathTraverser createTraverser(final Result currentResult) {
    final String[] types = pattern.hasTypes() ?
        pattern.getTypes().toArray(new String[0]) :
        null;

    // Resolved against this row before the traversal starts: a traverser cannot resolve a parameter or a
    // row-dependent value itself, and resolving here also keeps that work out of the per-edge loop.
    final Map<String, Object> props = InlineProperties.resolveAll(pattern.getProperties(), currentResult, context);

    final Direction direction = directionOverride != null ? directionOverride : pattern.getDirection();

    final VariableLengthPathTraverser traverser = pathMode != null ?
        new VariableLengthPathTraverser(
            direction, types, props,
            pattern.getEffectiveMinHops(), pattern.getEffectiveMaxHops(),
            true, useBFS, pathMode) :
        new VariableLengthPathTraverser(
            direction, types, props,
            pattern.getEffectiveMinHops(), pattern.getEffectiveMaxHops(),
            true, useBFS);

    // Inline WHERE, e.g. -[r:E*1..2 WHERE r.tag = 'ok']->: every traversed relationship must satisfy
    // it, matching the inline property map and the clause-level all(e IN r WHERE ...) spelling. Built
    // per source row so the predicate sees that row's bindings.
    traverser.withEdgePredicate(pattern.buildInlineWherePredicate(currentResult, context));
    return traverser;
  }

  /**
   * Checks if any edge in the traversal path conflicts with edges already
   * bound in the result (relationship uniqueness within a MATCH clause).
   * <p>
   * Cypher's relationship uniqueness rule only applies within a single MATCH clause.
   * Variables bound by previous MATCH clauses (or carried via WITH) must therefore
   * not block the current traversal even if they reference the same edge.
   */
  @SuppressWarnings("unchecked")
  private boolean hasEdgeConflict(final Result result, final TraversalPath path) {
    if (path.getEdges().isEmpty())
      return false;
    for (final String prop : result.getPropertyNames()) {
      // Skip our own variables
      if (prop.equals(relationshipVariable) || prop.equals(pathVariable) || prop.equals(targetVariable))
        continue;
      // Skip variables bound by previous MATCH clauses or WITH: Cypher's relationship
      // uniqueness only applies within the current MATCH clause
      if (previousStepVariables != null && previousStepVariables.contains(prop))
        continue;
      final Object val = result.getProperty(prop);
      if (val instanceof Edge) {
        for (final Edge pathEdge : path.getEdges())
          if (pathEdge.getIdentity().equals(((Edge) val).getIdentity()))
            return true;
      }
      if (val instanceof List) {
        for (final Object item : (List<Object>) val)
          if (item instanceof Edge) {
            for (final Edge pathEdge : path.getEdges())
              if (pathEdge.getIdentity().equals(((Edge) item).getIdentity()))
                return true;
          }
      }
    }
    return false;
  }

  /**
   * Applies the target node pattern labels to a traversed end vertex, with the same semantics
   * {@link com.arcadedb.query.opencypher.executor.steps.MatchNodeStep} uses when the node is
   * reached by a scan: a disjunction {@code (n:A|B)} accepts any of the labels, a conjunction
   * {@code (n:A:B)} requires all of them, and Cypher 25 dynamic {@code $(expression)} labels are
   * resolved against the current binding.
   */
  private boolean matchesTargetLabel(final Vertex vertex, final Result currentResult) {
    final List<String> labels = resolveEffectiveLabels(currentResult);
    if (labels.isEmpty())
      return true;

    if (targetNodePattern.isLabelDisjunction()) {
      for (final String label : labels)
        if (Labels.hasLabel(vertex, label))
          return true;
      return false;
    }

    for (final String label : labels)
      if (!Labels.hasLabel(vertex, label))
        return false;
    return true;
  }

  /**
   * Returns the labels the target vertex must satisfy, combining the statically written labels with
   * the result of evaluating any dynamic label expression against the current binding. A dynamic
   * expression may yield a single label or a collection of labels, all of which are required.
   */
  private List<String> resolveEffectiveLabels(final Result currentResult) {
    final List<String> staticLabels = targetNodePattern.getLabels();
    if (dynamicLabelEvaluator == null)
      return staticLabels;

    final List<String> labels = new ArrayList<>(staticLabels.size() + targetNodePattern.getDynamicLabels().size());
    labels.addAll(staticLabels);
    for (final Expression dynamicLabel : targetNodePattern.getDynamicLabels())
      appendResolvedLabels(labels, dynamicLabelEvaluator.evaluate(dynamicLabel, currentResult, context));
    return labels;
  }

  private static void appendResolvedLabels(final List<String> labels, final Object resolved) {
    if (resolved == null)
      return;
    if (resolved instanceof Iterable) {
      for (final Object item : (Iterable<?>) resolved)
        if (item != null)
          labels.add(item.toString());
    } else
      labels.add(resolved.toString());
  }

  private boolean matchesTargetProperties(final Vertex vertex, final Result currentResult) {
    return InlineProperties.matches(vertex, targetNodePattern.getProperties(), currentResult, context);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ EXPAND PATH ");
    builder.append("(").append(sourceVariable).append(")");
    builder.append(pattern);
    builder.append("(").append(targetVariable).append(")");
    builder.append(" [").append(useBFS ? "BFS" : "DFS").append("]");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
