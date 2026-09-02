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

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.QuantifiedPathPattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Executes a GQL Quantified Path Pattern whose inner pattern cannot be lowered onto the
 * variable-length-relationship engine (issue #4531, "Phase B"):
 *
 * <pre>
 *   MATCH (a) ( (x)-[r1:R1]-&gt;(y)-[r2:R2]-&gt;(z) WHERE r1.w &gt; 5 ){1,3} (b)
 * </pre>
 *
 * <p>For every input row the step depth-first-searches the repetitions of the inner pattern, starting
 * at the vertex bound to the left boundary variable. Repetition {@code i+1} starts where repetition
 * {@code i} ended, the inner {@code WHERE} is evaluated once per repetition against that repetition's
 * own bindings, and a row is emitted for every reachable end vertex whose repetition count lies inside
 * the quantifier's bounds.
 *
 * <p>Variables written inside the group are <b>group variables</b>: each binds outside the group to a
 * list with one element per repetition ({@code LIST<NODE>} for a node variable,
 * {@code LIST<RELATIONSHIP>} for a relationship variable), in repetition order. Zero repetitions -
 * legal for a {@code {0,n}} or {@code *} quantifier - bind them to empty lists and leave both
 * boundaries on the same vertex.
 *
 * <p>Relationship isomorphism is enforced across the whole group and against the relationships the
 * incoming row already binds within the same MATCH clause: no relationship is traversed twice, which
 * is also what terminates an open-ended quantifier on a cyclic graph.
 */
public class QuantifiedPathStep extends AbstractExecutionStep {
  private final String                sourceVariable;
  private final String                targetVariable;
  private final String                pathVariable;
  private final boolean               pathVariableBindsGroup;
  private final QuantifiedPathPattern group;
  private final NodePattern           targetNodePattern;
  private final Set<String>           previousStepVariables;
  private final List<NodePattern>     innerNodes;
  private final List<RelationshipPattern> innerRelationships;
  /**
   * Created only when a boundary node pattern carries Cypher 25 dynamic {@code $(expression)} labels:
   * every other pattern shape resolves its labels statically and must not pay for it.
   */
  private final ExpressionEvaluator  dynamicLabelEvaluator;

  /**
   * @param sourceVariable         variable holding the group's left boundary vertex
   * @param targetVariable         variable the group's right boundary vertex binds to
   * @param pathVariable           enclosing named path variable, or null
   * @param pathVariableBindsGroup true when {@code pathVariable} names nothing but this group, in which
   *                               case it binds to a {@code LIST<PATH>} with one path per repetition
   *                               (ISO/IEC 39075 §15.4 grouped path assignment); false when the path
   *                               spans further segments and must stay a single concatenated path
   * @param group                  the quantified path pattern to repeat
   * @param targetNodePattern      right boundary node pattern, for label/property filtering; may be null
   * @param previousStepVariables  variables bound before this MATCH clause, exempt from relationship
   *                               isomorphism (Cypher scopes it to a single MATCH)
   */
  public QuantifiedPathStep(final String sourceVariable, final String targetVariable, final String pathVariable,
      final boolean pathVariableBindsGroup, final QuantifiedPathPattern group, final NodePattern targetNodePattern,
      final Set<String> previousStepVariables, final CommandContext context) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;
    this.pathVariable = pathVariable != null && !pathVariable.isEmpty() ? pathVariable : null;
    this.pathVariableBindsGroup = pathVariableBindsGroup;
    this.group = group;
    this.targetNodePattern = targetNodePattern;
    this.previousStepVariables = previousStepVariables;
    this.innerNodes = group.getInnerPattern().getNodes();
    this.innerRelationships = group.getInnerPattern().getRelationships();
    this.dynamicLabelEvaluator = targetNodePattern != null && targetNodePattern.hasDynamicLabels() ?
        new ExpressionEvaluator(new CypherFunctionFactory(DefaultSQLFunctionFactory.getInstance())) : null;
  }

  /** One matched repetition: the vertices and relationships the inner pattern bound this time round. */
  private record Repetition(Vertex[] nodes, Edge[] edges) {
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("QuantifiedPathStep requires a previous step");

    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private ResultSet     prevResults = null;
      private List<Result>  buffer      = new ArrayList<>();
      private int           bufferIndex = 0;
      private boolean       finished    = false;

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
        buffer = new ArrayList<>();
        bufferIndex = 0;

        while (buffer.size() < n) {
          guard.check();
          if (prevResults == null)
            prevResults = prev.syncPull(context, n);
          if (!prevResults.hasNext()) {
            finished = true;
            break;
          }

          final Result input = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            expand(input, buffer, guard);
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
        if (context.isProfiling())
          rowCount += buffer.size();
      }

      @Override
      public void close() {
        QuantifiedPathStep.this.close();
      }
    };
  }

  /**
   * Depth-first-searches every legal repetition count from the row's left boundary vertex and appends
   * one output row per accepted end vertex.
   */
  private void expand(final Result input, final List<Result> out, final WorkGuard guard) {
    if (!(input.getProperty(sourceVariable) instanceof Vertex start))
      return;

    final Object targetValue = input.getProperty(targetVariable);
    final Vertex boundTarget = targetValue instanceof Vertex vertex ? vertex : null;

    new RowSearch(input, boundTarget, out, guard).run(start);
  }

  /**
   * One input row's search. Everything the inner pattern needs that depends on the row and not on the
   * candidate - resolved inline property maps, inline relationship predicates, the resolved boundary
   * labels - is computed once here rather than per candidate relationship, which is where a group over a
   * dense vertex spends its time.
   */
  private final class RowSearch {
    private final Result                input;
    private final Vertex                boundTarget;
    private final List<Result>          out;
    private final WorkGuard             guard;
    private final Set<RID>              usedEdges;
    private final List<Repetition>      repetitions = new ArrayList<>();
    private final Map<String, Object>[] relationshipProperties;
    private final Predicate<Edge>[]     relationshipPredicates;
    private final String[][]            relationshipTypes;
    private final Map<String, Object>[] nodeProperties;
    private final List<String>          targetLabels;

    @SuppressWarnings("unchecked")
    private RowSearch(final Result input, final Vertex boundTarget, final List<Result> out, final WorkGuard guard) {
      this.input = input;
      this.boundTarget = boundTarget;
      this.out = out;
      this.guard = guard;
      // Seeding the used-relationship set with the ones the row already binds makes isomorphism a single
      // check inside the search instead of a whole-match rejection at the end, so a conflicting branch is
      // pruned as soon as it appears.
      this.usedEdges = collectBoundRelationships(input);

      final int hops = innerRelationships.size();
      this.relationshipProperties = new Map[hops];
      this.relationshipPredicates = new Predicate[hops];
      this.relationshipTypes = new String[hops][];
      for (int i = 0; i < hops; i++) {
        final RelationshipPattern relationship = innerRelationships.get(i);
        if (relationship.hasProperties())
          relationshipProperties[i] = InlineProperties.resolveAll(relationship.getProperties(), input, context);
        // Built through the pattern's own factory so the inline-WHERE evaluation scope has one definition
        // shared with every other traversal evaluator, per RelationshipPattern#buildInlineWherePredicate.
        relationshipPredicates[i] = relationship.buildInlineWherePredicate(input, context);
        if (relationship.hasTypes())
          relationshipTypes[i] = relationship.getTypes().toArray(new String[0]);
      }

      this.nodeProperties = new Map[innerNodes.size()];
      for (int i = 0; i < innerNodes.size(); i++) {
        final NodePattern node = innerNodes.get(i);
        if (node.hasProperties())
          nodeProperties[i] = InlineProperties.resolveAll(node.getProperties(), input, context);
      }

      this.targetLabels = targetNodePattern != null ? resolveLabels(targetNodePattern, input) : null;
    }

    private void run(final Vertex start) {
      search(start);
    }

    private void search(final Vertex current) {
      guard.check();

      final int done = repetitions.size();
      if (done >= group.getMinRepetitions() && acceptsEndpoint(current))
        out.add(buildRow(input, current, repetitions));

      if (done >= group.getMaxRepetitions())
        return;

      // The inner start node's own constraints hold for every repetition, not only the first: the outer
      // boundary node can only carry them for the vertex it binds.
      if (!matchesInnerNode(current, 0))
        return;

      final Vertex[] nodes = new Vertex[innerNodes.size()];
      final Edge[] edges = new Edge[innerRelationships.size()];
      nodes[0] = current;
      matchRepetition(nodes, edges, 0);
    }

    /**
     * Matches one repetition of the inner pattern hop by hop, recursing into {@link #search} for every
     * complete assignment. Relationships consumed by the branch are held in {@code usedEdges} for the
     * duration of the recursion below them and released on backtracking.
     */
    private void matchRepetition(final Vertex[] nodes, final Edge[] edges, final int hop) {
      if (hop == innerRelationships.size()) {
        if (group.getInnerWhere() != null
            && !group.getInnerWhere().evaluate(repetitionScope(input, nodes, edges), context))
          return;
        repetitions.add(new Repetition(nodes.clone(), edges.clone()));
        search(nodes[nodes.length - 1]);
        repetitions.remove(repetitions.size() - 1);
        return;
      }

      final Direction direction = innerRelationships.get(hop).getDirection();
      final Vertex from = nodes[hop];
      final String[] types = relationshipTypes[hop];

      final Iterator<Edge> candidates = types != null ?
          from.getEdges(direction.toArcadeDirection(), types).iterator() :
          from.getEdges(direction.toArcadeDirection()).iterator();

      while (candidates.hasNext()) {
        guard.check();
        final Edge edge = candidates.next();
        final RID edgeId = edge.getIdentity();
        if (usedEdges.contains(edgeId))
          continue;
        if (relationshipProperties[hop] != null && !matchesResolved(edge, relationshipProperties[hop]))
          continue;
        if (relationshipPredicates[hop] != null && !relationshipPredicates[hop].test(edge))
          continue;

        final Vertex next = otherEnd(edge, from, direction);
        if (!matchesInnerNode(next, hop + 1))
          continue;

        edges[hop] = edge;
        nodes[hop + 1] = next;
        usedEdges.add(edgeId);
        matchRepetition(nodes, edges, hop + 1);
        usedEdges.remove(edgeId);
      }
    }

    /** Applies inner node {@code index}'s labels, resolved inline properties and inline WHERE. */
    private boolean matchesInnerNode(final Vertex vertex, final int index) {
      final NodePattern pattern = innerNodes.get(index);
      if (pattern.hasLabels() && !Labels.matches(vertex, pattern.getLabels(), pattern.isLabelDisjunction()))
        return false;
      if (nodeProperties[index] != null && !matchesResolved(vertex, nodeProperties[index]))
        return false;
      return !pattern.hasWhereExpression() || matchesNodeWhere(vertex, pattern);
    }

    /** Checks the right boundary: the pinned target if the row already bound one, then its own constraints. */
    private boolean acceptsEndpoint(final Vertex candidate) {
      if (boundTarget != null && !candidate.getIdentity().equals(boundTarget.getIdentity()))
        return false;
      if (targetNodePattern == null)
        return true;
      if ((targetNodePattern.hasLabels() || targetNodePattern.hasDynamicLabels())
          && !Labels.matches(candidate, targetLabels, targetNodePattern.isLabelDisjunction()))
        return false;
      if (targetNodePattern.hasProperties()
          && !InlineProperties.matches(candidate, targetNodePattern.getProperties(), input, context))
        return false;
      return !targetNodePattern.hasWhereExpression() || matchesNodeWhere(candidate, targetNodePattern);
    }

    private boolean matchesNodeWhere(final Vertex vertex, final NodePattern pattern) {
      final ResultInternal scope = new ResultInternal();
      for (final String property : input.getPropertyNames())
        scope.setProperty(property, input.getProperty(property));
      final String variable = pattern.getVariable();
      if (variable != null && !variable.isEmpty())
        scope.setProperty(variable, vertex);
      return pattern.getWhereExpression().evaluate(scope, context);
    }
  }

  private static boolean matchesResolved(final Document record, final Map<String, Object> resolved) {
    for (final Map.Entry<String, Object> entry : resolved.entrySet())
      if (!InlineProperties.matchesResolvedValue(record.get(entry.getKey()), entry.getValue()))
        return false;
    return true;
  }

  /**
   * Builds the scope one repetition's predicates are evaluated in: the incoming row's bindings plus this
   * repetition's own single-valued bindings. Single-valued, not the group lists - an inner {@code WHERE}
   * constrains the repetition it is written inside, so {@code WHERE r.weight > 5} reads one relationship.
   */
  private Result repetitionScope(final Result input, final Vertex[] nodes, final Edge[] edges) {
    final ResultInternal scope = new ResultInternal();
    for (final String property : input.getPropertyNames())
      scope.setProperty(property, input.getProperty(property));
    for (int i = 0; i < nodes.length; i++) {
      final String variable = innerNodes.get(i).getVariable();
      if (variable != null && !variable.isEmpty() && nodes[i] != null)
        scope.setProperty(variable, nodes[i]);
      if (i < edges.length) {
        final String relVariable = innerRelationships.get(i).getVariable();
        if (relVariable != null && !relVariable.isEmpty() && edges[i] != null)
          scope.setProperty(relVariable, edges[i]);
      }
    }
    return scope;
  }

  /**
   * The labels a node pattern requires, with any Cypher 25 dynamic {@code $(expression)} label resolved
   * against the current row. Only the boundary node patterns can carry one - a dynamic label inside the
   * group is refused at parse time, since it would resolve per row rather than per repetition.
   */
  private List<String> resolveLabels(final NodePattern pattern, final Result input) {
    if (!pattern.hasDynamicLabels())
      return pattern.getLabels();

    final List<String> labels = new ArrayList<>(pattern.getLabels());
    for (final Expression dynamicLabel : pattern.getDynamicLabels()) {
      final Object resolved = dynamicLabelEvaluator.evaluate(dynamicLabel, input, context);
      if (resolved instanceof Iterable<?> items) {
        for (final Object item : items)
          if (item != null)
            labels.add(item.toString());
      } else if (resolved != null)
        labels.add(resolved.toString());
    }
    return labels;
  }

  private Result buildRow(final Result input, final Vertex end, final List<Repetition> repetitions) {
    final ResultInternal row = new ResultInternal();
    for (final String property : input.getPropertyNames())
      row.setProperty(property, input.getProperty(property));

    for (int i = 0; i < innerNodes.size(); i++) {
      final String variable = innerNodes.get(i).getVariable();
      if (variable != null && !variable.isEmpty()) {
        final List<Vertex> values = new ArrayList<>(repetitions.size());
        for (final Repetition repetition : repetitions)
          values.add(repetition.nodes()[i]);
        row.setProperty(variable, values);
      }
    }
    for (int i = 0; i < innerRelationships.size(); i++) {
      final String variable = innerRelationships.get(i).getVariable();
      if (variable != null && !variable.isEmpty()) {
        final List<Edge> values = new ArrayList<>(repetitions.size());
        for (final Repetition repetition : repetitions)
          values.add(repetition.edges()[i]);
        row.setProperty(variable, values);
      }
    }

    row.setProperty(targetVariable, end);

    if (pathVariable != null) {
      if (pathVariableBindsGroup) {
        final List<TraversalPath> paths = new ArrayList<>(repetitions.size());
        for (final Repetition repetition : repetitions)
          paths.add(toPath(repetition));
        row.setProperty(pathVariable, paths);
      } else {
        TraversalPath concatenated = input.getProperty(pathVariable) instanceof TraversalPath existing ?
            existing : null;
        for (final Repetition repetition : repetitions) {
          final TraversalPath segment = toPath(repetition);
          concatenated = concatenated == null ? segment : new TraversalPath(concatenated, segment);
        }
        if (concatenated == null)
          concatenated = new TraversalPath(end);
        row.setProperty(pathVariable, concatenated);
      }
    }
    return row;
  }

  private static TraversalPath toPath(final Repetition repetition) {
    TraversalPath path = new TraversalPath(repetition.nodes()[0]);
    for (int i = 0; i < repetition.edges().length; i++)
      path = new TraversalPath(path, repetition.edges()[i], repetition.nodes()[i + 1]);
    return path;
  }

  /**
   * Collects the relationships the incoming row already binds within this MATCH clause, so the group
   * cannot reuse one. Variables carried in from a previous MATCH or a WITH are skipped: Cypher scopes
   * relationship isomorphism to a single MATCH clause.
   */
  private Set<RID> collectBoundRelationships(final Result input) {
    final Set<RID> used = new HashSet<>();
    final List<String> groupVariables = group.getGroupVariables();
    for (final String property : input.getPropertyNames()) {
      if (property.equals(targetVariable) || property.equals(pathVariable) || groupVariables.contains(property))
        continue;
      if (previousStepVariables != null && previousStepVariables.contains(property))
        continue;
      collectRelationships(input.getProperty(property), used);
    }
    return used;
  }

  private static void collectRelationships(final Object value, final Set<RID> used) {
    if (value instanceof Edge edge)
      used.add(edge.getIdentity());
    else if (value instanceof TraversalPath path) {
      for (final Edge edge : path.getEdges())
        used.add(edge.getIdentity());
    } else if (value instanceof Iterable<?> items) {
      for (final Object item : items)
        collectRelationships(item, used);
    }
  }

  private static Vertex otherEnd(final Edge edge, final Vertex from, final Direction direction) {
    if (direction == Direction.OUT)
      return edge.getInVertex();
    if (direction == Direction.IN)
      return edge.getOutVertex();
    final Vertex out = edge.getOutVertex();
    return out.getIdentity().equals(from.getIdentity()) ? edge.getInVertex() : out;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder("  ".repeat(Math.max(0, depth * indent)));
    builder.append("+ QUANTIFIED PATH (").append(sourceVariable).append(")").append(group)
        .append("(").append(targetVariable).append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
