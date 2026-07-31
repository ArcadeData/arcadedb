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
package com.arcadedb.query.opencypher.ast;

import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.executor.SelfLoops;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Expression representing a pattern comprehension.
 * Syntax: [(variable =)? pattern WHERE filterExpression | mapExpression]
 * <p>
 * Examples:
 * - [(a)-->(friend) | friend.name] -> list of friend names
 * - [(a)-->(friend) WHERE friend.name <> 'B' | friend.name] -> filtered list
 * - [(a)-[:KNOWS]->(friend) | friend.name] -> list of KNOWS friend names
 */
public class PatternComprehensionExpression implements Expression {
  private final String pathVariable;      // Optional path variable name
  private final PathPattern pathPattern;
  private final Expression whereExpression;  // Optional filter
  private final Expression mapExpression;    // Required mapping (after |)
  private final String text;

  public PatternComprehensionExpression(final String pathVariable, final PathPattern pathPattern,
      final Expression whereExpression, final Expression mapExpression, final String text) {
    this.pathVariable = pathVariable;
    this.pathPattern = pathPattern;
    this.whereExpression = whereExpression;
    this.mapExpression = mapExpression;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final List<Object> resultList = new ArrayList<>();
    final List<Object> pathElements = new ArrayList<>();
    traversePattern(result, context, 0, result, resultList, pathElements, null);
    return resultList;
  }

  /**
   * Recursively traverse pattern hops, collecting results at each complete match.
   *
   * @param knownStartVertex the vertex that starts this hop when it is already known (the end
   *                         vertex of the previous hop). This is required for hops whose start
   *                         node is anonymous (e.g. {@code (:Person)} in a 2-hop pattern), because
   *                         such nodes carry no variable to resolve from {@code currentResult}
   *                         (issue #5007). Null at the first hop, where the start is resolved from
   *                         the outer-bound variable instead.
   */
  private void traversePattern(final Result baseResult, final CommandContext context,
      final int hopIndex, final Result currentResult, final List<Object> resultList,
      final List<Object> pathElements, final Vertex knownStartVertex) {
    if (hopIndex >= pathPattern.getRelationshipCount()) {
      // All hops matched - apply WHERE filter and map expression
      if (whereExpression != null) {
        final Object filterValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(whereExpression, currentResult, context);
        if (filterValue == null || (filterValue instanceof Boolean b && !b))
          return;
      }
      // Bind path variable if present (e.g., [p = (n)-->() | p])
      Result evalResult = currentResult;
      if (pathVariable != null && !pathVariable.isEmpty()) {
        final ResultInternal pathResult = new ResultInternal();
        for (final String prop : currentResult.getPropertyNames())
          pathResult.setProperty(prop, currentResult.getProperty(prop));
        pathResult.setProperty(pathVariable, new ArrayList<>(pathElements));
        evalResult = pathResult;
      }
      resultList.add(OpenCypherQueryEngine.getExpressionEvaluator().evaluate(mapExpression, evalResult, context));
      return;
    }

    final NodePattern startNodePattern = pathPattern.getNode(hopIndex);
    // For hops after the first, the start vertex is the end vertex of the previous hop and is
    // passed in directly. This keeps anonymous intermediate nodes working, since they have no
    // variable to resolve from currentResult (issue #5007).
    final Vertex startVertex = knownStartVertex != null ? knownStartVertex : resolveVertex(startNodePattern, currentResult);

    // First-hop start node is uncorrelated (no outer-bound variable, or the variable
    // is not bound to a vertex): iterate over candidate vertices in the graph
    // (issue #4106).
    if (startVertex == null) {
      if (hopIndex == 0)
        traverseUncorrelatedStart(baseResult, context, currentResult, resultList, pathElements, startNodePattern);
      return;
    }

    // Inline WHERE predicate on the leading node, e.g. [(a WHERE a.v = 1)-[:E]->(x) | x]. Only the
    // first hop needs the check here: every later hop starts from the previous hop's end node, which
    // matchesEndPattern already validated, and an uncorrelated start is validated while iterating
    // candidates in traverseUncorrelatedStart.
    if (hopIndex == 0 && knownStartVertex == null
        && !matchesNodeWhereExpression(startVertex, startNodePattern, inlineWhereRow(startNodePattern, currentResult), context))
      return;

    // Add start vertex to path at first hop
    if (hopIndex == 0 && pathVariable != null)
      pathElements.add(startVertex);

    final RelationshipPattern relPattern = pathPattern.getRelationship(hopIndex);
    final NodePattern endNodePattern = pathPattern.getNode(hopIndex + 1);

    final List<String> relTypes = relPattern.getTypes();
    final String[] relTypeArray = relTypes != null && !relTypes.isEmpty() ? relTypes.toArray(new String[0]) : null;

    // An undirected hop walks the merged BOTH adjacency in a single pass (see adjacency()). Two
    // separate OUT and IN passes would emit each self-loop twice, because a self-loop is stored in
    // both lists of its vertex (issue #5456), and for a variable-length hop they would also miss
    // every path that changes direction mid-walk.
    final Direction direction = relPattern.getDirection();

    if (relPattern.isVariableLength()) {
      final int minHops = relPattern.getEffectiveMinHops();
      final int maxHops = relPattern.getEffectiveMaxHops();

      // Zero-length path: start and end are the same vertex (only valid if matches end pattern)
      if (minHops == 0
          && matchesEndPattern(startVertex, endNodePattern, baseResult, currentResult,
              inlineWhereRow(endNodePattern, currentResult), relPattern, null, context)) {
        final ResultInternal hopResult = buildHopResult(currentResult, endNodePattern, startVertex, relPattern, null);
        traversePattern(baseResult, context, hopIndex + 1, hopResult, resultList, pathElements, startVertex);
      }

      if (maxHops >= 1)
        traverseVariableLength(baseResult, context, hopIndex, currentResult, resultList, pathElements,
            startVertex, direction, relTypeArray, endNodePattern, relPattern, 0, minHops, maxHops, new HashSet<>());
    } else
      traverseEdges(baseResult, context, hopIndex, currentResult, resultList, pathElements,
          startVertex, direction, relTypeArray, endNodePattern, relPattern);

    // Remove start vertex from path when backtracking
    if (hopIndex == 0 && pathVariable != null && !pathElements.isEmpty())
      pathElements.removeLast();
  }

  /**
   * Iterates over candidate vertices when the start node of the pattern has no
   * outer-bound variable (issue #4106). Candidates are filtered by the start
   * node pattern's labels and inline properties.
   */
  private void traverseUncorrelatedStart(final Result baseResult, final CommandContext context,
      final Result currentResult, final List<Object> resultList, final List<Object> pathElements,
      final NodePattern startNodePattern) {
    final List<String> startLabels = startNodePattern.getLabels();
    final Iterable<? extends Record> candidates;
    if (startLabels != null && !startLabels.isEmpty()) {
      // Use the first label as the iteration root; remaining labels are checked per vertex.
      // Polymorphic iteration so subtypes (e.g. composite multi-label types) are visited.
      final String typeName = startLabels.get(0);
      if (!context.getDatabase().getSchema().existsType(typeName))
        return;
      candidates = () -> context.getDatabase().iterateType(typeName, true);
    } else {
      // No label constraint: iterate every vertex type registered in the schema.
      candidates = collectAllVertices(context);
    }

    // Row reused across every candidate vertex, for the same reason as the edge expansions below:
    // only the node variable changes per candidate, so the enclosing bindings are copied once.
    final ResultInternal whereEvalRow = inlineWhereRow(startNodePattern, currentResult);

    for (final Record record : candidates) {
      if (!(record instanceof Vertex candidate))
        continue;
      if (!matchesStartPattern(candidate, startNodePattern, currentResult, whereEvalRow, context))
        continue;

      final ResultInternal candidateResult = new ResultInternal();
      if (currentResult != null)
        for (final String prop : currentResult.getPropertyNames())
          candidateResult.setProperty(prop, currentResult.getProperty(prop));
      if (startNodePattern.getVariable() != null && !startNodePattern.getVariable().isEmpty())
        candidateResult.setProperty(startNodePattern.getVariable(), candidate);

      // Re-enter traversal with this candidate as the known start vertex. Passing it directly
      // (rather than re-resolving) also handles an anonymous uncorrelated start node, which has
      // no variable to bind and would otherwise re-trigger this iteration endlessly (issue #5007).
      traversePattern(baseResult, context, 0, candidateResult, resultList, pathElements, candidate);
    }
  }

  /**
   * Returns true if a vertex matches the start node pattern's labels, inline properties and inline
   * {@code WHERE} predicate.
   */
  private boolean matchesStartPattern(final Vertex vertex, final NodePattern startNodePattern, final Result bindings,
      final ResultInternal whereEvalRow, final CommandContext context) {
    if (startNodePattern.hasLabels()) {
      for (final String label : startNodePattern.getLabels()) {
        if (!Labels.hasLabel(vertex, label))
          return false;
      }
    }
    if (!InlineProperties.matches(vertex, startNodePattern.getProperties(), bindings, context))
      return false;
    return matchesNodeWhereExpression(vertex, startNodePattern, whereEvalRow, context);
  }

  /**
   * Collects an Iterable that walks all vertex-typed records in the database.
   */
  private static Iterable<Record> collectAllVertices(final CommandContext context) {
    final List<Record> all = new ArrayList<>();
    for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
      if (!(type instanceof VertexType))
        continue;
      final Iterator<Record> it = context.getDatabase().iterateType(type.getName(), false);
      while (it.hasNext())
        all.add(it.next());
    }
    return all;
  }

  private void traverseVariableLength(final Result baseResult, final CommandContext context,
      final int hopIndex, final Result currentResult, final List<Object> resultList,
      final List<Object> pathElements,
      final Vertex currentVertex, final Direction direction,
      final String[] relTypeArray, final NodePattern endNodePattern,
      final RelationshipPattern relPattern,
      final int currentHop, final int minHops, final int maxHops,
      final Set<RID> visitedEdges) {
    if (currentHop >= maxHops)
      return;

    final Iterator<Edge> edges = adjacency(currentVertex, direction, relTypeArray);

    // Row reused across every candidate edge of this expansion: only the relationship variable
    // changes per edge, so the enclosing bindings are copied once. Stays null when the pattern
    // carries no inline WHERE, leaving the common path allocation-free.
    final ResultInternal whereEvalRow = relPattern.hasWhereExpression() ? copyBindings(currentResult) : null;
    // Same reuse for the end node's inline WHERE: only the node variable changes per candidate.
    final ResultInternal nodeWhereEvalRow = inlineWhereRow(endNodePattern, currentResult);

    while (edges.hasNext()) {
      final Edge edge = edges.next();
      // Inline relationship property filter, e.g. [p = (a)-[:VE*1..4 {w:1}]->(c) | ...] (issue #5139).
      // Every relationship in the path must satisfy the map, so skip non-matching edges before recursing.
      if (!matchesEdgeProperties(edge, relPattern, currentResult, context))
        continue;
      // Inline relationship WHERE predicate, e.g. [(a)-[r:E*1..2 WHERE r.tag = 'ok']->(b) | b]. As with
      // the property map, every relationship in the path must satisfy it.
      if (whereEvalRow != null && !matchesEdgeWhereExpression(edge, relPattern, whereEvalRow, context))
        continue;
      final RID edgeRid = edge.getIdentity();
      // Trail semantics: do not repeat the same edge in a single path
      if (!visitedEdges.add(edgeRid))
        continue;

      final Vertex nextVertex;
      try {
        nextVertex = otherEnd(edge, currentVertex, direction);
      } catch (final RecordNotFoundException e) {
        // Ghost edge: dangling segment pointer to a missing edge/target record. Undo the visit mark and skip.
        GhostEdgeReporter.reportSkipped(e);
        visitedEdges.remove(edgeRid);
        continue;
      }
      final int nextHop = currentHop + 1;
      final boolean trackPath = pathVariable != null;
      if (trackPath) {
        pathElements.add(edge);
        pathElements.add(nextVertex);
      }

      if (nextHop >= minHops
          && matchesEndPattern(nextVertex, endNodePattern, baseResult, currentResult, nodeWhereEvalRow, relPattern, edge,
              context)) {
        final ResultInternal hopResult = buildHopResult(currentResult, endNodePattern, nextVertex, relPattern, edge);
        traversePattern(baseResult, context, hopIndex + 1, hopResult, resultList, pathElements, nextVertex);
      }

      if (nextHop < maxHops)
        traverseVariableLength(baseResult, context, hopIndex, currentResult, resultList, pathElements,
            nextVertex, direction, relTypeArray, endNodePattern, relPattern, nextHop, minHops, maxHops, visitedEdges);

      if (trackPath) {
        pathElements.removeLast();
        pathElements.removeLast();
      }
      visitedEdges.remove(edgeRid);
    }
  }

  private boolean matchesEndPattern(final Vertex vertex, final NodePattern endNodePattern, final Result baseResult,
      final Result bindings, final ResultInternal whereEvalRow, final RelationshipPattern relPattern, final Edge edge,
      final CommandContext context) {
    if (endNodePattern.hasLabels()) {
      for (final String label : endNodePattern.getLabels()) {
        if (!Labels.hasLabel(vertex, label))
          return false;
      }
    }
    if (!InlineProperties.matches(vertex, endNodePattern.getProperties(), bindings, context))
      return false;
    // Variable binding consistency (issue #4111): if the end variable is already
    // bound in the outer scope, the candidate target must equal that vertex.
    if (baseResult != null && endNodePattern.getVariable() != null) {
      final Object bound = baseResult.getProperty(endNodePattern.getVariable());
      if (bound instanceof Vertex boundVertex && !boundVertex.getIdentity().equals(vertex.getIdentity()))
        return false;
    }
    // Inline WHERE predicate, e.g. the WHERE x.v = 2 in [(a)-[:E]->(x:A WHERE x.v = 2) | x] (issue #5480).
    // The predicate may also reference the relationship variable bound by this same hop, as in
    // (x:A WHERE x.v > r.w), so that binding is published before the predicate is evaluated. The
    // relationship of a zero-length hop is null, leaving the variable unbound as the pattern implies.
    if (whereEvalRow != null && edge != null) {
      final String relVariable = relPattern.getVariable();
      if (relVariable != null && !relVariable.isEmpty())
        whereEvalRow.setProperty(relVariable, edge);
    }
    return matchesNodeWhereExpression(vertex, endNodePattern, whereEvalRow, context);
  }

  /**
   * Builds the row an inline node {@code WHERE} predicate is evaluated against: a private copy of
   * the bindings visible at that point, on which the node variable is later rebound per candidate.
   * Returns {@code null} when the pattern carries no predicate, leaving the common path
   * allocation-free. Callers hoist the call out of their candidate loop so a high-fan-out expansion
   * copies the bindings once rather than once per candidate.
   */
  private static ResultInternal inlineWhereRow(final NodePattern nodePattern, final Result bindings) {
    return nodePattern != null && nodePattern.hasWhereExpression() ? copyBindings(bindings) : null;
  }

  /**
   * Returns true if a vertex satisfies the node pattern's inline {@code WHERE} predicate, e.g. the
   * {@code WHERE x.v = 2} in {@code [(a)-[:E]->(x:A WHERE x.v = 2) | x]}. Mirrors the enforcement
   * done by the regular MATCH path, where the same predicate is hoisted into the clause WHERE, so
   * both spellings filter identically.
   *
   * @param whereEvalRow the row produced by {@link #inlineWhereRow(NodePattern, Result)} for this
   *                     pattern. The node variable is rebound on it for each candidate, so the
   *                     caller's own row is never mutated. Only read when the pattern actually
   *                     declares a predicate, so a {@code null} row is fine otherwise.
   */
  private boolean matchesNodeWhereExpression(final Vertex vertex, final NodePattern nodePattern,
      final ResultInternal whereEvalRow, final CommandContext context) {
    if (nodePattern == null || !nodePattern.hasWhereExpression())
      return true;

    final String variable = nodePattern.getVariable();
    if (variable != null && !variable.isEmpty())
      whereEvalRow.setProperty(variable, vertex);
    return nodePattern.getWhereExpression().evaluate(whereEvalRow, context);
  }

  /**
   * Returns true if an edge satisfies the relationship pattern's inline property map
   * (e.g. the {@code {w:1}} in {@code -[:VE {w:1}]->}). Mirrors the enforcement done by the
   * regular MATCH path (MatchRelationshipStep), so pattern comprehensions apply the same
   * required-property semantics (issue #5139).
   */
  private boolean matchesEdgeProperties(final Edge edge, final RelationshipPattern relPattern, final Result bindings,
      final CommandContext context) {
    return InlineProperties.matches(edge, relPattern.getProperties(), bindings, context);
  }

  /**
   * Returns true if an edge satisfies the relationship pattern's inline {@code WHERE} predicate
   * (e.g. the {@code WHERE r.tag = 'ok'} in {@code -[r:E WHERE r.tag = 'ok']->}). Mirrors the
   * enforcement done by the regular MATCH path (MatchRelationshipStep.matchesEdgeWhereExpression),
   * so pattern comprehensions apply the same predicate semantics.
   *
   * @param evalRow bindings visible to the predicate, pre-populated with the enclosing scope. The
   *                relationship variable is rebound on it for each candidate edge.
   */
  private boolean matchesEdgeWhereExpression(final Edge edge, final RelationshipPattern relPattern,
      final ResultInternal evalRow, final CommandContext context) {
    final String variable = relPattern.getVariable();
    if (variable != null && !variable.isEmpty())
      evalRow.setProperty(variable, edge);
    return relPattern.getWhereExpression().evaluate(evalRow, context);
  }

  private static ResultInternal copyBindings(final Result source) {
    final ResultInternal copy = new ResultInternal();
    if (source != null)
      for (final String prop : source.getPropertyNames())
        copy.setProperty(prop, source.getProperty(prop));
    return copy;
  }

  private ResultInternal buildHopResult(final Result currentResult, final NodePattern endNodePattern, final Vertex targetVertex,
      final RelationshipPattern relPattern, final Edge edge) {
    final ResultInternal hopResult = new ResultInternal();
    if (currentResult != null)
      for (final String prop : currentResult.getPropertyNames())
        hopResult.setProperty(prop, currentResult.getProperty(prop));
    if (endNodePattern.getVariable() != null)
      hopResult.setProperty(endNodePattern.getVariable(), targetVertex);
    if (relPattern.getVariable() != null && edge != null)
      hopResult.setProperty(relPattern.getVariable(), edge);
    return hopResult;
  }

  private void traverseEdges(final Result baseResult, final CommandContext context,
      final int hopIndex, final Result currentResult, final List<Object> resultList,
      final List<Object> pathElements,
      final Vertex startVertex, final Direction direction,
      final String[] relTypeArray, final NodePattern endNodePattern,
      final RelationshipPattern relPattern) {
    final Iterator<Edge> edges = adjacency(startVertex, direction, relTypeArray);

    // Row reused across every candidate edge of this expansion: only the relationship variable
    // changes per edge, so the enclosing bindings are copied once. Stays null when the pattern
    // carries no inline WHERE, leaving the common path allocation-free.
    final ResultInternal whereEvalRow = relPattern.hasWhereExpression() ? copyBindings(currentResult) : null;
    // Same reuse for the end node's inline WHERE: only the node variable changes per candidate.
    final ResultInternal nodeWhereEvalRow = inlineWhereRow(endNodePattern, currentResult);

    while (edges.hasNext()) {
      final Edge edge = edges.next();
      // Inline relationship property filter, e.g. [(a)-[:VE {w:1}]->(x) | ...] (issue #5139).
      if (!matchesEdgeProperties(edge, relPattern, currentResult, context))
        continue;
      // Inline relationship WHERE predicate, e.g. [(a)-[r:E WHERE r.tag = 'ok']->(x) | ...].
      if (whereEvalRow != null && !matchesEdgeWhereExpression(edge, relPattern, whereEvalRow, context))
        continue;

      final Vertex targetVertex;
      try {
        targetVertex = otherEnd(edge, startVertex, direction);
      } catch (final RecordNotFoundException e) {
        GhostEdgeReporter.reportSkipped(e);
        continue;
      }

      if (!matchesEndPattern(targetVertex, endNodePattern, baseResult, currentResult, nodeWhereEvalRow, relPattern, edge,
          context))
        continue;

      // Build result with matched variables
      final ResultInternal hopResult = new ResultInternal();
      // Copy base result properties
      if (currentResult != null)
        for (final String prop : currentResult.getPropertyNames())
          hopResult.setProperty(prop, currentResult.getProperty(prop));

      // Bind the target node variable
      if (endNodePattern.getVariable() != null)
        hopResult.setProperty(endNodePattern.getVariable(), targetVertex);

      // Bind the relationship variable
      if (relPattern.getVariable() != null)
        hopResult.setProperty(relPattern.getVariable(), edge);

      // Add edge and target to path elements for path variable
      if (pathVariable != null) {
        pathElements.add(edge);
        pathElements.add(targetVertex);
      }

      // Continue to next hop or collect result. The matched target is the start of the next hop,
      // which is required when the next hop's start node is anonymous (issue #5007).
      traversePattern(baseResult, context, hopIndex + 1, hopResult, resultList, pathElements, targetVertex);

      // Remove edge and target from path when backtracking
      if (pathVariable != null) {
        pathElements.removeLast();
        pathElements.removeLast();
      }
    }
  }

  /**
   * Walks the adjacency of a vertex in the direction the pattern asks for. An undirected hop is a
   * single pass over the merged BOTH list, deduplicated so a self-loop - which is stored in both the
   * outgoing and the incoming list of its vertex - contributes one match per relationship rather than
   * two (issue #5456). The same rule is applied by the MATCH executors through {@link SelfLoops}.
   */
  private static Iterator<Edge> adjacency(final Vertex vertex, final Direction direction, final String[] relTypeArray) {
    final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
    final Iterator<Edge> edges = relTypeArray != null ?
        vertex.getEdges(arcadeDirection, relTypeArray).iterator() :
        vertex.getEdges(arcadeDirection).iterator();
    return direction == Direction.BOTH ? SelfLoops.deduplicatingEdges(edges) : edges;
  }

  /**
   * Returns the endpoint of the edge the traversal moves to. For an undirected hop that is whichever
   * end is not the vertex we came from, so a walk can change direction between hops; for a self-loop
   * both ends are the same vertex.
   */
  private static Vertex otherEnd(final Edge edge, final Vertex from, final Direction direction) {
    if (direction == Direction.OUT)
      return edge.getInVertex();
    if (direction == Direction.IN)
      return edge.getOutVertex();
    return edge.getOut().equals(from.getIdentity()) ? edge.getInVertex() : edge.getOutVertex();
  }

  private Vertex resolveVertex(final NodePattern nodePattern, final Result result) {
    if (nodePattern == null)
      return null;
    final String variable = nodePattern.getVariable();
    if (variable == null || variable.isEmpty())
      return null;
    final Object obj = result.getProperty(variable);
    if (obj instanceof Vertex)
      return (Vertex) obj;
    return null;
  }

  @Override
  public boolean isAggregation() {
    return false;
  }

  @Override
  public boolean containsAggregation() {
    if (whereExpression != null && whereExpression.containsAggregation())
      return true;
    return mapExpression.containsAggregation();
  }

  @Override
  public String getText() {
    return text;
  }

  /**
   * The pattern matched by the comprehension. Exposed, with the two expressions below, so
   * {@code CypherExpressionWalker} can reach what this expression nests: without them a function call inside a
   * pattern comprehension is invisible to every check that runs through the walker (issue #5602).
   */
  public PathPattern getPathPattern() {
    return pathPattern;
  }

  /**
   * The optional filter after {@code WHERE}, or {@code null}.
   */
  public Expression getWhereExpression() {
    return whereExpression;
  }

  /**
   * The projection after {@code |}, which every pattern comprehension has.
   */
  public Expression getMapExpression() {
    return mapExpression;
  }
}
