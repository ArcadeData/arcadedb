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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.traversal.GraphTraverser;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.function.sql.graph.SQLFunctionShortestPath;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for shortestPath() and allShortestPaths() patterns in MATCH clauses.
 * <p>
 * Handles patterns like:
 * - MATCH p = shortestPath((a)-[:KNOWS*]-(b))
 * - MATCH p = allShortestPaths((a)-[:KNOWS*]-(b))
 * <p>
 * Uses the existing SQLFunctionShortestPath for path computation.
 * <p>
 * Relationship constraints honoured on every hop: the type list, the direction, the inline property map,
 * the inline WHERE predicate and the {@code *min..max} hop bounds. The bounds both prune the search (an
 * upper bound stops the traversal from expanding a layer that could only produce longer paths) and filter
 * the answer, so {@code [:LINK*..2]} returns nothing when the shortest path is 4 hops instead of behaving
 * like {@code [:LINK*]} - issue #7009.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ShortestPathStep extends AbstractExecutionStep {
  private final String sourceVariable;
  private final String targetVariable;
  private final String pathVariable;
  private final ShortestPathPattern pattern;

  /**
   * Creates a shortest path step.
   *
   * @param sourceVariable variable name for source vertex
   * @param targetVariable variable name for target vertex
   * @param pathVariable   variable name for the path result (can be null)
   * @param pattern        the shortest path pattern
   * @param context        command context
   */
  public ShortestPathStep(final String sourceVariable, final String targetVariable, final String pathVariable,
      final ShortestPathPattern pattern, final CommandContext context) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;
    this.pathVariable = pathVariable;
    this.pattern = pattern;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("ShortestPathStep requires a previous step");
    // The command deadline is tested per input row: a per-layer check alone leaves the whole BFS of one row
    // unbounded (issue #6266).
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    return new ResultSet() {
      private ResultSet prevResults = null;
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
          guard.check();
          if (prevResults == null) {
            prevResults = prev.syncPull(context, nRecords);
          }

          if (!prevResults.hasNext()) {
            finished = true;
            break;
          }

          final Result inputResult = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            // Get source and target vertices from bound variables
            final Object sourceObj = inputResult.getProperty(sourceVariable);
            final Object targetObj = inputResult.getProperty(targetVariable);

            if (!(sourceObj instanceof Vertex) || !(targetObj instanceof Vertex)) {
              // If source or target is not a vertex, skip this result
              continue;
            }

            final Vertex sourceVertex = (Vertex) sourceObj;
            final Vertex targetVertex = (Vertex) targetObj;

            // For allShortestPaths(), enumerate every path sharing the minimal length; for shortestPath()
            // (singular), keep returning just one. Reuse the same compute method for the single-path case
            // so existing behaviour and CSR-accelerated lookups in SQLFunctionShortestPath stay in play.
            final List<List<Object>> paths;
            if (pattern.isAllPaths()) {
              paths = computeAllShortestPaths(sourceVertex, targetVertex, inputResult, context);
            } else {
              final List<Object> single = computeShortestPath(sourceVertex, targetVertex, inputResult, context);
              paths = single == null || single.isEmpty() ? Collections.emptyList() : Collections.singletonList(single);
            }

            for (final List<Object> path : paths) {
              if (path == null || path.isEmpty())
                continue;

              // Create result with the path
              final ResultInternal result = new ResultInternal();

              // Copy all properties from previous result
              for (final String prop : inputResult.getPropertyNames()) {
                result.setProperty(prop, inputResult.getProperty(prop));
              }

              // Add path binding if path variable is specified
              if (pathVariable != null && !pathVariable.isEmpty()) {
                result.setProperty(pathVariable, path);
              }

              buffer.add(result);
            }
            // If no path found, skip this result (similar to a failed MATCH)
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }

      @Override
      public void close() {
        ShortestPathStep.this.close();
      }
    };
  }

  /**
   * Computes the shortest path between source and target vertices.
   * Returns a list of alternating Vertex and Edge objects representing the path.
   */
  private List<Object> computeShortestPath(final Vertex source, final Vertex target, final Result inputResult,
      final CommandContext context) {
    // Inline edge constraints (the property map in shortestPath((a)-[:LINK*1..3 {w: 1}]->(b)) and the
    // inline WHERE in shortestPath((a)-[r:LINK* WHERE r.tag = 'ok']->(b))) are not honoured by
    // SQLFunctionShortestPath, which only sees vertices. Route through an edge-aware BFS that walks
    // matching edges only.
    final HopBounds bounds = patternHopBounds();

    final EdgeConstraint constraint = edgeConstraint(inputResult, context);
    if (constraint != null)
      return computeFilteredShortestPath(source, target, patternDirection(), patternEdgeTypesArray(), constraint, bounds,
          context);

    // Collect every relationship type declared in the pattern. Variable-length type alternation
    // (e.g. [:R1|R2*]) is expressed as a single relationship with multiple types - all of them
    // must reach SQLFunctionShortestPath, otherwise paths that walk across more than one type
    // are silently dropped (issue #4190).
    final List<String> edgeTypes;
    if (pattern.getRelationshipCount() > 0 && pattern.getRelationship(0).hasTypes())
      edgeTypes = pattern.getRelationship(0).getTypes();
    else
      edgeTypes = null;

    // Get direction from pattern
    Vertex.DIRECTION vertexDirection = Vertex.DIRECTION.BOTH;
    String direction = "BOTH";
    if (pattern.getRelationshipCount() > 0) {
      final Direction dir = pattern.getRelationship(0).getDirection();
      switch (dir) {
        case OUT:
          direction = "OUT";
          vertexDirection = Vertex.DIRECTION.OUT;
          break;
        case IN:
          direction = "IN";
          vertexDirection = Vertex.DIRECTION.IN;
          break;
        default:
          direction = "BOTH";
      }
    }

    // Use SQLFunctionShortestPath to compute the path (returns vertex RIDs only).
    // When multiple edge types are present pass them as a List so the function honours all of them.
    final SQLFunctionShortestPath shortestPathFunction = new SQLFunctionShortestPath();
    final Object edgeTypeParam;
    if (edgeTypes == null || edgeTypes.isEmpty())
      edgeTypeParam = null;
    else if (edgeTypes.size() == 1)
      edgeTypeParam = edgeTypes.get(0);
    else
      edgeTypeParam = edgeTypes;

    final List<RID> pathRids = shortestPathFunction.execute(null, null, null,
        shortestPathArguments(source, target, direction, edgeTypeParam, bounds), context);
    if (pathRids == null || pathRids.isEmpty())
      return null;

    // A source that already is the target short-circuits to the zero-length path before any bound applies:
    // that is what MATCH p = shortestPath((a)-[:R*]-(a)) has always answered, and the implicit minimum of 1
    // that [*] carries would otherwise suppress it. See HopBounds and issue #7017.
    if (pathRids.size() > 1 && !bounds.accepts(pathRids.size() - 1))
      return null;

    // Build a proper path with alternating Vertex and Edge objects
    return resolvePathWithEdges(pathRids, vertexDirection, edgeTypes, context.getDatabase());
  }

  /**
   * Enumerates every simple path between {@code source} and {@code target} sharing the minimum length.
   * <p>
   * Implementation: layered BFS that records, for each visited vertex, the full set of predecessors that
   * reached it on the same BFS layer. Once {@code target} is discovered, BFS halts at the end of that
   * layer (any further expansion would only find longer paths) and all paths are reconstructed by
   * back-tracking through the predecessor multimap. Respects relationship direction and the type filter
   * declared in the pattern.
   * <p>
   * For issue #4239: {@code allShortestPaths()} must return every path of the minimal length, not just
   * one. The legacy implementation returned the single path that {@link SQLFunctionShortestPath} happened
   * to find first, violating the OpenCypher contract.
   */
  private List<List<Object>> computeAllShortestPaths(final Vertex source, final Vertex target, final Result inputResult,
      final CommandContext context) {
    // Inline edge constraints must be enforced on every hop; the vertex-only BFS below cannot see edge
    // properties, so delegate to the edge-aware variant when a property map or inline WHERE is declared.
    final HopBounds bounds = patternHopBounds();

    final EdgeConstraint constraint = edgeConstraint(inputResult, context);
    if (constraint != null)
      return computeFilteredAllShortestPaths(source, target, patternDirection(), patternEdgeTypesArray(), constraint,
          bounds, context.getDatabase(), context);

    final List<String> edgeTypes;
    if (pattern.getRelationshipCount() > 0 && pattern.getRelationship(0).hasTypes())
      edgeTypes = pattern.getRelationship(0).getTypes();
    else
      edgeTypes = null;

    Vertex.DIRECTION direction = Vertex.DIRECTION.BOTH;
    if (pattern.getRelationshipCount() > 0) {
      final Direction dir = pattern.getRelationship(0).getDirection();
      switch (dir) {
        case OUT:
          direction = Vertex.DIRECTION.OUT;
          break;
        case IN:
          direction = Vertex.DIRECTION.IN;
          break;
        default:
          direction = Vertex.DIRECTION.BOTH;
      }
    }

    final Database database = context.getDatabase();
    final RID sourceRid = source.getIdentity();
    final RID targetRid = target.getIdentity();

    if (sourceRid.equals(targetRid)) {
      final List<Object> singleNode = new ArrayList<>(1);
      singleNode.add(source);
      return Collections.singletonList(singleNode);
    }

    final String[] typesArray = edgeTypes == null || edgeTypes.isEmpty() ? null : edgeTypes.toArray(new String[0]);

    // distance from source. Acts as visited-set too.
    final Map<RID, Integer> distance = new HashMap<>();
    // For each vertex, the set of parents that reached it at the same BFS depth (= co-shortest predecessors).
    final Map<RID, List<RID>> predecessors = new HashMap<>();
    distance.put(sourceRid, 0);

    Deque<Vertex> currentLayer = new ArrayDeque<>();
    currentLayer.add(source);
    int currentDepth = 0;
    int foundDepth = -1;

    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    while (!currentLayer.isEmpty()) {
      if (Thread.interrupted())
        throw new CommandExecutionException("The allShortestPaths() function has been interrupted");
      guard.check();

      // Stop expanding once we've completed the layer where target was first discovered: any further hop
      // would only produce strictly longer (non co-shortest) paths.
      if (foundDepth >= 0 && currentDepth >= foundDepth)
        break;

      // Same reasoning for the declared upper bound: the next layer can only reach vertices more than
      // maxHops hops away, and no path through them could satisfy the pattern (issue #7009).
      if (currentDepth >= bounds.getMax())
        break;

      final Deque<Vertex> nextLayer = new ArrayDeque<>();
      final Set<RID> nextLayerSeen = new HashSet<>();

      for (final Vertex v : currentLayer) {
        final Iterable<Vertex> neighbors = typesArray != null ? v.getVertices(direction, typesArray) : v.getVertices(direction);
        for (final Vertex neighbor : neighbors) {
          final RID neighborRid = neighbor.getIdentity();
          final Integer existing = distance.get(neighborRid);
          if (existing == null) {
            distance.put(neighborRid, currentDepth + 1);
            final List<RID> parents = new ArrayList<>(1);
            parents.add(v.getIdentity());
            predecessors.put(neighborRid, parents);
            if (neighborRid.equals(targetRid))
              foundDepth = currentDepth + 1;
            else if (nextLayerSeen.add(neighborRid))
              nextLayer.add(neighbor);
          } else if (existing == currentDepth + 1) {
            // Another co-shortest predecessor at the same BFS depth.
            predecessors.get(neighborRid).add(v.getIdentity());
          }
        }
      }

      currentLayer = nextLayer;
      currentDepth++;
    }

    // Every path returned here has length foundDepth, so one bound check answers for all of them.
    if (foundDepth < 0 || !bounds.accepts(foundDepth))
      return Collections.emptyList();

    // Backtrack from target through every predecessor chain to produce every path of length foundDepth.
    final List<List<RID>> ridPaths = new ArrayList<>();
    final Deque<RID> stack = new ArrayDeque<>();
    stack.push(targetRid);
    buildAllPaths(targetRid, sourceRid, predecessors, stack, ridPaths);

    final List<List<Object>> result = new ArrayList<>(ridPaths.size());
    for (final List<RID> ridPath : ridPaths)
      result.add(resolvePathWithEdges(ridPath, direction, edgeTypes, database));
    return result;
  }

  /**
   * Returns the inline edge constraint declared on the pattern relationship, or {@code null} when the
   * pattern carries neither a property map nor an inline WHERE.
   */
  private EdgeConstraint edgeConstraint(final Result inputResult, final CommandContext context) {
    if (pattern.getRelationshipCount() == 0)
      return null;
    return EdgeConstraint.from(pattern.getRelationship(0), inputResult, context);
  }

  private Vertex.DIRECTION patternDirection() {
    if (pattern.getRelationshipCount() > 0) {
      switch (pattern.getRelationship(0).getDirection()) {
        case OUT:
          return Vertex.DIRECTION.OUT;
        case IN:
          return Vertex.DIRECTION.IN;
        default:
          return Vertex.DIRECTION.BOTH;
      }
    }
    return Vertex.DIRECTION.BOTH;
  }

  /**
   * Returns the {@code *min..max} hop bounds declared on the pattern relationship.
   */
  private HopBounds patternHopBounds() {
    return HopBounds.from(pattern.getRelationshipCount() > 0 ? pattern.getRelationship(0) : null);
  }

  private String[] patternEdgeTypesArray() {
    if (pattern.getRelationshipCount() > 0 && pattern.getRelationship(0).hasTypes()) {
      final List<String> types = pattern.getRelationship(0).getTypes();
      if (!types.isEmpty())
        return types.toArray(new String[0]);
    }
    return null;
  }

  /**
   * Per-edge constraint declared on a pattern relationship: the inline property map (e.g. {@code {w: 1}})
   * and/or the inline WHERE predicate (e.g. {@code WHERE r.tag = 'ok'}). Every relationship on a candidate
   * path must satisfy all declared constraints, mirroring what a variable-length MATCH enforces.
   * <p>
   * Shared by both shortestPath() evaluators: {@link ShortestPathStep} (the {@code MATCH p = shortestPath(...)}
   * form) and {@code ShortestPathExpression} (the {@code RETURN shortestPath(...)} form).
   * <p>
   * Not thread-safe and not reentrant: the row used to evaluate the WHERE predicate is allocated once and
   * the relationship variable is rebound on it for each candidate edge. One instance belongs to a single
   * traversal.
   */
  public static final class EdgeConstraint {
    private final Map<String, Object> properties;
    private final BooleanExpression  whereExpression;
    private final String             relationshipVariable;
    private final ResultInternal     whereEvalRow;
    private final CommandContext     context;

    private EdgeConstraint(final Map<String, Object> properties, final BooleanExpression whereExpression,
        final String relationshipVariable, final ResultInternal whereEvalRow, final CommandContext context) {
      this.properties = properties;
      this.whereExpression = whereExpression;
      this.relationshipVariable = relationshipVariable;
      this.whereEvalRow = whereEvalRow;
      this.context = context;
    }

    /**
     * Builds the constraint declared by {@code rel}, or returns {@code null} when the relationship carries
     * neither an inline property map nor an inline WHERE. Returning {@code null} keeps the unconstrained
     * traversal on the faster vertex-only path and leaves it allocation-free.
     *
     * @param currentRow bindings visible to the inline WHERE predicate; copied once so the enclosing scope
     *                   stays reachable while the relationship variable is rebound per candidate edge
     */
    public static EdgeConstraint from(final RelationshipPattern rel, final Result currentRow,
        final CommandContext context) {
      if (rel == null)
        return null;

      final Map<String, Object> properties = resolveProperties(rel, currentRow, context);
      final BooleanExpression whereExpression = rel.getWhereExpression();
      if (properties == null && whereExpression == null)
        return null;

      ResultInternal whereEvalRow = null;
      if (whereExpression != null) {
        whereEvalRow = new ResultInternal();
        if (currentRow != null)
          for (final String prop : currentRow.getPropertyNames())
            whereEvalRow.setProperty(prop, currentRow.getProperty(prop));
      }

      return new EdgeConstraint(properties, whereExpression, rel.getVariable(), whereEvalRow, context);
    }

    /**
     * Returns true when the edge satisfies every declared constraint.
     */
    public boolean matches(final Edge edge) {
      if (!GraphTraverser.matchesPropertyFilter(edge, properties))
        return false;
      if (whereExpression == null)
        return true;
      if (relationshipVariable != null && !relationshipVariable.isEmpty())
        whereEvalRow.setProperty(relationshipVariable, edge);
      return whereExpression.evaluate(whereEvalRow, context);
    }

    /**
     * Returns the inline property map declared by the relationship, or {@code null} when there is none.
     * A map supplied as a bare parameter (e.g. {@code -[:LINK* $props]->}) is resolved against the query
     * parameters here, so the parameter form constrains the traversal exactly like an inline map. The
     * values of an inline map are resolved too, so a {@code $param} or a row-dependent expression written
     * inside it filters on what it stands for instead of matching nothing (issue #5501).
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> resolveProperties(final RelationshipPattern rel, final Result currentRow,
        final CommandContext context) {
      if (!rel.getProperties().isEmpty())
        return InlineProperties.resolveAll(rel.getProperties(), currentRow, context);

      final String parameterName = rel.getPropertiesParameterName();
      if (parameterName == null || context == null || context.getInputParameters() == null)
        return null;

      final Object parameterValue = context.getInputParameters().get(parameterName);
      if (!(parameterValue instanceof Map))
        return null;

      final Map<String, Object> resolved = (Map<String, Object>) parameterValue;
      return resolved.isEmpty() ? null : resolved;
    }
  }

  /**
   * Edge-aware BFS returning a single shortest path (alternating Vertex/Edge objects) that only traverses
   * edges satisfying {@code constraint}. Tracks the actual edge used to reach each vertex so parallel edges
   * with different property values are disambiguated correctly.
   *
   * @param edgeTypes restrict edges to these types, or null/empty to allow any type
   * @param bounds    the {@code *min..max} hop bounds declared on the pattern relationship (issue #7009)
   * @param context   the command context the deadline is read from; {@code null} leaves only the interrupt check
   *                  (issue #6459 - this constrained BFS previously consulted neither, unlike the unconstrained
   *                  frontier walk it falls back from)
   */
  public static List<Object> computeFilteredShortestPath(final Vertex source, final Vertex target,
      final Vertex.DIRECTION direction, final String[] edgeTypes, final EdgeConstraint constraint,
      final HopBounds bounds, final CommandContext context) {
    final RID sourceRid = source.getIdentity();
    final RID targetRid = target.getIdentity();
    if (sourceRid.equals(targetRid)) {
      final List<Object> single = new ArrayList<>(1);
      single.add(source);
      return single;
    }

    final Vertex.DIRECTION[] directions = expandDirections(direction);
    final String[] typesArray = edgeTypes == null || edgeTypes.length == 0 ? null : edgeTypes;

    final Map<RID, Vertex> parentVertex = new HashMap<>();
    final Map<RID, Edge> incomingEdge = new HashMap<>();
    final Set<RID> visited = new HashSet<>();
    visited.add(sourceRid);

    Deque<Vertex> frontier = new ArrayDeque<>();
    frontier.add(source);

    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    // Hops already walked to reach the current frontier, so the layer expanded below lands at depth + 1.
    int depth = 0;

    while (!frontier.isEmpty() && depth < bounds.getMax()) {
      if (Thread.interrupted())
        throw new CommandExecutionException("The shortestPath() function has been interrupted");
      guard.check();

      final Deque<Vertex> next = new ArrayDeque<>();
      for (final Vertex v : frontier) {
        for (final Vertex.DIRECTION dir : directions) {
          final Iterable<Edge> edges = typesArray != null ? v.getEdges(dir, typesArray) : v.getEdges(dir);
          for (final Edge edge : edges) {
            if (!constraint.matches(edge))
              continue;
            final Vertex neighbor;
            try {
              neighbor = dir == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
            } catch (final RecordNotFoundException e) {
              GhostEdgeReporter.reportSkipped(e);
              continue;
            }
            final RID neighborRid = neighbor.getIdentity();
            if (!visited.add(neighborRid))
              continue;
            parentVertex.put(neighborRid, v);
            incomingEdge.put(neighborRid, edge);
            if (neighborRid.equals(targetRid))
              // A level-order walk reaches the target on its shortest layer first, so a length below the
              // declared minimum cannot be improved on by carrying the search further.
              return bounds.accepts(depth + 1) ?
                  reconstructFilteredPath(source, target, parentVertex, incomingEdge) :
                  null;
            next.add(neighbor);
          }
        }
      }
      frontier = next;
      depth++;
    }
    return null;
  }

  /**
   * Rebuilds the source-to-target path (alternating Vertex/Edge) by walking the predecessor maps backwards.
   */
  private static List<Object> reconstructFilteredPath(final Vertex source, final Vertex target,
      final Map<RID, Vertex> parentVertex, final Map<RID, Edge> incomingEdge) {
    final Deque<Object> path = new ArrayDeque<>();
    final RID sourceRid = source.getIdentity();
    Vertex current = target;
    path.addFirst(current);
    while (!current.getIdentity().equals(sourceRid)) {
      final RID currentRid = current.getIdentity();
      path.addFirst(incomingEdge.get(currentRid));
      current = parentVertex.get(currentRid);
      path.addFirst(current);
    }
    return new ArrayList<>(path);
  }

  /**
   * Edge-aware layered BFS returning EVERY co-shortest path honouring {@code constraint}. Records each
   * co-shortest predecessor together with the edge that reached the vertex, so parallel edges yield the
   * distinct paths OpenCypher requires.
   *
   * @param edgeTypes restrict edges to these types, or null/empty to allow any type
   * @param bounds    the {@code *min..max} hop bounds declared on the pattern relationship (issue #7009)
   * @param context   the command context the deadline is read from; {@code null} leaves only the interrupt check
   *                  (issue #6459 - this constrained BFS previously consulted neither, unlike the unconstrained
   *                  layered BFS it falls back from)
   */
  public static List<List<Object>> computeFilteredAllShortestPaths(final Vertex source, final Vertex target,
      final Vertex.DIRECTION direction, final String[] edgeTypes, final EdgeConstraint constraint,
      final HopBounds bounds, final Database database, final CommandContext context) {
    final RID sourceRid = source.getIdentity();
    final RID targetRid = target.getIdentity();
    if (sourceRid.equals(targetRid)) {
      final List<Object> single = new ArrayList<>(1);
      single.add(source);
      return Collections.singletonList(single);
    }

    final Vertex.DIRECTION[] directions = expandDirections(direction);
    final String[] typesArray = edgeTypes == null || edgeTypes.length == 0 ? null : edgeTypes;

    final Map<RID, Integer> distance = new HashMap<>();
    final Map<RID, List<PredecessorLink>> predecessors = new HashMap<>();
    distance.put(sourceRid, 0);

    Deque<Vertex> currentLayer = new ArrayDeque<>();
    currentLayer.add(source);
    int currentDepth = 0;
    int foundDepth = -1;

    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    while (!currentLayer.isEmpty()) {
      if (Thread.interrupted())
        throw new CommandExecutionException("The allShortestPaths() function has been interrupted");
      guard.check();

      if (foundDepth >= 0 && currentDepth >= foundDepth)
        break;

      // The next layer lands beyond the declared upper bound, so nothing it reaches can satisfy the
      // pattern (issue #7009).
      if (currentDepth >= bounds.getMax())
        break;

      final Deque<Vertex> nextLayer = new ArrayDeque<>();
      final Set<RID> nextLayerSeen = new HashSet<>();

      for (final Vertex v : currentLayer) {
        final RID vRid = v.getIdentity();
        for (final Vertex.DIRECTION dir : directions) {
          final Iterable<Edge> edges = typesArray != null ? v.getEdges(dir, typesArray) : v.getEdges(dir);
          for (final Edge edge : edges) {
            if (!constraint.matches(edge))
              continue;
            final Vertex neighbor;
            try {
              neighbor = dir == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
            } catch (final RecordNotFoundException e) {
              GhostEdgeReporter.reportSkipped(e);
              continue;
            }
            final RID neighborRid = neighbor.getIdentity();
            final Integer existing = distance.get(neighborRid);
            if (existing == null) {
              distance.put(neighborRid, currentDepth + 1);
              final List<PredecessorLink> parents = new ArrayList<>(1);
              parents.add(new PredecessorLink(vRid, edge));
              predecessors.put(neighborRid, parents);
              if (neighborRid.equals(targetRid))
                foundDepth = currentDepth + 1;
              else if (nextLayerSeen.add(neighborRid))
                nextLayer.add(neighbor);
            } else if (existing == currentDepth + 1) {
              // Another co-shortest predecessor (or a parallel edge from the same vertex) at this BFS depth.
              predecessors.get(neighborRid).add(new PredecessorLink(vRid, edge));
            }
          }
        }
      }

      currentLayer = nextLayer;
      currentDepth++;
    }

    // Every path returned here has length foundDepth, so one bound check answers for all of them.
    if (foundDepth < 0 || !bounds.accepts(foundDepth))
      return Collections.emptyList();

    final List<List<Object>> result = new ArrayList<>();
    final Deque<Object> stack = new ArrayDeque<>();
    buildAllFilteredPaths(targetRid, sourceRid, predecessors, database, stack, result);
    return result;
  }

  private static void buildAllFilteredPaths(final RID current, final RID sourceRid,
      final Map<RID, List<PredecessorLink>> predecessors, final Database database,
      final Deque<Object> stack, final List<List<Object>> out) {
    final Vertex currentVertex = (Vertex) database.lookupByRID(current, true);
    stack.push(currentVertex);
    if (current.equals(sourceRid)) {
      // stack head-to-tail already reads source-to-target because we push from target down to source.
      out.add(new ArrayList<>(stack));
      stack.pop();
      return;
    }
    final List<PredecessorLink> parents = predecessors.get(current);
    if (parents != null) {
      for (final PredecessorLink link : parents) {
        stack.push(link.edge);
        buildAllFilteredPaths(link.parent, sourceRid, predecessors, database, stack, out);
        stack.pop();
      }
    }
    stack.pop();
  }

  /**
   * Builds the positional argument list {@link SQLFunctionShortestPath} expects, carrying an upper hop bound
   * as its {@code maxDepth} option so a bounded pattern also bounds the search. Shared by the two evaluators
   * that delegate to the function - this step and {@code ShortestPathExpression} - so a bound cannot reach
   * one of them and not the other, which is how it came to be dropped in the first place.
   * <p>
   * The bound is still re-checked on the path that comes back: that check is what guarantees the contract,
   * and the pruning here only keeps a bounded search cheap.
   *
   * @param edgeTypeParam a single type name, a {@link List} of them, or {@code null} to allow any type
   */
  public static Object[] shortestPathArguments(final Vertex source, final Vertex target, final String direction,
      final Object edgeTypeParam, final HopBounds bounds) {
    // maxDepth counts the vertices on the path rather than the relationships between them, hence maxHops + 1.
    final Integer maxDepth = bounds.maxDepthParameter();
    if (maxDepth != null)
      return new Object[] { source, target, direction, edgeTypeParam,
          Map.of(SQLFunctionShortestPath.PARAM_MAX_DEPTH, maxDepth) };
    if (edgeTypeParam != null)
      return new Object[] { source, target, direction, edgeTypeParam };
    return new Object[] { source, target, direction };
  }

  /**
   * The {@code *min..max} hop bounds declared on a pattern relationship, resolved once so every
   * shortestPath()/allShortestPaths() evaluator applies the same rule to the path it found.
   * <p>
   * Both spellings of a missing bound are folded in here: {@code [:R*]} and {@code [:R*..2]} carry an
   * implicit minimum of one hop, and a relationship written without {@code *} at all is a single hop.
   * <p>
   * Only the upper bound can prune a breadth-first search, because a level-order walk reaches the target
   * on its shortest layer first. A shortest path below the declared minimum therefore yields no row
   * rather than a longer path satisfying it: finding that path is a different (non-shortest, simple-path
   * enumerating) search, and Neo4j rejects such a pattern outright.
   * <p>
   * One shape stays outside the check: endpoints resolving to the same vertex short-circuit to the
   * zero-length path in every evaluator, before any bound applies, so a declared minimum is unenforceable
   * there. That is the answer the engine has always given (pinned by
   * {@code CypherReduceAndShortestPathTest.shortestPathSameNode}), and {@code [*]} is lowered to
   * {@code minHops = 1} by the parser, so it cannot be told apart from an explicit {@code [*1..]} here.
   * Changing it is a semantic decision tracked by issue #7017.
   */
  public static final class HopBounds {
    /** No bound at all: accepts every path length. */
    public static final HopBounds UNBOUNDED = new HopBounds(0, Integer.MAX_VALUE);

    private final int min;
    private final int max;

    private HopBounds(final int min, final int max) {
      this.min = min;
      this.max = max;
    }

    /**
     * Reads the bounds off {@code rel}, or returns {@link #UNBOUNDED} when there is no relationship.
     */
    public static HopBounds from(final RelationshipPattern rel) {
      if (rel == null)
        return UNBOUNDED;
      final int min = rel.getEffectiveMinHops();
      final int max = rel.getEffectiveMaxHops();
      return min <= 0 && max == Integer.MAX_VALUE ? UNBOUNDED : new HopBounds(min, max);
    }

    /**
     * The largest number of relationships a path may carry; {@link Integer#MAX_VALUE} when unbounded.
     */
    public int getMax() {
      return max;
    }

    /**
     * Returns true when a path of {@code hops} relationships satisfies both bounds.
     */
    public boolean accepts(final int hops) {
      return hops >= min && hops <= max;
    }

    /**
     * The upper bound expressed as {@code SQLFunctionShortestPath}'s {@code maxDepth}, which counts the
     * vertices on the path rather than the relationships between them, or {@code null} when unbounded.
     */
    public Integer maxDepthParameter() {
      return max == Integer.MAX_VALUE ? null : max + 1;
    }

    @Override
    public String toString() {
      if (min == max)
        return "*" + min;
      return "*" + min + ".." + (max == Integer.MAX_VALUE ? "" : max);
    }
  }

  private static Vertex.DIRECTION[] expandDirections(final Vertex.DIRECTION direction) {
    return direction == Vertex.DIRECTION.BOTH ?
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN } :
        new Vertex.DIRECTION[] { direction };
  }

  /**
   * A co-shortest predecessor of a vertex together with the specific edge used to reach it. The edge is
   * retained (rather than re-derived) so parallel edges with differing properties stay disambiguated.
   */
  private static final class PredecessorLink {
    final RID  parent;
    final Edge edge;

    PredecessorLink(final RID parent, final Edge edge) {
      this.parent = parent;
      this.edge = edge;
    }
  }

  private static void buildAllPaths(final RID current, final RID sourceRid, final Map<RID, List<RID>> predecessors,
      final Deque<RID> stack, final List<List<RID>> out) {
    if (current.equals(sourceRid)) {
      // stack pushes from target down to source, so iterating head-to-tail yields source-to-target.
      out.add(new ArrayList<>(stack));
      return;
    }
    final List<RID> parents = predecessors.get(current);
    if (parents == null)
      return;
    for (final RID parent : parents) {
      stack.push(parent);
      buildAllPaths(parent, sourceRid, predecessors, stack, out);
      stack.pop();
    }
  }

  /**
   * Resolves a list of vertex RIDs into a proper path with alternating Vertex and Edge objects.
   *
   * @param edgeTypes restrict edges to these types, or null/empty to allow any type
   */
  public static List<Object> resolvePathWithEdges(final List<RID> vertexRids, final Vertex.DIRECTION direction,
      final List<String> edgeTypes, final Database database) {
    final List<Object> result = new ArrayList<>(vertexRids.size() * 2 - 1);

    Vertex prev = null;
    for (final RID rid : vertexRids) {
      final Vertex current = (Vertex) database.lookupByRID(rid, true);

      if (prev != null) {
        // Find the edge connecting prev to current
        final Edge edge = findConnectingEdge(prev, current, direction, edgeTypes);
        if (edge != null)
          result.add(edge);
      }

      result.add(current);
      prev = current;
    }

    return result;
  }

  /**
   * Backward-compatible overload that accepts a single edge type.
   */
  public static List<Object> resolvePathWithEdges(final List<RID> vertexRids, final Vertex.DIRECTION direction,
      final String edgeType, final Database database) {
    return resolvePathWithEdges(vertexRids, direction,
        edgeType == null ? null : Collections.singletonList(edgeType), database);
  }

  /**
   * Finds the edge connecting two vertices.
   */
  private static Edge findConnectingEdge(final Vertex from, final Vertex to, final Vertex.DIRECTION direction,
      final List<String> edgeTypes) {
    final Vertex.DIRECTION[] directions = direction == Vertex.DIRECTION.BOTH ?
        new Vertex.DIRECTION[] { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN } :
        new Vertex.DIRECTION[] { direction };

    final String[] typesArray = edgeTypes == null || edgeTypes.isEmpty() ? null :
        edgeTypes.toArray(new String[0]);

    for (final Vertex.DIRECTION dir : directions) {
      final Iterable<Edge> edges = typesArray != null ?
          from.getEdges(dir, typesArray) :
          from.getEdges(dir);

      for (final Edge edge : edges) {
        try {
          final RID connected = dir == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
          if (connected.equals(to.getIdentity()))
            return edge;
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
        }
      }
    }
    return null;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ SHORTEST PATH ");
    builder.append("(").append(sourceVariable).append(")");
    if (pattern.getRelationshipCount() > 0) {
      final RelationshipPattern rel = pattern.getRelationship(0);
      builder.append("-[");
      if (rel.hasTypes()) {
        builder.append(":").append(String.join("|", rel.getTypes()));
      }
      // Render the quantifier only when the pattern actually carries one: a plain -[:LINK]- is a single hop
      // and spelling it "*1..1" (or, as this line used to, a bare "*") describes a pattern nobody wrote.
      if (rel.isVariableLength())
        builder.append(patternHopBounds().toString());
      builder.append("]-");
    }
    builder.append("(").append(targetVariable).append(")");
    if (pattern.isAllPaths()) {
      builder.append(" [ALL]");
    }
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
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
