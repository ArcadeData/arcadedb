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
import com.arcadedb.function.sql.graph.SQLFunctionShortestPath;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
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
              paths = computeAllShortestPaths(sourceVertex, targetVertex, context);
            } else {
              final List<Object> single = computeShortestPath(sourceVertex, targetVertex, context);
              paths = single == null || single.isEmpty() ? List.of() : List.of(single);
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
  private List<Object> computeShortestPath(final Vertex source, final Vertex target, final CommandContext context) {
    // Inline edge property filters (e.g. shortestPath((a)-[:LINK*1..3 {w: 1}]->(b))) are not honoured
    // by SQLFunctionShortestPath, so route through an edge-aware BFS that only walks matching edges (issue #5096).
    final Map<String, Object> edgeFilters = edgePropertyFilters();
    if (edgeFilters != null)
      return computeShortestPathFiltered(source, target, edgeFilters, context);

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
      edgeTypeParam = edgeTypes.getFirst();
    else
      edgeTypeParam = edgeTypes;

    final Object[] params = edgeTypeParam != null ?
        new Object[] { source, target, direction, edgeTypeParam } :
        new Object[] { source, target, direction };

    final List<RID> pathRids = shortestPathFunction.execute(null, null, null, params, context);
    if (pathRids == null || pathRids.isEmpty())
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
  private List<List<Object>> computeAllShortestPaths(final Vertex source, final Vertex target, final CommandContext context) {
    // Inline edge property filters must be enforced on every hop (issue #5096); the vertex-only BFS below
    // cannot see edge properties, so delegate to the edge-aware variant when a filter is declared.
    final Map<String, Object> edgeFilters = edgePropertyFilters();
    if (edgeFilters != null)
      return computeAllShortestPathsFiltered(source, target, edgeFilters, context);

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
      return List.of(singleNode);
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

    while (!currentLayer.isEmpty()) {
      if (Thread.interrupted())
        throw new CommandExecutionException("The allShortestPaths() function has been interrupted");

      // Stop expanding once we've completed the layer where target was first discovered: any further hop
      // would only produce strictly longer (non co-shortest) paths.
      if (foundDepth >= 0 && currentDepth >= foundDepth)
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

    if (foundDepth < 0)
      return List.of();

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
   * Returns the inline edge property filters declared on the pattern relationship (e.g. {@code {w: 1}}),
   * or {@code null} when none are present. Mirrors what the standard variable-length MATCH path feeds into
   * {@code VariableLengthPathTraverser}, so shortestPath()/allShortestPaths() enforce the same constraint.
   */
  private Map<String, Object> edgePropertyFilters() {
    if (pattern.getRelationshipCount() > 0) {
      final RelationshipPattern rel = pattern.getRelationship(0);
      if (rel.hasProperties() && !rel.getProperties().isEmpty())
        return rel.getProperties();
    }
    return null;
  }

  private Vertex.DIRECTION patternDirection() {
    if (pattern.getRelationshipCount() > 0) {
      return switch (pattern.getRelationship(0).getDirection()) {
        case OUT -> Vertex.DIRECTION.OUT;
        case IN -> Vertex.DIRECTION.IN;
        default -> Vertex.DIRECTION.BOTH;
      };
    }
    return Vertex.DIRECTION.BOTH;
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
   * Checks an edge against the inline property filters. Uses the same numeric-coercion rules as
   * {@code GraphTraverser.matchesPropertyFilter} so filtered shortestPath() behaves identically to a
   * variable-length MATCH pattern carrying the same {@code {prop: value}} constraint.
   */
  private static boolean edgeMatchesFilter(final Edge edge, final Map<String, Object> filters) {
    for (final Map.Entry<String, Object> entry : filters.entrySet()) {
      final Object actual = edge.get(entry.getKey());
      final Object expected = entry.getValue();
      if (actual == null)
        return false;
      if (actual instanceof Number number && expected instanceof Number number1) {
        if (number.longValue() != number1.longValue())
          return false;
      } else if (!actual.equals(expected))
        return false;
    }
    return true;
  }

  /**
   * Edge-aware BFS returning a single shortest path (alternating Vertex/Edge objects) that only traverses
   * edges satisfying {@code edgeFilters}. Tracks the actual edge used to reach each vertex so parallel edges
   * with different property values are disambiguated correctly.
   */
  private List<Object> computeShortestPathFiltered(final Vertex source, final Vertex target,
      final Map<String, Object> edgeFilters, final CommandContext context) {
    final RID sourceRid = source.getIdentity();
    final RID targetRid = target.getIdentity();
    if (sourceRid.equals(targetRid)) {
      final List<Object> single = new ArrayList<>(1);
      single.add(source);
      return single;
    }

    final Vertex.DIRECTION[] directions = expandDirections(patternDirection());
    final String[] typesArray = patternEdgeTypesArray();

    final Map<RID, Vertex> parentVertex = new HashMap<>();
    final Map<RID, Edge> incomingEdge = new HashMap<>();
    final Set<RID> visited = new HashSet<>();
    visited.add(sourceRid);

    Deque<Vertex> frontier = new ArrayDeque<>();
    frontier.add(source);

    while (!frontier.isEmpty()) {
      if (Thread.interrupted())
        throw new CommandExecutionException("The shortestPath() function has been interrupted");

      final Deque<Vertex> next = new ArrayDeque<>();
      for (final Vertex v : frontier) {
        for (final Vertex.DIRECTION dir : directions) {
          final Iterable<Edge> edges = typesArray != null ? v.getEdges(dir, typesArray) : v.getEdges(dir);
          for (final Edge edge : edges) {
            if (!edgeMatchesFilter(edge, edgeFilters))
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
              return reconstructFilteredPath(source, target, parentVertex, incomingEdge);
            next.add(neighbor);
          }
        }
      }
      frontier = next;
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
   * Edge-aware layered BFS returning EVERY co-shortest path honouring {@code edgeFilters}. Records each
   * co-shortest predecessor together with the edge that reached the vertex, so parallel edges yield the
   * distinct paths OpenCypher requires.
   */
  private List<List<Object>> computeAllShortestPathsFiltered(final Vertex source, final Vertex target,
      final Map<String, Object> edgeFilters, final CommandContext context) {
    final RID sourceRid = source.getIdentity();
    final RID targetRid = target.getIdentity();
    if (sourceRid.equals(targetRid)) {
      final List<Object> single = new ArrayList<>(1);
      single.add(source);
      return List.of(single);
    }

    final Vertex.DIRECTION[] directions = expandDirections(patternDirection());
    final String[] typesArray = patternEdgeTypesArray();
    final Database database = context.getDatabase();

    final Map<RID, Integer> distance = new HashMap<>();
    final Map<RID, List<PredecessorLink>> predecessors = new HashMap<>();
    distance.put(sourceRid, 0);

    Deque<Vertex> currentLayer = new ArrayDeque<>();
    currentLayer.add(source);
    int currentDepth = 0;
    int foundDepth = -1;

    while (!currentLayer.isEmpty()) {
      if (Thread.interrupted())
        throw new CommandExecutionException("The allShortestPaths() function has been interrupted");

      if (foundDepth >= 0 && currentDepth >= foundDepth)
        break;

      final Deque<Vertex> nextLayer = new ArrayDeque<>();
      final Set<RID> nextLayerSeen = new HashSet<>();

      for (final Vertex v : currentLayer) {
        final RID vRid = v.getIdentity();
        for (final Vertex.DIRECTION dir : directions) {
          final Iterable<Edge> edges = typesArray != null ? v.getEdges(dir, typesArray) : v.getEdges(dir);
          for (final Edge edge : edges) {
            if (!edgeMatchesFilter(edge, edgeFilters))
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

    if (foundDepth < 0)
      return List.of();

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
        edgeType == null ? null : List.of(edgeType), database);
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
      builder.append("-[");
      if (pattern.getRelationship(0).hasTypes()) {
        builder.append(":").append(String.join("|", pattern.getRelationship(0).getTypes()));
      }
      builder.append("*]-");
    }
    builder.append("(").append(targetVariable).append(")");
    if (pattern.isAllPaths()) {
      builder.append(" [ALL]");
    }
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
