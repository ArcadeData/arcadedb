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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.ast.ShortestPathPattern;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.traversal.GraphTraverser;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
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
  // Direction arrays shared by every expansion, never written: callers only iterate them
  private static final Vertex.DIRECTION[] OUT_ONLY = { Vertex.DIRECTION.OUT };
  private static final Vertex.DIRECTION[] IN_ONLY  = { Vertex.DIRECTION.IN };
  private static final Vertex.DIRECTION[] OUT_IN   = { Vertex.DIRECTION.OUT, Vertex.DIRECTION.IN };

  private final String sourceVariable;
  private final String targetVariable;
  private final String pathVariable;
  private final ShortestPathPattern pattern;
  private final BooleanExpression   pathPredicate;

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
    this(sourceVariable, targetVariable, pathVariable, pattern, null, context);
  }

  /**
   * Creates a shortest path step whose search is restricted by a predicate on the path (issue #13574).
   *
   * @param pathPredicate the MATCH WHERE conjuncts that read the path variable, or {@code null}: the step then answers the
   *                      shortest path(s) satisfying it, as Neo4j does, instead of the shortest path(s) of the graph alone.
   *                      Ignored when the pattern binds no path variable
   */
  public ShortestPathStep(final String sourceVariable, final String targetVariable, final String pathVariable,
      final ShortestPathPattern pattern, final BooleanExpression pathPredicate, final CommandContext context) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.targetVariable = targetVariable;
    this.pathVariable = pathVariable;
    this.pattern = pattern;
    this.pathPredicate = pathVariable == null || pathVariable.isEmpty() ? null : pathPredicate;
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
      private Iterator<List<Object>> currentPaths = null;
      private Result currentInput = null;

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

          if (currentPaths != null) {
            // The paths of one input row are pulled a batch at a time, so a LIMIT or any consumer that stops reading
            // also stops the enumeration instead of waiting for every path to be built (issue #9123).
            final long begin = context.isProfiling() ? System.nanoTime() : 0;
            try {
              if (currentPaths.hasNext()) {
                final List<Object> path = currentPaths.next();
                if (path != null && !path.isEmpty())
                  buffer.add(toRow(currentInput, path));
                continue;
              }
              currentPaths = null;
              currentInput = null;
            } finally {
              if (context.isProfiling())
                cost += System.nanoTime() - begin;
            }
          }

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
            if (pathPredicate != null)
              // The predicate restricts the paths the search may answer, so it is decided during the search (issue #13574)
              currentPaths = searchWithPathPredicate(sourceVertex, targetVertex, inputResult, context);
            else if (pattern.isAllPaths())
              currentPaths = iterateAllShortestPaths(sourceVertex, targetVertex, inputResult, context);
            else {
              final List<Object> single = computeShortestPath(sourceVertex, targetVertex, inputResult, context);
              currentPaths = single == null || single.isEmpty() ?
                  Collections.emptyIterator() :
                  Collections.singletonList(single).iterator();
            }
            currentInput = inputResult;
            // If no path found, skip this result (similar to a failed MATCH)
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }
      }

      private Result toRow(final Result inputResult, final List<Object> path) {
        final ResultInternal result = new ResultInternal();

        // Copy all properties from previous result
        for (final String prop : inputResult.getPropertyNames()) {
          result.setProperty(prop, inputResult.getProperty(prop));
        }

        // Add path binding if path variable is specified: a TraversalPath, the value every other pattern binds, so a
        // Bolt client receives a path and valueType() answers PATH (issue #9913)
        if (pathVariable != null && !pathVariable.isEmpty()) {
          result.setProperty(pathVariable, TraversalPath.ofWalk(path));
        }
        return result;
      }

      @Override
      public void close() {
        currentPaths = null;
        currentInput = null;
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

    final Object[] arguments = shortestPathArguments(source, target, direction, edgeTypeParam, bounds);
    // A pattern: the function answers the incoming side of the unidirectional types (issue #8625)
    final List<RID> pathRids = IncomingEdgeLookup.walkingPattern(
        () -> shortestPathFunction.execute(null, null, null, arguments, context));
    if (pathRids == null || pathRids.isEmpty())
      return null;

    // Every answer is checked against both declared bounds, the zero-length path of a source that already is the target
    // included: it answers only a minimum of zero (issues #7017, #11913).
    final int hops = pathRids.size() - 1;
    if (!bounds.accepts(hops))
      return null;

    // Build a proper path with alternating Vertex and Edge objects
    return resolvePathWithEdges(pathRids, vertexDirection, edgeTypes, context.getDatabase());
  }

  /**
   * Enumerates every path between {@code source} and {@code target} sharing the minimum length, honouring the
   * pattern's inline edge constraint, direction, type filter and hop bounds.
   */
  private Iterator<List<Object>> iterateAllShortestPaths(final Vertex source, final Vertex target, final Result inputResult,
      final CommandContext context) {
    final HopBounds bounds = patternHopBounds();

    // Inline edge constraints must be enforced on every hop; the vertex-only BFS cannot see edge properties,
    // so delegate to the edge-aware variant when a property map or inline WHERE is declared.
    final EdgeConstraint constraint = edgeConstraint(inputResult, context);
    if (constraint != null)
      return iterateFilteredAllShortestPaths(source, target, patternDirection(), patternEdgeTypesArray(), constraint,
          bounds, context.getDatabase(), context);

    return iterateAllShortestPaths(source, target, patternDirection(), patternEdgeTypesArray(), bounds, context);
  }

  /**
   * Enumerates every path between {@code source} and {@code target} sharing the minimum length, with no per-edge
   * constraint: a layered BFS over vertices that stops after the layer reaching {@code target}, then back-tracks
   * through the co-shortest parents. Parallel relationships are distinct paths (issue #8995). Shared by the MATCH and
   * the RETURN-expression forms of {@code allShortestPaths()}. The list materializes every path: the RETURN expression
   * has to hand back a list, while the MATCH form pulls them lazily through {@link #iterateAllShortestPaths}.
   *
   * @param edgeTypes restrict edges to these types, or null/empty to allow any type
   * @param bounds    the {@code *min..max} hop bounds declared on the pattern relationship (issue #7009)
   */
  public static List<List<Object>> computeAllShortestPaths(final Vertex source, final Vertex target,
      final Vertex.DIRECTION direction, final String[] edgeTypes, final HopBounds bounds, final CommandContext context) {
    return drain(iterateAllShortestPaths(source, target, direction, edgeTypes, bounds, context));
  }

  /**
   * Same search as {@link #computeAllShortestPaths}, but the paths are produced one at a time: the BFS runs up front, the
   * back-tracking walks the co-shortest predecessors on demand, so memory holds the walk's current branch, not every path
   * (issue #9123).
   */
  public static Iterator<List<Object>> iterateAllShortestPaths(final Vertex source, final Vertex target,
      final Vertex.DIRECTION direction, final String[] edgeTypes, final HopBounds bounds, final CommandContext context) {
    final RID sourceRid = source.getIdentity();
    final RID targetRid = target.getIdentity();

    if (sourceRid.equals(targetRid)) {
      // The zero-length path is the only shortest path between a vertex and itself; it answers only a minimum of zero
      // (issues #7017, #11913).
      if (!bounds.accepts(0))
        return Collections.emptyIterator();
      final List<Object> singleNode = new ArrayList<>(1);
      singleNode.add(source);
      return Collections.singletonList(singleNode).iterator();
    }

    final String[] typesArray = edgeTypes == null || edgeTypes.length == 0 ? null : edgeTypes;

    // distance from source. Acts as visited-set too.
    final Map<RID, Integer> distance = new HashMap<>();
    // For each vertex, its distinct co-shortest parents; parallel relationships are enumerated at back-tracking time.
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
        final RID vRid = v.getIdentity();
        // The incoming side of a unidirectional edge type comes from the query's lookup (issue #8625)
        for (final Iterator<Vertex> neighbors = IncomingEdgeLookup.getVertices(context, v, direction, typesArray);
            neighbors.hasNext(); ) {
          final Vertex neighbor = neighbors.next();
          final RID neighborRid = neighbor.getIdentity();
          final Integer existing = distance.get(neighborRid);
          if (existing == null) {
            distance.put(neighborRid, currentDepth + 1);
            final List<RID> parents = new ArrayList<>(1);
            parents.add(vRid);
            predecessors.put(neighborRid, parents);
            if (neighborRid.equals(targetRid))
              foundDepth = currentDepth + 1;
            else if (nextLayerSeen.add(neighborRid))
              nextLayer.add(neighbor);
          } else if (existing == currentDepth + 1) {
            // All of v's relationships are walked before the next vertex, so a parallel one finds v as the last parent.
            final List<RID> parents = predecessors.get(neighborRid);
            if (!parents.get(parents.size() - 1).equals(vRid))
              parents.add(vRid);
          }
        }
      }

      currentLayer = nextLayer;
      currentDepth++;
    }

    // Every path returned here has length foundDepth, so one bound check answers for all of them.
    if (foundDepth < 0 || !bounds.accepts(foundDepth))
      return Collections.emptyIterator();

    final Database database = context.getDatabase();
    return new PathEnumerator(target, sourceRid, foundDepth,
        current -> resolveHops(current, predecessors.get(current.getIdentity()), direction, typesArray, database), guard);
  }

  /**
   * The shortest path(s) between {@code source} and {@code target} that satisfy {@link #pathPredicate} (issue #13574), which is
   * how Neo4j reads a MATCH WHERE on the path variable of a shortestPath()/allShortestPaths() pattern: the predicate limits the
   * search, it does not filter the one path the search found first. shortestPath() answers the first path that passes, of the
   * smallest length any passing path has; allShortestPaths() answers every passing path of that length.
   * <p>
   * The candidates are the paths a variable-length MATCH of the same pattern answers (no relationship twice, the direction, the
   * types, the inline constraint and the hop bounds honoured), enumerated length by length from the unconstrained shortest one.
   * The distances to the target, grown with the length, prune every branch that cannot reach the target in the hops left, and the
   * search stops at the first length no candidate can reach. Like Neo4j's fallback for such predicates the search can grow
   * exponentially with the length, so the command deadline is checked as it runs.
   * <p>
   * A source that is the target keeps the rule of {@link HopBounds}: only the zero-length path answers, and only a minimum of zero.
   * A shortest path below the declared minimum answers nothing either, as without a predicate.
   */
  private Iterator<List<Object>> searchWithPathPredicate(final Vertex source, final Vertex target, final Result inputResult,
      final CommandContext context) {
    final HopBounds bounds = patternHopBounds();
    final ResultInternal evalRow = new ResultInternal();
    for (final String prop : inputResult.getPropertyNames())
      evalRow.setProperty(prop, inputResult.getProperty(prop));

    final RID sourceRid = source.getIdentity();
    final RID targetRid = target.getIdentity();
    if (sourceRid.equals(targetRid)) {
      final List<Object> single = new ArrayList<>(1);
      single.add(source);
      return bounds.accepts(0) && passes(evalRow, single, context) ?
          Collections.singletonList(single).iterator() :
          Collections.emptyIterator();
    }

    final Vertex.DIRECTION direction = patternDirection();
    final String[] typesArray = patternEdgeTypesArray();
    final EdgeConstraint constraint = edgeConstraint(inputResult, context);
    final WorkGuard guard = WorkGuard.forCommandDeadline(context);

    // Hops from the vertices to the target, walking the relationships backwards: a lower bound of what any path still needs. Grown
    // until it reaches the source, which gives the length of the shortest path regardless of the predicate
    final LayeredDistances toTarget = new LayeredDistances(target, reverse(direction), typesArray, constraint, context, guard);
    while (toTarget.get(sourceRid) == null && !toTarget.isComplete() && toTarget.expandedDepth() < bounds.getMax())
      toTarget.expandTo(toTarget.expandedDepth() + 1);
    final Integer shortest = toTarget.get(sourceRid);
    if (shortest == null || !bounds.accepts(shortest))
      return Collections.emptyIterator();

    return new PredicatePathIterator(source, targetRid, shortest, bounds.getMax(), expandDirections(direction), typesArray,
        constraint, toTarget, evalRow, context, guard);
  }

  /**
   * Hands out the passing paths of {@link #searchWithPathPredicate} one at a time: the paths of a length are enumerated as the
   * consumer asks for them, so a LIMIT or a consumer that stops reading also stops the search (issue #9123), and the next length is
   * entered only when the current one had no passing path and a longer path can still exist. shortestPath() ends after the first
   * passing path.
   */
  private final class PredicatePathIterator implements Iterator<List<Object>> {
    private final Vertex              source;
    private final RID                 targetRid;
    private final int                 maxLength;
    private final Vertex.DIRECTION[]  directions;
    private final String[]            typesArray;
    private final EdgeConstraint      constraint;
    private final LayeredDistances    toTarget;
    private final ResultInternal      evalRow;
    private final CommandContext      context;
    private final WorkGuard           guard;
    private       int                 length;
    private       PathPredicateSearch search;
    private       boolean             foundAtThisLength;
    private       boolean             exhausted;
    private       List<Object>        pending;

    PredicatePathIterator(final Vertex source, final RID targetRid, final int shortest, final int maxLength,
        final Vertex.DIRECTION[] directions, final String[] typesArray, final EdgeConstraint constraint,
        final LayeredDistances toTarget, final ResultInternal evalRow, final CommandContext context, final WorkGuard guard) {
      this.source = source;
      this.targetRid = targetRid;
      this.maxLength = maxLength;
      this.directions = directions;
      this.typesArray = typesArray;
      this.constraint = constraint;
      this.toTarget = toTarget;
      this.evalRow = evalRow;
      this.context = context;
      this.guard = guard;
      this.length = shortest;
      this.search = newSearch();
    }

    @Override
    public boolean hasNext() {
      if (pending != null)
        return true;
      if (exhausted)
        return false;
      pending = advance();
      if (pending == null)
        exhausted = true;
      return pending != null;
    }

    @Override
    public List<Object> next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final List<Object> path = pending;
      pending = null;
      return path;
    }

    private List<Object> advance() {
      while (true) {
        for (List<Object> path = search.next(); path != null; path = search.next()) {
          if (!passes(evalRow, path, context))
            continue;
          foundAtThisLength = true;
          if (!pattern.isAllPaths())
            // shortestPath() answers one path: nothing follows it
            exhausted = true;
          return path;
        }
        if (foundAtThisLength || !search.couldGoLonger || length >= maxLength)
          return null;
        length++;
        search = newSearch();
      }
    }

    private PathPredicateSearch newSearch() {
      // A walk of this length never asks for a vertex farther from the target than the length itself
      toTarget.expandTo(length);
      return new PathPredicateSearch(source, targetRid, length, directions, typesArray, constraint, toTarget, context, guard);
    }
  }

  private boolean passes(final ResultInternal evalRow, final List<Object> path, final CommandContext context) {
    evalRow.setProperty(pathVariable, TraversalPath.ofWalk(path));
    return pathPredicate.evaluate(evalRow, context);
  }

  private static Vertex.DIRECTION reverse(final Vertex.DIRECTION direction) {
    return switch (direction) {
      case OUT -> Vertex.DIRECTION.IN;
      case IN -> Vertex.DIRECTION.OUT;
      default -> Vertex.DIRECTION.BOTH;
    };
  }

  /**
   * Breadth-first hop counts from one vertex to the vertices it reaches, walking one direction over the relationships the pattern
   * accepts, grown one layer at a time on demand: the search only pays for the ball of the radius it has asked for so far, as the
   * shortest-path search without a predicate does, instead of walking the whole component up front.
   */
  private static final class LayeredDistances {
    private final Map<RID, Integer>  distance = new HashMap<>();
    private final Vertex.DIRECTION[] directions;
    private final String[]           typesArray;
    private final EdgeConstraint     constraint;
    private final CommandContext     context;
    private final WorkGuard          guard;
    private       Deque<Vertex>      frontier = new ArrayDeque<>();
    private       int                depth    = 0;

    LayeredDistances(final Vertex from, final Vertex.DIRECTION direction, final String[] typesArray,
        final EdgeConstraint constraint, final CommandContext context, final WorkGuard guard) {
      this.directions = expandDirections(direction);
      this.typesArray = typesArray;
      this.constraint = constraint;
      this.context = context;
      this.guard = guard;
      distance.put(from.getIdentity(), 0);
      frontier.add(from);
    }

    /** True once every vertex reachable has its distance: a vertex without one is then unreachable. */
    boolean isComplete() {
      return frontier.isEmpty();
    }

    /** Every vertex up to {@code hops} away has its distance once this returns (or the walk is complete). */
    void expandTo(final int hops) {
      while (depth < hops && !frontier.isEmpty())
        expandLayer();
    }

    /** The hop count of {@code rid}, or null when it is unreachable or farther than the layers expanded so far. */
    Integer get(final RID rid) {
      return distance.get(rid);
    }

    int expandedDepth() {
      return depth;
    }

    private void expandLayer() {
      if (Thread.interrupted())
        throw new CommandExecutionException("The shortestPath() function has been interrupted");
      guard.check();
      final Deque<Vertex> next = new ArrayDeque<>();
      for (final Vertex v : frontier) {
        for (final Vertex.DIRECTION dir : directions) {
          for (final Iterator<Edge> edges = IncomingEdgeLookup.getEdges(context, v, dir, typesArray); edges.hasNext(); ) {
            final Edge edge = edges.next();
            if (constraint != null && !constraint.matches(edge))
              continue;
            final RID neighborRid = dir == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
            if (neighborRid == null || distance.containsKey(neighborRid))
              continue;
            final Vertex neighbor;
            try {
              neighbor = dir == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
            } catch (final RecordNotFoundException e) {
              GhostEdgeReporter.reportSkipped(e);
              continue;
            }
            distance.put(neighborRid, depth + 1);
            next.add(neighbor);
          }
        }
      }
      frontier = next;
      depth++;
    }
  }

  /**
   * Enumerates, one per call, the paths of exactly {@code length} relationships from the source to the target that use no
   * relationship twice: a depth-first walk over an explicit stack, so a long path cannot overflow the thread's stack. A branch is
   * cut as soon as the hops it has left are fewer than its vertex's distance to the target (a vertex the distances have not
   * reached yet is farther than the whole length). {@link #couldGoLonger} records whether
   * any branch was cut by the length alone, the only case in which a longer path can exist.
   */
  private static final class PathPredicateSearch {
    private final RID                targetRid;
    private final int                length;
    private final Vertex.DIRECTION[] directions;
    private final String[]           typesArray;
    private final EdgeConstraint     constraint;
    private final LayeredDistances   toTarget;
    private final CommandContext     context;
    private final WorkGuard          guard;
    private final Vertex[]           vertices;
    private final Edge[]             edges;
    private final int[]              directionIndex;
    @SuppressWarnings("unchecked")
    private final Iterator<Edge>[]   cursors;
    private final Set<RID>           usedEdges = new HashSet<>();
    private       int                depth     = 0;
    private       long               steps     = 0L;
    boolean couldGoLonger = false;

    @SuppressWarnings("unchecked")
    PathPredicateSearch(final Vertex source, final RID targetRid, final int length, final Vertex.DIRECTION[] directions,
        final String[] typesArray, final EdgeConstraint constraint, final LayeredDistances toTarget,
        final CommandContext context, final WorkGuard guard) {
      this.targetRid = targetRid;
      this.length = length;
      this.directions = directions;
      this.typesArray = typesArray;
      this.constraint = constraint;
      this.toTarget = toTarget;
      this.context = context;
      this.guard = guard;
      this.vertices = new Vertex[length + 1];
      this.edges = new Edge[length];
      this.directionIndex = new int[length + 1];
      this.cursors = new Iterator[length + 1];
      vertices[0] = source;
    }

    /** The next path of the length, or {@code null} once there is none left. */
    List<Object> next() {
      while (depth >= 0) {
        if ((++steps & 1023) == 0) {
          if (Thread.interrupted())
            throw new CommandExecutionException("The shortestPath() function has been interrupted");
          guard.check();
        }

        final Edge edge = nextEdge(depth);
        if (edge == null) {
          // Every relationship of this vertex was tried: back to the previous one, freeing the relationship that led here
          cursors[depth] = null;
          directionIndex[depth] = 0;
          depth--;
          if (depth >= 0)
            usedEdges.remove(edges[depth].getIdentity());
          continue;
        }

        final Vertex.DIRECTION dir = directions[directionIndex[depth]];
        final RID neighborRid;
        try {
          neighborRid = dir == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut();
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
          continue;
        }
        final int left = length - depth - 1;
        final Integer distance = toTarget.get(neighborRid);
        if (distance == null) {
          // Unreachable when the distances are complete, otherwise farther from the target than the length allows
          if (!toTarget.isComplete())
            couldGoLonger = true;
          continue;
        }
        if (distance > left) {
          couldGoLonger = true;
          continue;
        }
        if (!usedEdges.add(edge.getIdentity()))
          continue;
        final Vertex neighbor;
        try {
          neighbor = dir == Vertex.DIRECTION.OUT ? edge.getInVertex() : edge.getOutVertex();
        } catch (final RecordNotFoundException e) {
          usedEdges.remove(edge.getIdentity());
          GhostEdgeReporter.reportSkipped(e);
          continue;
        }

        edges[depth] = edge;
        vertices[depth + 1] = neighbor;
        if (left == 0) {
          // A walk through the target can go on and come back to it, so a path of this length does not end the search
          couldGoLonger = true;
          usedEdges.remove(edge.getIdentity());
          if (neighborRid.equals(targetRid))
            return buildPath();
          continue;
        }
        depth++;
      }
      return null;
    }

    /** The next relationship of the vertex at {@code level} the pattern accepts, across its directions, or null. */
    private Edge nextEdge(final int level) {
      while (directionIndex[level] < directions.length) {
        Iterator<Edge> cursor = cursors[level];
        if (cursor == null) {
          cursor = IncomingEdgeLookup.getEdges(context, vertices[level], directions[directionIndex[level]], typesArray);
          cursors[level] = cursor;
        }
        // An undirected walk meets a self-loop on both of its lists: it is one relationship, so it is taken from the outgoing one only
        final boolean skipSelfLoops = directionIndex[level] > 0 && directions[directionIndex[level]] == Vertex.DIRECTION.IN;
        while (cursor.hasNext()) {
          final Edge edge = cursor.next();
          if (skipSelfLoops && edge.getOut().equals(edge.getIn()))
            continue;
          if (constraint == null || constraint.matches(edge))
            return edge;
        }
        cursors[level] = null;
        directionIndex[level]++;
      }
      return null;
    }

    private List<Object> buildPath() {
      final List<Object> path = new ArrayList<>(2 * length + 1);
      path.add(vertices[0]);
      for (int i = 0; i < length; i++) {
        path.add(edges[i]);
        path.add(vertices[i + 1]);
      }
      return path;
    }
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
      // The zero-length path is the only shortest path between a vertex and itself; it answers only a minimum of zero
      // (issues #7017, #11913).
      if (!bounds.accepts(0))
        return null;
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
          for (final Iterator<Edge> edges = IncomingEdgeLookup.getEdges(context, v, dir, typesArray); edges.hasNext(); ) {
            final Edge edge = edges.next();
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
    return drain(iterateFilteredAllShortestPaths(source, target, direction, edgeTypes, constraint, bounds, database, context));
  }

  /**
   * Same search as {@link #computeFilteredAllShortestPaths}, with the paths produced one at a time (issue #9123).
   */
  public static Iterator<List<Object>> iterateFilteredAllShortestPaths(final Vertex source, final Vertex target,
      final Vertex.DIRECTION direction, final String[] edgeTypes, final EdgeConstraint constraint,
      final HopBounds bounds, final Database database, final CommandContext context) {
    final RID sourceRid = source.getIdentity();
    final RID targetRid = target.getIdentity();
    if (sourceRid.equals(targetRid)) {
      // The zero-length path is the only shortest path between a vertex and itself; it answers only a minimum of zero
      // (issues #7017, #11913).
      if (!bounds.accepts(0))
        return Collections.emptyIterator();
      final List<Object> single = new ArrayList<>(1);
      single.add(source);
      return Collections.singletonList(single).iterator();
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
          for (final Iterator<Edge> edges = IncomingEdgeLookup.getEdges(context, v, dir, typesArray); edges.hasNext(); ) {
            final Edge edge = edges.next();
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
      return Collections.emptyIterator();

    return new PathEnumerator(target, sourceRid, foundDepth,
        current -> resolveFilteredHops(predecessors.get(current.getIdentity()), database), guard);
  }

  /**
   * The hops into a vertex of the edge-aware search: one per co-shortest parent, holding every edge that reached the vertex
   * from it, so parallel relationships stay distinct paths.
   */
  private static HopLink[] resolveFilteredHops(final List<PredecessorLink> links, final Database database) {
    if (links == null)
      return new HopLink[0];
    final Map<RID, List<Edge>> edgesByParent = new LinkedHashMap<>();
    for (final PredecessorLink link : links)
      edgesByParent.computeIfAbsent(link.parent, k -> new ArrayList<>(1)).add(link.edge);
    final HopLink[] hops = new HopLink[edgesByParent.size()];
    int i = 0;
    for (final Map.Entry<RID, List<Edge>> entry : edgesByParent.entrySet())
      hops[i++] = new HopLink((Vertex) database.lookupByRID(entry.getKey(), true), entry.getValue().toArray(new Edge[0]));
    return hops;
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
    // maxDepth counts the relationships on the path, the same unit as the pattern's upper hop bound (issue #10439).
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
   * Endpoints resolving to the same vertex follow the same rule. The only shortest path from a vertex to itself is the
   * zero-length one, so it answers only a pattern whose minimum is zero ({@code [*0..]}). A bare {@code [*]}, a written
   * {@code [*1..]} and an unquantified relationship all declare at least one hop, so the pair has no row (issue #11913):
   * the zero-length path is shorter than the pattern allows (issue #7017 had kept it for a minimum of one). Neo4j 5 refuses
   * such a row by default ("The shortest path algorithm does not work when the start and end nodes are the same") and
   * drops it when {@code dbms.cypher.forbid_shortestpath_common_nodes} is false, which is the behaviour kept here. A cycle
   * back to the source of at least the minimum length is not answered instead, for the reason given above for distinct
   * endpoints.
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
     * relationships on the path just like the pattern's own bound, or {@code null} when unbounded (issue #10439).
     */
    public Integer maxDepthParameter() {
      return max == Integer.MAX_VALUE ? null : max;
    }

    @Override
    public String toString() {
      // "*", not "*1..": the parser lowers a bare [*] to minHops = 1, so an EXPLAIN that spelled it out
      // would render the commonest pattern of all under a name nobody writes it by. Exactly one, though -
      // a written [*0..] is a different pattern (it is the one spelling that admits the zero-length path from a vertex to
      // itself) and keeps its lower bound on show.
      if (min == 1 && max == Integer.MAX_VALUE)
        return "*";
      if (min == max)
        return "*" + min;
      return "*" + min + ".." + (max == Integer.MAX_VALUE ? "" : max);
    }
  }

  /** One of the shared direction arrays above. */
  private static Vertex.DIRECTION[] expandDirections(final Vertex.DIRECTION direction) {
    return switch (direction) {
      case OUT -> OUT_ONLY;
      case IN -> IN_ONLY;
      default -> OUT_IN;
    };
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

  /**
   * Walks the co-shortest predecessors from the target back to the source and hands out one path per call, source first:
   * a depth-first search over an explicit stack (one frame per hop, so {@code foundDepth + 1} frames) instead of the
   * recursion that collected every path before the first one could be returned. The hops into a vertex are resolved once
   * ({@code hopCache}), so the work is proportional to the hops, not the paths through them.
   * <p>
   * Every branch of the walk ends in a path, so the deadline is checked once every 1024 completed paths.
   */
  private static final class PathEnumerator implements Iterator<List<Object>> {
    private final RID                  sourceRid;
    private final HopResolver          resolver;
    private final WorkGuard            guard;
    private final Map<RID, HopLink[]>  hopCache = new HashMap<>();
    // frame i holds the vertex reached after i hops from the target and the cursor over the hops (and their parallel
    // edges) still to try into it; chosen[i] is the edge currently taken from frame i to frame i + 1
    private final Vertex[]             vertices;
    private final HopLink[][]          hops;
    private final int[]                hopIndex;
    private final int[]                edgeIndex;
    private final Edge[]              chosen;
    private       int                  depth    = 0;
    private       long                 emitted  = 0L;
    private       List<Object>         pending;
    private       boolean              exhausted;

    PathEnumerator(final Vertex target, final RID sourceRid, final int foundDepth, final HopResolver resolver,
        final WorkGuard guard) {
      this.sourceRid = sourceRid;
      this.resolver = resolver;
      this.guard = guard;
      this.vertices = new Vertex[foundDepth + 1];
      this.hops = new HopLink[foundDepth + 1][];
      this.hopIndex = new int[foundDepth + 1];
      this.edgeIndex = new int[foundDepth + 1];
      this.chosen = new Edge[foundDepth + 1];
      vertices[0] = target;
      hops[0] = hopsInto(target);
    }

    @Override
    public boolean hasNext() {
      if (pending != null)
        return true;
      if (exhausted)
        return false;
      pending = advance();
      if (pending == null)
        exhausted = true;
      return pending != null;
    }

    @Override
    public List<Object> next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final List<Object> path = pending;
      pending = null;
      return path;
    }

    private List<Object> advance() {
      while (depth >= 0) {
        final HopLink[] frameHops = hops[depth];
        if (hopIndex[depth] >= frameHops.length) {
          depth--;
          continue;
        }
        final HopLink hop = frameHops[hopIndex[depth]];
        if (edgeIndex[depth] >= hop.edges.length) {
          hopIndex[depth]++;
          edgeIndex[depth] = 0;
          continue;
        }
        chosen[depth] = hop.edges[edgeIndex[depth]++];
        final int child = depth + 1;
        vertices[child] = hop.parent;
        if (hop.parent.getIdentity().equals(sourceRid)) {
          checkPathEnumeration(guard, emitted++);
          return buildPath(child);
        }
        hops[child] = hopsInto(hop.parent);
        hopIndex[child] = 0;
        edgeIndex[child] = 0;
        depth = child;
      }
      return null;
    }

    private HopLink[] hopsInto(final Vertex vertex) {
      final RID rid = vertex.getIdentity();
      HopLink[] resolved = hopCache.get(rid);
      if (resolved == null) {
        resolved = resolver.hopsInto(vertex);
        hopCache.put(rid, resolved);
      }
      return resolved;
    }

    /** The stack holds the walk from the target; the path reads from the source (frame {@code last}) back to it. */
    private List<Object> buildPath(final int last) {
      final List<Object> path = new ArrayList<>(2 * last + 1);
      path.add(vertices[last]);
      for (int i = last - 1; i >= 0; i--) {
        path.add(chosen[i]);
        path.add(vertices[i]);
      }
      return path;
    }
  }

  /** Resolves the hops into a vertex: its co-shortest parents, each with the relationships that join them. */
  @FunctionalInterface
  private interface HopResolver {
    HopLink[] hopsInto(Vertex vertex);
  }

  private static List<List<Object>> drain(final Iterator<List<Object>> paths) {
    final List<List<Object>> result = new ArrayList<>();
    while (paths.hasNext())
      result.add(paths.next());
    return result;
  }

  /**
   * Checked once every 1024 completed paths, since the path count is the product of the parallel relationships per hop and
   * the enumeration can outlast the BFS.
   */
  private static void checkPathEnumeration(final WorkGuard guard, final long pathsSoFar) {
    if ((pathsSoFar & 1023) != 0)
      return;
    if (Thread.interrupted())
      throw new CommandExecutionException("The allShortestPaths() function has been interrupted");
    guard.check();
  }

  private static HopLink[] resolveHops(final Vertex current, final List<RID> parents, final Vertex.DIRECTION direction,
      final String[] edgeTypes, final Database database) {
    if (parents == null)
      return new HopLink[0];
    final HopLink[] hops = new HopLink[parents.size()];
    final List<Edge> connecting = new ArrayList<>(1);
    for (int i = 0; i < hops.length; i++) {
      final Vertex parent = (Vertex) database.lookupByRID(parents.get(i), true);
      connecting.clear();
      collectConnectingEdges(parent, current, direction, edgeTypes, false, connecting);
      hops[i] = new HopLink(parent, connecting.toArray(new Edge[0]));
    }
    return hops;
  }

  /**
   * A co-shortest parent of a vertex with every relationship joining the two in the walked direction.
   */
  private static final class HopLink {
    final Vertex parent;
    final Edge[] edges;

    HopLink(final Vertex parent, final Edge[] edges) {
      this.parent = parent;
      this.edges = edges;
    }
  }

  /**
   * Resolves a list of vertex RIDs into a proper path with alternating Vertex and Edge objects, or null when two
   * consecutive vertices are no longer joined by an edge (a ghost edge, or one deleted since the search): there is no path
   * to report then, where the list used to skip the hop and bind a path that was not one (issue #9913).
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
        if (edge == null)
          return null;
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
    final String[] typesArray = edgeTypes == null || edgeTypes.isEmpty() ? null : edgeTypes.toArray(new String[0]);
    final List<Edge> found = new ArrayList<>(1);
    collectConnectingEdges(from, to, direction, typesArray, true, found);
    return found.isEmpty() ? null : found.get(0);
  }

  /**
   * Collects the relationships joining {@code from} to {@code to} when walked in {@code direction}, stopping at the
   * first one when {@code firstOnly} is set.
   * <p>
   * The edge lists are filtered on the neighbour pointer they hold, so only the relationships that actually reach
   * {@code to} are loaded rather than every relationship of {@code from}; over an edge type declared unidirectional,
   * which stores no incoming side, they are looked for from the end that stores them (issue #8625).
   *
   * @param typesArray restrict edges to these types, or null to allow any type
   */
  private static void collectConnectingEdges(final Vertex from, final Vertex to, final Vertex.DIRECTION direction,
      final String[] typesArray, final boolean firstOnly, final List<Edge> out) {
    final RID fromRid = from.getIdentity();
    final RID toRid = to.getIdentity();

    for (final Vertex.DIRECTION dir : expandDirections(direction)) {
      final boolean incomingSideMissing = dir == Vertex.DIRECTION.IN
          && IncomingEdgeLookup.isIncomingSideMissing(from.getDatabase().getSchema(), dir, typesArray);

      final Iterator<Edge> connecting;
      if (from instanceof VertexInternal internal)
        connecting = incomingSideMissing ?
            IncomingEdgeLookup.getEdgesConnectedTo(internal, dir, toRid, typesArray) :
            ((DatabaseInternal) from.getDatabase()).getGraphEngine().getEdgesConnectedTo(internal, dir, toRid, typesArray);
      else if (incomingSideMissing)
        connecting = to.getEdges(Vertex.DIRECTION.OUT, typesArray).iterator();
      else
        connecting = (typesArray != null ? from.getEdges(dir, typesArray) : from.getEdges(dir)).iterator();

      while (connecting.hasNext()) {
        final Edge edge = connecting.next();
        try {
          // The neighbour-filtered iterators answer only edges joining the two ends; the check is for the unfiltered
          // fallbacks, where the incoming-side one walks every outgoing edge of the other end.
          final boolean joins = dir == Vertex.DIRECTION.OUT ?
              edge.getIn().equals(toRid) :
              edge.getOut().equals(toRid) && edge.getIn().equals(fromRid);
          if (joins) {
            out.add(edge);
            if (firstOnly)
              return;
          }
        } catch (final RecordNotFoundException e) {
          GhostEdgeReporter.reportSkipped(e);
        }
      }
    }
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
