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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.NodePattern;
import com.arcadedb.query.opencypher.ast.RelationshipPattern;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for matching relationship patterns.
 * Expands from source vertices to target vertices following relationship patterns.
 * <p>
 * Example: (a)-[r:KNOWS]->(b)
 * - Takes vertices bound to 'a' from previous step
 * - Follows KNOWS relationships in OUT direction
 * - Binds edges to 'r' and target vertices to 'b'
 */
public class MatchRelationshipStep extends AbstractExecutionStep {
  private final String sourceVariable;
  private final String relationshipVariable;
  private final String targetVariable;
  private final RelationshipPattern pattern;
  private final String pathVariable;
  private final NodePattern targetNodePattern;
  private final Set<String> boundVariableNames;
  private final Set<String> previousStepVariables; // Snapshot for uniqueness scoping
  private final Direction directionOverride; // When non-null, overrides pattern.getDirection()

  // GAV provider for CSR-accelerated fast path (null = not checked yet, resolved lazily)
  private volatile GraphTraversalProvider gavProvider;
  private volatile boolean gavProviderResolved = false;
  private volatile String gavProviderDebug = null;

  // Profiling: track fast path vs standard path usage (single-threaded access — one thread per step instance)
  private long fastPathCount = 0;
  private long standardPathCount = 0;
  private long gavPathCount = 0;
  private boolean gavUsed = false;

  /**
   * Creates a match relationship step.
   *
   * @param sourceVariable       variable name for source vertex
   * @param relationshipVariable variable name for relationship (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern to match
   * @param context              command context
   */
  public MatchRelationshipStep(final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final RelationshipPattern pattern, final CommandContext context) {
    this(sourceVariable, relationshipVariable, targetVariable, pattern, null, context);
  }

  /**
   * Creates a match relationship step with path variable support.
   *
   * @param sourceVariable       variable name for source vertex
   * @param relationshipVariable variable name for relationship (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern to match
   * @param pathVariable         path variable name (e.g., p in p = (a)-[r]->(b)), can be null
   * @param context              command context
   */
  public MatchRelationshipStep(final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final RelationshipPattern pattern, final String pathVariable, final CommandContext context) {
    this(sourceVariable, relationshipVariable, targetVariable, pattern, pathVariable, null, null, context);
  }

  /**
   * Creates a match relationship step with target node filtering and bound variable awareness.
   *
   * @param sourceVariable       variable name for source vertex
   * @param relationshipVariable variable name for relationship (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern to match
   * @param pathVariable         path variable name (e.g., p in p = (a)-[r]->(b)), can be null
   * @param targetNodePattern    target node pattern for label filtering (can be null)
   * @param boundVariableNames   set of variable names already bound in previous steps (can be null)
   * @param context              command context
   */
  public MatchRelationshipStep(final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final RelationshipPattern pattern, final String pathVariable, final NodePattern targetNodePattern,
      final Set<String> boundVariableNames, final CommandContext context) {
    this(sourceVariable, relationshipVariable, targetVariable, pattern, pathVariable, targetNodePattern,
        boundVariableNames, null, context);
  }

  /**
   * Creates a match relationship step with separate uniqueness scoping.
   *
   * @param sourceVariable       variable name for source vertex
   * @param relationshipVariable variable name for relationship (can be null)
   * @param targetVariable       variable name for target vertex
   * @param pattern              relationship pattern to match
   * @param pathVariable         path variable name, can be null
   * @param targetNodePattern    target node pattern for label filtering, can be null
   * @param boundVariableNames   set of variable names already bound (used for identity checking)
   * @param previousStepVariables snapshot of variables from previous steps only (used for uniqueness scoping)
   * @param context              command context
   */
  public MatchRelationshipStep(final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final RelationshipPattern pattern, final String pathVariable, final NodePattern targetNodePattern,
      final Set<String> boundVariableNames, final Set<String> previousStepVariables, final CommandContext context) {
    this(sourceVariable, relationshipVariable, targetVariable, pattern, pathVariable, targetNodePattern,
        boundVariableNames, previousStepVariables, null, context);
  }

  /**
   * Creates a match relationship step with direction override.
   * Used when the plan builder reverses the traversal direction (e.g., when the target
   * is bound but the source is not, the plan starts from the bound target and reverses).
   */
  public MatchRelationshipStep(final String sourceVariable, final String relationshipVariable, final String targetVariable,
      final RelationshipPattern pattern, final String pathVariable, final NodePattern targetNodePattern,
      final Set<String> boundVariableNames, final Set<String> previousStepVariables,
      final Direction directionOverride, final CommandContext context) {
    super(context);
    this.sourceVariable = sourceVariable;
    this.relationshipVariable = relationshipVariable;
    this.targetVariable = targetVariable;
    this.pattern = pattern;
    this.pathVariable = pathVariable;
    this.targetNodePattern = targetNodePattern;
    this.boundVariableNames = boundVariableNames;
    this.previousStepVariables = previousStepVariables;
    this.directionOverride = directionOverride;
  }

  /**
   * Checks whether all edge types in the pattern exist in the schema.
   * If any type does not exist, the relationship match can never produce results.
   */
  private boolean edgeTypesExistInSchema(final CommandContext context) {
    if (!pattern.hasTypes())
      return true; // No type filter means all edges match
    final var schema = context.getDatabase().getSchema();
    for (final String type : pattern.getTypes())
      if (!schema.existsType(type))
        return false;
    return true;
  }

  /**
   * Checks whether all target node labels exist in the schema.
   * If any label does not exist, no target vertex can match.
   */
  private boolean targetLabelsExistInSchema(final CommandContext context) {
    if (targetNodePattern == null || !targetNodePattern.hasLabels())
      return true;
    final var schema = context.getDatabase().getSchema();
    for (final String label : targetNodePattern.getLabels())
      if (!schema.existsType(label))
        return false;
    return true;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("MatchRelationshipStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
      private Result lastResult = null;
      private Iterator<Edge> currentEdges = null;
      private Iterator<Vertex> currentVertices = null;
      // Short-circuit flag: checked lazily after first pull from predecessor
      // (predecessor might create the edge type via CREATE/FOREACH/MERGE)
      private Boolean schemaShortCircuit = null;
      private boolean useFastPath = false;
      private Set<RID> seenEdges = null;
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
          // Process using fast path (vertices directly) or standard path (edges)
          if (useFastPath && currentVertices != null && currentVertices.hasNext()) {
            processFastPath(n);
          } else if (!useFastPath && currentEdges != null && currentEdges.hasNext()) {
            processStandardPath(n);
          } else {
            final long begin;
            if (context.isProfiling()) {
              begin = System.nanoTime();
              if (cost < 0)
                cost = 0;
            } else
              begin = 0;
            try {
              // Initialize prevResults on first call
              if (prevResults == null)
                prevResults = prev.syncPull(context, nRecords);

              // Get next source vertex from previous step
              if (!prevResults.hasNext()) {
                finished = true;
                break;
              }

              // Lazy schema short-circuit: check AFTER first pull from predecessor
              // so that upstream CREATE/FOREACH/MERGE steps have a chance to create types.
              // prevResults.hasNext() triggers the lazy pull chain, executing predecessor steps.
              if (schemaShortCircuit == null)
                schemaShortCircuit = !edgeTypesExistInSchema(context) || !targetLabelsExistInSchema(context);
              if (schemaShortCircuit) {
                finished = true;
                break;
              }

              lastResult = prevResults.next();
              final Object sourceObj = lastResult.getProperty(sourceVariable);

              if (sourceObj instanceof Vertex) {
                final Vertex sourceVertex = (Vertex) sourceObj;

                // Determine if we can use fast path for this vertex
                useFastPath = canUseFastPath(lastResult);

                if (useFastPath) {
                  // Fast path: get vertices directly without loading edges
                  currentVertices = getVertices(sourceVertex);
                  currentEdges = null;
                  seenEdges = null;
                } else {
                  // Standard path: load edges
                  currentEdges = getEdges(sourceVertex);
                  currentVertices = null;
                  // Track seen edges for BOTH direction to deduplicate self-loops
                  seenEdges = getEffectiveDirection() == Direction.BOTH ? new HashSet<>() : null;
                }
              } else {
                // Source is not a vertex, skip
                currentEdges = null;
                currentVertices = null;
                seenEdges = null;
                useFastPath = false;
              }
            } finally {
              if (context.isProfiling())
                cost += (System.nanoTime() - begin);
            }
          }
        }
      }

      private void processFastPath(final int n) {
        while (currentVertices.hasNext() && buffer.size() < n) {
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            final Vertex targetVertex = currentVertices.next();

            // Filter by target node label if specified in the pattern
            if (targetNodePattern != null && targetNodePattern.hasLabels()) {
              if (!matchesTargetLabel(targetVertex)) {
                continue;
              }
            }

            // Filter by target node properties if specified in the pattern
            if (targetNodePattern != null && targetNodePattern.hasProperties()) {
              if (!matchesTargetProperties(targetVertex))
                continue;
            }

            // If the target variable is already bound from a previous step,
            // verify the traversed vertex matches the bound value (identity check)
            if (boundVariableNames != null && boundVariableNames.contains(targetVariable)) {
              final Object boundValue = lastResult.getProperty(targetVariable);
              if (boundValue instanceof Vertex) {
                if (!((Vertex) boundValue).getIdentity().equals(targetVertex.getIdentity())) {
                  continue;
                }
              }
            }

            // Create result with target vertex (no edge binding in fast path)
            final ResultInternal result = new ResultInternal();

            // Copy all properties from previous result
            for (final String prop : lastResult.getPropertyNames()) {
              result.setProperty(prop, lastResult.getProperty(prop));
            }

            // Add target vertex binding
            result.setProperty(targetVariable, targetVertex);

            buffer.add(result);
            if (context.isProfiling())
              fastPathCount++;
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }
      }

      private void processStandardPath(final int n) {
        while (currentEdges.hasNext() && buffer.size() < n) {
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            final Edge edge = currentEdges.next();

            // For undirected patterns, deduplicate self-loop edges
            // (self-loops appear twice: once as OUT, once as IN)
            if (seenEdges != null && !seenEdges.add(edge.getIdentity()))
              continue;

            final Vertex targetVertex = getTargetVertex(edge, (Vertex) lastResult.getProperty(sourceVariable));

            // Filter by edge type if specified
            if (pattern.hasTypes() && !matchesEdgeType(edge))
              continue;

            // Filter by inline relationship properties if specified
            if (pattern.hasProperties() && !matchesEdgeProperties(edge))
              continue;

            // Relationship uniqueness: Cypher requires each relationship in a pattern
            // to be matched to a distinct edge (no edge traversed twice)
            if (isEdgeAlreadyUsed(lastResult, edge))
              continue;

            // Filter by target node label if specified in the pattern
            if (targetNodePattern != null && targetNodePattern.hasLabels()) {
              if (!matchesTargetLabel(targetVertex)) {
                continue;
              }
            }

            // Filter by target node properties if specified in the pattern
            if (targetNodePattern != null && targetNodePattern.hasProperties()) {
              if (!matchesTargetProperties(targetVertex))
                continue;
            }

            // If the relationship variable is already bound from a previous step,
            // verify the traversed edge matches the bound value (identity check)
            if (relationshipVariable != null && boundVariableNames != null
                && boundVariableNames.contains(relationshipVariable)) {
              final Object boundRel = lastResult.getProperty(relationshipVariable);
              if (boundRel instanceof Edge) {
                if (!((Edge) boundRel).getIdentity().equals(edge.getIdentity()))
                  continue;
              }
            }

            // If the target variable is already bound from a previous step,
            // verify the traversed vertex matches the bound value (identity check)
            if (boundVariableNames != null && boundVariableNames.contains(targetVariable)) {
              final Object boundValue = lastResult.getProperty(targetVariable);
              if (boundValue instanceof Vertex) {
                if (!((Vertex) boundValue).getIdentity().equals(targetVertex.getIdentity())) {
                  continue;
                }
              }
            }

            // Create result with edge and target vertex
            final ResultInternal result = new ResultInternal();

            // Copy all properties from previous result
            for (final String prop : lastResult.getPropertyNames()) {
              result.setProperty(prop, lastResult.getProperty(prop));
            }

            // Add relationship binding if variable is specified
            if (relationshipVariable != null && !relationshipVariable.isEmpty()) {
              result.setProperty(relationshipVariable, edge);
            }

            // Add target vertex binding
            result.setProperty(targetVariable, targetVertex);

            // Add path binding if path variable is specified (e.g., p = (a)-[r]->(b))
            if (pathVariable != null && !pathVariable.isEmpty()) {
              // Check if there's an existing path from a previous hop to extend
              final Object existingPath = lastResult.getProperty(pathVariable);
              final TraversalPath path;
              if (existingPath instanceof TraversalPath)
                // Extend existing path (multi-hop pattern)
                path = new TraversalPath((TraversalPath) existingPath, edge, targetVertex);
              else {
                // Create new path starting from source vertex
                path = new TraversalPath((Vertex) lastResult.getProperty(sourceVariable));
                path.addStep(edge, targetVertex);
              }
              result.setProperty(pathVariable, path);
            }

            buffer.add(result);
            if (context.isProfiling())
              standardPathCount++;
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {
        MatchRelationshipStep.this.close();
      }
    };
  }

  /**
   * Determines if we can use the fast path (skip loading edge objects, use vertex-only traversal).
   * <p>
   * Philosophy: fast path (and GAV/CSR acceleration) is the DEFAULT. We only fall back to
   * standard edge-loading when there is a hard structural reason that requires edge objects.
   * This ensures GAV is used for all common patterns: aggregations, GROUP BY, ORDER BY,
   * WHERE on vertices, OPTIONAL MATCH, WITH, etc.
   * <p>
   * Hard blockers (edge objects are required):
   * <ol>
   *   <li>User-visible edge variable (e.g., [r:TYPE]) — the query references the edge object</li>
   *   <li>Edge property filter (e.g., [{weight: 5}]) — GAV has no edge property data</li>
   *   <li>Path variable (e.g., p = ...) — paths need edge objects for reconstruction</li>
   *   <li>Pre-bound edge variable — identity check requires the edge object</li>
   *   <li>Edge uniqueness — other edges in result require deduplication via edge identity</li>
   *   <li>Multi-hop relationship variable — internal anonymous vars (e.g., "  rel0") signal
   *       that cross-hop edge uniqueness checking is needed</li>
   * </ol>
   * <p>
   * Note: BOTH direction is allowed on the fast path. Self-loop deduplication (a self-loop
   * edge appears once in OUT and once in IN) is handled in {@link #getVertices(Vertex)}
   * by halving self-loop entries in the neighbor array.
   * <p>
   * The plan builder cooperates: for single-hop anonymous patterns it passes null as the
   * relationship variable (enabling fast path), while for multi-hop patterns it generates
   * internal anonymous variables that trigger condition 6, preserving correctness.
   */
  private boolean canUseFastPath(final Result result) {
    // 1. Any relationship variable (user-visible or internal anonymous) — either the query
    //    references the edge object, or the plan builder needs edge identity for cross-hop
    //    uniqueness checking in multi-hop patterns
    if (relationshipVariable != null && !relationshipVariable.isEmpty())
      return false;

    // 2. Edge property filter — GAV doesn't store edge properties
    if (pattern.hasProperties())
      return false;

    // 3. Path variable — path reconstruction needs edge objects
    if (pathVariable != null && !pathVariable.isEmpty())
      return false;

    // 4. Edge uniqueness — other hops in the same MATCH already produced edges,
    //    so Cypher requires deduplication via edge identity
    if (resultContainsEdges(result))
      return false;

    return true;
  }

  /**
   * Checks if the result contains any edges that would require uniqueness checking.
   */
  @SuppressWarnings("unchecked")
  private boolean resultContainsEdges(final Result result) {
    for (final String prop : result.getPropertyNames()) {
      // Skip the current relationship variable if it's being rebound
      if (prop.equals(relationshipVariable))
        continue;
      // Skip variables from previous MATCH clauses
      if (previousStepVariables != null && previousStepVariables.contains(prop))
        continue;
      final Object val = result.getProperty(prop);
      if (val instanceof Edge)
        return true;
      if (val instanceof TraversalPath)
        return true;
      if (val instanceof List) {
        for (final Object item : (List<Object>) val)
          if (item instanceof Edge)
            return true;
      }
    }
    return false;
  }

  /**
   * Returns the effective direction, using the override if set, otherwise the pattern direction.
   */
  private Direction getEffectiveDirection() {
    return directionOverride != null ? directionOverride : pattern.getDirection();
  }

  /**
   * Gets edges from a vertex based on the relationship pattern.
   */
  private Iterator<Edge> getEdges(final Vertex vertex) {
    final Direction direction = getEffectiveDirection();
    final String[] types = pattern.hasTypes() ?
        pattern.getTypes().toArray(new String[0]) :
        null;

    if (types == null || types.length == 0) {
      return vertex.getEdges(direction.toArcadeDirection()).iterator();
    } else {
      return vertex.getEdges(direction.toArcadeDirection(), types).iterator();
    }
  }

  /**
   * Gets vertices directly from a vertex based on the relationship pattern.
   * This is an optimized path that skips loading edge objects.
   * When a GAV provider covers the required edge types, uses CSR arrays for O(1) neighbor lookup.
   */
  private Iterator<Vertex> getVertices(final Vertex vertex) {
    final Direction direction = getEffectiveDirection();
    final String[] types = pattern.hasTypes() ?
        pattern.getTypes().toArray(new String[0]) :
        null;

    // Try GAV-accelerated lookup (lazy resolution, cached)
    final GraphTraversalProvider provider = resolveGavProvider(types);
    if (provider != null) {
      final int nodeId = provider.getNodeId(vertex.getIdentity());
      if (nodeId >= 0) {
        gavUsed = true;
        int[] neighborIds = provider.getNeighborIds(nodeId, direction.toArcadeDirection(), types);

        // For BOTH direction, deduplicate self-loop entries: each self-loop edge
        // appears once in the forward and once in the backward neighbor list,
        // doubling it in the merged result. Keep only half the self-loop entries.
        if (direction == Direction.BOTH)
          neighborIds = deduplicateSelfLoops(neighborIds, nodeId);

        final int[] neighbors = neighborIds;
        return new Iterator<>() {
          private int idx = 0;

          @Override
          public boolean hasNext() {
            return idx < neighbors.length;
          }

          @Override
          public Vertex next() {
            if (!hasNext())
              throw new NoSuchElementException();
            final RID rid = provider.getRID(neighbors[idx++]);
            gavPathCount++;
            return (Vertex) context.getDatabase().lookupByRID(rid, true);
          }
        };
      }
    }

    final Iterator<Vertex> it;
    if (types == null || types.length == 0)
      it = vertex.getVertices(direction.toArcadeDirection()).iterator();
    else
      it = vertex.getVertices(direction.toArcadeDirection(), types).iterator();

    // For BOTH direction, deduplicate self-loop vertices: the OLTP iterator
    // concatenates OUT and IN edge iterators, so self-loops yield the source
    // vertex twice. Skip every other occurrence of the source vertex.
    // This is semantically equivalent to the CSR path's deduplicateSelfLoops(),
    // which removes selfLoopCount/2 entries from the neighbor array — both rely
    // on the invariant that each self-loop produces exactly 2 entries.
    //
    // Structural invariant: ArcadeDB stores each edge in exactly two edge lists
    // (one OUT, one IN) — see GraphEngine.getVertices() BOTH case which concatenates
    // outEdges.vertexIterator() + inEdges.vertexIterator(). A self-loop edge (src==tgt)
    // therefore appears once in each list, producing exactly 2 entries in the merged
    // iterator. This invariant holds as long as the storage model uses separate
    // per-direction edge linked lists (EdgeLinkedList).
    if (direction == Direction.BOTH) {
      final RID sourceRid = vertex.getIdentity();
      return new Iterator<>() {
        private Vertex nextVertex = null;
        private int selfLoopSeen = 0;

        @Override
        public boolean hasNext() {
          if (nextVertex != null)
            return true;
          while (it.hasNext()) {
            final Vertex v = it.next();
            if (v.getIdentity().equals(sourceRid)) {
              selfLoopSeen++;
              if (selfLoopSeen % 2 == 0)
                continue; // skip every other self-loop occurrence
            }
            nextVertex = v;
            return true;
          }
          return false;
        }

        @Override
        public Vertex next() {
          if (!hasNext())
            throw new NoSuchElementException();
          final Vertex v = nextVertex;
          nextVertex = null;
          return v;
        }
      };
    }
    return it;
  }

  /**
   * Lazily resolves a GAV provider that covers the required edge types.
   * Result is cached for the lifetime of this step.
   * <p>
   * Note: concurrent threads may both enter the {@code !gavProviderResolved} branch and call
   * {@code findProvider()} redundantly. This is intentional — the result is idempotent and a
   * full {@code synchronized} block would add contention on the hot path for no correctness gain.
   */
  private GraphTraversalProvider resolveGavProvider(final String[] edgeTypes) {
    if (!gavProviderResolved) {
      final Database db = context.getDatabase();
      final GraphTraversalProvider resolved = GraphTraversalProviderRegistry.findProvider(db, edgeTypes);
      if (resolved == null && context.isProfiling()) {
        final List<GraphTraversalProvider> allProviders = GraphTraversalProviderRegistry.getProviders(db);
        gavProviderDebug = "db=" + db.getClass().getSimpleName() + ", providers=" + allProviders.size();
        if (!allProviders.isEmpty())
          gavProviderDebug += " [ready=" + allProviders.stream().filter(GraphTraversalProvider::isReady).count()
              + ", edgeTypes=" + java.util.Arrays.toString(edgeTypes) + "]";
      }
      // Assign provider before setting resolved flag to prevent another thread from seeing
      // resolved=true with gavProvider still null
      gavProvider = resolved;
      gavProviderResolved = true;
    }
    return gavProvider;
  }

  /**
   * Gets the target vertex from an edge based on direction.
   * Optimized to avoid loading both vertices when direction is known.
   */
  private Vertex getTargetVertex(final Edge edge, final Vertex sourceVertex) {
    final Direction direction = getEffectiveDirection();
    // Optimize for directional patterns - only load the target vertex
    if (direction == Direction.OUT) {
      return edge.getInVertex();
    } else if (direction == Direction.IN) {
      return edge.getOutVertex();
    } else {
      // BOTH direction - need to check which vertex is the source
      // Load out vertex first (more common case for directed graphs)
      final Vertex out = edge.getOutVertex();
      if (out.getIdentity().equals(sourceVertex.getIdentity())) {
        return edge.getInVertex();
      } else {
        return out;
      }
    }
  }

  /**
   * Checks if a target vertex matches the label constraints from the target node pattern.
   */
  private boolean matchesTargetLabel(final Vertex vertex) {
    if (targetNodePattern == null || !targetNodePattern.hasLabels())
      return true;

    // Check that the vertex has ALL required labels using type hierarchy
    for (final String label : targetNodePattern.getLabels())
      if (!vertex.getType().instanceOf(label))
        return false;
    return true;
  }

  /**
   * Checks if a target vertex matches the inline property constraints from the target node pattern.
   */
  private boolean matchesTargetProperties(final Vertex vertex) {
    if (targetNodePattern == null || !targetNodePattern.hasProperties())
      return true;

    for (final Map.Entry<String, Object> entry : targetNodePattern.getProperties().entrySet()) {
      final Object actual = vertex.get(entry.getKey());
      Object expected = entry.getValue();
      // Handle string literals: remove quotes
      if (expected instanceof String) {
        final String s = (String) expected;
        if ((s.startsWith("'") && s.endsWith("'")) || (s.startsWith("\"") && s.endsWith("\"")))
          expected = s.substring(1, s.length() - 1);
      }
      if (actual == null || !actual.equals(expected))
        return false;
    }
    return true;
  }

  /**
   * Checks if an edge is already used in the result.
   * Enforces Cypher's relationship uniqueness constraint.
   * Checks both Edge-typed properties and edges inside TraversalPaths.
   */
  @SuppressWarnings("unchecked")
  private boolean isEdgeAlreadyUsed(final Result result, final Edge edge) {
    final RID edgeRid = edge.getIdentity();
    for (final String prop : result.getPropertyNames()) {
      // Skip the current relationship variable: it holds a bound value from a previous
      // step that we're about to overwrite, not a different relationship in the pattern
      if (prop.equals(relationshipVariable))
        continue;
      // Skip variables from previous MATCH clauses (via WITH/previous MATCHes): Cypher's
      // relationship uniqueness only applies within a single MATCH clause
      if (previousStepVariables != null && previousStepVariables.contains(prop))
        continue;
      final Object val = result.getProperty(prop);
      if (val instanceof Edge && ((Edge) val).getIdentity().equals(edgeRid))
        return true;
      if (val instanceof TraversalPath) {
        for (final Edge pathEdge : ((TraversalPath) val).getEdges())
          if (pathEdge.getIdentity().equals(edgeRid))
            return true;
      }
      // Check edge lists from VLP relationship variables
      if (val instanceof List) {
        for (final Object item : (List<Object>) val)
          if (item instanceof Edge && ((Edge) item).getIdentity().equals(edgeRid))
            return true;
      }
    }
    return false;
  }

  /**
   * Checks if an edge matches the type filter.
   */
  private boolean matchesEdgeType(final Edge edge) {
    if (!pattern.hasTypes())
      return true;

    final String edgeType = edge.getTypeName();
    for (final String type : pattern.getTypes())
      if (type.equals(edgeType))
        return true;
    return false;
  }

  /**
   * Checks if an edge matches the inline property filters.
   */
  private boolean matchesEdgeProperties(final Edge edge) {
    if (!pattern.hasProperties())
      return true;

    for (final Map.Entry<String, Object> entry : pattern.getProperties().entrySet()) {
      final Object actual = edge.get(entry.getKey());
      final Object expected = entry.getValue();
      if (actual == null || !actual.equals(expected))
        return false;
    }
    return true;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ MATCH RELATIONSHIP ");
    builder.append("(").append(sourceVariable).append(")");
    builder.append(pattern);
    builder.append("(").append(targetVariable).append(")");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());

      // Show fast path vs standard path statistics
      if (gavUsed || fastPathCount > 0 || standardPathCount > 0) {
        builder.append(", traversal: ");
        if (gavUsed) {
          builder.append("GAV/CSR (").append(gavPathCount).append(" vertices");
          if (gavProvider != null)
            builder.append(", provider=").append(gavProvider.getName());
          builder.append(")");
        } else if (fastPathCount > 0 && standardPathCount > 0) {
          // Mixed: both paths used
          builder.append("mixed [fast: ").append(fastPathCount);
          builder.append(", edges: ").append(standardPathCount).append("]");
        } else if (fastPathCount > 0) {
          // Fast path only
          builder.append("optimized (").append(fastPathCount).append(" vertices)");
        } else {
          // Standard path only
          builder.append("standard (").append(standardPathCount).append(" edges)");
        }
      }
      if (gavProviderDebug != null)
        builder.append(", GAV-debug: ").append(gavProviderDebug);

      builder.append(")");
    }
    return builder.toString();
  }

  /**
   * Removes duplicate self-loop entries from a neighbor ID array for BOTH direction.
   * Each self-loop edge contributes the source node to both the forward and backward
   * neighbor lists, so it appears twice in the merged array. This method keeps only
   * half the self-loop entries, preserving correct multiplicity for multi-self-loop cases.
   * <p>
   * <b>Invariant:</b> the self-loop count is always even because every self-loop edge
   * contributes exactly one entry to the forward neighbor list and one to the backward
   * neighbor list — whether from base CSR or from the delta overlay (which adds to both
   * ovOut and ovIn). This mirrors the OLTP path's skip-every-other deduplication in
   * {@link #getVertices(Vertex)}, which also relies on the OUT+IN iterator concatenation
   * producing exactly 2 entries per self-loop edge. See
   * {@code GraphEngine.getVertices()} BOTH case for the structural guarantee.
   */
  private static int[] deduplicateSelfLoops(final int[] neighborIds, final int sourceNodeId) {
    int selfLoopCount = 0;
    for (final int id : neighborIds)
      if (id == sourceNodeId)
        selfLoopCount++;
    if (selfLoopCount <= 1)
      return neighborIds; // 0 or 1 self-loop entries: no duplicates to remove
    final int toRemove = selfLoopCount / 2;
    final int[] result = new int[neighborIds.length - toRemove];
    int w = 0;
    int skipped = 0;
    for (final int id : neighborIds) {
      if (id == sourceNodeId && skipped < toRemove) {
        skipped++;
        continue;
      }
      result[w++] = id;
    }
    return result;
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
