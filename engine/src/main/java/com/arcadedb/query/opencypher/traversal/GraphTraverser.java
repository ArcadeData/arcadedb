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
package com.arcadedb.query.opencypher.traversal;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeEntryRIDIterator;
import com.arcadedb.graph.GhostEdgeReporter;
import com.arcadedb.graph.IncomingEdgeLookup;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.ast.Direction;
import com.arcadedb.query.opencypher.ast.PathMode;
import com.arcadedb.query.opencypher.executor.SelfLoops;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.utility.RidHashSet;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

/**
 * Base class for graph traversal implementations.
 * Provides common functionality for BFS, DFS, and variable-length path traversals.
 */
public abstract class GraphTraverser {
  protected final Direction direction;
  protected final String[] relationshipTypes;
  protected final Map<String, Object> edgePropertyFilters;
  protected final int minHops;
  protected final int maxHops;
  protected final boolean trackPaths;
  protected final boolean detectCycles;
  protected final PathMode pathMode;
  /**
   * Optional per-relationship predicate, carrying an inline {@code WHERE} such as the
   * {@code WHERE r.tag = 'ok'} in {@code -[r:E*1..2 WHERE r.tag = 'ok']->}. Every relationship the
   * path traverses must satisfy it, the same rule the inline property map follows. Null means
   * unconstrained, which keeps the common path free of any per-edge evaluation.
   */
  protected Predicate<Edge> edgePredicate;
  /**
   * True when nothing reads the relationships this traversal walks - no variable, path, property map or WHERE - so an
   * edge only has to lead to its far end and be told apart from the others: it is then followed on the endpoints its
   * edge-list entry holds, without loading its record (issue #8537).
   */
  protected boolean edgesUnread;
  /**
   * The query this traversal runs for, whose lookup answers the incoming side of the unidirectional edge types
   * (issue #8625). Null walks the adjacency lists only.
   */
  protected CommandContext context;
  private   Boolean        incomingLookup;

  /**
   * Creates a graph traverser with specified parameters.
   *
   * @param direction         direction to traverse (OUT, IN, BOTH)
   * @param relationshipTypes relationship types to follow (null = all types)
   * @param minHops           minimum number of hops
   * @param maxHops           maximum number of hops
   * @param trackPaths        whether to track full paths
   * @param detectCycles      whether to detect and avoid cycles
   */
  protected GraphTraverser(final Direction direction, final String[] relationshipTypes, final int minHops, final int maxHops,
      final boolean trackPaths, final boolean detectCycles) {
    this(direction, relationshipTypes, null, minHops, maxHops, trackPaths, detectCycles);
  }

  protected GraphTraverser(final Direction direction, final String[] relationshipTypes,
      final Map<String, Object> edgePropertyFilters, final int minHops, final int maxHops,
      final boolean trackPaths, final boolean detectCycles) {
    this(direction, relationshipTypes, edgePropertyFilters, minHops, maxHops, trackPaths,
        detectCycles ? PathMode.ACYCLIC : PathMode.WALK);
  }

  protected GraphTraverser(final Direction direction, final String[] relationshipTypes,
      final Map<String, Object> edgePropertyFilters, final int minHops, final int maxHops,
      final boolean trackPaths, final PathMode pathMode) {
    this.direction = direction != null ? direction : Direction.BOTH;
    this.relationshipTypes = relationshipTypes;
    this.edgePropertyFilters = edgePropertyFilters != null ? edgePropertyFilters : Collections.emptyMap();
    this.minHops = Math.max(0, minHops);
    this.maxHops = maxHops >= 0 ? maxHops : Integer.MAX_VALUE;
    this.trackPaths = trackPaths;
    this.pathMode = pathMode != null ? pathMode : PathMode.TRAIL;
    this.detectCycles = this.pathMode != PathMode.WALK;
  }

  /**
   * Traverses from a start vertex and returns matching end vertices.
   *
   * @param startVertex vertex to start traversal from
   * @return iterator of matching vertices
   */
  public abstract Iterator<Vertex> traverse(Vertex startVertex);

  /**
   * Traverses from a start vertex and returns matching paths.
   *
   * @param startVertex vertex to start traversal from
   * @return iterator of matching paths
   */
  public abstract Iterator<TraversalPath> traversePaths(Vertex startVertex);

  /**
   * Gets edges from a vertex based on direction and relationship types.
   * <p>
   * NOTE: Use getNextVertices() when you don't need the edge objects for better performance.
   *
   * @param vertex vertex to get edges from
   * @return iterable of matching edges
   */
  protected Iterable<Edge> getEdges(final Vertex vertex) {
    final Iterator<Edge> edges;
    if (isIncomingLookupNeeded(vertex))
      edges = IncomingEdgeLookup.getEdges(context, vertex, direction.toArcadeDirection(), relationshipTypes);
    else if (edgesUnread && vertex instanceof VertexInternal internal) {
      final DatabaseInternal database = (DatabaseInternal) internal.getDatabase();
      edges = new LiveEdges(database,
          database.getGraphEngine().getEdgesKnowingEndpoints(internal, direction.toArcadeDirection(), relationshipTypes));
    } else if (relationshipTypes == null || relationshipTypes.length == 0)
      edges = vertex.getEdges(direction.toArcadeDirection()).iterator();
    else
      edges = vertex.getEdges(direction.toArcadeDirection(), relationshipTypes).iterator();

    // An undirected hop reaches a self-loop from both of its vertex's lists: it is still one relationship, and the
    // two sightings used to become two paths (issue #8537, found alongside)
    final Iterator<Edge> result = direction == Direction.BOTH ? SelfLoops.deduplicatingEdges(edges) : edges;
    return () -> result;
  }

  /**
   * The edges of a walk that follows them on the endpoints their adjacency entries hold, without a ghost among them: an entry whose
   * edge record is gone is skipped and reported (issue #10149), as it is by every walk that loads the record. The bucket is asked
   * about the edge RID the entry holds, which reads no record, so the walk keeps what it gained by not loading the edges.
   */
  private static final class LiveEdges implements Iterator<Edge> {
    private final DatabaseInternal database;
    private final Iterator<Edge>   edges;
    private       Edge             next;

    private LiveEdges(final DatabaseInternal database, final Iterator<Edge> edges) {
      this.database = database;
      this.edges = edges;
    }

    @Override
    public boolean hasNext() {
      while (next == null && edges.hasNext()) {
        final Edge candidate = edges.next();
        if (GhostEdgeReporter.isLiveEntry(database, candidate.getIdentity()))
          next = candidate;
      }
      return next != null;
    }

    @Override
    public Edge next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final Edge result = next;
      next = null;
      return result;
    }
  }

  /**
   * The adjacency entries of a vertex, for a walk that wants the vertices they lead to and nothing of the edges: the far vertex of
   * each entry and, on request, whether the edge the entry names still has its record.
   * <p>
   * The far end is read off the entry and the edge is not loaded, which is what makes these walks cheap. What that cannot tell is a
   * ghost, an entry whose edge record is gone, and a walk that followed it answered differently from one that read the edge
   * ({@code RETURN DISTINCT b.name} against {@code RETURN DISTINCT b.name, size(r)}) and reported nothing (issue #10149). So the
   * walk asks {@link #liveEdge()} for the entry it is about to follow, after the test for a vertex it has met already, which turns
   * away the large majority of the entries: the probe is paid once per vertex reached and not once per entry, and it asks the
   * bucket about the edge RID the entry holds, so no record is read (the same rule {@code path.subgraphNodes} follows, #10148).
   */
  protected final class Neighbours {
    private final Database                database;
    private final VertexInternal          source;
    // Only for the sides that are not read off an adjacency list of this vertex (the incoming side of a unidirectional edge type, which
    // the lookup answers with the vertices themselves, and a vertex that is not an engine vertex): read after the lists
    private final Iterator<Vertex>        resolved;
    private       Vertex                  resolvedVertex;
    // Whether the entry next() returned last came from resolved, which has no edge RID to ask the bucket about
    private       boolean                 lastResolved;
    // The adjacency list being read, null once every list is read
    private       EdgeEntryRIDIterator    entries;
    // The list still to be read after the current one: the incoming one of an undirected hop
    private       Vertex.DIRECTION        pending;

    private Neighbours(final Iterator<Vertex> resolved, final Database database) {
      this.resolved = resolved;
      this.database = database;
      this.source = null;
    }

    /**
     * @param direction the side of the vertex to read off its adjacency list(s), null for none
     * @param resolved  the vertices of the sides not read that way, null for none
     */
    private Neighbours(final VertexInternal source, final Vertex.DIRECTION direction, final Iterator<Vertex> resolved) {
      this.resolved = resolved;
      this.source = source;
      this.database = source.getDatabase();
      if (direction == Vertex.DIRECTION.BOTH) {
        entries = open(Vertex.DIRECTION.OUT);
        pending = Vertex.DIRECTION.IN;
      } else if (direction != null)
        entries = open(direction);
    }

    private EdgeEntryRIDIterator open(final Vertex.DIRECTION side) {
      return ((DatabaseInternal) database).getGraphEngine().getConnectedVertexEntries(source, side, relationshipTypes);
    }

    public boolean hasNext() {
      while (entries != null) {
        if (entries.hasNext())
          return true;
        if (pending == null)
          entries = null;
        else {
          entries = open(pending);
          pending = null;
        }
      }
      return resolved != null && resolved.hasNext();
    }

    /** The RID of the vertex the next entry leads to, null for an entry that names none. */
    public RID next() {
      // hasNext() leaves entries on the list the next entry is in, and null when the lists are read and it is in resolved
      if (entries != null) {
        lastResolved = false;
        return entries.next();
      }
      lastResolved = true;
      resolvedVertex = resolved.next();
      return resolvedVertex.getIdentity();
    }

    /**
     * Whether the edge of the entry {@link #next()} last returned still has its record. A ghost is reported and the walk skips it,
     * without spending the vertex it names: another, live, entry may reach it.
     */
    public boolean liveEdge() {
      // The vertices a lookup answered have no edge RID to ask about, and need none: the incoming side of a unidirectional type is
      // answered from a scan of the edge RECORDS of the type, which a ghost has none of, and a stored incoming side reads the edge
      // when it reads the source, which fails on a ghost where the expansions catch it. The outgoing list of an undirected hop over
      // such a type is stored, so it is read by its entries like every other list and probed here (issue #10463)
      return lastResolved || GhostEdgeReporter.isLiveEntry(database, entries.getEdgeRID());
    }

    /** The vertex the entry {@link #next()} last returned leads to, or null when its record is gone. */
    public Vertex vertex(final RID rid) {
      if (lastResolved)
        return resolvedVertex;
      try {
        return (Vertex) database.lookupByRID(rid, false);
      } catch (final RecordNotFoundException e) {
        return null;
      }
    }
  }

  /**
   * The adjacency entries of {@code vertex} in the direction and over the relationship types of this walk, see {@link Neighbours}.
   */
  protected Neighbours getNeighbours(final Vertex vertex) {
    if (!(vertex instanceof VertexInternal internal))
      return new Neighbours(getNextVertices(vertex).iterator(), vertex.getDatabase());

    final Vertex.DIRECTION arcadeDirection = direction.toArcadeDirection();
    if (isIncomingLookupNeeded(vertex))
      // Over a unidirectional type the lookup answers the incoming side; the outgoing list of an undirected hop is stored, and read
      // by its entries to be asked about each of them (issue #10463)
      return new Neighbours(internal, arcadeDirection == Vertex.DIRECTION.BOTH ? Vertex.DIRECTION.OUT : null,
          IncomingEdgeLookup.getIncomingVertices(context, vertex, arcadeDirection, relationshipTypes));
    return new Neighbours(internal, arcadeDirection, null);
  }

  /**
   * Gets connected vertices directly without loading edge records.
   * <p>
   * PERFORMANCE: This is much faster than getEdges() + getOtherVertex()
   * because it skips loading the edge records entirely. It follows a ghost entry like any other: a walk that has to agree with the
   * ones that read the edge uses {@link #getNeighbours} and asks about the entries it follows.
   *
   * @param vertex vertex to get connected vertices from
   * @return iterable of connected vertices
   */
  protected Iterable<Vertex> getNextVertices(final Vertex vertex) {
    if (isIncomingLookupNeeded(vertex)) {
      final Iterator<Vertex> vertices = IncomingEdgeLookup.getVertices(context, vertex, direction.toArcadeDirection(),
          relationshipTypes);
      return () -> vertices;
    }
    if (relationshipTypes == null || relationshipTypes.length == 0) {
      return vertex.getVertices(direction.toArcadeDirection());
    } else {
      return vertex.getVertices(direction.toArcadeDirection(), relationshipTypes);
    }
  }

  /**
   * Gets the other vertex from an edge.
   *
   * @param edge   edge to traverse
   * @param from   starting vertex
   * @return other vertex
   */
  protected Vertex getOtherVertex(final Edge edge, final Vertex from) {
    // Compares the endpoint RIDs and resolves only the far end
    return edge.getOut().equals(from.getIdentity()) ? edge.getInVertex() : edge.getOutVertex();
  }

  /**
   * Checks if a vertex matches the relationship type filter.
   * <p>
   * Polymorphic: an edge of type {@code SUB} matches the filter for {@code BASE} when
   * {@code SUB} extends {@code BASE}. This mirrors the polymorphic semantics of the
   * native Java traversal API (which already returns sub-type edges via
   * {@code getBucketIds(true)}) and avoids dropping sub-type edges in variable-length
   * patterns like {@code [:BASE*1..3]}.
   *
   * @param edge edge to check
   * @return true if edge matches type filter
   */
  protected boolean matchesTypeFilter(final Edge edge) {
    if (relationshipTypes == null || relationshipTypes.length == 0) {
      return true;
    }

    final var edgeType = edge.getType();
    for (final String type : relationshipTypes) {
      if (edgeType.instanceOf(type)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if an edge matches the property filter constraints.
   *
   * @param edge edge to check
   * @return true if edge matches all property filters (or no filters set)
   */
  protected boolean matchesPropertyFilter(final Edge edge) {
    return matchesPropertyFilter(edge, edgePropertyFilters);
  }

  /**
   * Attaches the per-relationship inline {@code WHERE} predicate. Traversers that delegate to a
   * nested traverser must forward it, otherwise the predicate is silently dropped for that strategy.
   *
   * @param edgePredicate predicate every traversed relationship must satisfy, null for unconstrained
   *
   * @return this traverser, for chaining at the construction site
   */
  public GraphTraverser withEdgePredicate(final Predicate<Edge> edgePredicate) {
    this.edgePredicate = edgePredicate;
    return this;
  }

  /**
   * Declares that nothing reads the walked relationships (see {@link #edgesUnread}). Traversers that delegate to a
   * nested traverser must forward it.
   *
   * @return this traverser, for chaining at the construction site
   */
  public GraphTraverser withEdgesUnread(final boolean edgesUnread) {
    this.edgesUnread = edgesUnread;
    return this;
  }

  /**
   * Attaches the query this traversal runs for (see {@link #context}). Traversers that delegate to a nested traverser
   * must forward it.
   *
   * @return this traverser, for chaining at the construction site
   */
  public GraphTraverser withContext(final CommandContext context) {
    this.context = context;
    this.incomingLookup = null;
    return this;
  }

  private boolean isIncomingLookupNeeded(final Vertex vertex) {
    if (context == null)
      return false;
    Boolean needed = incomingLookup;
    if (needed == null) {
      needed = IncomingEdgeLookup.isNeeded(context, vertex.getDatabase(), direction.toArcadeDirection(), relationshipTypes);
      incomingLookup = needed;
    }
    return needed;
  }

  /**
   * Checks an edge against the inline {@code WHERE} predicate, if one was attached.
   *
   * @param edge edge to check
   *
   * @return true if the edge satisfies the predicate (or no predicate is set)
   */
  protected boolean matchesEdgePredicate(final Edge edge) {
    return edgePredicate == null || edgePredicate.test(edge);
  }

  /**
   * Checks an edge against an inline {@code {prop: value}} filter map, using the same comparison rules as
   * every other pattern evaluator ({@link InlineProperties#matchesResolvedValue}).
   * <p>
   * The filter values must already be resolved: a traverser has neither the query parameters nor the row
   * a dynamic value would be evaluated against, so the caller resolves the map once per row through
   * {@link InlineProperties#resolveAll} before handing it over. Filtering on the declared values instead
   * silently matched nothing whenever the map held anything but a literal (issue #5501).
   *
   * @param edge    edge to check
   * @param filters resolved property constraints, may be null or empty
   *
   * @return true if edge matches all property filters (or no filters set)
   */
  public static boolean matchesPropertyFilter(final Edge edge, final Map<String, Object> filters) {
    if (filters == null || filters.isEmpty())
      return true;

    for (final Map.Entry<String, Object> entry : filters.entrySet()) {
      if (!InlineProperties.matchesProperty(edge, entry.getKey(), entry.getValue()))
        return false;
    }
    return true;
  }

  /**
   * True when {@code variable} names a relationship list the row already binds, e.g. the {@code rs} of
   * {@code WITH [r1, r2] AS rs MATCH (a)-[rs*]->(b)}: the variable-length pattern must then match that list as its one
   * path rather than expand the graph (openCypher TCK Match4 [8], issue #9828). A name with two leading spaces is the
   * planner's own edge tracking, never a binding the query wrote, so it never constrains an expansion.
   */
  public static boolean isBoundRelationshipList(final Result row, final String variable) {
    return variable != null && !variable.isEmpty() && !variable.startsWith("  ") && row != null && row.hasProperty(variable);
  }

  /**
   * Matches a relationship list already bound to the pattern's variable as the only path this traversal may take from
   * {@code start}. The list is accepted when its length is inside the hop range, each relationship passes the same
   * type, inline property and inline {@code WHERE} filters and the same path-mode uniqueness rule a traversed one
   * does, and the relationships connect head to tail from {@code start} in this traversal's direction.
   *
   * @param start         the vertex the traversal starts from
   * @param relationships the bound value: null matches nothing, anything but a list of relationships is a TypeError
   * @param fromLast      true when the plan walks the pattern against its written direction (anchored on the far end):
   *                      the list is always in written order, so it is then consumed from its last relationship
   * @param variable      the bound variable, named by the TypeError
   *
   * @return the path in traversal order, or null when the list is not a path this traversal could have walked
   */
  public TraversalPath matchBoundPath(final Vertex start, final Object relationships, final boolean fromLast,
      final String variable) {
    if (relationships == null)
      return null;
    if (!(relationships instanceof List<?> list))
      throw new IllegalArgumentException("TypeError: variable-length relationship '" + variable
          + "' must be bound to a list of relationships, got " + relationships.getClass().getSimpleName());

    final int hops = list.size();
    boolean hasNull = false;
    for (int i = 0; i < hops; i++) {
      final Object element = list.get(i);
      if (element == null)
        hasNull = true;
      else if (!(element instanceof Edge))
        throw new IllegalArgumentException("TypeError: variable-length relationship '" + variable
            + "' must be bound to a list of relationships, but it contains " + element.getClass().getSimpleName());
    }
    if (hasNull || hops < minHops || hops > maxHops)
      return null;

    final TraversalPath path = new TraversalPath(start);
    Vertex current = start;
    for (int i = 0; i < hops; i++) {
      final Edge edge = (Edge) list.get(fromLast ? hops - 1 - i : i);
      if (!matchesTypeFilter(edge) || !matchesPropertyFilter(edge) || !matchesEdgePredicate(edge))
        return null;
      if (pathMode != PathMode.WALK && path.containsEdge(edge))
        return null;

      final RID at = current.getIdentity();
      final Vertex next;
      if (direction != Direction.IN && edge.getOut().equals(at))
        next = edge.getInVertex();
      else if (direction != Direction.OUT && edge.getIn().equals(at))
        next = edge.getOutVertex();
      else
        return null;

      if (pathMode == PathMode.ACYCLIC && path.containsVertex(next))
        return null;
      path.addStep(edge, next);
      current = next;
    }
    return path;
  }

  /**
   * Creates a visited set for cycle detection.
   * Uses RidHashSet for zero-boxing O(1) lookups with ~8.7x memory savings over HashSet&lt;RID&gt;.
   *
   * @return new RidHashSet for tracking visited vertex RIDs
   */
  protected RidHashSet createVisitedSet() {
    return new RidHashSet();
  }

  /**
   * Checks if a vertex has been visited.
   * O(1) hash lookup using primitive bucketId + offset — no temporary RID allocation.
   *
   * @param vertex  vertex to check
   * @param visited set of visited vertex RIDs
   * @return true if visited
   */
  protected boolean isVisited(final Vertex vertex, final RidHashSet visited) {
    return visited.contains(vertex.getIdentity());
  }

  /**
   * Marks a vertex as visited.
   * Stores only primitive bucketId + offset for memory efficiency and O(1) lookups.
   *
   * @param vertex  vertex to mark
   * @param visited set of visited vertex RIDs
   */
  protected void markVisited(final Vertex vertex, final RidHashSet visited) {
    visited.add(vertex.getIdentity());
  }
}
