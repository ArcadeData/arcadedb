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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordCallback;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.utility.MultiIterator;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * The incoming side of the edge types declared unidirectional, for the query languages (issue #8625).
 * <p>
 * A unidirectional edge type writes the outgoing pointer only, so a vertex holds no trace of the edges of such a type
 * that point AT it: walking IN (or BOTH) from it over the type answers nothing, or only half. The vertex API keeps
 * that contract - it is what the type was declared for - but a query pattern is not a walk: {@code (t)<-[:X]-(q)} and
 * {@code {t}.in('X'){q}} ask which edges end in {@code t}, and the answer cannot depend on which end the edges happen
 * to be stored on. The planners walk such a pattern from its source side whenever they can; this class answers it
 * when they cannot (the target is bound by an earlier clause, the hop is undirected, a pattern expression starts from
 * the target), and makes every IN walk over such a type complete instead of silently empty.
 * <p>
 * The answer comes from one scan per unidirectional type, taken the first time a query needs that type and shared by
 * the whole query (its sub-queries and parallel workers included, through the root {@link CommandContext}): the edge
 * records of the type, plus the outgoing lists of the vertices for a type that is stored lightweight (one vertex walk
 * serves every lightweight type a walk needs at once). A scan is sorted by target into primitive arrays, so each later
 * lookup is a binary search rather than another scan, and the heap it takes is charged to the query's budget like any
 * other in-heap buffer: a type too large to index in heap fails the query loudly rather than filling the heap.
 * <p>
 * Edges of a bidirectional type are read from the vertex as usual. An incoming pointer found on a vertex for a
 * unidirectional type - written by a bulk load that did not follow the type - is skipped, since the scan already
 * answers for that edge.
 * <p>
 * A scan is a snapshot of the edges visible when it is taken, and stays the one the query reads: the edges its own
 * transaction creates or deletes afterwards are added or left out from the {@link UnidirectionalEdgeChanges} the
 * transaction keeps, so a {@code MERGE} or a script that writes such edges and then reads them sees them without
 * paying for a new scan. A scan is taken again only when the transaction it was taken in ended since (a script that
 * commits statement by statement), because the changes it missed are no longer kept. Other transactions' changes are not
 * seen, as a query does not see them in the records it already read either.
 * <p>
 * An edge or a source vertex another transaction deletes after the scan is still in it: the Cypher expansions meet the
 * missing record as they meet a ghost edge-list entry and skip it through {@link GhostEdgeReporter}; elsewhere the
 * {@code RecordNotFoundException} fails the query, as a record deleted under any running read does. With an overlay
 * (the query's own writes since the scan), the edges of the target are materialized rather than streamed.
 * <p>
 * A scan reads the edge records of the type, which are the edges the source vertices list: an edge record that no
 * vertex lists (the leftover of a failed write, which {@code CHECK DATABASE} reports) would be answered here and not by
 * a walk from its source.
 * <p>
 * The heap a scan takes is charged to the query when it is taken and given back when the scan is replaced. Nothing ends
 * a query explicitly, so the last scans are given back with the query's {@code QueryHeapTracker}, when the query is no
 * longer reachable - which is also when their arrays stop taking heap.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IncomingEdgeLookup {
  private static final long       LIGHTWEIGHT_POSITION = -1L;
  // SCANS TAKEN SINCE THE JVM STARTED, ONE PER TYPE: WHAT A TEST READS TO TELL A KEPT SCAN FROM A REPEATED ONE
  private static final AtomicLong SCANS_TAKEN          = new AtomicLong();
  // WHETHER THE PER-VERTEX FALLBACK OF A DELETE WAS REPORTED AT WARNING ALREADY
  private static final AtomicBoolean FALLBACK_WARNED   = new AtomicBoolean();
  // THE LAST CLOSURE THE STATIC CHECKS COMPUTED ON THIS THREAD: STEPS ASK PER ROW FOR THE SAME TYPES
  private static final ThreadLocal<CachedClosure> LAST_STATIC_CLOSURE = new ThreadLocal<>();
  // HOW MANY PATTERN WALKS THE THREAD IS INSIDE: THE SQL GRAPH FUNCTIONS ANSWER THE INCOMING SIDE ONLY THERE
  private static final ThreadLocal<int[]> PATTERN_WALKS = ThreadLocal.withInitial(() -> new int[1]);

  // CREATED ON FIRST USE: A QUERY OVER A SCHEMA WITHOUT UNIDIRECTIONAL TYPES NEVER NEEDS IT
  private volatile Map<String, Snapshot> snapshots;
  // THE STEPS ASK ONCE PER ROW, MOSTLY FOR THE SAME TYPES: THE LAST ANSWER AND THE UNTYPED ONE ARE KEPT, UNTIL THE
  // TYPES CHANGE (A SCRIPT MAY CREATE OR DROP AN EDGE TYPE BETWEEN TWO STATEMENTS THAT SHARE THIS LOOKUP)
  private volatile ClosureEntry          lastClosure;
  private volatile Closure               untypedClosure;
  private volatile long                  memoizedAt = -1L;

  /**
   * The edges of {@code vertex} in {@code direction} over {@code edgeTypes} (all when empty), as
   * {@link Vertex#getEdges(Vertex.DIRECTION, String...)} answers them, completed with the incoming edges of the
   * unidirectional types among them.
   */
  public static Iterator<Edge> getEdges(final CommandContext context, final Vertex vertex, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    final Involved involved = involved(context, vertex, direction, edgeTypes);
    if (involved == null)
      return vertex.getEdges(direction, edgeTypes).iterator();

    final MultiIterator<Edge> result = new MultiIterator<>();
    if (direction == Vertex.DIRECTION.BOTH)
      result.addIterator(vertex.getEdges(Vertex.DIRECTION.OUT, edgeTypes).iterator());
    if (involved.closure.anyBidirectional)
      result.addIterator(new StoredIncomingEdges(vertex.getEdges(Vertex.DIRECTION.IN, edgeTypes).iterator(), involved.schema));
    for (final Snapshot snapshot : involved.snapshots)
      result.addIterator(snapshot.edgesInto(vertex.getIdentity()));
    return result;
  }

  /**
   * The vertices adjacent to {@code vertex} in {@code direction} over {@code edgeTypes}, as
   * {@link Vertex#getVertices(Vertex.DIRECTION, String...)} answers them, completed with the sources of the incoming
   * edges of the unidirectional types among them.
   */
  public static Iterator<Vertex> getVertices(final CommandContext context, final Vertex vertex,
      final Vertex.DIRECTION direction, final String... edgeTypes) {
    final Involved involved = involved(context, vertex, direction, edgeTypes);
    if (involved == null)
      return vertex.getVertices(direction, edgeTypes).iterator();

    final MultiIterator<Vertex> result = new MultiIterator<>();
    if (direction == Vertex.DIRECTION.BOTH)
      result.addIterator(vertex.getVertices(Vertex.DIRECTION.OUT, edgeTypes).iterator());
    if (involved.closure.anyBidirectional)
      result.addIterator(new SourceVertices(
          new StoredIncomingEdges(vertex.getEdges(Vertex.DIRECTION.IN, edgeTypes).iterator(), involved.schema)));
    for (final Snapshot snapshot : involved.snapshots)
      result.addIterator(snapshot.sourcesOf(vertex.getIdentity()));
    return result;
  }

  /**
   * The number of edges {@link #getEdges} answers.
   */
  public static long countEdges(final CommandContext context, final Vertex vertex, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    final Involved involved = involved(context, vertex, direction, edgeTypes);
    if (involved == null)
      return vertex.countEdges(direction, edgeTypes);

    long count = direction == Vertex.DIRECTION.BOTH ? vertex.countEdges(Vertex.DIRECTION.OUT, edgeTypes) : 0L;
    if (involved.closure.anyBidirectional)
      for (final Iterator<Edge> it = new StoredIncomingEdges(vertex.getEdges(Vertex.DIRECTION.IN, edgeTypes).iterator(),
          involved.schema); it.hasNext(); it.next())
        ++count;
    for (final Snapshot snapshot : involved.snapshots)
      count += snapshot.count(vertex.getIdentity());
    return count;
  }

  /**
   * The edges joining {@code vertex} to {@code target} in {@code direction} over {@code edgeTypes}, found from the end
   * that stores each of them: an incoming edge of {@code vertex} is an outgoing one of {@code target}, and the outgoing
   * side is written for every edge type. Complete for unidirectional types without the scan, and filtered on the
   * neighbour pointer the edge lists hold, so no edge that does not reach the other end is loaded. A self-loop is
   * answered once.
   */
  public static Iterator<Edge> getEdgesConnectedTo(final VertexInternal vertex, final Vertex.DIRECTION direction,
      final RID target, final String... edgeTypes) {
    final DatabaseInternal database = (DatabaseInternal) vertex.getDatabase();
    final GraphEngine graphEngine = database.getGraphEngine();
    if (direction == Vertex.DIRECTION.OUT)
      return graphEngine.getEdgesConnectedTo(vertex, Vertex.DIRECTION.OUT, target, edgeTypes);

    Iterator<Edge> incoming;
    try {
      final VertexInternal targetVertex = (VertexInternal) database.lookupByRID(target, true);
      incoming = graphEngine.getEdgesConnectedTo(targetVertex, Vertex.DIRECTION.OUT, vertex.getIdentity(), edgeTypes);
    } catch (final RecordNotFoundException e) {
      // THE OTHER END WAS DELETED: NO EDGE OF IT CAN REACH THIS VERTEX
      incoming = Collections.emptyIterator();
    }
    if (direction == Vertex.DIRECTION.IN || vertex.getIdentity().equals(target))
      return incoming;

    final MultiIterator<Edge> both = new MultiIterator<>();
    both.addIterator(graphEngine.getEdgesConnectedTo(vertex, Vertex.DIRECTION.OUT, target, edgeTypes));
    both.addIterator(incoming);
    return both;
  }

  /**
   * The edges of the unidirectional types that end in {@code target}, for the delete of that vertex (issue #8676): the
   * target holds no trace of them, so they are found here, as the edges of the incoming side a query reads. The
   * result is a list, since the caller deletes the edges while it walks it.
   * <p>
   * The deletes of a transaction share one scan per type, kept with the transaction's {@link UnidirectionalEdgeChanges}
   * (the edges the transaction deletes or creates in between are overlaid) and dropped when it ends, so deleting N
   * vertices in one transaction costs one scan rather than N. A type too large to index in heap, or a delete with no
   * transaction, is answered by a scan of its own for this vertex alone: slower, but on no heap.
   * <p>
   * The buckets the caller cannot read are left out, as in a query: a user who may delete the vertex but not read the
   * edges leaves those edges behind. An edge another transaction creates into the vertex after this transaction's scan
   * is not seen either, as the target holds no trace of it to conflict on.
   * <p>
   * A type with no edge record is skipped without a scan; a lightweight type has none, and is scanned through the
   * outgoing lists of every vertex, which is the price of finding an edge stored only on its source.
   */
  public static List<Edge> getIncomingUnidirectionalEdges(final DatabaseInternal database, final RID target) {
    final Schema schema = database.getSchema();
    if (!schema.hasUnidirectionalEdgeTypes())
      return Collections.emptyList();

    final List<String> types = new ArrayList<>(2);
    for (final String name : cachedClosure(schema, null).unidirectional)
      if (schema.getType(name) instanceof EdgeType edgeType && (edgeType.isLightweight() || database.countType(name, false) > 0))
        types.add(name);
    if (types.isEmpty())
      return Collections.emptyList();

    final String[] names = types.toArray(new String[0]);
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null) {
      try {
        final IncomingEdgeLookup lookup = tx.getUnidirectionalEdgeChanges().getDeleteLookup();
        final List<Edge> result = new ArrayList<>();
        for (final Snapshot snapshot : lookup.snapshots(database, names, null))
          for (final Iterator<Edge> it = snapshot.edgesInto(target); it.hasNext(); )
            result.add(it.next());
        return result;
      } catch (final HeapLimitExceededException e) {
        // THE TYPE IS TOO LARGE TO INDEX IN HEAP: SCANNED FOR THIS VERTEX ALONE BELOW. SAID ONCE AT WARNING, AS EVERY
        // DELETE OF THE TRANSACTION (AND OF THE NEXT ONES) PAYS FOR A FULL SCAN OF THE TYPES
        LogManager.instance().log(IncomingEdgeLookup.class, FALLBACK_WARNED.compareAndSet(false, true) ? Level.WARNING : Level.FINE,
            "Cannot index the unidirectional edge types in heap to delete vertex %s, scanning them for it alone (every vertex "
                + "delete pays a full scan of them; raise %s to index them): %s", target,
            GlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey(), e.getMessage());
      }
    }
    return Snapshot.scanEdgesInto(database, names, target);
  }

  /**
   * Whether {@link #getEdges}, {@link #getVertices} and {@link #countEdges} answer differently from the vertex API for
   * this walk, i.e. whether it reaches the incoming side of a unidirectional edge type within a query. Cheap: the scan
   * is not built by asking.
   */
  public static boolean isNeeded(final CommandContext context, final Database database, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    if (direction == Vertex.DIRECTION.OUT || context == null)
      return false;
    final IncomingEdgeLookup lookup = context.getIncomingEdgeLookup();
    if (lookup == null)
      return false;
    final Schema schema = database.getSchema();
    if (!schema.hasUnidirectionalEdgeTypes())
      return false;
    lookup.refreshMemos(schema);
    return lookup.closureOf(schema, edgeTypes).unidirectional.length > 0;
  }

  /**
   * Whether walking {@code direction} over {@code edgeTypes} (all when empty) reaches edges no vertex stores on its
   * incoming side, i.e. whether a walk from the target end needs this lookup to be complete. Planners use it to prefer
   * the source end of such a hop.
   */
  public static boolean isIncomingSideMissing(final Schema schema, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    return direction != Vertex.DIRECTION.OUT && schema.hasUnidirectionalEdgeTypes()
        && cachedClosure(schema, edgeTypes).unidirectional.length > 0;
  }

  /**
   * Whether any of {@code edgeTypes} (all when empty), or one of their subtypes, is declared unidirectional.
   */
  public static boolean isAnyUnidirectional(final Schema schema, final String... edgeTypes) {
    return schema.hasUnidirectionalEdgeTypes() && cachedClosure(schema, edgeTypes).unidirectional.length > 0;
  }

  /**
   * Runs {@code walk} as the evaluation of a pattern (on the calling thread: the SQL MATCH traversers and Cypher's
   * shortestPath() run the SQL graph functions there, never on a parallel worker): the SQL graph functions it calls ({@code in()}, {@code inE()},
   * {@code both()}, {@code bothE()}, {@code shortestPath()}) answer the incoming side of the unidirectional types.
   * Called on their own, those functions read what the vertices store, as the vertex API does - embedded, remote and
   * through Gremlin alike: a SQL {@code MATCH} or a Cypher pattern asks which edges end in a vertex, a function call
   * asks what the vertex holds. Only the hops of a {@code MATCH} are pattern walks: an {@code in()} written inside a
   * {@code where:} or {@code while:} condition is an expression, and answers as the function does anywhere else. A
   * {@code GraphTraversalProvider} (an analytical view) answers the incoming side in both cases, its reverse index being
   * built from the outgoing lists.
   */
  public static <T> T walkingPattern(final Supplier<T> walk) {
    final int[] depth = PATTERN_WALKS.get();
    ++depth[0];
    try {
      return walk.get();
    } finally {
      --depth[0];
    }
  }

  /** Whether the thread is evaluating a pattern (see {@link #walkingPattern}). */
  public static boolean isWalkingPattern() {
    return PATTERN_WALKS.get()[0] > 0;
  }

  /** The scans taken since the JVM started. */
  static long getScansTaken() {
    return SCANS_TAKEN.get();
  }

  private static Involved involved(final CommandContext context, final Vertex vertex, final Vertex.DIRECTION direction,
      final String[] edgeTypes) {
    if (direction == Vertex.DIRECTION.OUT || context == null)
      return null;
    final IncomingEdgeLookup lookup = context.getIncomingEdgeLookup();
    if (lookup == null)
      return null;

    final Database database = vertex.getDatabase();
    final Schema schema = database.getSchema();
    if (!schema.hasUnidirectionalEdgeTypes())
      return null;
    lookup.refreshMemos(schema);

    final Closure closure = lookup.closureOf(schema, edgeTypes);
    if (closure.unidirectional.length == 0)
      return null;

    return new Involved(schema, closure, lookup.snapshots((DatabaseInternal) database, closure.unidirectional, context));
  }

  private void refreshMemos(final Schema schema) {
    final LocalSchema embedded = schema.getEmbedded();
    final long serial = embedded != null ? embedded.getTypesChangeSerial() : -1L;
    if (serial != memoizedAt || serial < 0) {
      untypedClosure = null;
      lastClosure = null;
      memoizedAt = serial;
    }
  }

  private Closure closureOf(final Schema schema, final String[] edgeTypes) {
    if (edgeTypes == null || edgeTypes.length == 0) {
      Closure closure = untypedClosure;
      if (closure == null) {
        closure = closure(schema, null);
        untypedClosure = closure;
      }
      return closure;
    }
    final ClosureEntry last = lastClosure;
    if (last != null && Arrays.equals(last.edgeTypes, edgeTypes))
      return last.closure;
    final Closure closure = closure(schema, edgeTypes);
    lastClosure = new ClosureEntry(edgeTypes.clone(), closure);
    return closure;
  }

  /**
   * The scans of {@code unidirectionalTypes}, taking the ones this query has not taken yet, or took in a transaction
   * that has ended since (see {@link UnidirectionalEdgeChanges}). The common case - every scan present and current -
   * reads a concurrent map and takes no lock; the missing ones are taken under the lock, so parallel workers wait for
   * one scan rather than repeat it - a worker that needs a scan already taken also waits while another type is being
   * scanned, which is the price of never scanning a type twice.
   */
  private Snapshot[] snapshots(final DatabaseInternal database, final String[] unidirectionalTypes,
      final CommandContext context) {
    final TransactionContext tx = database.getTransactionIfExists();
    final UnidirectionalEdgeChanges changes = tx != null ? tx.getUnidirectionalEdgeChangesIfAny() : null;
    final Snapshot[] result = new Snapshot[unidirectionalTypes.length];
    Map<String, Snapshot> map = snapshots;
    if (map != null) {
      boolean allCurrent = true;
      for (int i = 0; i < unidirectionalTypes.length && allCurrent; i++) {
        final Snapshot snapshot = map.get(unidirectionalTypes[i]);
        if (snapshot == null || snapshot.isStale(changes, tx))
          allCurrent = false;
        else
          result[i] = snapshot;
      }
      if (allCurrent)
        return result;
    }

    synchronized (this) {
      map = snapshots;
      if (map == null) {
        map = new ConcurrentHashMap<>();
        snapshots = map;
      }
      final List<String> missing = new ArrayList<>(unidirectionalTypes.length);
      for (final String typeName : unidirectionalTypes) {
        final Snapshot snapshot = map.get(typeName);
        if (snapshot == null || snapshot.isStale(changes, tx))
          missing.add(typeName);
      }
      if (!missing.isEmpty()) {
        final UnidirectionalEdgeChanges builtBy = tx != null ? tx.getUnidirectionalEdgeChanges() : null;
        if (builtBy != null)
          builtBy.scanTaken();
        for (final Snapshot snapshot : Snapshot.build(database, missing, builtBy, context)) {
          final Snapshot previous = map.put(snapshot.typeName, snapshot);
          if (previous != null)
            previous.limit.release();
        }
      }
      for (int i = 0; i < unidirectionalTypes.length; i++)
        result[i] = map.get(unidirectionalTypes[i]);
    }
    return result;
  }

  /**
   * The unidirectional edge types a walk over {@code edgeTypes} reaches (sorted, so equal sets share a snapshot), and
   * whether it also reaches a bidirectional one. A type reaches its subtypes, as the vertex API walks them
   * polymorphically; no type means every edge type.
   */
  private static Closure closure(final Schema schema, final String[] edgeTypes) {
    final List<String> unidirectional = new ArrayList<>(2);
    boolean anyBidirectional = false;
    if (edgeTypes == null || edgeTypes.length == 0) {
      for (final DocumentType type : schema.getTypes())
        if (type instanceof EdgeType edgeType) {
          if (edgeType.isBidirectional())
            anyBidirectional = true;
          else
            unidirectional.add(type.getName());
        }
    } else {
      final Set<String> visited = new HashSet<>();
      for (final String name : edgeTypes) {
        if (name == null)
          continue;
        final DocumentType type = schema.getTypeOrNull(name);
        if (type instanceof EdgeType)
          anyBidirectional |= collect(type, visited, unidirectional);
      }
    }

    if (unidirectional.isEmpty())
      return anyBidirectional ? Closure.BIDIRECTIONAL_ONLY : Closure.NONE;
    final String[] sorted = unidirectional.toArray(new String[0]);
    Arrays.sort(sorted);
    return new Closure(sorted, anyBidirectional);
  }

  private static boolean collect(final DocumentType type, final Set<String> visited, final List<String> unidirectional) {
    if (!visited.add(type.getName()))
      return false;
    boolean anyBidirectional = false;
    if (type instanceof EdgeType edgeType && !edgeType.isBidirectional())
      unidirectional.add(type.getName());
    else
      anyBidirectional = true;
    for (final DocumentType subType : type.getSubTypes())
      anyBidirectional |= collect(subType, visited, unidirectional);
    return anyBidirectional;
  }

  private record Closure(String[] unidirectional, boolean anyBidirectional) {
    static final Closure NONE               = new Closure(new String[0], false);
    static final Closure BIDIRECTIONAL_ONLY = new Closure(new String[0], true);
  }

  private record ClosureEntry(String[] edgeTypes, Closure closure) {
  }

  // THE SCHEMA IS HELD WEAKLY: A POOLED THREAD MUST NOT KEEP A CLOSED DATABASE REACHABLE
  private record CachedClosure(WeakReference<LocalSchema> schema, long serial, String[] edgeTypes, Closure closure) {
  }

  /**
   * {@link #closure} memoized per thread for the static checks, which steps call per row: kept while the schema, its
   * types and the requested types stay the same, so a row costs a comparison rather than a walk of the types.
   */
  private static Closure cachedClosure(final Schema schema, final String[] edgeTypes) {
    final LocalSchema embedded = schema.getEmbedded();
    if (embedded == null)
      return closure(schema, edgeTypes);
    final long serial = embedded.getTypesChangeSerial();
    final CachedClosure last = LAST_STATIC_CLOSURE.get();
    if (last != null && last.schema.get() == embedded && last.serial == serial && Arrays.equals(last.edgeTypes, edgeTypes))
      return last.closure;
    final Closure closure = closure(schema, edgeTypes);
    LAST_STATIC_CLOSURE.set(
        new CachedClosure(new WeakReference<>(embedded), serial, edgeTypes == null ? null : edgeTypes.clone(), closure));
    return closure;
  }

  private record Involved(Schema schema, Closure closure, Snapshot[] snapshots) {
  }

  /**
   * The edges of one unidirectional type, sorted by target: six parallel primitive arrays, the endpoints and the edge
   * as bucket/position pairs, so the scan holds no object per edge. A lightweight edge has no record, and is kept as its
   * type's bucket with {@link #LIGHTWEIGHT_POSITION}. The arrays are the builder's own, sorted in place and kept with
   * their growth slack (charged to the query as held), so building one never holds a second copy.
   */
  private static final class Snapshot {
    private final String                    typeName;
    // THE CHANGES OF THE TRANSACTION THE SCAN WAS TAKEN IN, AND HOW FAR THEY WENT THEN: LATER ONES ARE OVERLAID
    private final UnidirectionalEdgeChanges builtBy;
    private final long                      builtAt;
    private final long                      builtIn;
    private final long                      builtOverflow;
    private final OperationHeapLimit        limit;
    private final DatabaseInternal          database;
    private final int                       size;
    private final int[]                     targetBuckets;
    private final long[]                    targetPositions;
    private final int[]                     sourceBuckets;
    private final long[]                    sourcePositions;
    private final int[]                     edgeBuckets;
    private final long[]                    edgePositions;

    private Snapshot(final String typeName, final UnidirectionalEdgeChanges builtBy, final DatabaseInternal database,
        final Builder builder) {
      builder.sortByTarget();
      this.typeName = typeName;
      this.builtBy = builtBy;
      this.builtAt = builtBy != null ? builtBy.getSequence() : 0L;
      this.builtIn = builtBy != null ? builtBy.getTransaction() : 0L;
      this.builtOverflow = builtBy != null ? builtBy.getOverflows() : 0L;
      this.database = database;
      this.limit = builder.limit;
      this.size = builder.size;
      this.targetBuckets = builder.targetBuckets;
      this.targetPositions = builder.targetPositions;
      this.sourceBuckets = builder.sourceBuckets;
      this.sourcePositions = builder.sourcePositions;
      this.edgeBuckets = builder.edgeBuckets;
      this.edgePositions = builder.edgePositions;
    }

    static List<Snapshot> build(final DatabaseInternal database, final List<String> typeNames,
        final UnidirectionalEdgeChanges builtBy, final CommandContext context) {
      SCANS_TAKEN.addAndGet(typeNames.size());
      final Schema schema = database.getSchema();
      final Map<String, Builder> builders = new HashMap<>();
      final List<Builder> lightweight = new ArrayList<>();
      try {
        return build(database, typeNames, builtBy, context, schema, builders, lightweight);
      } catch (final RuntimeException | Error e) {
        // A SCAN THAT FAILED (THE HEAP CAP, AN UNREADABLE RECORD) GIVES BACK WHAT ITS BUILDERS CHARGED AT ONCE
        for (final Builder builder : builders.values())
          builder.limit.release();
        throw e;
      }
    }

    private static List<Snapshot> build(final DatabaseInternal database, final List<String> typeNames,
        final UnidirectionalEdgeChanges builtBy, final CommandContext context, final Schema schema,
        final Map<String, Builder> builders, final List<Builder> lightweight) {
      final WorkGuard guard = WorkGuard.forCommandDeadline(context);
      for (final String typeName : typeNames) {
        final Builder builder = new Builder(OperationHeapLimit.of(context, "edges",
            "incoming-edge lookup over the unidirectional edge type " + typeName));
        builders.put(typeName, builder);

        // OWN BUCKETS ONLY: THE SUBTYPES ARE IN THE SET ON THEIR OWN. SCANNED RATHER THAN ITERATED: THE ITERATORS APPLY
        // THE USER'S resultSetLimit, AND A SCAN CUT SHORT WOULD ANSWER PART OF THE EDGES WITH NO ERROR. A BUCKET THE
        // CALLER CANNOT READ IS LEFT OUT, AS IN THE LIGHTWEIGHT WALK BELOW
        for (final Bucket bucket : schema.getType(typeName).getBuckets(false))
          if (SecurityHelper.canAccessFile(database, bucket.getFileId(), SecurityDatabaseUser.ACCESS.READ_RECORD))
            scan(database, bucket, guard, record -> {
              final Edge edge = record.asEdge();
              builder.add(edge.getIn(), edge.getOut(), edge.getIdentity().getBucketId(), edge.getIdentity().getPosition());
              return true;
            });
        if (schema.getType(typeName) instanceof EdgeType edgeType && edgeType.isLightweight())
          lightweight.add(builder);
      }

      if (!lightweight.isEmpty())
        addLightweightEdges(database, builders, lightweight.size(), guard);

      final List<Snapshot> result = new ArrayList<>(typeNames.size());
      for (final String typeName : typeNames) {
        final Builder builder = builders.get(typeName);
        // A query that pays for a scan says so: it is the cost a slow query over such a type is made of
        LogManager.instance().log(IncomingEdgeLookup.class, Level.FINE,
            "Scanned %d edges of the unidirectional edge type '%s' to answer its incoming side", builder.size, typeName);
        result.add(new Snapshot(typeName, builtBy, database, builder));
      }
      return result;
    }

    /**
     * A lightweight edge lives only in the lists of its two vertices, and for a unidirectional type only in the
     * outgoing one: the vertices are walked for it, as {@code SELECT FROM} does for such a type (issue #7477), once for
     * all the lightweight types being scanned. A vertex bucket the caller cannot read is left out, like the edges it
     * holds.
     */
    private static void addLightweightEdges(final DatabaseInternal database, final Map<String, Builder> builders,
        final int lightweightTypes, final WorkGuard guard) {
      final Schema schema = database.getSchema();
      final String[] names = new String[lightweightTypes];
      int n = 0;
      for (final Map.Entry<String, Builder> entry : builders.entrySet())
        if (schema.getType(entry.getKey()) instanceof EdgeType edgeType && edgeType.isLightweight())
          names[n++] = entry.getKey();

      for (final DocumentType type : schema.getTypes()) {
        if (type.getType() != Vertex.RECORD_TYPE)
          continue;
        for (final Bucket bucket : type.getBuckets(false)) {
          if (!SecurityHelper.canAccessFile(database, bucket.getFileId(), SecurityDatabaseUser.ACCESS.READ_RECORD))
            continue;
          scan(database, bucket, guard, record -> {
            final Vertex vertex = record.asVertex();
            for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT, names)) {
              final RID identity = edge.getIdentity();
              // A RECORD-BACKED EDGE CAME FROM THE TYPE SCAN ALREADY; A TYPE NOT BEING SCANNED IS NOT ADDED
              if (identity.getPosition() >= 0)
                continue;
              final Builder builder = builders.get(edge.getTypeName());
              if (builder != null)
                builder.add(edge.getIn(), vertex.getIdentity(), identity.getBucketId(), LIGHTWEIGHT_POSITION);
            }
            return true;
          });
        }
      }
    }

    /** The edges of {@code typeNames} that end in {@code target}, found by scanning them with no index kept in heap. */
    static List<Edge> scanEdgesInto(final DatabaseInternal database, final String[] typeNames, final RID target) {
      final Schema schema = database.getSchema();
      final WorkGuard guard = WorkGuard.forCommandDeadline(null);
      final List<Edge> result = new ArrayList<>();
      final List<String> lightweight = new ArrayList<>();
      for (final String typeName : typeNames) {
        for (final Bucket bucket : schema.getType(typeName).getBuckets(false))
          if (SecurityHelper.canAccessFile(database, bucket.getFileId(), SecurityDatabaseUser.ACCESS.READ_RECORD))
            scan(database, bucket, guard, record -> {
              final Edge edge = record.asEdge();
              if (target.equals(edge.getIn()))
                result.add(edge);
              return true;
            });
        if (schema.getType(typeName) instanceof EdgeType edgeType && edgeType.isLightweight())
          lightweight.add(typeName);
      }
      if (lightweight.isEmpty())
        return result;

      final String[] names = lightweight.toArray(new String[0]);
      for (final DocumentType type : schema.getTypes()) {
        if (type.getType() != Vertex.RECORD_TYPE)
          continue;
        for (final Bucket bucket : type.getBuckets(false))
          if (SecurityHelper.canAccessFile(database, bucket.getFileId(), SecurityDatabaseUser.ACCESS.READ_RECORD))
            scan(database, bucket, guard, record -> {
              for (final Edge edge : record.asVertex().getEdges(Vertex.DIRECTION.OUT, names))
                if (edge.getIdentity().getPosition() < 0 && target.equals(edge.getIn()))
                  result.add(edge);
              return true;
            });
      }
      return result;
    }

    /**
     * Whether the transaction the scan was taken in ended since, so the changes it made after the scan are no longer
     * kept for the overlay. A scan read from another transaction's thread (a parallel worker) is read as taken. A scan
     * taken with no transaction on the thread has no changes to overlay, and is taken again once the thread has one:
     * other threads' writes never make it stale.
     */
    boolean isStale(final UnidirectionalEdgeChanges current, final TransactionContext tx) {
      if (builtBy == null)
        // TAKEN WITH NO TRANSACTION ON THE THREAD: ONLY THIS THREAD STARTING ONE (TO WRITE) CAN MAKE IT MISS AN EDGE
        return tx != null;
      return current == builtBy && (builtIn != builtBy.getTransaction() || builtOverflow != builtBy.getOverflows());
    }

    /** The changes to overlay: the current transaction's, when it is the one the scan was taken in and changed since. */
    private UnidirectionalEdgeChanges overlay() {
      if (builtBy == null || builtBy.getSequence() == builtAt)
        return null;
      final TransactionContext tx = database.getTransactionIfExists();
      return tx != null && tx.getUnidirectionalEdgeChangesIfAny() == builtBy ? builtBy : null;
    }

    /**
     * Scans a bucket, failing on the first record that cannot be read or indexed: the bucket scan logs such a record and
     * goes on, which here would leave a partial scan answering with no error - the heap cap included.
     */
    private static void scan(final DatabaseInternal database, final Bucket bucket, final WorkGuard guard,
        final RecordCallback callback) {
      final Throwable[] failure = new Throwable[1];
      final int[] records = new int[1];
      // THE SCAN IS PART OF THE QUERY: ITS DEADLINE (TIMEOUT, COMMAND TIMEOUT) APPLIES WHILE IT RUNS
      database.scanBucket(bucket.getName(), record -> {
        guard.checkPeriodically(++records[0]);
        return callback.onRecord(record);
      }, (rid, e) -> {
        failure[0] = e;
        return false;
      });
      if (failure[0] instanceof RuntimeException e)
        throw e;
      if (failure[0] != null)
        throw new DatabaseOperationException("Cannot scan bucket '" + bucket.getName() + "'", failure[0]);
    }

    long count(final RID target) {
      final UnidirectionalEdgeChanges changes = overlay();
      if (changes == null) {
        final int from = firstIndex(target);
        int to = from;
        while (to < size && isTarget(to, target))
          ++to;
        return to - from;
      }
      // COUNTED ON THE ARRAYS AND THE CHANGES ALONE: NO EDGE IS MATERIALIZED
      long count = 0;
      final boolean deletions = changes.hasDeletions();
      for (int i = firstIndex(target); i < size && isTarget(i, target); i++)
        if (!deletions || !changes.isDeletedAfter(edgeIdentityAt(i), builtAt))
          ++count;
      for (final UnidirectionalEdgeChanges.Created created : changes.createdInto(typeName, target))
        if (isLive(created, changes))
          ++count;
      return count;
    }

    Iterator<Edge> edgesInto(final RID target) {
      final UnidirectionalEdgeChanges changes = overlay();
      return changes == null ? storedEdgesInto(target) : edgesInto(target, changes);
    }

    Iterator<Vertex> sourcesOf(final RID target) {
      final UnidirectionalEdgeChanges changes = overlay();
      if (changes != null)
        return new SourceVertices(edgesInto(target, changes));

      final int from = firstIndex(target);
      if (from >= size || !isTarget(from, target))
        return Collections.emptyIterator();
      return new RangeIterator<>(from, target) {
        @Override
        Vertex get(final int i) {
          return (Vertex) database.lookupByRID(database.newRID(sourceBuckets[i], sourcePositions[i]), false);
        }
      };
    }

    /** The scanned edges into {@code target}, less the ones the transaction deleted since, plus the ones it created. */
    private Iterator<Edge> edgesInto(final RID target, final UnidirectionalEdgeChanges changes) {
      // STREAMED: THE SCANNED EDGES FIRST, A DELETED ONE TOLD BY THE IDENTITY THE ARRAYS HOLD, THEN THE CREATED ONES
      final boolean deletions = changes.hasDeletions();
      final Iterator<UnidirectionalEdgeChanges.Created> created = changes.createdInto(typeName, target).iterator();
      return new Iterator<>() {
        private int  index = firstIndex(target);
        private Edge next;

        @Override
        public boolean hasNext() {
          while (next == null) {
            if (index < size && isTarget(index, target)) {
              final int i = index++;
              if (!deletions || !changes.isDeletedAfter(edgeIdentityAt(i), builtAt))
                next = edgeAt(i);
            } else if (created.hasNext()) {
              final UnidirectionalEdgeChanges.Created entry = created.next();
              if (isLive(entry, changes))
                next = entry.edge();
            } else
              return false;
          }
          return true;
        }

        @Override
        public Edge next() {
          if (!hasNext())
            throw new NoSuchElementException();
          final Edge edge = next;
          next = null;
          return edge;
        }
      };
    }

    /** Whether an edge the transaction created is still there: created after the scan and not deleted since. */
    private boolean isLive(final UnidirectionalEdgeChanges.Created created, final UnidirectionalEdgeChanges changes) {
      return created.sequence() > builtAt && !changes.isDeletedAfter(created.edge().getIdentity(), created.sequence());
    }

    /** The identity of the edge at {@code i}, as the vertex API gives it, built from the arrays with no record load. */
    private RID edgeIdentityAt(final int i) {
      if (edgePositions[i] == LIGHTWEIGHT_POSITION)
        return new LightEdgeRID(database, edgeBuckets[i], database.newRID(sourceBuckets[i], sourcePositions[i]),
            database.newRID(targetBuckets[i], targetPositions[i]));
      return database.newRID(edgeBuckets[i], edgePositions[i]);
    }

    private Iterator<Edge> storedEdgesInto(final RID target) {
      final int from = firstIndex(target);
      if (from >= size || !isTarget(from, target))
        return Collections.emptyIterator();
      return new RangeIterator<>(from, target) {
        @Override
        Edge get(final int i) {
          return edgeAt(i);
        }
      };
    }

    private Edge edgeAt(final int i) {
      final RID source = database.newRID(sourceBuckets[i], sourcePositions[i]);
      final RID target = database.newRID(targetBuckets[i], targetPositions[i]);
      if (edgePositions[i] == LIGHTWEIGHT_POSITION)
        return new ImmutableLightEdge(database, database.getSchema().getTypeByBucketId(edgeBuckets[i]), edgeBuckets[i], source,
            target);

      final Edge edge = (Edge) database.lookupByRID(database.newRID(edgeBuckets[i], edgePositions[i]), false);
      if (edge instanceof ImmutableEdge immutable)
        immutable.setEndpointsFromEdgeList(source, target);
      return edge;
    }

    private boolean isTarget(final int i, final RID target) {
      return targetBuckets[i] == target.getBucketId() && targetPositions[i] == target.getPosition();
    }

    /** The first index whose target is not below {@code target}. */
    private int firstIndex(final RID target) {
      final int bucket = target.getBucketId();
      final long position = target.getPosition();
      int low = 0;
      int high = size;
      while (low < high) {
        final int mid = (low + high) >>> 1;
        final int cmp = targetBuckets[mid] != bucket ? Integer.compare(targetBuckets[mid], bucket) :
            Long.compare(targetPositions[mid], position);
        if (cmp < 0)
          low = mid + 1;
        else
          high = mid;
      }
      return low;
    }

    private abstract class RangeIterator<T> implements Iterator<T> {
      private final RID target;
      private       int next;

      RangeIterator(final int from, final RID target) {
        this.next = from;
        this.target = target;
      }

      abstract T get(int i);

      @Override
      public boolean hasNext() {
        return next < size && isTarget(next, target);
      }

      @Override
      public T next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return get(next++);
      }
    }
  }

  /**
   * Grows the scan in parallel primitive arrays. The query's heap budget is charged for the capacity the arrays take,
   * slack included, as it grows, so a type too large to hold fails before it is held.
   */
  private static final class Builder {
    private static final int BYTES_PER_EDGE = 3 * (Integer.BYTES + Long.BYTES);

    private final OperationHeapLimit limit;
    private       int                size;
    private       int[]              targetBuckets   = new int[64];
    private       long[]             targetPositions = new long[64];
    private       int[]              sourceBuckets   = new int[64];
    private       long[]             sourcePositions = new long[64];
    private       int[]              edgeBuckets     = new int[64];
    private       long[]             edgePositions   = new long[64];

    Builder(final OperationHeapLimit limit) {
      this.limit = limit;
      limit.charge((long) targetBuckets.length * BYTES_PER_EDGE);
    }

    void add(final RID target, final RID source, final int edgeBucket, final long edgePosition) {
      limit.check(size + 1L);
      if (size == targetBuckets.length) {
        final int capacity = size + (size >> 1);
        limit.charge((long) (capacity - size) * BYTES_PER_EDGE);
        targetBuckets = Arrays.copyOf(targetBuckets, capacity);
        targetPositions = Arrays.copyOf(targetPositions, capacity);
        sourceBuckets = Arrays.copyOf(sourceBuckets, capacity);
        sourcePositions = Arrays.copyOf(sourcePositions, capacity);
        edgeBuckets = Arrays.copyOf(edgeBuckets, capacity);
        edgePositions = Arrays.copyOf(edgePositions, capacity);
      }
      targetBuckets[size] = target.getBucketId();
      targetPositions[size] = target.getPosition();
      sourceBuckets[size] = source.getBucketId();
      sourcePositions[size] = source.getPosition();
      edgeBuckets[size] = edgeBucket;
      edgePositions[size] = edgePosition;
      ++size;
    }

    /**
     * Sorts the six arrays by target, in place: an introsort - a quicksort recursing on the smaller side only, that
     * falls back to a heapsort on a range it has partitioned too many times, so no order of the targets makes it
     * quadratic.
     */
    void sortByTarget() {
      sort(0, size - 1, 2 * (32 - Integer.numberOfLeadingZeros(Math.max(size, 1))));
    }

    private void sort(int low, int high, int depth) {
      while (high - low > 16) {
        if (depth-- == 0) {
          heapSort(low, high);
          return;
        }
        final int mid = medianOfThree(low, (low + high) >>> 1, high);
        final int pivotBucket = targetBuckets[mid];
        final long pivotPosition = targetPositions[mid];
        final int pivotEdgeBucket = edgeBuckets[mid];
        final long pivotEdgePosition = edgePositions[mid];
        final int pivotSourceBucket = sourceBuckets[mid];
        final long pivotSourcePosition = sourcePositions[mid];
        int i = low;
        int j = high;
        while (i <= j) {
          while (compareTo(i, pivotBucket, pivotPosition, pivotEdgeBucket, pivotEdgePosition, pivotSourceBucket,
              pivotSourcePosition) < 0)
            ++i;
          while (compareTo(j, pivotBucket, pivotPosition, pivotEdgeBucket, pivotEdgePosition, pivotSourceBucket,
              pivotSourcePosition) > 0)
            --j;
          if (i <= j)
            swap(i++, j--);
        }
        if (j - low < high - i) {
          sort(low, j, depth);
          low = i;
        } else {
          sort(i, high, depth);
          high = j;
        }
      }
      for (int i = low + 1; i <= high; i++)
        for (int j = i; j > low && compare(j - 1, j) > 0; j--)
          swap(j - 1, j);
    }

    private void heapSort(final int low, final int high) {
      final int n = high - low + 1;
      for (int i = n / 2 - 1; i >= 0; i--)
        siftDown(low, i, n);
      for (int end = n - 1; end > 0; end--) {
        swap(low, low + end);
        siftDown(low, 0, end);
      }
    }

    private void siftDown(final int base, int node, final int n) {
      while (true) {
        int largest = node;
        final int left = 2 * node + 1;
        final int right = left + 1;
        if (left < n && compare(base + left, base + largest) > 0)
          largest = left;
        if (right < n && compare(base + right, base + largest) > 0)
          largest = right;
        if (largest == node)
          return;
        swap(base + node, base + largest);
        node = largest;
      }
    }

    private int medianOfThree(final int a, final int b, final int c) {
      if (compare(a, b) < 0) {
        if (compare(b, c) < 0)
          return b;
        return compare(a, c) < 0 ? c : a;
      }
      if (compare(a, c) < 0)
        return a;
      return compare(b, c) < 0 ? c : b;
    }

    private int compare(final int a, final int b) {
      return compareTo(a, targetBuckets[b], targetPositions[b], edgeBuckets[b], edgePositions[b], sourceBuckets[b],
          sourcePositions[b]);
    }

    /**
     * By target, then by edge and source: the edges of one target come back in a reproducible order, whatever order the
     * scan met them in (a lightweight edge has no position of its own, so its source tells it apart).
     */
    private int compareTo(final int i, final int bucket, final long position, final int edgeBucket, final long edgePosition,
        final int sourceBucket, final long sourcePosition) {
      if (targetBuckets[i] != bucket)
        return Integer.compare(targetBuckets[i], bucket);
      if (targetPositions[i] != position)
        return Long.compare(targetPositions[i], position);
      if (edgeBuckets[i] != edgeBucket)
        return Integer.compare(edgeBuckets[i], edgeBucket);
      if (edgePositions[i] != edgePosition)
        return Long.compare(edgePositions[i], edgePosition);
      if (sourceBuckets[i] != sourceBucket)
        return Integer.compare(sourceBuckets[i], sourceBucket);
      return Long.compare(sourcePositions[i], sourcePosition);
    }

    private void swap(final int a, final int b) {
      final int tb = targetBuckets[a];
      targetBuckets[a] = targetBuckets[b];
      targetBuckets[b] = tb;
      final long tp = targetPositions[a];
      targetPositions[a] = targetPositions[b];
      targetPositions[b] = tp;
      final int sb = sourceBuckets[a];
      sourceBuckets[a] = sourceBuckets[b];
      sourceBuckets[b] = sb;
      final long sp = sourcePositions[a];
      sourcePositions[a] = sourcePositions[b];
      sourcePositions[b] = sp;
      final int eb = edgeBuckets[a];
      edgeBuckets[a] = edgeBuckets[b];
      edgeBuckets[b] = eb;
      final long ep = edgePositions[a];
      edgePositions[a] = edgePositions[b];
      edgePositions[b] = ep;
    }
  }

  /** The incoming edges a vertex stores, less the ones of a unidirectional type, which the snapshot answers for. */
  private static final class StoredIncomingEdges implements Iterator<Edge> {
    private final Iterator<Edge> stored;
    private final Schema         schema;
    private       Edge           next;

    StoredIncomingEdges(final Iterator<Edge> stored, final Schema schema) {
      this.stored = stored;
      this.schema = schema;
    }

    @Override
    public boolean hasNext() {
      while (next == null && stored.hasNext()) {
        final Edge edge = stored.next();
        if (!(schema.getTypeByBucketId(edge.getIdentity().getBucketId()) instanceof EdgeType type) || type.isBidirectional())
          next = edge;
      }
      return next != null;
    }

    @Override
    public Edge next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final Edge edge = next;
      next = null;
      return edge;
    }
  }

  private static final class SourceVertices implements Iterator<Vertex> {
    private final Iterator<Edge> edges;

    SourceVertices(final Iterator<Edge> edges) {
      this.edges = edges;
    }

    @Override
    public boolean hasNext() {
      return edges.hasNext();
    }

    @Override
    public Vertex next() {
      return edges.next().getOutVertex();
    }
  }
}
