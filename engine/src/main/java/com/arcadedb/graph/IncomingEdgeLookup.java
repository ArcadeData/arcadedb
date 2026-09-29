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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
import com.arcadedb.utility.MultiIterator;

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
 * A scan is a snapshot of the edges visible when it is taken. It is taken again when an edge of a unidirectional type
 * was created or deleted since, in any transaction ({@link GraphEngine#getUnidirectionalEdgeChanges()}), so a script
 * that writes such edges and then reads them, or a query that deletes some, sees what the vertex lists hold now.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IncomingEdgeLookup {
  private static final long LIGHTWEIGHT_POSITION = -1L;

  // CREATED ON FIRST USE: A QUERY OVER A SCHEMA WITHOUT UNIDIRECTIONAL TYPES NEVER NEEDS IT
  private volatile Map<String, Snapshot> snapshots;
  private volatile Boolean               anyUnidirectionalType;
  // THE STEPS ASK ONCE PER ROW, MOSTLY FOR THE SAME TYPES: THE LAST ANSWER AND THE UNTYPED ONE ARE KEPT, UNTIL THE
  // DATABASE CHANGES (A SCRIPT MAY CREATE OR DROP AN EDGE TYPE BETWEEN TWO STATEMENTS THAT SHARE THIS LOOKUP)
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

    final VertexInternal targetVertex = (VertexInternal) database.lookupByRID(target, false);
    final Iterator<Edge> incoming = graphEngine.getEdgesConnectedTo(targetVertex, Vertex.DIRECTION.OUT,
        vertex.getIdentity(), edgeTypes);
    if (direction == Vertex.DIRECTION.IN || vertex.getIdentity().equals(target))
      return incoming;

    final MultiIterator<Edge> both = new MultiIterator<>();
    both.addIterator(graphEngine.getEdgesConnectedTo(vertex, Vertex.DIRECTION.OUT, target, edgeTypes));
    both.addIterator(incoming);
    return both;
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
    lookup.refreshMemos(database);
    return lookup.hasAnyUnidirectionalType(database.getSchema())
        && lookup.closureOf(database.getSchema(), edgeTypes).unidirectional.length > 0;
  }

  /**
   * Whether walking {@code direction} over {@code edgeTypes} (all when empty) reaches edges no vertex stores on its
   * incoming side, i.e. whether a walk from the target end needs this lookup to be complete. Planners use it to prefer
   * the source end of such a hop.
   */
  public static boolean isIncomingSideMissing(final Schema schema, final Vertex.DIRECTION direction,
      final String... edgeTypes) {
    return direction != Vertex.DIRECTION.OUT && closure(schema, edgeTypes).unidirectional.length > 0;
  }

  /**
   * Whether any of {@code edgeTypes} (all when empty), or one of their subtypes, is declared unidirectional.
   */
  public static boolean isAnyUnidirectional(final Schema schema, final String... edgeTypes) {
    return closure(schema, edgeTypes).unidirectional.length > 0;
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
    lookup.refreshMemos(database);
    if (!lookup.hasAnyUnidirectionalType(schema))
      return null;

    final Closure closure = lookup.closureOf(schema, edgeTypes);
    if (closure.unidirectional.length == 0)
      return null;

    return new Involved(schema, closure, lookup.snapshots((DatabaseInternal) database, closure.unidirectional, context));
  }

  private void refreshMemos(final Database database) {
    if (!(database instanceof DatabaseInternal internal))
      return;
    final long modifications = internal.getModificationCount();
    if (modifications != memoizedAt) {
      anyUnidirectionalType = null;
      untypedClosure = null;
      lastClosure = null;
      memoizedAt = modifications;
    }
  }

  private boolean hasAnyUnidirectionalType(final Schema schema) {
    Boolean any = anyUnidirectionalType;
    if (any == null) {
      any = Boolean.FALSE;
      for (final DocumentType type : schema.getTypes())
        if (type instanceof EdgeType edgeType && !edgeType.isBidirectional()) {
          any = Boolean.TRUE;
          break;
        }
      anyUnidirectionalType = any;
    }
    return any;
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
   * The scans of {@code unidirectionalTypes}, taking the ones this query has not taken yet or whose edges changed since
   * (see {@link GraphEngine#getUnidirectionalEdgeChanges()}). The common case - every scan present and current - reads
   * a concurrent map and takes no lock; the missing ones are taken under the lock, so parallel workers wait for one scan
   * rather than repeat it.
   */
  private Snapshot[] snapshots(final DatabaseInternal database, final String[] unidirectionalTypes,
      final CommandContext context) {
    final long version = database.getGraphEngine().getUnidirectionalEdgeChanges();
    final Snapshot[] result = new Snapshot[unidirectionalTypes.length];
    Map<String, Snapshot> map = snapshots;
    if (map != null) {
      boolean allCurrent = true;
      for (int i = 0; i < unidirectionalTypes.length && allCurrent; i++) {
        final Snapshot snapshot = map.get(unidirectionalTypes[i]);
        if (snapshot == null || snapshot.version != version)
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
        if (snapshot == null || snapshot.version != version)
          missing.add(typeName);
      }
      if (!missing.isEmpty())
        for (final Snapshot snapshot : Snapshot.build(database, missing, version, context)) {
          final Snapshot previous = map.put(snapshot.typeName, snapshot);
          if (previous != null)
            previous.limit.release();
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

  private record Involved(Schema schema, Closure closure, Snapshot[] snapshots) {
  }

  /**
   * The edges of one unidirectional type, sorted by target: six parallel primitive arrays, the endpoints and the edge
   * as bucket/position pairs, so the scan holds no object per edge. A lightweight edge has no record, and is kept as its
   * type's bucket with {@link #LIGHTWEIGHT_POSITION}. The arrays are the builder's own, sorted in place, so building
   * one never holds a second copy.
   */
  private static final class Snapshot {
    private final String             typeName;
    private final long               version;
    private final OperationHeapLimit limit;
    private final DatabaseInternal   database;
    private final int                size;
    private final int[]              targetBuckets;
    private final long[]             targetPositions;
    private final int[]              sourceBuckets;
    private final long[]             sourcePositions;
    private final int[]              edgeBuckets;
    private final long[]             edgePositions;

    private Snapshot(final String typeName, final long version, final DatabaseInternal database, final Builder builder) {
      builder.sortByTarget();
      this.typeName = typeName;
      this.version = version;
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

    static List<Snapshot> build(final DatabaseInternal database, final List<String> typeNames, final long version,
        final CommandContext context) {
      final Schema schema = database.getSchema();
      final Map<String, Builder> builders = new HashMap<>();
      final List<Builder> lightweight = new ArrayList<>();
      for (final String typeName : typeNames) {
        final Builder builder = new Builder(OperationHeapLimit.of(context, "edges",
            "incoming-edge lookup over the unidirectional edge type " + typeName));
        builders.put(typeName, builder);

        // OWN BUCKETS ONLY: THE SUBTYPES ARE IN THE SET ON THEIR OWN
        final Iterator<Record> records = database.iterateType(typeName, false);
        while (records.hasNext()) {
          final Edge edge = records.next().asEdge();
          builder.add(edge.getIn(), edge.getOut(), edge.getIdentity().getBucketId(), edge.getIdentity().getPosition());
        }
        if (schema.getType(typeName) instanceof EdgeType edgeType && edgeType.isLightweight())
          lightweight.add(builder);
      }

      if (!lightweight.isEmpty())
        addLightweightEdges(database, builders, lightweight.size());

      final List<Snapshot> result = new ArrayList<>(typeNames.size());
      for (final String typeName : typeNames)
        result.add(new Snapshot(typeName, version, database, builders.get(typeName)));
      return result;
    }

    /**
     * A lightweight edge lives only in the lists of its two vertices, and for a unidirectional type only in the
     * outgoing one: the vertices are walked for it, as {@code SELECT FROM} does for such a type (issue #7477), once for
     * all the lightweight types being scanned. A vertex bucket the caller cannot read is left out, like the edges it
     * holds.
     */
    private static void addLightweightEdges(final DatabaseInternal database, final Map<String, Builder> builders,
        final int lightweightTypes) {
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
          for (final Iterator<Record> it = bucket.iterator(); it.hasNext(); ) {
            final Vertex vertex = it.next().asVertex();
            for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT, names)) {
              final RID identity = edge.getIdentity();
              // A RECORD-BACKED EDGE CAME FROM THE TYPE SCAN ALREADY; A TYPE NOT BEING SCANNED IS NOT ADDED
              if (identity.getPosition() >= 0)
                continue;
              final Builder builder = builders.get(edge.getTypeName());
              if (builder != null)
                builder.add(edge.getIn(), vertex.getIdentity(), identity.getBucketId(), LIGHTWEIGHT_POSITION);
            }
          }
        }
      }
    }

    long count(final RID target) {
      final int from = firstIndex(target);
      int to = from;
      while (to < size && isTarget(to, target))
        ++to;
      return to - from;
    }

    Iterator<Edge> edgesInto(final RID target) {
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

    Iterator<Vertex> sourcesOf(final RID target) {
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

    /** Sorts the six arrays by target, in place: a quicksort recursing on the smaller side only. */
    void sortByTarget() {
      sort(0, size - 1);
    }

    private void sort(int low, int high) {
      while (high - low > 16) {
        final int mid = medianOfThree(low, (low + high) >>> 1, high);
        final int pivotBucket = targetBuckets[mid];
        final long pivotPosition = targetPositions[mid];
        int i = low;
        int j = high;
        while (i <= j) {
          while (compareTo(i, pivotBucket, pivotPosition) < 0)
            ++i;
          while (compareTo(j, pivotBucket, pivotPosition) > 0)
            --j;
          if (i <= j)
            swap(i++, j--);
        }
        if (j - low < high - i) {
          sort(low, j);
          low = i;
        } else {
          sort(i, high);
          high = j;
        }
      }
      for (int i = low + 1; i <= high; i++)
        for (int j = i; j > low && compareTo(j - 1, targetBuckets[j], targetPositions[j]) > 0; j--)
          swap(j - 1, j);
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
      return compareTo(a, targetBuckets[b], targetPositions[b]);
    }

    private int compareTo(final int i, final int bucket, final long position) {
      if (targetBuckets[i] != bucket)
        return Integer.compare(targetBuckets[i], bucket);
      return Long.compare(targetPositions[i], position);
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
