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
 * The answer comes from one scan of the edges of the unidirectional types involved, built the first time a query needs
 * it and shared by the whole query (its sub-queries and parallel workers included, through the root
 * {@link CommandContext}): the edge records of each type, plus the outgoing lists of the vertices for a type that is
 * stored lightweight. The scan is sorted by target into primitive arrays, so each later lookup is a binary search
 * rather than another scan, and the heap it takes is charged to the query's budget like any other in-heap buffer: a
 * type too large to index in heap fails the query loudly rather than filling the heap.
 * <p>
 * Edges of a bidirectional type are read from the vertex as usual. An incoming pointer found on a vertex for a
 * unidirectional type - written by a bulk load that did not follow the type - is skipped, since the scan already
 * answers for that edge.
 * <p>
 * The scan is a snapshot of the edges visible when it is taken: an edge created later in the same query is not in it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IncomingEdgeLookup {
  private static final long LIGHTWEIGHT_POSITION = -1L;

  private final    Map<String, Snapshot> snapshots = new ConcurrentHashMap<>();
  private volatile Boolean               anyUnidirectionalType;
  // THE STEPS ASK ONCE PER ROW, MOSTLY FOR THE SAME TYPES: THE LAST ANSWER AND THE UNTYPED ONE ARE KEPT
  private volatile ClosureEntry          lastClosure;
  private volatile Closure               untypedClosure;

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
    result.addIterator(involved.snapshot.edgesInto(vertex.getIdentity()));
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
    result.addIterator(involved.snapshot.sourcesOf(vertex.getIdentity()));
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
    return count + involved.snapshot.count(vertex.getIdentity());
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
    return lookup != null && lookup.hasAnyUnidirectionalType(database.getSchema())
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
    if (!lookup.hasAnyUnidirectionalType(schema))
      return null;

    final Closure closure = lookup.closureOf(schema, edgeTypes);
    if (closure.unidirectional.length == 0)
      return null;

    return new Involved(schema, closure, lookup.snapshot((DatabaseInternal) database, closure.unidirectional, context));
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

  private Snapshot snapshot(final DatabaseInternal database, final String[] unidirectionalTypes,
      final CommandContext context) {
    final String key = unidirectionalTypes.length == 1 ? unidirectionalTypes[0] : String.join(",", unidirectionalTypes);
    final Snapshot existing = snapshots.get(key);
    if (existing != null)
      return existing;
    // BUILT ONCE PER QUERY: PARALLEL WORKERS ASKING FOR THE SAME TYPES WAIT FOR THE FIRST BUILD RATHER THAN REPEAT IT
    return snapshots.computeIfAbsent(key, k -> Snapshot.build(database, unidirectionalTypes, context));
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

  private record Involved(Schema schema, Closure closure, Snapshot snapshot) {
  }

  /**
   * The edges of a set of unidirectional types, sorted by target: six parallel primitive arrays, the endpoints and the
   * edge as bucket/position pairs, so the lookup holds no object per edge. A lightweight edge has no record, and is
   * kept as its type's bucket with {@link #LIGHTWEIGHT_POSITION}.
   */
  private static final class Snapshot {
    private final DatabaseInternal database;
    private final int[]            targetBuckets;
    private final long[]           targetPositions;
    private final int[]            sourceBuckets;
    private final long[]           sourcePositions;
    private final int[]            edgeBuckets;
    private final long[]           edgePositions;

    private Snapshot(final DatabaseInternal database, final Builder builder) {
      this.database = database;
      final int size = builder.size;
      final int[] order = new int[size];
      for (int i = 0; i < size; i++)
        order[i] = i;
      builder.sortByTarget(order);

      targetBuckets = new int[size];
      targetPositions = new long[size];
      sourceBuckets = new int[size];
      sourcePositions = new long[size];
      edgeBuckets = new int[size];
      edgePositions = new long[size];
      for (int i = 0; i < size; i++) {
        final int from = order[i];
        targetBuckets[i] = builder.targetBuckets[from];
        targetPositions[i] = builder.targetPositions[from];
        sourceBuckets[i] = builder.sourceBuckets[from];
        sourcePositions[i] = builder.sourcePositions[from];
        edgeBuckets[i] = builder.edgeBuckets[from];
        edgePositions[i] = builder.edgePositions[from];
      }
    }

    static Snapshot build(final DatabaseInternal database, final String[] unidirectionalTypes,
        final CommandContext context) {
      final Schema schema = database.getSchema();
      final Builder builder = new Builder(OperationHeapLimit.of(context, "edges",
          "incoming-edge lookup over the unidirectional edge type(s) " + String.join(", ", unidirectionalTypes)));

      final Set<String> lightweightTypes = new HashSet<>();
      for (final String typeName : unidirectionalTypes) {
        // OWN BUCKETS ONLY: THE SUBTYPES ARE IN THE SET ON THEIR OWN
        final Iterator<Record> records = database.iterateType(typeName, false);
        while (records.hasNext()) {
          final Edge edge = records.next().asEdge();
          builder.add(edge.getIn(), edge.getOut(), edge.getIdentity().getBucketId(), edge.getIdentity().getPosition());
        }
        if (schema.getType(typeName) instanceof EdgeType edgeType && edgeType.isLightweight())
          lightweightTypes.add(typeName);
      }

      if (!lightweightTypes.isEmpty())
        addLightweightEdges(database, lightweightTypes, builder);

      return new Snapshot(database, builder);
    }

    /**
     * A lightweight edge lives only in the lists of its two vertices, and for a unidirectional type only in the
     * outgoing one: the vertices are walked for it, as {@code SELECT FROM} does for such a type (issue #7477). A vertex
     * bucket the caller cannot read is left out, like the edges it holds.
     */
    private static void addLightweightEdges(final DatabaseInternal database, final Set<String> lightweightTypes,
        final Builder builder) {
      final Schema schema = database.getSchema();
      final String[] names = lightweightTypes.toArray(new String[0]);
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
              // A RECORD-BACKED EDGE CAME FROM THE TYPE SCAN ALREADY; A SUBTYPE'S EDGE IS THE SUBTYPE'S TO ADD
              if (identity.getPosition() >= 0 || !lightweightTypes.contains(edge.getTypeName()))
                continue;
              builder.add(edge.getIn(), vertex.getIdentity(), identity.getBucketId(), LIGHTWEIGHT_POSITION);
            }
          }
        }
      }
    }

    long count(final RID target) {
      final int from = firstIndex(target);
      int to = from;
      while (to < targetBuckets.length && isTarget(to, target))
        ++to;
      return to - from;
    }

    Iterator<Edge> edgesInto(final RID target) {
      final int from = firstIndex(target);
      if (from >= targetBuckets.length || !isTarget(from, target))
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
      if (from >= targetBuckets.length || !isTarget(from, target))
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
      int high = targetBuckets.length;
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
        return next < targetBuckets.length && isTarget(next, target);
      }

      @Override
      public T next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return get(next++);
      }
    }
  }

  /** Grows the scan in parallel primitive arrays, charging the query's heap budget as it goes. */
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
    }

    void add(final RID target, final RID source, final int edgeBucket, final long edgePosition) {
      limit.check(size + 1L);
      limit.charge(BYTES_PER_EDGE);
      if (size == targetBuckets.length) {
        final int capacity = size + (size >> 1);
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

    /** Sorts {@code order} by target: a quicksort over the index array, recursing on the smaller side only. */
    void sortByTarget(final int[] order) {
      sort(order, 0, order.length - 1);
    }

    private void sort(final int[] order, int low, int high) {
      while (high - low > 16) {
        final int pivot = order[medianOfThree(order, low, (low + high) >>> 1, high)];
        int i = low;
        int j = high;
        while (i <= j) {
          while (compare(order[i], pivot) < 0)
            ++i;
          while (compare(order[j], pivot) > 0)
            --j;
          if (i <= j) {
            final int tmp = order[i];
            order[i++] = order[j];
            order[j--] = tmp;
          }
        }
        if (j - low < high - i) {
          sort(order, low, j);
          low = i;
        } else {
          sort(order, i, high);
          high = j;
        }
      }
      insertionSort(order, low, high);
    }

    private void insertionSort(final int[] order, final int low, final int high) {
      for (int i = low + 1; i <= high; i++) {
        final int value = order[i];
        int j = i - 1;
        while (j >= low && compare(order[j], value) > 0) {
          order[j + 1] = order[j];
          --j;
        }
        order[j + 1] = value;
      }
    }

    private int medianOfThree(final int[] order, final int a, final int b, final int c) {
      if (compare(order[a], order[b]) < 0) {
        if (compare(order[b], order[c]) < 0)
          return b;
        return compare(order[a], order[c]) < 0 ? c : a;
      }
      if (compare(order[a], order[c]) < 0)
        return a;
      return compare(order[b], order[c]) < 0 ? c : b;
    }

    private int compare(final int a, final int b) {
      if (targetBuckets[a] != targetBuckets[b])
        return Integer.compare(targetBuckets[a], targetBuckets[b]);
      return Long.compare(targetPositions[a], targetPositions[b]);
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
