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
import com.arcadedb.graph.olap.NodeIdMapping;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * Builds the {@code int[][]} adjacency of a vertex list from the edge-list entries, without reading an edge record (issue #11104):
 * what the {@code algo.*} procedures fall back to when no Graph Analytical View serves them.
 * <p>
 * The record walk it replaces loaded every edge to read its far end, and resolved that end through a boxed {@code Map<RID,
 * Integer>}, twice per edge (a counting pass, then a filling pass). Here each entry comes from {@link EdgeListScanner} as the four
 * numbers it is stored as, and the far end is resolved through a {@link NodeIdMapping} built once from the caller's map: a per-bucket
 * array lookup and a binary search, nothing boxed and nothing allocated per entry. The two passes stay, because the count is what
 * the caller's heap bound is asked about before the rows are allocated (issue #6317).
 * <p>
 * What the entries answer, and why it is the same answer as before but for one shape:
 * <ul>
 * <li>the order: each list newest first, the OUT list before the IN list for {@code BOTH}, as {@code Vertex.getEdges()} walks
 * them, so an algorithm that breaks ties by neighbour order answers as it did;</li>
 * <li>the edge types: the entry's edge bucket against the {@link EdgeBucketMask} of the types (subtypes included), the filter the
 * typed record walk applies to the same entries before it loads an edge;</li>
 * <li>the permission: the record walk read each edge, which asks for READ_RECORD on its bucket; so does this walk, once per edge
 * bucket met, for every entry the type filter accepts. A lightweight edge has no record, and was never checked;</li>
 * <li>a ghost entry, one whose edge record is gone, is FOLLOWED where the record walk skipped it: this is a per-entry walk of the
 * whole adjacency, which engine/CLAUDE.md puts with the counts and the Graph Analytical View build, and the view build follows the
 * entry. An algorithm therefore answers the same with and without a view, where it used to answer differently on a database with
 * a ghost; {@code CHECK DATABASE ... FIX} prunes the entry for both.</li>
 * </ul>
 * <p>
 * The vertices come as a list with the map that numbers them, or as the {@link NodeIdMapping} that numbers them by itself, which is what
 * an algorithm holding a large graph passes (issue #12554): the row of a vertex is then its id in the mapping, no map is rebuilt from
 * the caller's, and the edge lists are scanned by the position of the vertex, so not one vertex record is loaded.
 * <p>
 * Not thread-safe: one instance per build.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AdjacencyListBuilder implements EdgeListScanner.EntryVisitor {
  private final DatabaseInternal database;
  private final EdgeListScanner  scanner;
  // THE FAR END OF AN ENTRY TO ITS ROW: THE CALLER'S MAP, UNBOXED ONCE
  private final NodeIdMapping    targets;
  // null WHEN THE ROW OF A VERTEX IS ITS ID IN targets
  private final int[]            rowOfTarget;
  // CALLED WITH A COUNTER THAT ADVANCES BY ONE PER ROW WALKED AND PER ENTRY READ, SO A CALLER CAN ABORT THE BUILD, EVEN INSIDE A SUPERNODE'S
  // LIST: null FOR NONE
  private final IntConsumer      checkpoint;
  private       int              steps;
  // THE EDGE BUCKET WHOSE READ PERMISSION WAS ASKED LAST: ENTRIES COME IN RUNS OF ONE BUCKET
  private       int              lastReadableEdgeBucketId = -1;

  // THE VERTICES WALKED, ROW BY ROW
  private final Rows     rows;

  // THE WALK IN PROGRESS: WHICH ENTRIES IT TAKES AND WHERE IT PUTS THEM
  private boolean        filtered;
  private EdgeBucketMask filter;
  private EdgeBucketMask unidirectional;
  private boolean        skipUnidirectional;
  private boolean        addToOwnRow;
  private boolean        transposeUnidirectional;
  private int            row;
  // THE PASS IN PROGRESS: sizes COUNTS, AND WHEN adj IS SET IS ALSO THE FILL POSITION OF EACH ROW
  private int[]          sizes;
  private int[][]        adj;

  /** The vertices of a build, numbered from 0: what a walk scans the edge lists of. */
  private interface Rows {
    /** How many vertices there are to walk. */
    int size();

    /** Starts a pass: rows are then asked for in ascending order. */
    void restart();

    /** Hands every entry of the {@code direction} edge list of the vertex of row {@code row} to {@code visitor}. */
    void scan(EdgeListScanner scanner, int row, Vertex.DIRECTION direction, EdgeListScanner.EntryVisitor visitor);
  }

  /** Vertices the caller already holds. */
  private record ListRows(List<Vertex> vertices) implements Rows {
    @Override
    public int size() {
      return vertices.size();
    }

    @Override
    public void restart() {
      // Nothing to restart: a vertex of a list is found by its row, whatever the order the rows are asked for in
    }

    @Override
    public void scan(final EdgeListScanner scanner, final int row, final Vertex.DIRECTION direction,
        final EdgeListScanner.EntryVisitor visitor) {
      scanner.scan((VertexInternal) vertices.get(row), direction, visitor);
    }
  }

  /**
   * Vertices known by their RID alone: the row of a vertex is its id in the mapping, so the position of a row is read from the bucket's
   * array, walking along it (looked up for a reordered mapping, whose ids do not follow the buckets), and no vertex is loaded. A row
   * whose record is gone since the mapping was made has no entries.
   */
  private static final class MappedRows implements Rows {
    private final NodeIdMapping ids;
    private       int           bucketIdx;
    private       int           bucketBase;
    private       int           bucketEnd;

    private MappedRows(final NodeIdMapping ids) {
      this.ids = ids;
    }

    @Override
    public int size() {
      return ids.size();
    }

    @Override
    public void restart() {
      bucketIdx = 0;
      bucketBase = 0;
      bucketEnd = ids.getNumBuckets() > 0 ? ids.getBucketSize(0) : 0;
    }

    @Override
    public void scan(final EdgeListScanner scanner, final int row, final Vertex.DIRECTION direction,
        final EdgeListScanner.EntryVisitor visitor) {
      if (ids.isReordered()) {
        // The ids of a reordered mapping are not in the order of its buckets: each row is looked up
        final long packed = ids.getBucketIdxAndLocalId(row);
        final int reorderedBucketIdx = NodeIdMapping.unpackBucketIdx(packed);
        scanner.scan(ids.getBucketId(reorderedBucketIdx), ids.getPosition(reorderedBucketIdx, NodeIdMapping.unpackLocalId(packed)),
            direction, visitor);
        return;
      }
      while (row >= bucketEnd) {
        bucketIdx++;
        bucketBase = bucketEnd;
        bucketEnd += ids.getBucketSize(bucketIdx);
      }
      scanner.scan(ids.getBucketId(bucketIdx), ids.getPosition(bucketIdx, row - bucketBase), direction, visitor);
    }
  }

  private AdjacencyListBuilder(final DatabaseInternal database, final NodeIdMapping targets, final int[] rowOfTarget,
      final Rows rows, final IntConsumer checkpoint) {
    this.database = database;
    this.scanner = new EdgeListScanner(database);
    this.targets = targets;
    this.rowOfTarget = rowOfTarget;
    this.rows = rows;
    this.checkpoint = checkpoint;
  }

  /**
   * @see GraphEngine#buildAdjacencyList(List, Map, Vertex.DIRECTION, String[], LongConsumer)
   */
  static int[][] build(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx, final Vertex.DIRECTION dir,
      final String[] relTypes, final LongConsumer entryCount) {
    if (vertices.isEmpty()) {
      if (entryCount != null)
        entryCount.accept(0L);
      return new int[0][];
    }

    final NodeIdMapping mapping = new NodeIdMapping(4);
    for (final RID rid : ridToIdx.keySet())
      // A RID OF NO BUCKET NAMES A RECORD NOT SAVED YET: NO ENTRY CAN POINT TO IT
      if (rid.getBucketId() >= 0)
        mapping.addNode(mapping.registerBucket(rid.getBucketId(), null, 64), rid.getPosition());
    mapping.compact();
    final int[] rowOfTarget = new int[mapping.size()];
    for (final Map.Entry<RID, Integer> entry : ridToIdx.entrySet()) {
      final RID rid = entry.getKey();
      if (rid.getBucketId() >= 0)
        rowOfTarget[mapping.getGlobalId(rid.getBucketId(), rid.getPosition())] = entry.getValue();
    }

    final DatabaseInternal database = (DatabaseInternal) vertices.getFirst().getDatabase();
    return build(new AdjacencyListBuilder(database, mapping, rowOfTarget, new ListRows(vertices), null), database, dir, relTypes,
        entryCount);
  }

  /**
   * @see GraphEngine#buildAdjacencyList(NodeIdMapping, Database, Vertex.DIRECTION, String[], LongConsumer, IntConsumer)
   */
  static int[][] build(final NodeIdMapping vertices, final DatabaseInternal database, final Vertex.DIRECTION dir,
      final String[] relTypes, final LongConsumer entryCount, final IntConsumer checkpoint) {
    if (vertices.size() == 0) {
      if (entryCount != null)
        entryCount.accept(0L);
      return new int[0][];
    }
    return build(new AdjacencyListBuilder(database, vertices, null, new MappedRows(vertices), checkpoint), database, dir, relTypes,
        entryCount);
  }

  private static int[][] build(final AdjacencyListBuilder builder, final DatabaseInternal database, final Vertex.DIRECTION dir,
      final String[] relTypes, final LongConsumer entryCount) {
    final int n = builder.rows.size();
    final boolean typed = relTypes != null && relTypes.length > 0;
    final String[] unidirectionalTypes = dir != Vertex.DIRECTION.OUT ?
        IncomingEdgeLookup.getUnidirectionalTypes(database.getSchema(), relTypes) :
        new String[0];

    final EdgeBucketMask typeMask = typed ? EdgeBucketMask.of(database, relTypes) : null;
    final EdgeBucketMask unidirectionalMask = unidirectionalTypes.length > 0 ? EdgeBucketMask.of(database, unidirectionalTypes) : null;

    final int[] counts = new int[n];
    builder.pass(dir, typed, typeMask, unidirectionalTypes.length > 0, unidirectionalMask, counts, null);

    if (entryCount != null) {
      long total = 0;
      for (int i = 0; i < n; i++)
        total += counts[i];
      entryCount.accept(total);
    }

    final int[][] adj = new int[n][];
    for (int i = 0; i < n; i++)
      adj[i] = new int[counts[i]];
    // THE SECOND PASS RE-COUNTS INTO A ZEROED ARRAY, USED AS THE FILL POSITION OF EACH ROW
    final int[] filled = new int[n];
    builder.pass(dir, typed, typeMask, unidirectionalTypes.length > 0, unidirectionalMask, filled, adj);

    // A COMMIT BETWEEN THE TWO PASSES CAN TAKE AN ENTRY AWAY: A ROW KEEPS ONLY WHAT WAS FILLED, NEVER A TRAILING ZERO THAT WOULD NAME
    // VERTEX 0. ONE THAT ADDED AN ENTRY IS CAPPED BY THE COUNT IN visit()
    for (int i = 0; i < n; i++)
      if (filled[i] < adj[i].length)
        adj[i] = Arrays.copyOf(adj[i], filled[i]);
    return adj;
  }

  /**
   * One pass over every vertex. Over a direction that reads the incoming side of an edge type declared unidirectional (issue #8629),
   * which writes the outgoing pointer only, the incoming side of a vertex holds no trace of those edges; but the rows only ever list
   * vertices of the set, and every edge between two of them sits in the outgoing list of its source, so the incoming rows are the
   * transpose of the outgoing lists of the same vertices, which is what a Graph Analytical View derives its backward CSR from. A row
   * gets its entries from its own lists and from the transposed edges of the vertices before and after it, in the order the walk
   * meets them, the same in both passes.
   */
  private void pass(final Vertex.DIRECTION dir, final boolean typed, final EdgeBucketMask typeMask, final boolean transposed,
      final EdgeBucketMask unidirectionalMask, final int[] sizes, final int[][] adj) {
    this.sizes = sizes;
    this.adj = adj;
    this.unidirectional = unidirectionalMask;
    rows.restart();
    try {
      final int count = rows.size();
      for (int i = 0; i < count; i++) {
        if (checkpoint != null)
          checkpoint.accept(steps++);
        row = i;
        if (!transposed) {
          walk(dir == Vertex.DIRECTION.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.OUT, typed, typeMask, false, true, false);
          if (dir == Vertex.DIRECTION.BOTH)
            walk(Vertex.DIRECTION.IN, typed, typeMask, false, true, false);
        } else {
          if (dir == Vertex.DIRECTION.BOTH)
            // THE OUTGOING ROW OF EVERY TYPE, AND EACH UNIDIRECTIONAL EDGE ALSO INTO ITS TARGET'S ROW
            walk(Vertex.DIRECTION.OUT, typed, typeMask, false, true, true);
          else
            // IN: ONLY THE UNIDIRECTIONAL EDGES, EACH INTO ITS TARGET'S ROW
            walk(Vertex.DIRECTION.OUT, true, unidirectionalMask, false, false, true);
          // THE INCOMING LIST STILL ANSWERS FOR THE BIDIRECTIONAL TYPES, LEAVING OUT A POINTER A BULK LOAD STORED FOR A
          // UNIDIRECTIONAL ONE, WHICH THE TRANSPOSE ALREADY ANSWERS FOR
          walk(Vertex.DIRECTION.IN, typed, typeMask, true, true, false);
        }
      }
    } finally {
      this.sizes = null;
      this.adj = null;
    }
  }

  private void walk(final Vertex.DIRECTION direction, final boolean filtered, final EdgeBucketMask filter,
      final boolean skipUnidirectional, final boolean addToOwnRow, final boolean transposeUnidirectional) {
    if (filtered && filter == null)
      // NONE OF THE TYPES ASKED FOR HAS A BUCKET: NO ENTRY CAN MATCH
      return;
    this.filtered = filtered;
    this.filter = filter;
    this.skipUnidirectional = skipUnidirectional;
    this.addToOwnRow = addToOwnRow;
    this.transposeUnidirectional = transposeUnidirectional;
    rows.scan(scanner, row, direction, this);
  }

  @Override
  public void visit(final int edgeBucketId, final long edgePosition, final int vertexBucketId, final long vertexPosition) {
    // Per entry too: one supernode's list can hold millions of them, and a checkpoint per row would leave all of it unabortable
    if (checkpoint != null)
      checkpoint.accept(steps++);
    if (filtered && !filter.matches(edgeBucketId))
      return;
    if (edgePosition >= 0 && edgeBucketId != lastReadableEdgeBucketId) {
      // WHAT LOADING THE EDGE ASKED: A USER WHO CANNOT READ THE EDGE TYPE IS REFUSED, NOT HANDED ITS TOPOLOGY
      database.checkPermissionsOnFile(edgeBucketId, SecurityDatabaseUser.ACCESS.READ_RECORD);
      lastReadableEdgeBucketId = edgeBucketId;
    }
    final boolean unidirectionalEdge = unidirectional != null && unidirectional.matches(edgeBucketId);
    if (skipUnidirectional && unidirectionalEdge)
      return;

    final int target = targets.getGlobalId(vertexBucketId, vertexPosition);
    if (target < 0)
      return;
    final int targetRow = rowOfTarget != null ? rowOfTarget[target] : target;
    if (addToOwnRow)
      add(row, targetRow);
    if (transposeUnidirectional && unidirectionalEdge)
      add(targetRow, row);
  }

  private void add(final int rowIndex, final int neighbour) {
    if (adj == null)
      ++sizes[rowIndex];
    else if (sizes[rowIndex] < adj[rowIndex].length)
      adj[rowIndex][sizes[rowIndex]++] = neighbour;
  }
}
