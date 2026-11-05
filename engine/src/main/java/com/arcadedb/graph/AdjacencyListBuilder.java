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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.olap.NodeIdMapping;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
 * Not thread-safe: one instance per build.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AdjacencyListBuilder implements EdgeListScanner.EntryVisitor {
  private final DatabaseInternal database;
  private final EdgeListScanner  scanner;
  // THE FAR END OF AN ENTRY TO ITS ROW: THE CALLER'S MAP, UNBOXED ONCE
  private final NodeIdMapping    targets;
  private final int[]            rowOfTarget;
  // THE EDGE BUCKET WHOSE READ PERMISSION WAS ASKED LAST: ENTRIES COME IN RUNS OF ONE BUCKET
  private       int              lastReadableEdgeBucketId = -1;

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

  private AdjacencyListBuilder(final DatabaseInternal database, final Map<RID, Integer> ridToIdx) {
    this.database = database;
    this.scanner = new EdgeListScanner(database);

    final NodeIdMapping mapping = new NodeIdMapping(4);
    for (final RID rid : ridToIdx.keySet())
      // A RID OF NO BUCKET NAMES A RECORD NOT SAVED YET: NO ENTRY CAN POINT TO IT
      if (rid.getBucketId() >= 0)
        mapping.addNode(mapping.registerBucket(rid.getBucketId(), null, 64), rid.getPosition());
    mapping.compact();
    this.targets = mapping;
    this.rowOfTarget = new int[mapping.size()];
    for (final Map.Entry<RID, Integer> entry : ridToIdx.entrySet()) {
      final RID rid = entry.getKey();
      if (rid.getBucketId() >= 0)
        rowOfTarget[mapping.getGlobalId(rid.getBucketId(), rid.getPosition())] = entry.getValue();
    }
  }

  /**
   * @see GraphEngine#buildAdjacencyList(List, Map, Vertex.DIRECTION, String[], LongConsumer)
   */
  static int[][] build(final List<Vertex> vertices, final Map<RID, Integer> ridToIdx, final Vertex.DIRECTION dir,
      final String[] relTypes, final LongConsumer entryCount) {
    final int n = vertices.size();
    if (n == 0) {
      if (entryCount != null)
        entryCount.accept(0L);
      return new int[0][];
    }

    final DatabaseInternal database = (DatabaseInternal) vertices.getFirst().getDatabase();
    final boolean typed = relTypes != null && relTypes.length > 0;
    final String[] unidirectionalTypes = dir != Vertex.DIRECTION.OUT ?
        IncomingEdgeLookup.getUnidirectionalTypes(database.getSchema(), relTypes) :
        new String[0];

    final AdjacencyListBuilder builder = new AdjacencyListBuilder(database, ridToIdx);
    final EdgeBucketMask typeMask = typed ? EdgeBucketMask.of(database, relTypes) : null;
    final EdgeBucketMask unidirectionalMask = unidirectionalTypes.length > 0 ? EdgeBucketMask.of(database, unidirectionalTypes) : null;

    final int[] counts = new int[n];
    builder.pass(vertices, dir, typed, typeMask, unidirectionalTypes.length > 0, unidirectionalMask, counts, null);

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
    builder.pass(vertices, dir, typed, typeMask, unidirectionalTypes.length > 0, unidirectionalMask, filled, adj);

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
  private void pass(final List<Vertex> vertices, final Vertex.DIRECTION dir, final boolean typed, final EdgeBucketMask typeMask,
      final boolean transposed, final EdgeBucketMask unidirectionalMask, final int[] sizes, final int[][] adj) {
    this.sizes = sizes;
    this.adj = adj;
    this.unidirectional = unidirectionalMask;
    try {
      for (int i = 0; i < vertices.size(); i++) {
        final VertexInternal vertex = (VertexInternal) vertices.get(i);
        row = i;
        if (!transposed) {
          walk(vertex, dir == Vertex.DIRECTION.IN ? Vertex.DIRECTION.IN : Vertex.DIRECTION.OUT, typed, typeMask, false, true, false);
          if (dir == Vertex.DIRECTION.BOTH)
            walk(vertex, Vertex.DIRECTION.IN, typed, typeMask, false, true, false);
        } else {
          if (dir == Vertex.DIRECTION.BOTH)
            // THE OUTGOING ROW OF EVERY TYPE, AND EACH UNIDIRECTIONAL EDGE ALSO INTO ITS TARGET'S ROW
            walk(vertex, Vertex.DIRECTION.OUT, typed, typeMask, false, true, true);
          else
            // IN: ONLY THE UNIDIRECTIONAL EDGES, EACH INTO ITS TARGET'S ROW
            walk(vertex, Vertex.DIRECTION.OUT, true, unidirectionalMask, false, false, true);
          // THE INCOMING LIST STILL ANSWERS FOR THE BIDIRECTIONAL TYPES, LEAVING OUT A POINTER A BULK LOAD STORED FOR A
          // UNIDIRECTIONAL ONE, WHICH THE TRANSPOSE ALREADY ANSWERS FOR
          walk(vertex, Vertex.DIRECTION.IN, typed, typeMask, true, true, false);
        }
      }
    } finally {
      this.sizes = null;
      this.adj = null;
    }
  }

  private void walk(final VertexInternal vertex, final Vertex.DIRECTION direction, final boolean filtered,
      final EdgeBucketMask filter, final boolean skipUnidirectional, final boolean addToOwnRow,
      final boolean transposeUnidirectional) {
    if (filtered && filter == null)
      // NONE OF THE TYPES ASKED FOR HAS A BUCKET: NO ENTRY CAN MATCH
      return;
    this.filtered = filtered;
    this.filter = filter;
    this.skipUnidirectional = skipUnidirectional;
    this.addToOwnRow = addToOwnRow;
    this.transposeUnidirectional = transposeUnidirectional;
    scanner.scan(vertex, direction, this);
  }

  @Override
  public void visit(final int edgeBucketId, final long edgePosition, final int vertexBucketId, final long vertexPosition) {
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
    final int targetRow = rowOfTarget[target];
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
