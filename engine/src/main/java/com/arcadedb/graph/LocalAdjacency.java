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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * The adjacency rows of the vertices a call actually reads, read from the records on demand and numbered as they are met
 * (issue #12176): what an {@code algo.*} procedure that asks about one or two vertices and their neighbourhoods (a random walk,
 * the link-prediction scores) reads when no Graph Analytical View serves it, instead of the {@code int[][]} adjacency of the
 * whole database {@link GraphEngine#buildAdjacencyList} builds after loading every vertex.
 * <p>
 * A row answers what the row of the same vertex in that whole-database adjacency answers:
 * <ul>
 * <li>the entries of the vertex's edge lists, read without loading an edge record, newest first, the OUT list before the IN list
 * for {@link Vertex.DIRECTION#BOTH}, so a walk that picks a neighbour by its position picks the same one;</li>
 * <li>the edge types filtered by the entry's edge bucket, subtypes included, and READ_RECORD asked once per edge bucket met;</li>
 * <li>a ghost entry (its edge record gone) followed, and an entry whose far vertex is gone left out, as the whole-database build
 * leaves out a target that is not among the vertices it loaded;</li>
 * <li>repetitions kept: a parallel edge, or an edge stored in both directions, is one entry each, and a self-loop is the vertex
 * itself, twice over {@code BOTH}.</li>
 * </ul>
 * Over a direction that reads the incoming side of an edge type declared unidirectional (issue #8629) the vertex stores no trace
 * of those edges, so such a row is read from the edges through {@link IncomingEdgeLookup}, as a walk of the records reads it; the
 * row holds the same neighbours, possibly in another order.
 * <p>
 * A vertex is asked whether it exists once, when it is first met, and a row is read once and kept, so a call pays for the
 * vertices it reaches and the rows it reads, never for the rest of the database. Not thread-safe: one instance per call.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class LocalAdjacency implements EdgeListScanner.EntryVisitor {
  private static final int[]    EMPTY    = new int[0];
  private static final String[] NO_TYPES = new String[0];

  private final DatabaseInternal  database;
  private final EdgeListScanner   scanner;
  private final String[]          edgeTypes;
  private final boolean           typed;
  private final EdgeBucketMask    typeMask;
  private final CommandContext    context;
  private final Map<RID, Integer> nodes = new HashMap<>();
  private       RID[]             rids  = new RID[16];
  // ONE ROW CACHE PER DIRECTION, INDEXED BY Vertex.DIRECTION.ordinal(), CREATED ON FIRST USE
  private final int[][][]         rows  = new int[3][][];
  // PER DIRECTION (BY ORDINAL): 0 NOT ASKED YET, 1 THE ENTRIES ANSWER, 2 THE EDGES ANSWER (THE INCOMING SIDE OF A UNIDIRECTIONAL TYPE)
  private final byte[]            incomingFromEdges = new byte[3];
  private       int               nodeCount;
  private       long              entryCount;
  // THE EDGE BUCKET WHOSE READ PERMISSION WAS ASKED LAST: ENTRIES COME IN RUNS OF ONE BUCKET
  private       int               lastReadableEdgeBucketId = -1;

  // THE ROW BEING READ: THE FAR ENDS OF THE ENTRIES THE TYPE FILTER ACCEPTED, AS (BUCKET, POSITION)
  private int[]  farBuckets   = new int[16];
  private long[] farPositions = new long[16];
  private int    farCount;

  /**
   * @param database  the database
   * @param edgeTypes the edge types to follow, subtypes included; null or empty for all of them
   * @param context   the call's context, which the incoming side of the unidirectional types is read through and shares its scans
   *                  with; may be null when no unidirectional type can be involved
   */
  public LocalAdjacency(final DatabaseInternal database, final String[] edgeTypes, final CommandContext context) {
    this.database = database;
    this.scanner = new EdgeListScanner(database);
    this.edgeTypes = edgeTypes != null ? edgeTypes : NO_TYPES;
    this.typed = this.edgeTypes.length > 0;
    this.typeMask = typed ? EdgeBucketMask.of(database, this.edgeTypes) : null;
    this.context = context;
  }

  /**
   * The node number of the vertex {@code rid}, numbering it when it is met for the first time; -1 when no vertex lives there (a
   * record deleted, a RID not saved yet).
   */
  public int nodeOf(final RID rid) {
    if (rid == null || rid.getBucketId() < 0 || rid.getPosition() < 0)
      return -1;
    final Integer known = nodes.get(rid);
    if (known != null)
      return known;
    final int node = database.existsRecord(rid) ? register(rid) : -1;
    nodes.put(rid, node);
    return node;
  }

  /** The RID of node {@code node}. */
  public RID ridOf(final int node) {
    return rids[node];
  }

  /** How many vertices were met so far, rows read or not. */
  public int nodeCount() {
    return nodeCount;
  }

  /** How many neighbour entries the rows read so far hold, all directions together. */
  public long entryCount() {
    return entryCount;
  }

  /**
   * The neighbours of {@code node} in {@code direction}, as node numbers: read from the records the first time and kept. The array
   * is shared: do not modify it.
   */
  public int[] row(final int node, final Vertex.DIRECTION direction) {
    int[][] cache = rows[direction.ordinal()];
    if (cache == null || cache.length < rids.length) {
      cache = cache == null ? new int[rids.length][] : Arrays.copyOf(cache, rids.length);
      rows[direction.ordinal()] = cache;
    }
    int[] row = cache[node];
    if (row == null) {
      row = read(rids[node], direction);
      // THE READ CAN NUMBER NEW NODES AND GROW THE CACHES: STORED IN THE CURRENT ONE
      rows[direction.ordinal()][node] = row;
      entryCount += row.length;
    }
    return row;
  }

  private int register(final RID rid) {
    if (nodeCount == rids.length) {
      rids = Arrays.copyOf(rids, nodeCount * 2);
      for (int d = 0; d < rows.length; d++)
        if (rows[d] != null)
          rows[d] = Arrays.copyOf(rows[d], rids.length);
    }
    rids[nodeCount] = rid;
    return nodeCount++;
  }

  private int[] read(final RID vertex, final Vertex.DIRECTION direction) {
    if (typed && typeMask == null)
      // NONE OF THE TYPES ASKED FOR HAS A BUCKET: NO ENTRY CAN MATCH
      return EMPTY;
    farCount = 0;
    if (readsEdges(direction))
      readEdges(vertex, direction);
    else {
      if (direction != Vertex.DIRECTION.IN)
        scanner.scan(vertex.getBucketId(), vertex.getPosition(), Vertex.DIRECTION.OUT, this);
      if (direction != Vertex.DIRECTION.OUT)
        scanner.scan(vertex.getBucketId(), vertex.getPosition(), Vertex.DIRECTION.IN, this);
    }

    // RESOLVED AFTER THE SCAN, NOT IN visit(): ASKING WHETHER A VERTEX EXISTS READS A PAGE, WHICH THE SCANNER'S READ IS NOT THE PLACE FOR
    final int[] row = new int[farCount];
    int size = 0;
    for (int i = 0; i < farCount; i++) {
      final int neighbour = nodeOf(new RID(farBuckets[i], farPositions[i]));
      if (neighbour >= 0)
        row[size++] = neighbour;
    }
    return size == row.length ? row : Arrays.copyOf(row, size);
  }

  /** Whether a row in {@code direction} reaches the incoming side of a unidirectional type: asked once per direction. */
  private boolean readsEdges(final Vertex.DIRECTION direction) {
    byte answer = incomingFromEdges[direction.ordinal()];
    if (answer == 0) {
      answer = IncomingEdgeLookup.isNeeded(context, database, direction, edgeTypes) ? (byte) 2 : (byte) 1;
      incomingFromEdges[direction.ordinal()] = answer;
    }
    return answer == 2;
  }

  /** The row of a vertex whose incoming side over a unidirectional type is stored on the other end: read off the edges. */
  private void readEdges(final RID vertexRid, final Vertex.DIRECTION direction) {
    final Vertex vertex;
    try {
      vertex = vertexRid.asVertex();
    } catch (final RecordNotFoundException e) {
      return;
    }
    for (final Edge edge : (Iterable<Edge>) () -> IncomingEdgeLookup.getEdges(context, vertex, direction, edgeTypes)) {
      final RID far;
      try {
        far = GraphEngine.neighborRid(edge, vertexRid, direction);
      } catch (final RecordNotFoundException rnf) {
        GhostEdgeReporter.reportSkipped(rnf);
        continue;
      }
      if (far != null)
        addFar(far.getBucketId(), far.getPosition());
    }
  }

  @Override
  public void visit(final int edgeBucketId, final long edgePosition, final int vertexBucketId, final long vertexPosition) {
    if (typed && !typeMask.matches(edgeBucketId))
      return;
    if (edgePosition >= 0 && edgeBucketId != lastReadableEdgeBucketId) {
      // WHAT LOADING THE EDGE ASKED: A USER WHO CANNOT READ THE EDGE TYPE IS REFUSED, NOT HANDED ITS TOPOLOGY
      database.checkPermissionsOnFile(edgeBucketId, SecurityDatabaseUser.ACCESS.READ_RECORD);
      lastReadableEdgeBucketId = edgeBucketId;
    }
    addFar(vertexBucketId, vertexPosition);
  }

  private void addFar(final int bucketId, final long position) {
    if (farCount == farBuckets.length) {
      farBuckets = Arrays.copyOf(farBuckets, farCount * 2);
      farPositions = Arrays.copyOf(farPositions, farCount * 2);
    }
    farBuckets[farCount] = bucketId;
    farPositions[farCount] = position;
    farCount++;
  }
}
