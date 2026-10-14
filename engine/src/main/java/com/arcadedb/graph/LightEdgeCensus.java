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
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.LongObjectHashMap;
import com.arcadedb.utility.RidIntHashMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * The twins of every lightweight edge of ONE edge list, counted in one walk and shared by every lightweight edge read
 * off that list by one iteration (issue #10008).
 * <p>
 * {@link LightEdgeRID#getOccurrence()} answers "which of the entries sharing my triple am I" by walking the list it was
 * read from, once per edge. That is free where it was born - the relationship-uniqueness checks of a pattern, which
 * walk a list while that same list is being iterated - but a value operation ({@code DISTINCT r}, {@code r1 = r2},
 * the dedup of {@code DELETE}) asks it once per ROW, and the rows of an undirected expansion of a vertex all come
 * off that vertex's list: one walk each is O(degree²) per vertex, which on a super-node is not a constant factor any
 * more. This census turns those walks into one: the second time an edge of the iteration needs its occurrence, the
 * whole list is walked once and every later question is a hash lookup.
 * <p>
 * The numbering is the one {@link LightEdgeRID} uses when it walks alone - entries of a triple counted in chain order,
 * generation after generation of a striped list - so an edge resolved by the census and one resolved by its own walk
 * agree. The first question is left to the edge's own walk ({@link #resolve} declines it): a list asked only once
 * would pay a full walk of every stripe for an answer a walk of one stripe gives.
 * <p>
 * <b>A census is the list as it was when it was counted.</b> That is what an edge read before a change of the list
 * needs: its origin (chunk, position) names the entry it was read from in THAT list, and a walk made after an entry
 * before it was removed (the entries shift back) would find another entry at that position. An answer is only given
 * when it can be checked against the census: every lightweight entry is recorded with its position and its triple,
 * and an edge whose origin held another triple when the list was counted, or no entry at all (the list changed since:
 * an entry was added before it, or it was written after the count), is declined and walks its list alone, which answers
 * for the list as it is now.
 * <p>
 * Memory is O(lightweight entries of the list) in primitive arrays and maps, built in one walk: about 24 bytes an entry
 * for its position, triple and occurrence, and one int per distinct (edge type, neighbour) for the counts. It lives as
 * long as an edge read by the iteration does: a query that keeps such edges (a {@code collect}, the keys of a
 * {@code DISTINCT}, an {@code ORDER BY} buffer) keeps the census of every list they came from - for a super-node, the
 * same order as the edges of it the query keeps.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LightEdgeCensus {
  private static final AtomicBoolean FAILURE_WARNED = new AtomicBoolean();
  private final DatabaseInternal                 database;
  private final RID                              owner;
  private final Vertex.DIRECTION                 direction;
  // GUARDED BY THIS: QUESTIONS ASKED BEFORE THE BUILD, AND WHETHER THE BUILD FAILED (THE LIST COULD NOT BE READ: EVERY
  // EDGE THEN WALKS ALONE, RATHER THAN EACH OF THEM FAILING THE SAME WALK OF THE WHOLE LIST AGAIN)
  private          int     requests;
  private volatile boolean failed;
  // THE COUNTS, PUBLISHED ONCE BUILT AND NEVER CHANGED AFTER: READ WITHOUT THE MONITOR
  private volatile Counts  counts;

  /**
   * What the census counted: {@code twinsByType}, edge type bucket -> neighbour -> how many entries of the triple the
   * list holds; {@code chunks}, chunk -> its lightweight entries, the triple each holds and which of its twins it is.
   */
  private record Counts(LongObjectHashMap<RidIntHashMap> twinsByType, Map<RID, ChunkEntries> chunks, boolean anyTwin) {
  }

  /**
   * The lightweight entries of one chunk as the census read them, in parallel primitive arrays ordered by byte position
   * (the order a chunk is read in): where each entry is, the triple it holds and its occurrence. EVERY answer is checked
   * against them, a triple counted once included: the list can change after it was counted (an entry added shifts the
   * entries after it), and an edge read off it afterwards must not be taken for the entry that used to be at its position.
   */
  private static final class ChunkEntries {
    // THE CHUNK'S USED BYTES WHEN IT WAS COUNTED. AN EDGE READ OFF IT AT ANOTHER SIZE (AN ENTRY REMOVED SHIFTS THE ONES
    // AFTER IT, SO A TWIN OF THE SAME TRIPLE CAN LAND ON A COUNTED POSITION) IS NOT ANSWERED FROM IT; AN EDGE READ BEFORE A
    // CHANGE THAT CAME AFTER THE COUNT STILL IS, BY THE NUMBERING OF THE LIST IT WAS READ FROM
    private int    usedAtCount = -1;
    private int    size;
    private int[]  positions       = new int[8];
    private int[]  edgeBuckets     = new int[8];
    private int[]  neighborBuckets = new int[8];
    private long[] neighborOffsets = new long[8];
    private int[]  occurrences     = new int[8];

    private void add(final int position, final int edgeBucket, final RID neighbor, final int occurrence) {
      if (size == positions.length) {
        final int capacity = size * 2;
        positions = Arrays.copyOf(positions, capacity);
        edgeBuckets = Arrays.copyOf(edgeBuckets, capacity);
        neighborBuckets = Arrays.copyOf(neighborBuckets, capacity);
        neighborOffsets = Arrays.copyOf(neighborOffsets, capacity);
        occurrences = Arrays.copyOf(occurrences, capacity);
      }
      positions[size] = position;
      edgeBuckets[size] = edgeBucket;
      neighborBuckets[size] = neighbor.getBucketId();
      neighborOffsets[size] = neighbor.getPosition();
      occurrences[size] = occurrence;
      ++size;
    }

    /** @return the occurrence of the entry at {@code position} when it holds the triple, -1 otherwise */
    private int occurrenceOf(final int position, final int edgeBucket, final RID neighbor) {
      final int i = Arrays.binarySearch(positions, 0, size, position);
      if (i < 0 || edgeBuckets[i] != edgeBucket || neighborBuckets[i] != neighbor.getBucketId()
          || neighborOffsets[i] != neighbor.getPosition())
        return -1;
      return occurrences[i];
    }
  }

  LightEdgeCensus(final DatabaseInternal database, final RID owner, final Vertex.DIRECTION direction) {
    this.database = database;
    this.owner = owner;
    this.direction = direction;
  }

  /**
   * Numbers {@code edge} from the census, building it on the second question of the iteration.
   * <p>
   * Once built, the counts are read without any lock. The build itself is synchronized: the rows of one iteration can be
   * resolved by more than one thread (a parallel aggregation), and without the monitor each of them would walk the whole
   * list to build the same census. The walk runs once per list read, so a thread that waits on it waits for what it would
   * otherwise have done itself.
   *
   * @return false when the census declines and the edge has to walk its list alone
   */
  boolean resolve(final LightEdgeRID edge, final RID segment, final int position, final int chunkUsed) {
    Counts c = counts;
    if (c == null) {
      c = buildOnSecondQuestion();
      if (c == null)
        return false;
    }

    final RID neighbor = direction == Vertex.DIRECTION.OUT ? edge.getInRID() : edge.getOutRID();
    final ChunkEntries chunk = c.chunks.get(segment);
    if (chunk == null || chunk.usedAtCount != chunkUsed)
      // THE CHUNK HOLDS NO ENTRY THE CENSUS COUNTED, OR THE EDGE WAS READ OFF ANOTHER IMAGE OF IT (AN ENTRY REMOVED OR
      // ADDED BETWEEN THE COUNT AND THE READ): ONLY A WALK OF THE LIST AS IT IS NOW CAN SAY
      return false;
    final int occurrence = chunk.occurrenceOf(position, edge.getBucketId(), neighbor);
    if (occurrence < 0)
      // THE ORIGIN DID NOT HOLD THIS TRIPLE WHEN THE LIST WAS COUNTED: THE EDGE WAS READ AFTER THE LIST CHANGED, AND ONLY
      // A WALK OF THE LIST AS IT IS NOW CAN SAY
      return false;
    // EVERY RECORDED ENTRY WAS COUNTED IN twinsByType: THE CHECKS BELOW ARE FOR AN INVARIANT, NOT FOR A CASE
    final RidIntHashMap byNeighbor = c.twinsByType.get(edge.getBucketId());
    final int twins = byNeighbor != null ? byNeighbor.get(neighbor, 0) : 0;
    if (twins <= occurrence)
      return false;
    edge.numbered(occurrence, twins);
    return true;
  }

  /** Whether the list held twins when the census counted it. For tests. */
  boolean hasTwins() {
    final Counts c = counts;
    return c != null && c.anyTwin;
  }

  /** Whether the census was counted. For tests. */
  boolean isBuilt() {
    return counts != null;
  }

  /** Whether counting the list failed, so that every edge walks alone. For tests. */
  boolean hasFailed() {
    return failed;
  }

  /** @return the counts, built now if this is the second question; null while the census declines */
  private synchronized Counts buildOnSecondQuestion() {
    if (counts != null)
      return counts;
    if (failed || ++requests < 2)
      return null;
    final Counts built = build();
    if (built == null)
      failed = true;
    else
      counts = built;
    return built;
  }

  /**
   * One walk of the list: every lightweight entry is recorded with its triple and its occurrence, which is how many
   * entries of the triple came before it in chain order - the numbering the edge's own walk gives.
   */
  private Counts build() {
    final LongObjectHashMap<RidIntHashMap> perType = new LongObjectHashMap<>();
    final Map<RID, ChunkEntries> chunks = new HashMap<>();
    final ListCounts listCounts = new ListCounts();
    try {
      final VertexInternal vertex = (VertexInternal) database.lookupByRID(owner, false);
      LightEdgeRID.walkList(database, vertex, direction, null, (segment, position, edgeRID, neighbor) -> {
        if (edgeRID.getPosition() >= 0)
          return;
        RidIntHashMap byNeighbor = perType.get(edgeRID.getBucketId());
        if (byNeighbor == null) {
          byNeighbor = new RidIntHashMap();
          perType.put(edgeRID.getBucketId(), byNeighbor);
        }
        final int before = byNeighbor.get(neighbor, 0);
        byNeighbor.put(neighbor, before + 1);
        if (before > 0)
          listCounts.anyTwin = true;
        if (listCounts.chunk == null || !segment.equals(listCounts.chunkRID)) {
          listCounts.chunkRID = segment;
          listCounts.chunk = chunks.computeIfAbsent(segment, k -> new ChunkEntries());
        }
        listCounts.chunk.add(position, edgeRID.getBucketId(), neighbor, before);
      });
      // EACH CHUNK'S SIZE, READ AFTER THE WALK THAT READ ITS ENTRIES (THE SAME TRANSACTION, THE SAME IMAGE)
      for (final Map.Entry<RID, ChunkEntries> entry : chunks.entrySet())
        entry.getValue().usedAtCount = ((EdgeSegment) database.lookupByRID(entry.getKey(), true)).getUsed();
      return new Counts(perType, chunks, listCounts.anyTwin);
    } catch (final RuntimeException e) {
      // NOT ONLY A RECORD GONE: ANY FAILURE TO READ THE LIST. THE CENSUS IS GIVEN UP ON, AND EACH EDGE WALKS ITS LIST ALONE,
      // WHERE A FAILURE THAT IS NOT TRANSIENT SURFACES AS IT DID BEFORE THE CENSUS EXISTED
      // THE FIRST ONE OF THE PROCESS AT WARNING, THE OTHERS AT FINE: A LIST THAT CANNOT BE READ IS WORTH SEEING, BUT NOT
      // ONCE PER VERTEX OF EVERY QUERY THAT MEETS IT
      LogManager.instance().log(this, FAILURE_WARNED.compareAndSet(false, true) ? Level.WARNING : Level.FINE,
          "Cannot count the lightweight edges of %s (%s), each edge walks its list alone: %s", owner, direction, e.toString());
      return null;
    }
  }

  /** What the walk carries from one entry to the next. */
  private static final class ListCounts {
    private boolean      anyTwin;
    private RID          chunkRID;
    private ChunkEntries chunk;
  }
}
