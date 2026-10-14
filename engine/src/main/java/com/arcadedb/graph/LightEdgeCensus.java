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
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.LongObjectHashMap;
import com.arcadedb.utility.RidIntHashMap;

import java.util.HashMap;
import java.util.Map;
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
 * when it can be checked against the census: an entry of a twinned triple is recorded with its triple, and an edge
 * whose origin holds another triple in the census, or whose triple the census never saw (written after it was counted),
 * is declined and walks its list alone, which answers for the list as it is now. A triple counted once has a single
 * relationship whatever position it was read at.
 * <p>
 * Memory is one int per distinct (edge type, neighbour) of the list's lightweight entries, in primitive maps, plus one
 * small entry per entry of a twinned triple (none at all for a list without twins, whose second pass is skipped). It
 * lives as long as an edge read by the iteration does: a query that keeps such edges (a {@code collect}, the keys of a
 * {@code DISTINCT}, an {@code ORDER BY} buffer) keeps the census of every list they came from, which for a super-node
 * is O(distinct neighbours) - the same order as the edges the query keeps.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LightEdgeCensus {
  private final DatabaseInternal                 database;
  private final RID                              owner;
  private final Vertex.DIRECTION                 direction;
  private       int                              requests;
  private       boolean                          built;
  // EDGE TYPE BUCKET -> NEIGHBOUR -> HOW MANY ENTRIES OF THE TRIPLE THE LIST HOLDS
  private       LongObjectHashMap<RidIntHashMap> twinsByType;
  // CHUNK -> BYTE POSITION -> THE ENTRY, FOR THE ENTRIES OF A TRIPLE WITH TWINS ONLY; NULL FOR A LIST WITHOUT TWINS
  private       Map<RID, LongObjectHashMap<Twin>> twinEntries;

  /** An entry of a twinned triple: the triple it holds, so an answer can be checked, and which of its twins it is. */
  private record Twin(int edgeBucketId, RID neighbor, int occurrence) {
  }

  LightEdgeCensus(final DatabaseInternal database, final RID owner, final Vertex.DIRECTION direction) {
    this.database = database;
    this.owner = owner;
    this.direction = direction;
  }

  /**
   * Numbers {@code edge} from the census, building it on the second question of the iteration.
   * <p>
   * Synchronized, the build included: the rows of one iteration can be resolved by more than one thread (a parallel
   * aggregation), and without the monitor each of them would walk the whole list to build the same census. The walk
   * runs once per list read, so the threads that wait on it wait for what they would otherwise each have done; every
   * later call is a lookup.
   *
   * @return false when the census declines and the edge has to walk its list alone
   */
  synchronized boolean resolve(final LightEdgeRID edge, final RID segment, final int position) {
    if (!built) {
      if (++requests < 2)
        return false;
      if (!build())
        return false;
    }

    final RidIntHashMap byNeighbor = twinsByType.get(edge.getBucketId());
    final RID neighbor = direction == Vertex.DIRECTION.OUT ? edge.getInRID() : edge.getOutRID();
    final int twins = byNeighbor != null ? byNeighbor.get(neighbor, 0) : 0;
    if (twins == 0)
      // A TRIPLE THE LIST DID NOT HOLD WHEN IT WAS COUNTED: ONLY A WALK OF THE LIST AS IT IS NOW CAN SAY
      return false;
    if (twins == 1) {
      edge.numbered(0, 1);
      return true;
    }

    final LongObjectHashMap<Twin> inChunk = twinEntries.get(segment);
    final Twin twin = inChunk != null ? inChunk.get(position) : null;
    if (twin == null || twin.edgeBucketId != edge.getBucketId() || !twin.neighbor.equals(neighbor))
      // THE ORIGIN HELD ANOTHER ENTRY WHEN THE LIST WAS COUNTED: THE EDGE WAS READ AFTER THE LIST CHANGED
      return false;
    edge.numbered(twin.occurrence, twins);
    return true;
  }

  /** Whether the census numbered twins, i.e. made its second pass. For tests. */
  synchronized boolean hasTwins() {
    return twinEntries != null;
  }

  /** Whether the census was counted. For tests. */
  synchronized boolean isBuilt() {
    return built;
  }

  private boolean build() {
    final LongObjectHashMap<RidIntHashMap> counts = new LongObjectHashMap<>();
    final ListCounts listCounts = new ListCounts();
    try {
      final VertexInternal vertex = (VertexInternal) database.lookupByRID(owner, false);
      LightEdgeRID.walkList(database, vertex, direction, null, (segment, position, edgeRID, neighbor) -> {
        if (edgeRID.getPosition() >= 0)
          return;
        RidIntHashMap byNeighbor = counts.get(edgeRID.getBucketId());
        if (byNeighbor == null) {
          byNeighbor = new RidIntHashMap();
          counts.put(edgeRID.getBucketId(), byNeighbor);
        }
        final int before = byNeighbor.get(neighbor, 0);
        byNeighbor.put(neighbor, before + 1);
        if (before > 0)
          listCounts.anyTwin = true;
      });

      if (listCounts.anyTwin) {
        // SECOND PASS, ONLY FOR A LIST THAT HOLDS TWINS: NUMBER THE ENTRIES OF EVERY TWINNED TRIPLE IN CHAIN ORDER
        final LongObjectHashMap<RidIntHashMap> seen = new LongObjectHashMap<>();
        final Map<RID, LongObjectHashMap<Twin>> entries = new HashMap<>();
        LightEdgeRID.walkList(database, vertex, direction, null, (segment, position, edgeRID, neighbor) -> {
          if (edgeRID.getPosition() >= 0 || counts.get(edgeRID.getBucketId()).get(neighbor, 0) <= 1)
            return;
          RidIntHashMap seenOfType = seen.get(edgeRID.getBucketId());
          if (seenOfType == null) {
            seenOfType = new RidIntHashMap();
            seen.put(edgeRID.getBucketId(), seenOfType);
          }
          final int occurrence = seenOfType.get(neighbor, 0);
          seenOfType.put(neighbor, occurrence + 1);
          entries.computeIfAbsent(segment, k -> new LongObjectHashMap<>())
              .put(position, new Twin(edgeRID.getBucketId(), neighbor, occurrence));
        });
        twinEntries = entries;
      }
    } catch (final RecordNotFoundException e) {
      LogManager.instance().log(this, Level.FINE, "Cannot count the lightweight edges of %s (%s): %s", owner, direction,
          e.getMessage());
      return false;
    }
    twinsByType = counts;
    built = true;
    return true;
  }

  /** What the first pass learns about the list as a whole. */
  private static final class ListCounts {
    private boolean anyTwin;
  }
}
