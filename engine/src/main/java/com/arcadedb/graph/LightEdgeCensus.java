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
import com.arcadedb.utility.IntIntHashMap;
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
 * Memory is one int per distinct (edge type, neighbour) of the list's lightweight entries, held as primitive maps, plus
 * one entry per TWIN entry. It lives as long as an edge read by the iteration does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class LightEdgeCensus {
  private final DatabaseInternal               database;
  private final RID                            owner;
  private final Vertex.DIRECTION               direction;
  private       int                            requests;
  private       boolean                        built;
  // EDGE TYPE BUCKET -> NEIGHBOUR -> HOW MANY ENTRIES OF THE TRIPLE THE LIST HOLDS
  private       Map<Integer, RidIntHashMap>    twinsByType;
  // CHUNK -> BYTE POSITION -> OCCURRENCE, FOR THE ENTRIES OF A TRIPLE WITH TWINS ONLY
  private       Map<RID, IntIntHashMap>        occurrences;

  LightEdgeCensus(final DatabaseInternal database, final RID owner, final Vertex.DIRECTION direction) {
    this.database = database;
    this.owner = owner;
    this.direction = direction;
  }

  /**
   * Numbers {@code edge} from the census, building it on the second question of the iteration.
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
    if (twins <= 1) {
      edge.numbered(0, twins);
      return true;
    }

    final IntIntHashMap inChunk = occurrences != null ? occurrences.get(segment) : null;
    final int occurrence = inChunk != null ? inChunk.get(position, -1) : -1;
    if (occurrence < 0)
      // THE ENTRY IS NOT IN THE LIST ANY MORE: THE SAME ANSWER THE EDGE'S OWN WALK GIVES
      edge.numbered(0, 0);
    else
      edge.numbered(occurrence, twins);
    return true;
  }

  private boolean build() {
    final Map<Integer, RidIntHashMap> counts = new HashMap<>();
    try {
      final VertexInternal vertex = (VertexInternal) database.lookupByRID(owner, false);
      final boolean[] anyTwin = new boolean[1];
      LightEdgeRID.walkList(database, vertex, direction, null, (segment, position, edgeRID, neighbor) -> {
        if (edgeRID.getPosition() >= 0)
          return;
        final RidIntHashMap byNeighbor = counts.computeIfAbsent(edgeRID.getBucketId(), k -> new RidIntHashMap());
        final int before = byNeighbor.get(neighbor, 0);
        byNeighbor.put(neighbor, before + 1);
        if (before > 0)
          anyTwin[0] = true;
      });

      if (anyTwin[0]) {
        // SECOND PASS, ONLY FOR A LIST THAT HOLDS TWINS: NUMBER THE ENTRIES OF EVERY TWINNED TRIPLE IN CHAIN ORDER
        final Map<Integer, RidIntHashMap> seen = new HashMap<>();
        final Map<RID, IntIntHashMap> numbered = new HashMap<>();
        LightEdgeRID.walkList(database, vertex, direction, null, (segment, position, edgeRID, neighbor) -> {
          if (edgeRID.getPosition() >= 0)
            return;
          if (counts.get(edgeRID.getBucketId()).get(neighbor, 0) <= 1)
            return;
          final RidIntHashMap seenOfType = seen.computeIfAbsent(edgeRID.getBucketId(), k -> new RidIntHashMap());
          final int occurrence = seenOfType.get(neighbor, 0);
          seenOfType.put(neighbor, occurrence + 1);
          numbered.computeIfAbsent(segment, k -> new IntIntHashMap()).put(position, occurrence);
        });
        occurrences = numbered;
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
}
