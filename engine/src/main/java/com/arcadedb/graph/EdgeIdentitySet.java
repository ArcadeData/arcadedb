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

import com.arcadedb.database.RID;
import com.arcadedb.utility.RidHashSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Set of edge identities, used wherever a traversal has to answer "have I already walked this edge?" - Cypher
 * relationship uniqueness, the BOTH-direction dedup, the path procedures.
 * <p>
 * Record-backed edges keep the existing zero-boxing {@link RidHashSet} path: their identity is the (bucket, offset)
 * pair and nothing else, so it flattens to primitives with no allocation.
 * <p>
 * A lightweight edge cannot: it has no record, so its bucket/offset pair is a type marker shared by every lightweight
 * edge of the type, and flattening it would make them all the same key - which is what silently truncated multi-hop
 * traversals over lightweight edges. Its identity lives in the {@link LightEdgeRID} endpoints, so those go into a
 * plain object set that honours {@link RID#equals}. That set is allocated lazily, so a graph with no lightweight
 * edges never pays for it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EdgeIdentitySet {
  private final RidHashSet           recordBacked = new RidHashSet();
  private       Map<RID, LightUsage> recordLess;

  /**
   * @return true if the set did not already contain the edge
   */
  public boolean add(final RID edgeRID) {
    if (edgeRID.getPosition() >= 0)
      return recordBacked.add(edgeRID);

    if (recordLess == null)
      recordLess = new HashMap<>();
    final LightUsage usage = recordLess.get(edgeRID);
    if (usage == null) {
      recordLess.put(edgeRID, new LightUsage(edgeRID));
      return true;
    }
    return usage.add(edgeRID);
  }

  public boolean contains(final RID edgeRID) {
    if (edgeRID.getPosition() >= 0)
      return recordBacked.contains(edgeRID);

    if (recordLess == null)
      return false;
    final LightUsage usage = recordLess.get(edgeRID);
    return usage != null && usage.contains(edgeRID);
  }

  public int size() {
    int size = recordBacked.size();
    if (recordLess != null)
      for (final LightUsage usage : recordLess.values())
        size += usage.size();
    return size;
  }

  public boolean isEmpty() {
    return size() == 0;
  }

  public void clear() {
    recordBacked.clear();
    if (recordLess != null)
      recordLess.clear();
  }

  private static int occurrenceOf(final RID edgeRID) {
    return edgeRID instanceof LightEdgeRID light ? light.getOccurrence() : 0;
  }

  /**
   * The lightweight edges of one (type, out, in) triple that the set holds. The common case is one edge, kept as the
   * RID it came with and resolved to nothing: only when a second edge with the same triple arrives is it asked which
   * of the entries of the triple each one is (issue #9573), and only then does the set learn about their numbers.
   */
  private static final class LightUsage {
    private final RID          first;
    private       Set<Integer> occurrences;

    private LightUsage(final RID first) {
      this.first = first;
    }

    private boolean add(final RID edgeRID) {
      if (occurrences == null) {
        // AN EDGE NOT READ OFF A LIST HAS OCCURRENCE 0, SO IT IS THE FIRST ENTRY OF ITS TRIPLE: THE IDENTITY IT HAD BEFORE
        // OCCURRENCES EXISTED. IF THE LIST CHANGED UNDER THE QUERY THAT CAN UNDER-COUNT, NEVER OVER-COUNT
        if (LightEdgeRID.isSameEdge(first, edgeRID))
          return false;
        occurrences = new HashSet<>();
        occurrences.add(occurrenceOf(first));
      }
      return occurrences.add(occurrenceOf(edgeRID));
    }

    private boolean contains(final RID edgeRID) {
      return occurrences == null ? LightEdgeRID.isSameEdge(first, edgeRID) : occurrences.contains(occurrenceOf(edgeRID));
    }

    private int size() {
      return occurrences == null ? 1 : occurrences.size();
    }
  }
}
