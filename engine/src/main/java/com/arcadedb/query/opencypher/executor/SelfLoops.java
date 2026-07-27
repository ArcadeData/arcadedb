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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * An undirected pattern hop must yield each relationship once, but ArcadeDB stores every edge in two
 * lists - the source's outgoing list and the target's incoming list - so a self-loop is reached twice
 * when both lists are walked. Both the step-based and the operator-based executors have to remove
 * exactly one of the two, and both do it here.
 * <p>
 * <b>Invariant:</b> a self-loop contributes exactly one entry to each list, so the number of
 * self-loop entries is always even, whether they come from the base CSR or from the delta overlay
 * (which appends to both {@code ovOut} and {@code ovIn}). See {@code GraphEngine.getVertices()},
 * BOTH case, for the structural guarantee.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SelfLoops {
  private SelfLoops() {
  }

  /**
   * Removes the duplicated half of the self-loop entries from a CSR neighbour array, preserving the
   * multiplicity of a vertex that carries several self-loops.
   *
   * @param neighborIds  neighbour node ids for a BOTH-direction expansion
   * @param sourceNodeId node id the expansion starts from
   *
   * @return the input array when there is nothing to remove, a compacted copy otherwise
   */
  public static int[] deduplicate(final int[] neighborIds, final int sourceNodeId) {
    int selfLoopCount = 0;
    for (final int id : neighborIds)
      if (id == sourceNodeId)
        selfLoopCount++;

    if (selfLoopCount <= 1)
      return neighborIds; // 0 or 1 entries: nothing was duplicated

    final int toRemove = selfLoopCount / 2;
    final int[] result = new int[neighborIds.length - toRemove];
    int written = 0;
    int skipped = 0;
    for (final int id : neighborIds) {
      if (id == sourceNodeId && skipped < toRemove) {
        skipped++;
        continue;
      }
      result[written++] = id;
    }
    return result;
  }

  /**
   * Wraps a BOTH-direction neighbour iterator so a self-loop yields its vertex once per relationship
   * instead of twice. Relies on the same invariant as {@link #deduplicate(int[], int)}: the merged
   * out+in iteration returns exactly two entries per self-loop, so every second one is dropped.
   *
   * @param neighbors  neighbour vertices produced by a BOTH-direction traversal
   * @param sourceRid  identity of the vertex the traversal starts from
   */
  public static Iterator<Vertex> deduplicating(final Iterator<Vertex> neighbors, final RID sourceRid) {
    return new Iterator<>() {
      private Vertex nextVertex   = null;
      private int    selfLoopSeen = 0;

      @Override
      public boolean hasNext() {
        if (nextVertex != null)
          return true;
        while (neighbors.hasNext()) {
          final Vertex candidate = neighbors.next();
          if (candidate.getIdentity().equals(sourceRid)) {
            selfLoopSeen++;
            if (selfLoopSeen % 2 == 0)
              continue; // the same self-loop already yielded this vertex from the other list
          }
          nextVertex = candidate;
          return true;
        }
        return false;
      }

      @Override
      public Vertex next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Vertex result = nextVertex;
        nextVertex = null;
        return result;
      }
    };
  }

  /**
   * Wraps a BOTH-direction edge iterator so a self-loop is yielded once per relationship instead of
   * once per adjacency list.
   * <p>
   * Unlike {@link #deduplicating(Iterator, RID)}, which only has to preserve the multiplicity of a
   * neighbour vertex and can therefore drop every second sighting, this variant must keep every
   * <i>distinct</i> relationship: with two parallel self-loops the merged iteration is
   * {@code loop1, loop2, loop1, loop2}, so a parity rule would keep {@code loop1} twice and lose
   * {@code loop2}. The identity of the already-yielded loops is tracked instead, and the set is
   * allocated only for a vertex that actually carries one, so the common case pays nothing.
   *
   * @param edges edges produced by a BOTH-direction traversal of a single vertex
   */
  public static Iterator<Edge> deduplicatingEdges(final Iterator<Edge> edges) {
    return new Iterator<>() {
      private Edge      nextEdge = null;
      private Set<RID>  emitted  = null;

      @Override
      public boolean hasNext() {
        if (nextEdge != null)
          return true;
        while (edges.hasNext()) {
          final Edge candidate = edges.next();
          if (candidate.getOut().equals(candidate.getIn())) {
            // A self-loop sits in both the outgoing and the incoming list of its vertex, so walking
            // both lists reaches the very same relationship twice.
            if (emitted == null)
              emitted = new HashSet<>();
            if (!emitted.add(candidate.getIdentity()))
              continue;
          }
          nextEdge = candidate;
          return true;
        }
        return false;
      }

      @Override
      public Edge next() {
        if (!hasNext())
          throw new NoSuchElementException();
        final Edge result = nextEdge;
        nextEdge = null;
        return result;
      }
    };
  }
}
