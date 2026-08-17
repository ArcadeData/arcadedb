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
import com.arcadedb.utility.LongHashSet;
import com.arcadedb.utility.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Answers the back-reference probes {@link GraphDatabaseChecker} makes, remembering the adjacency lists it had to
 * walk (issue #6062).
 * <p>
 * WHAT IT REPLACES, and why it is worth a class of its own. The checker asks two questions of a NEIGHBOUR's
 * adjacency list, both once per edge and both O(degree):
 * <ul>
 *   <li>{@code checkEdges} asks both endpoints of every edge record whether their list names it
 *   ({@code EdgeLinkedList.containsEdge} - a linear walk of the chunk chain, or of every stripe chain for a promoted
 *   super-node);</li>
 *   <li>{@code checkVertices} asks the far vertex of every adjacency entry whether it points back
 *   ({@code Vertex.isConnectedTo} → {@code EdgeLinkedList.containsVertex} - the same walk, rejecting on the
 *   neighbour pointer instead of the edge one).</li>
 * </ul>
 * Nothing was remembered between two probes of the SAME list, so a hub of degree D was walked D times by each of
 * them: O(D²) on the vertex the whole point of the check is to survive, with every walk risking a page read once the
 * graph exceeds the page cache. A 657GB database with hubs in the hundreds of thousands of edges measured 80 hours
 * for one {@code CHECK DATABASE FIX}.
 * <p>
 * WHAT IT DOES. The first probe of a (vertex, direction) list materialises it into primitive hash sets, and every
 * later probe of that list is a hash lookup. The whole run therefore walks each list ONCE rather than once per
 * incident edge, which takes the probe cost of a pass from {@code sum(degree²)} to {@code sum(degree)}.
 * <p>
 * EXACT, never approximate. A bloom/sketch answer would be worse than useless here: a false positive reads as "the
 * back-reference is there" and silently drops a real finding, and under {@code FIX DELETE ORPHANS} the same answer
 * decides whether a record is deleted. Every structure below is an exact set, and anything that cannot be
 * represented exactly falls back to the original walk rather than being guessed at.
 * <p>
 * BOUNDED, on two axes because neither implies the other: at most {@code maxEntries} adjacency entries across all
 * cached lists, AND at most {@link #MAX_CACHED_LISTS} lists - a list the cache could not represent is remembered as
 * such, and such an entry carries no adjacency entries for the first bound to see. Eviction is
 * least-recently-probed on both, so a hub - the list that pays for itself - is the one that stays, and a single list
 * bigger than the budget is never cached at all -
 * such a list is answered by the original {@code EdgeLinkedList} probe. {@code maxEntries <= 0} disables the cache
 * entirely and every probe takes that original path, which is the pre-#6062 behaviour and the escape hatch if the
 * memory is ever unwelcome.
 * <p>
 * A SELF-REFERENCING CHUNK ends the materialising walk rather than feeding it forever: the guard the direct
 * {@code containsEdge}/{@code containsVertex} walks always had is now in {@link EdgeVertexIterator} too, which is
 * what this class materialises through - and which the checker's own walk of a vertex's list goes through as well,
 * where the missing guard meant a check that never returned.
 * <p>
 * SCOPED TO ONE PASS OVER ONE TYPE, not to the whole run. {@code checkVertices}/{@code checkEdges} each build their
 * own, and {@code DatabaseChecker} calls them once per vertex/edge type, so a hub referenced from two source types
 * has its list materialised once per type pass rather than once per {@code CHECK DATABASE}. Deliberate: the images
 * are only valid while nothing writes to the lists, and each pass commits its own repairs, so a cache outliving a
 * pass would have to survive exactly the writes it cannot survive. The cost is a constant factor in the number of
 * types, not a return to the quadratic shape - each pass is still one walk per list.
 * <p>
 * STALENESS IS THE CALLER'S JOB, and there is exactly one caller. A cached image is only valid while the list it was
 * built from does not change. The edge pass writes nothing while it scans; the vertex pass prunes dangling entries,
 * and calls {@link #clear()} at that site rather than naming the lists it touched - see that method. Both drop the
 * cache before the post-scan repair loop. NOT thread-safe, and neither is the checker that owns it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class AdjacencyProbeCache {
  /**
   * Ceiling on how many LISTS the map holds, alongside the adjacency-entry budget - because the two bounds do not
   * imply each other. An entry that remembers "this list cannot be represented" carries no adjacency entries at all,
   * so the entry budget never evicts one, and on the database shape this class exists for - a damaged
   * multi-hundred-GB graph - there is one per damaged or oversized list. Left as a constant rather than a second
   * knob: at 65,536 lists the map costs single-digit MB whatever the entry budget is set to, which is small beside
   * the budget's own footprint and large enough that a run never thrashes on it.
   */
  private static final int MAX_CACHED_LISTS = 1 << 16;

  private final GraphEngine                 graphEngine;
  private final int                         maxEntries;
  /** Access-ordered, so the eldest entry is the least recently PROBED list - a hub therefore never evicts itself. */
  private final LinkedHashMap<Key, ListImage> images;
  /** Edge-type name to its (polymorphic) bucket file-ids: the filter {@code isConnectedTo} rebuilt on every call. */
  private final Map<String, int[]>          bucketFilters = new HashMap<>();

  private int  cachedEntries;
  private long probes;
  private long listWalks;
  private long entriesScanned;

  AdjacencyProbeCache(final GraphEngine graphEngine, final int maxEntries) {
    this.graphEngine = graphEngine;
    this.maxEntries = Math.max(0, maxEntries);
    this.images = new LinkedHashMap<>(16, 0.75f, true);
  }

  /**
   * Whether {@code vertex}'s list in {@code direction} names {@code edgeRID}: the {@code checkEdges} back-reference
   * probe. A vertex with no list at all answers false, exactly as the {@code inEdges == null} arm it replaces did.
   *
   * @throws RuntimeException whatever reading the list threw - an unreadable list is a finding of its own and the
   *                          caller reports it as such, never as a fault of the edge
   */
  boolean containsEdge(final VertexInternal vertex, final Vertex.DIRECTION direction, final RID edgeRID) {
    ++probes;

    final VertexInternal source = graphEngine.getMostUpdatedVertex(vertex);
    final EdgeLinkedList list = graphEngine.getEdgeHeadChunk(source, direction);
    if (list == null)
      return false;

    final ListImage image = imageOf(source.getIdentity(), direction, list, false);
    if (image != null)
      return image.containsEdge(edgeRID);

    ++listWalks;
    return list.containsEdge(edgeRID);
  }

  /**
   * Whether {@code vertex}'s list in {@code direction} reaches {@code neighbour} through an edge of {@code edgeType}:
   * the {@code checkVertices} back-reference probe, and the exact question
   * {@code GraphEngine.isVertexConnectedTo(vertex, neighbour, direction, edgeType)} answers - same single direction,
   * same polymorphic bucket filter, same "the entry's own neighbour pointer decides, no edge record is loaded".
   * <p>
   * MEMOISED PER CHAIN, not per list, and that is not an optimisation detail - it is what keeps the answer identical
   * on a promoted super-node. A neighbour-keyed probe of a {@link StripedEdgeList} does not read the whole list: it
   * reads only the stripe of each generation that can hold that neighbour, and it is STRICT, turning a stripe head
   * it cannot resolve into a retryable {@code ConcurrentModificationException} rather than skipping it (see
   * {@code StripedEdgeList.chainsForNeighbour}). Building the image from the whole list instead would silently skip
   * that chain and answer "not connected" where the engine promises to fail loudly. So the SELECTION stays with the
   * list - {@link EdgeLinkedList#chainsForNeighbourProbe} is the very method the probe uses - and only the walk of
   * each selected chain is remembered, keyed by that chain's head.
   */
  boolean isConnectedTo(final VertexInternal vertex, final Vertex.DIRECTION direction, final RID neighbour,
      final String edgeType) {
    ++probes;

    final VertexInternal source = graphEngine.getMostUpdatedVertex(vertex);
    final int[] bucketFilter = bucketFilterOf(source, edgeType);

    final EdgeLinkedList list = graphEngine.getEdgeHeadChunk(source, direction);
    if (list == null)
      return false;

    final List<EdgeLinkedList> chains = list.chainsForNeighbourProbe(neighbour);
    for (int i = 0; i < chains.size(); i++) {
      final EdgeLinkedList chain = chains.get(i);
      final ListImage image = chainImageOf(chain);
      if (image != null) {
        if (image.containsNeighbour(neighbour, bucketFilter))
          return true;
      } else {
        ++listWalks;
        if (chain.containsVertex(neighbour, bucketFilter))
          return true;
      }
    }
    return false;
  }

  /**
   * Forgets every image, because something wrote to an adjacency list. The caller's only invalidation tool on
   * purpose: a prune in the vertex pass deletes the edge record, which takes the edge out of BOTH endpoints' lists,
   * so "which lists did that change" cannot be answered without trusting the pointers the prune is happening
   * because of. Also called when a pass stops probing, to release the memory.
   */
  void clear() {
    images.clear();
    cachedEntries = 0;
  }

  /** Back-reference questions asked. */
  long getProbes() {
    return probes;
  }

  /**
   * WALKS of an adjacency list performed to answer those questions - not probes that needed one, which is the same
   * number everywhere except in one case worth stating because it is the case an operator stares at this counter
   * for. The FIRST probe of a list the cache turns out not to be able to represent counts TWO: the materialisation
   * attempt walks it up to the budget before abandoning, and the fallback then walks it for the answer. Both
   * happened, so both are counted; the verdict is remembered, so every later probe of that list counts one.
   */
  long getListWalks() {
    return listWalks;
  }

  /** Adjacency entries visited while materialising lists: the linear cost that replaced the quadratic one. */
  long getEntriesScanned() {
    return entriesScanned;
  }

  /**
   * The cached image of ONE CHAIN, keyed by its head segment. Used by the neighbour probe, whose selection of which
   * chains to read belongs to the list - see {@link #isConnectedTo}.
   *
   * @return null when the chain has no head to key on (a stripe directory rather than a chain) or is not
   * representable, and the caller must fall back to the original walk
   */
  private ListImage chainImageOf(final EdgeLinkedList chain) {
    if (maxEntries == 0)
      return null;

    final RID head = chain.getChainHeadRID();
    if (head == null)
      return null;

    return imageOf(new Key(head.getBucketId(), head.getPosition(), null), chain, true);
  }

  /**
   * The cached image of one list, materialising it on first use.
   *
   * @return null when the list is not representable in the cache (too big for the budget, or holding an entry the
   * primitive sets cannot store) and the caller must fall back to the original walk
   */
  private ListImage imageOf(final RID vertexRID, final Vertex.DIRECTION direction, final EdgeLinkedList list,
      final boolean neighbourImage) {
    if (maxEntries == 0)
      return null;

    return imageOf(new Key(vertexRID.getBucketId(), vertexRID.getPosition(), direction), list, neighbourImage);
  }

  private ListImage imageOf(final Key key, final EdgeLinkedList list, final boolean neighbourImage) {
    final ListImage cached = images.get(key);
    if (cached != null && cached.neighbourImage == neighbourImage)
      // A remembered FAILURE is a cache hit too, and the important one: without it a list the cache cannot
      // represent - one bigger than the whole budget above all - would be walked to the budget and abandoned once
      // per probe, which is strictly worse than never having cached anything.
      return cached.uncacheable ? null : cached;

    ++listWalks;
    final ListImage built = build(list, neighbourImage);

    if (cached != null)
      cachedEntries -= cached.entryCount;
    images.put(key, built != null ? built : ListImage.uncacheable(neighbourImage));
    if (built != null)
      cachedEntries += built.entryCount;
    // ALSO after storing a remembered failure: it holds no adjacency entries, so only the list-count bound can
    // ever evict one, and that bound is only tested here.
    evictUntilWithinBudget();
    return built;
  }

  /**
   * Walks the list once, indexing whichever of the two probe questions the caller asked. Only ONE of the two
   * structures is built: the vertex pass never asks the edge question of a list, nor the edge pass the neighbour
   * one, so building both would double the footprint for nothing.
   *
   * @return null when the walk exceeded the budget (this one list is bigger than the whole cache may hold) or met an
   * entry the primitive sets cannot represent
   */
  private ListImage build(final EdgeLinkedList list, final boolean neighbourImage) {
    final Map<Integer, LongHashSet> edges = neighbourImage ? null : new HashMap<>();
    final Map<Long, LongHashSet> neighbours = neighbourImage ? new HashMap<>() : null;

    int count = 0;
    final Iterator<Pair<RID, RID>> iterator = list.entryIterator();
    while (iterator.hasNext()) {
      final Pair<RID, RID> entry = iterator.next();
      ++entriesScanned;

      if (++count > maxEntries)
        // BIGGER THAN THE WHOLE BUDGET: caching it would evict everything else and still not fit. Answered by the
        // original walk instead, which also short-circuits on a hit rather than materialising the whole list.
        return null;

      final RID edgeRID = entry.getFirst();
      final RID vertexRID = entry.getSecond();
      if (edgeRID == null || vertexRID == null)
        // A CORRUPT ENTRY, which the checker reports through its own walk of this list. Not representable here, and
        // not worth a special case: the original probe skips it the same way whether or not it is cached.
        return null;

      final long edgePosition = edgeRID.getPosition();
      final long vertexPosition = vertexRID.getPosition();
      if (edgePosition == Long.MIN_VALUE || vertexPosition == Long.MIN_VALUE)
        // LongHashSet reserves Long.MIN_VALUE as its empty-slot sentinel. No RID the engine writes reaches it - a
        // lightweight edge uses -1 - but a set that silently could not hold a value would answer "not referenced"
        // for an edge that is, so the impossible case falls back rather than being assumed away.
        return null;

      if (neighbourImage)
        neighbours.computeIfAbsent(neighbourKey(edgeRID.getBucketId(), vertexRID.getBucketId()),
            k -> new LongHashSet()).add(vertexPosition);
      else
        edges.computeIfAbsent(edgeRID.getBucketId(), k -> new LongHashSet()).add(edgePosition);
    }

    return new ListImage(neighbourImage, count, edges, neighbours);
  }

  /**
   * Evicts least-recently-probed lists until BOTH bounds hold - the adjacency-entry budget and
   * {@link #MAX_CACHED_LISTS} - never the one just built. The second bound is what keeps the remembered failures in
   * check: they carry no adjacency entries, so the first bound alone would let them accumulate one per damaged list
   * for the whole pass.
   */
  private void evictUntilWithinBudget() {
    // size() > 1 is what protects the image just stored: the map is access-ordered and a put counts as an access,
    // so that one is the most recent and can only be the eldest when it is the only one left.
    while ((cachedEntries > maxEntries || images.size() > MAX_CACHED_LISTS) && images.size() > 1) {
      final Iterator<Map.Entry<Key, ListImage>> eldest = images.entrySet().iterator();
      cachedEntries -= eldest.next().getValue().entryCount;
      eldest.remove();
    }
  }

  /**
   * The polymorphic bucket file-ids of an edge type, memoised. {@code isVertexConnectedTo} rebuilt this through a
   * stream per call, so a hub probed once per incident edge paid an array allocation per edge on top of the walk.
   * <p>
   * Held for the life of the cache, which is one pass over one type. A bucket added to an edge type WHILE that pass
   * runs is therefore missed until the next pass, where the unmemoised form would have picked it up on the next
   * probe. Left as is: a bucket added mid-scan holds no edges the scan has already read past either, so the filter
   * is not the thing that would make such a run coherent - a schema change concurrent with a check is outside what
   * either form promises.
   */
  private int[] bucketFilterOf(final VertexInternal vertex, final String edgeType) {
    return bucketFilters.computeIfAbsent(edgeType,
        t -> vertex.getDatabase().getSchema().getType(t).getBuckets(true).stream().mapToInt(b -> b.getFileId())
            .toArray());
  }

  /** (edge bucket, neighbour bucket) packed into one key, so the type filter is applied without a nested map. */
  private static long neighbourKey(final int edgeBucketId, final int vertexBucketId) {
    return ((long) edgeBucketId << 32) | (vertexBucketId & 0xFFFFFFFFL);
  }

  /**
   * What a cached image belongs to. Either a whole list - the owning VERTEX's RID plus the direction, as the edge
   * probe reads it - or one chain of a striped list, its head SEGMENT's RID with a null direction. The two cannot
   * collide even without the direction: a vertex record and an edge-list segment never share a bucket.
   */
  private record Key(int bucketId, long position, Vertex.DIRECTION direction) {
  }

  /**
   * One materialised list. Positions are held in {@link LongHashSet} - a primitive open-addressing set - keyed by
   * bucket, so a super-node's list costs roughly 11 bytes an entry instead of the ~80 a {@code HashSet<RID>} would.
   */
  private static final class ListImage {
    /** The remembered verdict "this list cannot be represented"; see {@link #imageOf(Key, EdgeLinkedList, boolean)}. */
    private static final ListImage UNCACHEABLE_EDGES     = new ListImage(false, 0, null, null, true);
    private static final ListImage UNCACHEABLE_NEIGHBOURS = new ListImage(true, 0, null, null, true);

    private final boolean                   neighbourImage;
    private final boolean                   uncacheable;
    private final int                       entryCount;
    /** Edge bucket file-id to the positions of the edges in it. Null on a neighbour image. */
    private final Map<Integer, LongHashSet> edges;
    /** (edge bucket, neighbour bucket) to the neighbour positions. Null on an edge image. */
    private final Map<Long, LongHashSet>    neighbours;

    private ListImage(final boolean neighbourImage, final int entryCount, final Map<Integer, LongHashSet> edges,
        final Map<Long, LongHashSet> neighbours) {
      this(neighbourImage, entryCount, edges, neighbours, false);
    }

    private ListImage(final boolean neighbourImage, final int entryCount, final Map<Integer, LongHashSet> edges,
        final Map<Long, LongHashSet> neighbours, final boolean uncacheable) {
      this.neighbourImage = neighbourImage;
      this.entryCount = entryCount;
      this.edges = edges;
      this.neighbours = neighbours;
      this.uncacheable = uncacheable;
    }

    private static ListImage uncacheable(final boolean neighbourImage) {
      return neighbourImage ? UNCACHEABLE_NEIGHBOURS : UNCACHEABLE_EDGES;
    }

    private boolean containsEdge(final RID edgeRID) {
      final LongHashSet positions = edges.get(edgeRID.getBucketId());
      return positions != null && positions.contains(edgeRID.getPosition());
    }

    /**
     * Mirrors {@code MutableEdgeSegment.getFirstEdgeConnectedToVertex}: the entry matches when its neighbour is the
     * one asked about AND its edge sits in one of the filtered buckets. The filter is always concrete here - the
     * checker's probe always names an edge type, which is what {@link #bucketFilterOf} resolves - so there is no
     * accept-any-bucket branch to sweep the map for. An edge type with no buckets yields an empty filter and
     * therefore false, exactly as {@code containsVertex} does with the same array.
     */
    private boolean containsNeighbour(final RID neighbour, final int[] edgeBucketFilter) {
      final int vertexBucketId = neighbour.getBucketId();
      final long vertexPosition = neighbour.getPosition();

      for (int i = 0; i < edgeBucketFilter.length; i++) {
        final LongHashSet positions = neighbours.get(neighbourKey(edgeBucketFilter[i], vertexBucketId));
        if (positions != null && positions.contains(vertexPosition))
          return true;
      }
      return false;
    }
  }
}
