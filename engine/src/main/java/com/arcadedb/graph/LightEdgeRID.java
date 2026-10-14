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

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DatabaseRID;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Identity of a lightweight edge.
 * <p>
 * A lightweight edge has no record, so it has no address. Its bucket/offset pair is {@code #<edgeType first
 * bucket>:-1}: the bucket names the edge type and the negative offset says "there is nothing to load". That pair is
 * therefore shared by <b>every</b> lightweight edge of the type and cannot serve as an identity - which is what this
 * class supplies, by carrying the endpoints the edge connects.
 * <p>
 * The identity of a lightweight edge is the triple <b>(edge type, out vertex, in vertex)</b>, and nothing else: with no
 * properties there is no observable difference between two lightweight edges of the same type over the same ordered
 * pair, so they are the same edge. See the lightweight edge section of the documentation for what happens if an
 * application creates a second one anyway.
 * <p>
 * The bucket and offset are unchanged from what the edge-list chunk holds, so this class costs <b>nothing on disk</b>:
 * {@link com.arcadedb.graph.MutableEdgeSegment} writes {@link #getBucketId()} and {@link #getPosition()} exactly as
 * before. It replaces the throwaway marker RID that the traversal path already built for every lightweight edge it
 * materialised, and adds to it only the origin of the entry (the chunk, the position and the direction) and the
 * cached occurrence and twin count (about 20 bytes).
 *
 * <p>
 * Two lightweight edges of one type over the same ordered pair - an application mistake the <code>UNIQUE</code> flag
 * exists to prevent - are two entries in the edge lists and, to a query, two relationships (issue #9573). They share
 * this triple, so {@link #equals} cannot tell them apart and must not (the triple is also how one is located to be
 * deleted). What can is the <b>occurrence</b>: how many entries with the same triple precede this one in the edge list
 * it was read from. An iterator that reads an edge off a list records where (segment, position) for a few bytes; the
 * occurrence itself is resolved lazily, by re-walking the list, and only when {@link EdgeIdentitySet} meets a second
 * edge with the same triple - which is when the question "same entry or its twin?" has to be answered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LightEdgeRID extends DatabaseRID {
  /**
   * Offset stamped on every record-less RID. Negative by contract: {@link RID#equals} and the edge-list walk both use
   * {@code offset < 0} to mean "this RID addresses no record".
   */
  public static final long NO_RECORD_OFFSET = -1L;

  private final RID out;
  private final RID in;

  // WHERE THE EDGE WAS READ FROM, WHEN IT WAS READ OFF AN EDGE LIST: THE CHUNK, THE BYTE POSITION IN IT AND THE LIST'S
  // DIRECTION (WHICH NAMES THE VERTEX THAT OWNS IT). NULL FOR AN EDGE NOT READ OFF A LIST (JUST CREATED, REBUILT FROM
  // AN ENDPOINT PAIR): ITS OCCURRENCE IS THEN 0, THE IDENTITY IT ALWAYS HAD
  private RID              originSegment;
  private int              originPosition;
  private Vertex.DIRECTION originDirection;
  // THE USED BYTES OF THE CHUNK WHEN THE ENTRY WAS READ OFF IT, -1 WHEN NOT KNOWN: A CENSUS ANSWERS ONLY FOR THE CHUNK IT
  // COUNTED, AND A CHUNK OF ANOTHER SIZE IS ANOTHER IMAGE OF IT (ISSUE #10008)
  private int              originChunkUsed = -1;
  // THE TWINS OF THE WHOLE LIST, COUNTED ONCE FOR EVERY EDGE THE SAME ITERATION READ (ISSUE #10008). NULL WHEN THE
  // EDGE WAS NOT READ BY AN ITERATION THAT OFFERS ONE: IT THEN WALKS ITS LIST ALONE
  private LightEdgeCensus  census;
  // -1 = NOT RESOLVED YET. VOLATILE, AND ALWAYS WRITTEN AFTER twins, SO A THREAD THAT SEES A RESOLVED OCCURRENCE SEES
  // ITS TWIN COUNT TOO (THE RID IS HANDED BETWEEN THREADS WITH THE ROW THAT HOLDS IT)
  private volatile int     occurrence = -1;
  private int              twins;

  /**
   * Extends {@link DatabaseRID} rather than {@link RID} deliberately: {@code BaseRecord.upgradeRID} rebuilds any RID
   * that is not already a {@code DatabaseRID} from its bucket and offset alone, which would drop the endpoints and
   * put every lightweight edge of a type back on one shared identity.
   */
  public LightEdgeRID(final BasicDatabase database, final int edgeTypeBucketId, final RID out, final RID in) {
    super(database, edgeTypeBucketId, NO_RECORD_OFFSET);
    this.out = out;
    this.in = in;
  }

  /**
   * Remembers the list entry this identity was read from, so that {@link #getOccurrence()} can tell it from a twin.
   * Called once, by the iterator that built it, before the edge is handed out.
   */
  LightEdgeRID readFrom(final RID segment, final int position, final Vertex.DIRECTION direction, final int chunkUsed,
      final LightEdgeCensus census) {
    this.originSegment = segment;
    this.originPosition = position;
    this.originDirection = direction;
    this.originChunkUsed = chunkUsed;
    this.census = census;
    return this;
  }

  /** The chunk of the list entry this edge was read from, or null. For tests. */
  RID getOriginSegment() {
    return originSegment;
  }

  /** The byte position of the list entry this edge was read from in its chunk. For tests. */
  int getOriginPosition() {
    return originPosition;
  }

  /** The used bytes of the chunk when this edge was read off it, -1 when not known. For tests. */
  int getOriginChunkUsed() {
    return originChunkUsed;
  }

  /** The census this edge shares with the iteration that read it, or null. For tests. */
  LightEdgeCensus getCensus() {
    return census;
  }

  /**
   * For an edge not read off an edge list but off a snapshot that already sorts twins next to each other: which of
   * {@code twinCount} twins it is.
   */
  void numbered(final int occurrence, final int twinCount) {
    this.twins = twinCount;
    this.occurrence = occurrence;
  }

  /**
   * Which of the entries sharing this edge's (type, out, in) triple it is, counting from the start of the list it was
   * read from: 0 for the first (and, in the normal case of no duplicates, only) one.
   * <p>
   * Walks the list on the first call, O(degree of the owner), and caches the answer. It is meant to be asked only
   * when a second edge with the same triple turns up, never per edge. The walk visits entries in the same order the
   * iterators do, so the OUT list of the source and the IN list of the target - which hold the same entries appended
   * in the same order - number twins alike; and where they do not, the twins are indistinguishable anyway (no
   * properties), so any consistent numbering counts the same relationships.
   */
  public int getOccurrence() {
    int result = occurrence;
    if (result < 0) {
      resolve();
      result = occurrence;
    }
    return result;
  }

  /**
   * How many entries with this edge's triple the list it was read from holds: 1 when it has no twin, 0 when it was not
   * read off a list and so cannot say.
   */
  public int getTwinCount() {
    if (occurrence < 0)
      resolve();
    return twins;
  }

  /**
   * True when this edge already knows, without walking anything, that its triple has no twin in the list it was read
   * from: whatever other edge has the same triple is then the same relationship.
   */
  private boolean isKnownSingle() {
    return occurrence >= 0 && twins == 1;
  }

  private boolean sameListEntry(final LightEdgeRID other) {
    return originSegment != null && originPosition == other.originPosition && originDirection == other.originDirection
        && originSegment.equals(other.originSegment);
  }

  /**
   * Whether {@code candidate} is the same relationship as {@code used}: equal, and for lightweight edges that share
   * their triple, the same entry of it rather than a twin (issue #9573). Symmetric: the two arguments are named for
   * the pattern checks that call it, not because their order matters.
   * <p>
   * The only costly case is two distinct lightweight edge objects with one triple read through different lists - an
   * undirected walk meeting the edge from its other end, or a value operation ({@code =}, {@code DISTINCT}) comparing
   * the same relationship read twice. It is settled by whichever side already knows its triple has no twin, then by
   * one pass over the candidate's list (shared by every edge of the iteration that read it, see
   * {@link LightEdgeCensus}); the same entry of the same list is recognised without any pass.
   */
  public static boolean isSameEdge(final RID used, final RID candidate) {
    if (!used.equals(candidate))
      return false;
    if (used == candidate || used.getPosition() >= 0)
      return true;
    if (!(used instanceof LightEdgeRID usedLight) || !(candidate instanceof LightEdgeRID candidateLight))
      return true;
    // THE SAME ENTRY OF THE SAME LIST NEEDS NO WALK (A RE-READ OF THE EDGE JUST WALKED), AND NEITHER DOES A TRIPLE
    // ONE SIDE HAS ALREADY COUNTED
    if (usedLight.sameListEntry(candidateLight) || usedLight.isKnownSingle() || candidateLight.isKnownSingle())
      return true;
    final int candidateTwins = candidateLight.getTwinCount();
    if (candidateTwins == 1)
      return true;
    // 0: THE CANDIDATE CANNOT SAY (NOT READ OFF A LIST, OR NO LONGER IN IT). IT IS THEN THE FIRST ENTRY OF ITS TRIPLE,
    // WHICH IS ALL THE OTHER SIDE CAN BE TOO UNLESS IT KNOWS OF TWINS
    if (candidateTwins == 0 && usedLight.getTwinCount() <= 1)
      return true;
    return usedLight.getOccurrence() == candidateLight.getOccurrence();
  }

  /**
   * A key for hash-based duplicate elimination ({@code DISTINCT}, grouping, {@code UNION}) that is equal for the same
   * relationship and different for twins (issue #10008). It hashes like the RID, so only edges with the same triple
   * ever compare: the occurrence of an edge is resolved only when a second edge with its triple turns up, never per row.
   * <p>
   * Two keys compare by occurrence alone, not through the shortcuts of {@link #isSameEdge}: a shortcut answers from
   * whatever one side has already resolved, so its answer could depend on the order the keys met, and a hash set needs
   * an equality that is stable and transitive. An occurrence, once resolved, is cached on the RID, so every comparison
   * of a key answers the same.
   */
  public Object distinctKey() {
    return new DistinctKey(this);
  }

  private record DistinctKey(LightEdgeRID edge) {
    @Override
    public boolean equals(final Object obj) {
      if (!(obj instanceof DistinctKey other))
        return false;
      return edge == other.edge || (edge.equals(other.edge) && edge.getOccurrence() == other.edge.getOccurrence());
    }

    @Override
    public int hashCode() {
      return edge.hashCode();
    }

    @Override
    public String toString() {
      return edge.toString();
    }
  }

  /** Receives the entries of an edge list, in the order the iterators read them. */
  @FunctionalInterface
  interface ListEntryVisitor {
    void visit(RID segment, int position, RID edge, RID vertex);
  }

  /**
   * Walks the edge list of {@code vertex} in {@code direction}, chunk by chunk in the order the iterators read it. With
   * a {@code neighbor}, a striped (super-node) list is walked only on the chains that can hold an entry for it: one per
   * generation, the stripe the neighbour hashes to - twins share their neighbour, so they share a stripe. Without one,
   * every stripe of every generation is walked, generation after generation, which numbers the entries of any one
   * neighbour in the same order. This leans on {@link StripeDirectory#stripeOf} placing an entry by its neighbour alone:
   * a placement that also looked at the edge would have to be mirrored here.
   */
  static void walkList(final DatabaseInternal db, final VertexInternal vertex, final Vertex.DIRECTION direction,
      final RID neighbor, final ListEntryVisitor visitor) {
    final RID head = direction == Vertex.DIRECTION.OUT ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk();
    if (head == null)
      return;
    if (db.lookupByRID(head, true) instanceof StripeDirectory directory) {
      for (int g = 0; g < directory.getGenerationCount(); g++) {
        final int stripes = directory.getStripes(g);
        if (neighbor != null)
          walkChain(db, directory.getHead(g, StripeDirectory.stripeOf(neighbor, stripes)), visitor);
        else
          for (int stripe = 0; stripe < stripes; stripe++)
            walkChain(db, directory.getHead(g, stripe), visitor);
      }
    } else
      walkChain(db, head, visitor);
  }

  private static void walkChain(final DatabaseInternal db, final RID head, final ListEntryVisitor visitor) {
    if (head == null)
      return;
    RID segmentRID = head;
    final ChainCycleGuard guard = new ChainCycleGuard(segmentRID);
    final AtomicInteger cursor = new AtomicInteger();
    while (segmentRID != null) {
      final EdgeSegment segment = (EdgeSegment) db.lookupByRID(segmentRID, true);
      final RID segmentIdentity = segment.getIdentity();
      cursor.set(MutableEdgeSegment.CONTENT_START_POSITION);
      final int used = segment.getUsed();
      while (cursor.get() < used) {
        final int at = cursor.get();
        final RID entryEdge = segment.getRID(cursor);
        final RID entryVertex = segment.getRID(cursor);
        visitor.visit(segmentIdentity, at, entryEdge, entryVertex);
      }
      final RID previous = segment.getPreviousRID();
      segmentRID = previous == null || previous.equals(segmentIdentity) || guard.revisits(previous) ? null : previous;
    }
  }

  private void resolve() {
    if (originSegment == null) {
      numbered(0, 0);
      return;
    }
    if (census != null && census.resolve(this, originSegment, originPosition, originChunkUsed))
      return;

    final DatabaseInternal db = (DatabaseInternal) getBoundDatabase();
    final boolean outgoing = originDirection == Vertex.DIRECTION.OUT;
    final RID owner = outgoing ? out : in;
    final RID neighbor = outgoing ? in : out;
    final int bucketId = getBucketId();
    final OriginWalk walk = new OriginWalk();
    try {
      final VertexInternal vertex = (VertexInternal) db.lookupByRID(owner, false);
      walkList(db, vertex, originDirection, neighbor, (segment, position, entryEdge, entryVertex) -> {
        if (!walk.reached && position == originPosition && segment.equals(originSegment)) {
          walk.reached = true;
          walk.before = walk.found;
        }
        if (entryEdge.getPosition() < 0 && entryEdge.getBucketId() == bucketId && entryVertex.equals(neighbor))
          ++walk.found;
      });
      if (!walk.reached) {
        // THE ENTRY IS NOT IN THE LIST ANY MORE (THE LIST CHANGED UNDER THE QUERY): IT KEEPS THE IDENTITY IT HAD BEFORE
        // THE OCCURRENCE EXISTED
        LogManager.instance().log(this, Level.FINE, "Lightweight edge %s (%s -> %s) is no longer in the edge list it was read from",
            this, out, in);
        walk.found = 0;
        walk.before = 0;
      }
    } catch (final RecordNotFoundException e) {
      LogManager.instance().log(this, Level.FINE, "Cannot resolve the occurrence of lightweight edge %s (%s -> %s): %s", this, out,
          in, e.getMessage());
      walk.found = 0;
      walk.before = 0;
    }
    // TWO THREADS RESOLVING THE SAME RID BOTH WALK THE LIST AND STORE THE SAME ANSWER: HARMLESS
    numbered(walk.before, walk.found);
  }

  /** What one walk of the list learns about the entry an edge was read from. */
  private static final class OriginWalk {
    // ENTRIES OF THE TRIPLE FOUND, HOW MANY OF THEM PRECEDE THE ORIGIN ENTRY, WHETHER THE ORIGIN WAS REACHED
    private int     found;
    private int     before;
    private boolean reached;
  }

  @Override
  public RID getOutRID() {
    return out;
  }

  @Override
  public RID getInRID() {
    return in;
  }

  @Override
  public Record getRecord(final boolean loadContent) {
    throw new RecordNotFoundException(
        "Lightweight edges have no record: " + this + " (" + out + " -> " + in + "). Read it from one of its vertices",
        this);
  }
}
