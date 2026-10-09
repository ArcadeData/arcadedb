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

import java.util.concurrent.atomic.AtomicInteger;

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
 * before. It also costs nothing extra in allocations - it replaces the throwaway marker RID that the traversal path
 * already built for every lightweight edge it materialised.
 *
 * <p>
 * Two lightweight edges of one type over the same ordered pair - an application mistake the <code>UNIQUE</code> flag
 * exists to prevent - are two entries in the edge lists and, to a query, two relationships (issue #9573). They share
 * this triple, so {@link #equals} cannot tell them apart and must not (the triple is also how one is located to be
 * deleted). What can is the <b>occurrence</b>: how many entries with the same triple precede this one in the edge list
 * it was read from. An iterator that reads an edge off a list records where (segment, position) at no cost; the
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
  // -1 = NOT RESOLVED YET
  private int              occurrence = -1;
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
  public LightEdgeRID readFrom(final RID segment, final int position, final Vertex.DIRECTION direction) {
    this.originSegment = segment;
    this.originPosition = position;
    this.originDirection = direction;
    return this;
  }

  /**
   * For an edge not read off an edge list but off a snapshot that already sorts twins next to each other: which of
   * {@code twinCount} twins it is.
   */
  void numbered(final int occurrence, final int twinCount) {
    this.occurrence = occurrence;
    this.twins = twinCount;
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
    if (occurrence < 0)
      resolve();
    return occurrence;
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
   * Whether {@code candidate} is the same relationship as {@code used}: equal, and for lightweight edges that share
   * their triple, the same entry of it rather than a twin (issue #9573). The only expensive case is two distinct
   * lightweight edges with one triple - for a path that walks an edge back over itself, that is every step - and it is
   * settled from the candidate's own list alone whenever the edge has no twin, which is the list the caller is
   * walking anyway.
   */
  public static boolean isSameEdge(final RID used, final RID candidate) {
    if (!used.equals(candidate))
      return false;
    if (used == candidate || used.getPosition() >= 0)
      return true;
    if (!(used instanceof LightEdgeRID usedLight) || !(candidate instanceof LightEdgeRID candidateLight))
      return true;
    if (candidateLight.getTwinCount() <= 1)
      return true;
    return usedLight.getOccurrence() == candidateLight.getOccurrence();
  }

  private void resolve() {
    if (originSegment == null) {
      occurrence = 0;
      twins = 0;
      return;
    }
    final DatabaseInternal db = (DatabaseInternal) getBoundDatabase();
    final boolean outgoing = originDirection == Vertex.DIRECTION.OUT;
    final RID owner = outgoing ? out : in;
    final RID neighbor = outgoing ? in : out;
    int found = 0;
    int before = 0;
    try {
      final VertexInternal vertex = (VertexInternal) db.lookupByRID(owner, false);
      RID segmentRID = outgoing ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk();
      final ChainCycleGuard guard = new ChainCycleGuard(segmentRID);
      boolean reached = false;
      while (segmentRID != null) {
        final EdgeSegment segment = (EdgeSegment) db.lookupByRID(segmentRID, true);
        final boolean originChunk = segment.getIdentity().equals(originSegment);
        final AtomicInteger cursor = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);
        final int used = segment.getUsed();
        while (cursor.get() < used) {
          final int at = cursor.get();
          final RID entryEdge = segment.getRID(cursor);
          final RID entryVertex = segment.getRID(cursor);
          if (originChunk && at == originPosition) {
            reached = true;
            before = found;
          }
          if (entryEdge.getPosition() < 0 && entryEdge.getBucketId() == getBucketId() && entryVertex.equals(neighbor))
            ++found;
        }
        final RID previous = segment.getPreviousRID();
        segmentRID = previous == null || previous.equals(segment.getIdentity()) || guard.revisits(previous) ? null : previous;
      }
      if (!reached) {
        // THE ENTRY IS NOT IN THE LIST ANY MORE (THE LIST CHANGED UNDER THE QUERY): IT KEEPS THE IDENTITY IT HAD BEFORE
        // THE OCCURRENCE EXISTED
        before = 0;
        found = 0;
      }
    } catch (final RecordNotFoundException e) {
      before = 0;
      found = 0;
    }
    twins = found;
    occurrence = before;
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
