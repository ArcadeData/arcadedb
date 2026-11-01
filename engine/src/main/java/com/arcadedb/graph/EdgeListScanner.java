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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Walks the edge list of a vertex entry by entry without allocating per entry, for the full scans of the adjacency: the build
 * of a Graph Analytical View, an algorithm that reads every list (issue #11012).
 * <p>
 * An ordinary walk ({@link EdgeLinkedList#entryIterator()}) hands out each entry as a pair of RIDs, and reads each segment as
 * a record: a view of its page, a copy of its bytes, a segment object, and a hop to the previous segment through the database.
 * A build of 20M edges allocated about 8 GB that way, about 400 bytes per edge, for a 160 MB view. Here each segment is read
 * where it lies on its page ({@link LocalBucket#getPlainRecordForAbsoluteReads}) and each entry is handed to an
 * {@link EntryVisitor} as the four numbers it is stored as: nothing is allocated per entry, and per segment at most the page
 * identifier of the read, reused while the reads stay on one page.
 * <p>
 * The answer is the one the record walk gives: the page comes from the transaction, so a page it holds is read as it holds it,
 * and the scanner falls back to the record walk, segment by segment, wherever the in-place read cannot vouch for that:
 * <ul>
 * <li>a segment the transaction has written: it is read from the transaction's own copy, which its pages do not hold yet;</li>
 * <li>a segment that does not lie whole in its own slot (a record moved behind a placeholder pointer to free room on its
 * page), or that the read finds damaged: the database reads it, and raises what it raises;</li>
 * <li>a striped list (a super-node, #5156), whose head is not a segment: it is walked by its own iterator;</li>
 * <li>a database with a record read listener: every segment is read through the database, so the listener runs; and so is
 * the vertex a scan starts from by position, when its type has one.</li>
 * </ul>
 * A chain that closes on itself ends the walk silently, as {@link ResettableIteratorBase#moveToPreviousChunk()} ends it.
 * <p>
 * Not thread-safe: one instance per scanning thread, reused for every list it walks.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class EdgeListScanner {
  // THE SEGMENT LAYOUT: RECORD TYPE (1 BYTE), USED BYTES (INT), PREVIOUS SEGMENT AS BUCKET (INT) + POSITION (LONG), THEN ENTRIES
  private static final int USED_OFFSET              = Binary.BYTE_SERIALIZED_SIZE;
  private static final int PREVIOUS_BUCKET_OFFSET   = USED_OFFSET + Binary.INT_SERIALIZED_SIZE;
  private static final int PREVIOUS_POSITION_OFFSET = PREVIOUS_BUCKET_OFFSET + Binary.INT_SERIALIZED_SIZE;
  // THE VERTEX PREFIX: RECORD TYPE (1 BYTE), OUT HEAD AS BUCKET (INT) + POSITION (LONG), IN HEAD THE SAME (SEE ImmutableVertex)
  private static final int VERTEX_OUT_HEAD_OFFSET   = Binary.BYTE_SERIALIZED_SIZE;
  private static final int VERTEX_IN_HEAD_OFFSET    = VERTEX_OUT_HEAD_OFFSET + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;
  private static final int VERTEX_PREFIX_SIZE       = VERTEX_IN_HEAD_OFFSET + Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

  private final DatabaseInternal database;
  private final int[]            location    = new int[2];
  // THE WALK RUN UNDER THE READ LOCK, ONE FOR THE SCANNER'S LIFE: A LAMBDA PER LIST WAS THE LAST ALLOCATION PER VERTEX
  private final Callable<Object> pendingWalk = this::walkPending;
  private       VertexInternal   pendingVertex;
  private       int              pendingVertexBucketId;
  private       long             pendingVertexPosition;
  private       Vertex.DIRECTION pendingDirection;
  private       EntryVisitor     pendingVisitor;
  // THE SEGMENT (OR LIST HEAD) THE LAST READ LEFT TO WALK NEXT, -1 FOR NONE: HANDED BETWEEN THE READS WITHOUT AN OBJECT
  private       int              nextBucketId;
  private       long             nextPosition;
  // THE PAGES THE LAST SEGMENT AND THE LAST VERTEX WERE READ FROM: THE NEXT READ ON THE SAME PAGE BUILDS NO PageId
  private       PageId           lastSegmentPageId;
  private       PageId           lastVertexPageId;
  private       long             segmentsReadInPlace;
  private       long             segmentsReadAsRecords;

  public EdgeListScanner(final DatabaseInternal database) {
    this.database = database;
  }

  /**
   * Hands every entry of the {@code direction} edge list of {@code vertex} to {@code visitor}.
   *
   * @param direction {@link Vertex.DIRECTION#OUT} or {@link Vertex.DIRECTION#IN}
   */
  public void scan(final VertexInternal vertex, final Vertex.DIRECTION direction, final EntryVisitor visitor) {
    checkDirection(direction);
    final RID head = direction == Vertex.DIRECTION.OUT ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk();
    if (head == null)
      return;
    run(vertex, -1, -1L, direction, visitor);
  }

  /**
   * Hands every entry of the {@code direction} edge list of the vertex at {@code vertexBucketId}:{@code vertexPosition} to
   * {@code visitor}, reading the head of the list from the vertex record where it lies: no vertex is materialized, for a
   * scan that needs nothing of the vertex but its edges. A position that holds no record visits nothing.
   *
   * @param direction {@link Vertex.DIRECTION#OUT} or {@link Vertex.DIRECTION#IN}
   */
  public void scan(final int vertexBucketId, final long vertexPosition, final Vertex.DIRECTION direction,
      final EntryVisitor visitor) {
    checkDirection(direction);
    run(null, vertexBucketId, vertexPosition, direction, visitor);
  }

  private void run(final VertexInternal vertex, final int vertexBucketId, final long vertexPosition,
      final Vertex.DIRECTION direction, final EntryVisitor visitor) {
    if (hasReadListeners() || (vertex == null && vertexTypeHasReadListeners(vertexBucketId))) {
      final VertexInternal loaded = vertex != null ? vertex : loadVertex(vertexBucketId, vertexPosition);
      if (loaded != null)
        scanAsRecords(loaded, direction, visitor);
      return;
    }

    // UNDER THE DATABASE'S READ LOCK, AS EVERY RECORD READ OF THE WALK IT REPLACES: ONCE PER LIST, NOT ONCE PER SEGMENT
    pendingVertex = vertex;
    pendingVertexBucketId = vertexBucketId;
    pendingVertexPosition = vertexPosition;
    pendingDirection = direction;
    pendingVisitor = visitor;
    try {
      database.executeInReadLock(pendingWalk);
    } finally {
      pendingVertex = null;
      pendingVisitor = null;
    }
  }

  private Object walkPending() {
    // COPIED BEFORE THE WALK: A VISITOR THAT SCANS ANOTHER LIST WITH THIS SCANNER REPLACES THE FIELDS
    final VertexInternal vertex = pendingVertex;
    final int vertexBucketId = pendingVertexBucketId;
    final long vertexPosition = pendingVertexPosition;
    final Vertex.DIRECTION direction = pendingDirection;
    final EntryVisitor visitor = pendingVisitor;
    final TransactionContext transaction = database.getTransaction();

    if (vertex != null) {
      final RID head = direction == Vertex.DIRECTION.OUT ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk();
      walk(transaction, vertex, -1, -1L, head.getBucketId(), head.getPosition(), direction, visitor);
    } else if (readListHead(transaction, vertexBucketId, vertexPosition, direction) && nextBucketId >= 0)
      walk(transaction, null, vertexBucketId, vertexPosition, nextBucketId, nextPosition, direction, visitor);
    return null;
  }

  /** Segments read in place, from their page, since this scanner was created. For tests and diagnostics. */
  public long getSegmentsReadInPlace() {
    return segmentsReadInPlace;
  }

  /** Segments read through the database as records since this scanner was created. For tests and diagnostics. */
  public long getSegmentsReadAsRecords() {
    return segmentsReadAsRecords;
  }

  private void walk(final TransactionContext transaction, final VertexInternal vertex, final int vertexBucketId,
      final long vertexPosition, int bucketId, long position, final Vertex.DIRECTION direction, final EntryVisitor visitor) {
    boolean headRead;
    try {
      headRead = readSegment(transaction, bucketId, position, visitor);
    } catch (final RecordNotFoundException e) {
      headRead = false;
    }
    if (!headRead) {
      // THE HEAD IS NOT A SEGMENT (A STRIPED SUPER-NODE LIST), OR IS GONE: THE RECORD WALK ANSWERS, AS IT ALWAYS HAS
      final VertexInternal loaded = vertex != null ? vertex : loadVertex(vertexBucketId, vertexPosition);
      if (loaded != null)
        scanAsRecords(loaded, direction, visitor);
      return;
    }

    // ChainCycleGuard's DETECTION, OVER PRIMITIVES: A SEGMENT NAMING ITSELF AS PREVIOUS ENDS THE WALK, AND SO DOES A RETURN TO A
    // CHECKPOINT THAT MOVES TO THE CURRENT SEGMENT AFTER 1, 2, 4... HOPS (BRENT), SO A LONGER CYCLE ENDS IT WITHIN TWICE ITS LENGTH
    int checkpointBucketId = bucketId;
    long checkpointPosition = position;
    int hops = 0;
    int limit = 1;

    while (nextBucketId >= 0) {
      if (nextBucketId == bucketId && nextPosition == position)
        return;
      bucketId = nextBucketId;
      position = nextPosition;
      if (bucketId == checkpointBucketId && position == checkpointPosition)
        return;
      if (++hops == limit) {
        checkpointBucketId = bucketId;
        checkpointPosition = position;
        hops = 0;
        limit <<= 1;
      }

      // A PREVIOUS SEGMENT THAT IS GONE FAILS THE WALK, AS IT FAILS EdgeSegment.getPrevious(): readSegment() RAISES THE
      // RecordNotFoundException OF THE LOOKUP. ONE THAT NAMES A RECORD OF ANOTHER KIND FAILED IT TOO, WITH THE CAST THE RECORD
      // WALK MAKES: A VIEW BUILT FROM WHAT CAME BEFORE IT WOULD MISS THE REST OF THE LIST WITHOUT A WORD
      if (!readSegment(transaction, bucketId, position, visitor))
        throw new DatabaseOperationException(
            "Edge list chain names record #" + bucketId + ":" + position + ", which is not an edge segment: run CHECK DATABASE");
    }
  }

  /**
   * Visits the entries of one segment and leaves the previous one in {@link #nextBucketId} / {@link #nextPosition} (-1 for
   * none).
   *
   * @return false when the record is not a segment
   */
  private boolean readSegment(final TransactionContext transaction, final int bucketId, final long position,
      final EntryVisitor visitor) {
    if (transaction.hasWrittenRecords()) {
      final Record written = transaction.getWrittenRecord(new RID(bucketId, position));
      if (written != null)
        return visitRecord(written, visitor);
    }

    if (database.getSchema().getBucketByIdIfExists(bucketId) instanceof LocalBucket bucket) {
      final BasePage page;
      try {
        page = bucket.getPlainRecordForAbsoluteReads(position, location, lastSegmentPageId);
      } catch (final IOException e) {
        throw new DatabaseOperationException("Error on reading edge segment #" + bucketId + ":" + position, e);
      }
      if (page != null) {
        lastSegmentPageId = page.getPageId();
        if (readSegmentInPlace(page, location[0], location[1], visitor))
          return true;
      }
    }

    // NOT A PLAIN RECORD OF ITS OWN SLOT, OR NOT ONE THE READ COULD MAKE SENSE OF: THE DATABASE READS IT, AND RAISES WHAT IT RAISES
    return visitRecord(database.lookupByRID(new RID(bucketId, position), true), visitor);
  }

  private boolean readSegmentInPlace(final BasePage page, final int offset, final int length, final EntryVisitor visitor) {
    if (length < MutableEdgeSegment.CONTENT_START_POSITION || page.readByte(offset) != MutableEdgeSegment.RECORD_TYPE)
      return false;
    final int used = page.readInt(offset + USED_OFFSET);
    if (used < MutableEdgeSegment.CONTENT_START_POSITION || used > length)
      // DAMAGED: THE RECORD WALK READS IT AND FAILS, OR NOT, AS IT ALWAYS HAS
      return false;

    final int end = offset + used;
    // EVERY ENTRY MUST END INSIDE THE USED BYTES, AND THE LAST ONE EXACTLY ON THEIR END: CHECKED BEFORE THE VISITOR SEES ANY, SO A
    // DAMAGED SEGMENT GOES TO THE RECORD WALK WHOLE INSTEAD OF HANDING OUT ENTRIES DECODED FROM THE BYTES PAST IT
    int cursor = offset + MutableEdgeSegment.CONTENT_START_POSITION;
    try {
      while (cursor < end)
        for (int number = 0; number < 4; number++) {
          if (cursor >= end)
            return false;
          cursor += page.readNumberSize(cursor);
        }
    } catch (final IllegalArgumentException e) {
      // A NUMBER THAT RUNS OFF THE PAGE
      return false;
    }
    if (cursor != end)
      return false;

    cursor = offset + MutableEdgeSegment.CONTENT_START_POSITION;
    while (cursor < end) {
      final long edgeBucketId = page.readNumber(cursor);
      cursor += page.readNumberSize(cursor);
      final long edgePosition = page.readNumber(cursor);
      cursor += page.readNumberSize(cursor);
      final long vertexBucketId = page.readNumber(cursor);
      cursor += page.readNumberSize(cursor);
      final long vertexPosition = page.readNumber(cursor);
      cursor += page.readNumberSize(cursor);
      visitor.visit((int) edgeBucketId, edgePosition, (int) vertexBucketId, vertexPosition);
    }

    nextBucketId = page.readInt(offset + PREVIOUS_BUCKET_OFFSET);
    nextPosition = page.readLong(offset + PREVIOUS_POSITION_OFFSET);
    if (nextBucketId < 0 || nextPosition < 0)
      nextBucketId = -1;
    ++segmentsReadInPlace;
    return true;
  }

  /**
   * Reads the head of the {@code direction} list of the vertex at {@code vertexBucketId}:{@code vertexPosition} into
   * {@link #nextBucketId} / {@link #nextPosition} (-1 for a vertex with no such list), from the vertex record where it lies
   * when it can, through the database otherwise.
   *
   * @return false when the position holds no vertex
   */
  private boolean readListHead(final TransactionContext transaction, final int vertexBucketId, final long vertexPosition,
      final Vertex.DIRECTION direction) {
    if (transaction.hasWrittenRecords()) {
      final Record written = transaction.getWrittenRecord(new RID(vertexBucketId, vertexPosition));
      if (written != null)
        return listHeadOf(written, direction);
    }

    if (database.getSchema().getBucketByIdIfExists(vertexBucketId) instanceof LocalBucket bucket) {
      final BasePage page;
      try {
        page = bucket.getPlainRecordForAbsoluteReads(vertexPosition, location, lastVertexPageId);
      } catch (final IOException e) {
        throw new DatabaseOperationException("Error on reading vertex #" + vertexBucketId + ":" + vertexPosition, e);
      }
      if (page != null)
        lastVertexPageId = page.getPageId();
      if (page != null && location[1] >= VERTEX_PREFIX_SIZE && page.readByte(location[0]) == Vertex.RECORD_TYPE) {
        final int head = location[0] + (direction == Vertex.DIRECTION.OUT ? VERTEX_OUT_HEAD_OFFSET : VERTEX_IN_HEAD_OFFSET);
        nextBucketId = page.readInt(head);
        nextPosition = page.readLong(head + Binary.INT_SERIALIZED_SIZE);
        // A BUCKET OF -1 IS "NO LIST", AS ImmutableVertex READS IT
        if (nextBucketId < 0)
          nextBucketId = -1;
        return true;
      }
    }

    final VertexInternal vertex = loadVertex(vertexBucketId, vertexPosition);
    return vertex != null && listHeadOf(vertex, direction);
  }

  private boolean listHeadOf(final Record record, final Vertex.DIRECTION direction) {
    if (!(record instanceof VertexInternal vertex))
      return false;
    final RID head = direction == Vertex.DIRECTION.OUT ? vertex.getOutEdgesHeadChunk() : vertex.getInEdgesHeadChunk();
    if (head != null) {
      nextBucketId = head.getBucketId();
      nextPosition = head.getPosition();
    } else
      nextBucketId = -1;
    return true;
  }

  /** The vertex at the position, read through the database (so its read listeners run), or null when there is none. */
  private VertexInternal loadVertex(final int vertexBucketId, final long vertexPosition) {
    try {
      return database.lookupByRID(new RID(vertexBucketId, vertexPosition), true) instanceof VertexInternal vertex ? vertex : null;
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  private static void checkDirection(final Vertex.DIRECTION direction) {
    if (direction != Vertex.DIRECTION.OUT && direction != Vertex.DIRECTION.IN)
      throw new IllegalArgumentException("Invalid direction " + direction);
  }

  private boolean visitRecord(final Record record, final EntryVisitor visitor) {
    if (!(record instanceof MutableEdgeSegment segment))
      return false;
    segment.forEachEntry(visitor);
    final RID previous = segment.getPreviousRID();
    if (previous != null && previous.getBucketId() >= 0 && previous.getPosition() >= 0) {
      nextBucketId = previous.getBucketId();
      nextPosition = previous.getPosition();
    } else
      nextBucketId = -1;
    ++segmentsReadAsRecords;
    return true;
  }

  /** The record walk, for a list or a database the in-place read does not serve. */
  private void scanAsRecords(final VertexInternal vertex, final Vertex.DIRECTION direction, final EntryVisitor visitor) {
    final EdgeLinkedList list = database.getGraphEngine().getEdgeHeadChunk(vertex, direction);
    if (list == null)
      return;
    final Iterator<Pair<RID, RID>> entries = list.entryIterator();
    while (entries.hasNext()) {
      final Pair<RID, RID> entry = entries.next();
      final RID edge = entry.getFirst();
      final RID far = entry.getSecond();
      visitor.visit(edge.getBucketId(), edge.getPosition(), far.getBucketId(), far.getPosition());
    }
  }

  /** A vertex read in place fires no read event, so a type that listens to its reads has its vertices read as records. */
  private boolean vertexTypeHasReadListeners(final int vertexBucketId) {
    final DocumentType type = database.getSchema().getTypeByBucketId(vertexBucketId);
    return type != null && (!(type instanceof LocalDocumentType localType) || localType.hasBeforeReadListeners()
        || localType.hasAfterReadListeners());
  }

  private boolean hasReadListeners() {
    final RecordEvents events = database.getEvents();
    return !(events instanceof RecordEventsRegistry registry) || registry.hasBeforeReadListeners()
        || registry.hasAfterReadListeners();
  }

  /** Receives the entries of a list, one call per entry, newest first like every walk of the list. */
  @FunctionalInterface
  public interface EntryVisitor {
    /**
     * @param edgeBucketId   bucket of the edge: for a lightweight edge, the bucket of its type
     * @param edgePosition   position of the edge, negative for a lightweight edge
     * @param vertexBucketId bucket of the vertex at the other end
     * @param vertexPosition position of the vertex at the other end
     */
    void visit(int edgeBucketId, long edgePosition, int vertexBucketId, long vertexPosition);
  }
}
