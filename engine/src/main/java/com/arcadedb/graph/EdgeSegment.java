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
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@ExcludeFromJacocoGeneratedReport
public interface EdgeSegment extends Record {
  byte RECORD_TYPE = 3;

  boolean add(RID edgeRID, RID vertexRID);

  /**
   * Appends the edge at the end of the segment (O(1), no memmove). Suitable for bulk loading where insertion order does not matter.
   */
  boolean addAtEnd(RID edgeRID, RID vertexRID);

  /**
   * Appends the edge at the end using raw primitive values, bypassing RID object creation and temp buffer allocation.
   * Inlines zigzag + VLQ encoding directly into the segment buffer for maximum throughput during bulk import.
   */
  boolean addAtEndDirect(int edgeBucketId, long edgePosition, int vertexBucketId, long vertexPosition);

  /**
   * Appends multiple edges at the end of the segment in a single operation.
   * Reads used-bytes once, writes in a tight loop, updates used-bytes once.
   * Returns the number of edges actually written (may be less than requested if segment fills).
   */
  int addManyAtEndDirect(int[] edgeBucketIds, long[] edgePositions,
      int[] vertexBucketIds, long[] vertexPositions, int from, int to);

  boolean containsEdge(RID edgeRID);

  RID getFirstEdgeConnectedToVertex(RID vertexRID, final int[] edgeBucketFilter);

  int removeEdge(RID edgeRID);

  int removeVertex(RID vertexRID);

  /**
   * True if this segment holds a lightweight edge of the given type reaching the given vertex.
   * <p>
   * A lightweight edge has no record, so it cannot be located by its own RID the way a regular edge is: every
   * lightweight edge of a type shares the marker {@code #<type first bucket>:-1}. It is located instead by the triple
   * it is made of, which here narrows to the edge-type bucket plus the far endpoint. All three terms matter:
   * {@code edgeTypeBucketId} keeps a lightweight edge of another type out, and the record-less test keeps a
   * <b>regular</b> edge of this same type out - both would otherwise be unlinked in its place.
   */
  boolean containsLightEdge(int edgeTypeBucketId, RID vertexRID);

  /**
   * Removes one lightweight edge of the given type reaching the given vertex. See {@link #containsLightEdge} for why
   * the match needs all three terms. Removes a single entry: duplicates of one lightweight edge are the same edge, so
   * which one goes is immaterial, and stopping at the first keeps the walk bounded.
   *
   * @return 1 when an entry was removed, 0 otherwise
   */
  int removeLightEdge(int edgeTypeBucketId, RID vertexRID);

  EdgeSegment getPrevious();

  /** Returns the RID of the previous chunk in the list without loading it, or {@code null} if this is the tail. */
  RID getPreviousRID();

  void setPrevious(EdgeSegment next);

  Binary getContent();

  int getUsed();

  RID getRID(AtomicInteger currentPosition);

  /**
   * Returns the position of the first entry at or after {@code position} whose edge bucket {@code bucketMask}
   * accepts, or {@link #getUsed()} if no entry left in this segment matches.
   * <p>
   * The rejected entries are skipped on the raw numbers stored in the segment, without being decoded into
   * {@link RID} objects: a type-filtered walk over a vertex dominated by another edge type pays a few byte reads per
   * foreign entry instead of two allocations (issue #8417).
   *
   * @param position   the position of an entry boundary, as kept by the iterators
   * @param bucketMask the edge buckets of the requested types
   */
  int nextEntryInBuckets(int position, EdgeBucketMask bucketMask);

  /**
   * Advances {@code currentPosition} past one RID without decoding it, the allocation-free twin of
   * {@link #getRID(AtomicInteger)} for a caller that only needs to step over it.
   */
  void skipRID(AtomicInteger currentPosition);

  int getRecordSize();

  long count(Set<Integer> fileIds);

  /**
   * Adds to {@code counts[i]} the entries of this segment whose edge bucket {@code edgeMasks[i]} accepts and whose
   * far-end vertex bucket {@code neighborMasks[i]} accepts, a null neighbor mask accepting any vertex. Every filter is
   * answered in ONE pass over the raw entries, without decoding a {@link RID} or loading a record, so a light edge (no
   * record) weighs the same as a regular one and a label on the far end costs no vertex lookup (issue #9539).
   *
   * @param edgeMasks     one non-null mask per filter
   * @param neighborMasks one mask per filter, null for none
   * @param skipSelfLoops one flag per filter, true to leave out the entries reaching {@code owner} itself, null for none:
   *                      a self loop sits in both lists of its vertex, and an undirected count must take it from one
   * @param owner         the vertex the list belongs to, read only for {@code skipSelfLoops}
   * @param counts        one counter per filter, incremented in place
   */
  void countInto(EdgeBucketMask[] edgeMasks, EdgeBucketMask[] neighborMasks, boolean[] skipSelfLoops, RID owner, long[] counts);

  boolean removeEntry(int currentItemPosition, int nextItemPosition);

  EdgeSegment copy();

  boolean isEmpty();
}
