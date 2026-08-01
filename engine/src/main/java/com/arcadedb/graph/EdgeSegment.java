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

  int getRecordSize();

  long count(Set<Integer> fileIds);

  boolean removeEntry(int currentItemPosition, int nextItemPosition);

  EdgeSegment copy();

  boolean isEmpty();
}
