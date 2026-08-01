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
import com.arcadedb.utility.ResettableIterator;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ResettableIteratorBase<T> implements ResettableIterator<T> {
  protected final DatabaseInternal database;
  private final   EdgeSegment      initialContainer;
  protected       EdgeSegment      currentContainer;
  protected final AtomicInteger    currentPosition = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);
  protected       int              browsed         = 0;
  private         int              neighborBucketId = -1;
  private         long             neighborPosition = -1;
  private         boolean          neighborFiltered = false;

  protected ResettableIteratorBase(final DatabaseInternal database, final EdgeSegment current) {
    if (current == null)
      throw new IllegalArgumentException("Edge chunk is null");
    this.database = database;
    this.initialContainer = current;
    this.currentContainer = current;
  }

  /**
   * Restricts the iteration to the entries whose neighbour vertex is the given one.
   * <p>
   * Both the edge pointer and the neighbour pointer are stored inline in the segment, so the
   * neighbour is already in hand when an entry is examined. Rejecting on it costs two primitive
   * comparisons and, crucially, happens <b>before</b> the edge record is materialised. That is what
   * makes an "is A connected to B" probe over a super-node cost a scan of the edge list rather than
   * one record load - plus one property deserialization - per edge in it. The scan itself is
   * unchanged: the segment still yields an RID per entry whether or not the filter is set.
   *
   * @param neighbor the only neighbour vertex to accept, or {@code null} to iterate everything
   */
  public void setNeighborVertexFilter(final RID neighbor) {
    this.neighborFiltered = neighbor != null;
    this.neighborBucketId = neighbor != null ? neighbor.getBucketId() : -1;
    this.neighborPosition = neighbor != null ? neighbor.getPosition() : -1;
  }

  /** Tells whether the entry just read out of the segment survives the neighbour filter. */
  protected final boolean matchesNeighborFilter(final RID neighbor) {
    return !neighborFiltered
        || (neighbor != null && neighbor.getBucketId() == neighborBucketId && neighbor.getPosition() == neighborPosition);
  }

  @Override
  public void reset() {
    this.currentContainer = initialContainer;
    currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
    browsed = 0;
  }

  @Override
  public long countEntries() {
    long total = browsed;

    final EdgeSegment savedContainer = currentContainer;
    final int savedCurrentPosition = currentPosition.get();
    final int savedBrowsed = browsed;

    try {
      while (hasNext()) {
        next();
        ++total;
      }
    } finally {
      // RESTORE SAVED POSITION
      currentContainer = savedContainer;
      currentPosition.set(savedCurrentPosition);
      browsed = savedBrowsed;
    }

    return total;
  }

  @Override
  public long getBrowsed() {
    return browsed;
  }
}
