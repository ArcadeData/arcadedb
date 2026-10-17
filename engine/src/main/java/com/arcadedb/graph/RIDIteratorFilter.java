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

import java.util.NoSuchElementException;

/**
 * Iterator that returns connected vertex RIDs from edge segments, filtered by edge type,
 * without loading vertex records from disk. Unlike {@link VertexIteratorFilter}, this iterator
 * skips the {@code lookupByRID} validation call, making it significantly faster for bulk
 * neighbor enumeration where only RIDs are needed.
 */
public class RIDIteratorFilter extends ResettableIteratorBase<RID> implements EdgeEntryRIDIterator {
  private final EdgeBucketMask validBuckets;
  private       RID            next;
  // WHERE THE ENTRY HELD IN next BEGINS: ITS EDGE POINTER IS SKIPPED ON THE RAW NUMBERS AND DECODED ONLY IF getEdgeRID() IS ASKED
  private       EdgeSegment    nextContainer;
  private       int            nextBegin;

  public RIDIteratorFilter(final DatabaseInternal database, final EdgeSegment current, final String[] edgeTypes) {
    super(database, current);
    validBuckets = EdgeBucketMask.of(database, edgeTypes);
  }

  @Override
  public boolean hasNext() {
    if (next != null)
      return true;

    if (currentContainer == null || validBuckets == null)
      return false;

    while (true) {
      final int used = currentContainer.getUsed();
      int position = currentPosition.get();
      if (position < used)
        // SKIP THE ENTRIES OF THE OTHER EDGE TYPES ON THEIR RAW BUCKET NUMBER, WITHOUT DECODING THEM (#8417)
        position = currentContainer.nextEntryInBuckets(position, validBuckets);

      if (position < used) {
        currentPosition.set(position);
        nextContainer = currentContainer;
        nextBegin = position;
        currentContainer.skipRID(currentPosition); // THE EDGE: ITS BUCKET ALREADY MATCHED, NOTHING ELSE OF IT IS NEEDED
        next = currentContainer.getRID(currentPosition);
        return true;
      } else {
        // Guarded hop: a chunk whose previous pointer names itself ends the walk instead of looping (issue #8568)
        if (moveToPreviousChunk() != null)
          currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
        else
          break;
      }
    }

    return false;
  }

  @Override
  public RID next() {
    if (!hasNext())
      throw new NoSuchElementException();

    try {
      lastEntryAt(nextContainer, nextBegin);
      return next;
    } finally {
      next = null;
      ++browsed;
    }
  }

  @Override
  public RID getEdgeRID() {
    return lastEntryEdgeRID();
  }

  /** Also forgets the entry {@code hasNext()} peeked, which would otherwise be handed out again before the walk restarts (#10151). */
  @Override
  public void reset() {
    super.reset();
    next = null;
    nextContainer = null;
  }
}
