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
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public abstract class IteratorFilterBase<T> extends ResettableIteratorBase<T> {
  private         int          lastElementPosition   = currentPosition.get();
  protected       RID          nextEdge;
  protected       RID          nextVertex;
  protected       RID          next;
  // NULL WHEN NONE OF THE REQUESTED TYPES IS AN EDGE TYPE. A PRIMITIVE MASK INSTEAD OF A Set<Integer>, SO REJECTING AN
  // ENTRY OF ANOTHER TYPE COSTS NEITHER AN RID DECODE NOR A BOXED LOOKUP (#8417)
  protected final EdgeBucketMask validBuckets;
  protected       int            fullStackTracePrinted = 0;

  protected IteratorFilterBase(final DatabaseInternal database, final EdgeSegment current, final String[] edgeTypes) {
    super(database, current);
    validBuckets = EdgeBucketMask.of(database, edgeTypes);
  }

  protected boolean hasNext(final boolean edge) {
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
        lastElementPosition = position;
        currentPosition.set(position);

        nextEdge = currentContainer.getRID(currentPosition);
        nextVertex = currentContainer.getRID(currentPosition);

        // THE NEIGHBOUR CHECK IS SHARED WITH THE VERTEX ITERATOR BELOW (edge == false). NO CALLER ARMS THE FILTER
        // ON A VERTEX ITERATOR TODAY, SO IT IS INERT THERE; IT IS LEFT SHARED BECAUSE THE ENTRY IT FILTERS ON IS
        // THE SAME PAIR EITHER WAY, SO A FUTURE "VERTICES CONNECTED TO X" WOULD BEHAVE IDENTICALLY
        if (!matchesNeighborFilter(nextVertex)) {
          // FILTER IT OUT. THE CHECK RUNS ON THE POINTERS READ FROM THE SEGMENT, BEFORE ANY RECORD IS TOUCHED
          nextEdge = null;
          nextVertex = null;
          next = null;
          continue;
        }

        if (edge) {
          next = nextEdge;

          // VALIDATE RID
          if (nextEdge.getPosition() > -1)
            try {
              database.lookupByRID(nextEdge, false);
            } catch (final Exception e) {
              handleCorruption(e, nextEdge, nextVertex);
              continue;
            }

        } else {
          next = nextVertex;

          // VALIDATE RID
          try {
            database.lookupByRID(nextVertex, false);
          } catch (final Exception e) {
            handleCorruption(e, nextEdge, nextVertex);
            continue;
          }
        }

        return true;

      } else {
        // FETCH NEXT CHUNK
        if (moveToPreviousChunk() != null) {
          currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
          lastElementPosition = currentPosition.get();
        } else
          // END (also reached when the chunk's "previous" pointer names itself - see moveToPreviousChunk())
          break;
      }
    }

    return false;
  }

  protected void handleCorruption(final Exception e, final RID edge, final RID nextVertex) {
    if (fullStackTracePrinted < 10) {
      ++fullStackTracePrinted;
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s. Skip it.", e, edge);
    } else
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s. Skip it. Error: %s", edge, e.getMessage());
  }

  @Override
  public void remove() {
    if (currentContainer != null) {
      currentContainer.removeEntry(lastElementPosition, currentPosition.get());
      database.updateRecord(currentContainer);
      currentPosition.set(lastElementPosition);
    }
  }

  public RID getNextVertex() {
    return nextVertex;
  }
}
