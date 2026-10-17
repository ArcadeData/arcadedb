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
import com.arcadedb.database.Record;
import com.arcadedb.log.LogManager;

import java.util.logging.Level;

public abstract class IteratorFilterBase<T> extends ResettableIteratorBase<T> {
  private         int          lastElementPosition   = currentPosition.get();
  protected       RID          nextEdge;
  protected       RID          nextVertex;
  protected       RID          next;
  // THE RECORD hasNext() LOOKED UP TO VALIDATE THE RID OF next (THE VERTEX OR THE EDGE), HANDED OUT BY next() INSTEAD OF A
  // SECOND LOOKUP (#10013, #10163). NULL FOR A LIGHTWEIGHT EDGE, WHICH HAS NO RECORD TO LOOK UP
  protected       Record       validatedRecord;
  // NULL WHEN NONE OF THE REQUESTED TYPES IS AN EDGE TYPE. A PRIMITIVE MASK INSTEAD OF A Set<Integer>, SO REJECTING AN
  // ENTRY OF ANOTHER TYPE COSTS NEITHER AN RID DECODE NOR A BOXED LOOKUP (#8417)
  protected final EdgeBucketMask validBuckets;
  protected       int            fullStackTracePrinted = 0;
  // THE VERTEX AND DIRECTION OF THE LIST WALKED, FOR remove(). A CONSTRUCTOR ARGUMENT RATHER THAN AN OPTION, SO AN ITERATOR
  // THAT COULD ONLY UNLINK THE WALKED SIDE CANNOT BE BUILT (#9779)
  private final   RID              listOwner;
  private final   Vertex.DIRECTION listDirection;

  protected IteratorFilterBase(final DatabaseInternal database, final EdgeSegment current, final String[] edgeTypes,
      final RID listOwner, final Vertex.DIRECTION listDirection) {
    super(database, current);
    validBuckets = EdgeBucketMask.of(database, edgeTypes);
    this.listOwner = listOwner;
    this.listDirection = listDirection;
  }

  /**
   * The byte position in {@link #currentContainer} of the entry the last {@code hasNext()} stopped on.
   */
  protected int getEntryPosition() {
    return lastElementPosition;
  }

  protected boolean hasNext(final boolean edge) {
    checkNotInvalidated();
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
        // THE RECORD OF AN ENTRY SKIPPED BELOW MUST NOT STAY REACHABLE UNTIL THE NEXT ONE IS VALIDATED
        validatedRecord = null;

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
          if (nextEdge.getPosition() > -1) {
            try {
              validatedRecord = database.lookupByRID(nextEdge, false);
            } catch (final Exception e) {
              handleCorruption(e, nextEdge, nextVertex);
              continue;
            }
          } else
            // LIGHTWEIGHT EDGE: NO RECORD. DO NOT KEEP THE PREVIOUS ENTRY'S RECORD REACHABLE
            validatedRecord = null;

        } else {
          next = nextVertex;

          // VALIDATE RID
          try {
            validatedRecord = database.lookupByRID(nextVertex, false);
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

  /** To call from {@code next()} once {@code hasNext()} has stopped on the entry it hands out. */
  protected final void entryReturned() {
    entryReturned(currentContainer, lastElementPosition, currentPosition.get(), nextEdge, nextVertex);
  }

  /**
   * Deletes the edge of the entry the last {@code next()} returned, see {@link ResettableIteratorBase#removeReturnedEntry}.
   * It used to take the entry out of the list and nothing else (#9779): no {@code DELETE_RECORD} check, the edge record
   * left alive and the far endpoint's list still pointing at it.
   */
  @Override
  public void remove() {
    removeReturnedEntry(listOwner, listDirection);
  }

  /**
   * Also forgets the entry {@code hasNext()} peeked, which would otherwise be handed out again before the walk restarts
   * (#10151), and where it was: every field {@code hasNext()} sets goes back to what a new iterator starts from.
   */
  @Override
  public void reset() {
    super.reset();
    next = null;
    nextEdge = null;
    nextVertex = null;
    validatedRecord = null;
    lastElementPosition = currentPosition.get();
  }

  @Override
  protected void entriesShiftedBack(final int removed) {
    if (next != null)
      // THE ENTRY hasNext() ALREADY PEEKED FOLLOWS THE REMOVED ONE IN THE SAME SEGMENT
      lastElementPosition -= removed;
  }

  /**
   * The record {@code hasNext()} resolved to validate {@link #next}, consumed: a second call returns {@code null}. Also
   * {@code null} when that record is not the one behind {@code next} any more, so the caller falls back to a fresh lookup.
   */
  protected final Record takeValidatedRecord() {
    final Record validated = validatedRecord;
    validatedRecord = null;
    return validated != null && next.equals(validated.getIdentity()) ? validated : null;
  }

  public RID getNextVertex() {
    return nextVertex;
  }
}
