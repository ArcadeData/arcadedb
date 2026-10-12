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
import com.arcadedb.utility.Pair;

import java.util.NoSuchElementException;

public class EdgeVertexIterator extends ResettableIteratorBase<Pair<RID, RID>> {
  private final RID              vertex;
  private final Vertex.DIRECTION direction;

  public EdgeVertexIterator(final EdgeSegment current, final RID vertex, final Vertex.DIRECTION direction) {
    super(null, current);
    this.vertex = vertex;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    checkNotInvalidated();
    if (currentContainer == null)
      return false;

    // A SEGMENT CAN BE EMPTY ANYWHERE IN THE CHAIN (A DELETE EMPTIES THE HEAD, A GRAPHBATCH THEN PREPENDS A NEW ONE OVER IT):
    // STEP OVER IT INSTEAD OF ENDING THE WALK THERE (#9715)
    while (true) {
      if (currentPosition.get() < currentContainer.getUsed())
        return true;

      if (moveToPreviousChunk() == null)
        return false;

      currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
    }
  }

  @Override
  public Pair<RID, RID> next() {
    if (!hasNext())
      throw new NoSuchElementException();

    // THE SEGMENT AND BOUNDS OF THE ENTRY ARE RECORDED: a hasNext() in between may already have moved the cursor to
    // another segment, over empty ones too (#9715), and remove() must take the entry out of the segment it came from
    final int begin = currentPosition.get();
    final RID edgeRID = currentContainer.getRID(currentPosition);
    final RID vertexRID = currentContainer.getRID(currentPosition);
    entryReturned(currentContainer, begin, currentPosition.get(), edgeRID, vertexRID);

    ++browsed;
    return new Pair<>(edgeRID, vertexRID);
  }

  /** See {@link ResettableIteratorBase#removeReturnedEntry}: the refusal of the delete reaches the caller (#9779). */
  @Override
  public void remove() {
    removeReturnedEntry(vertex, direction);
  }

  /**
   * Takes the entry the last {@code next()} returned out of the list WITHOUT deleting the edge it names: for
   * {@code CHECK DATABASE}, which prunes an entry as damage even when the edge behind it cannot be deleted.
   */
  public void removeEntryKeepingEdge() {
    unlinkReturnedEntry();
  }
}
