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
import com.arcadedb.exception.RecordNotFoundException;

import java.util.NoSuchElementException;

public class VertexIterator extends ResettableIteratorBase<Vertex> {
  // THE VERTEX AND DIRECTION OF THE LIST WALKED, FOR remove()
  private final RID              vertex;
  private final Vertex.DIRECTION direction;

  /** With the vertex and direction of the list walked, so {@link #remove()} can delete the edge of the entry. */
  public VertexIterator(final DatabaseInternal database, final EdgeSegment current, final RID vertex,
      final Vertex.DIRECTION direction) {
    super(database, current);
    this.vertex = vertex;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    checkNotInvalidated();
    if (currentContainer == null)
      return false;

    while (true) {
      if (currentPosition.get() < currentContainer.getUsed())
        return true;

      if (moveToPreviousChunk() == null)
        break;

      currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
    }

    return false;
  }

  @Override
  public Vertex next() {
    while (true) {
      if (!hasNext())
        throw new NoSuchElementException();

      final int begin = currentPosition.get();
      final RID edgeRID = currentContainer.getRID(currentPosition);
      final RID rid = currentContainer.getRID(currentPosition);

      ++browsed;

      try {
        // LAZY LOAD THE CONTENT TO IMPROVE PERFORMANCE WITH TRAVERSAL. NOTE: THE RECORD NOT FOUND WILL NEVER BE TRIGGERED HERE ANYMORE
        final Vertex result = (Vertex) database.lookupByRID(rid, false);
        entryReturned(currentContainer, begin, currentPosition.get(), edgeRID, rid);
        return result;
      } catch (final RecordNotFoundException e) {
        // SKIP
      }
    }
  }

  /**
   * Deletes the edge that leads to the vertex the last {@code next()} returned, see
   * {@link ResettableIteratorBase#removeReturnedEntry} (#9779).
   */
  @Override
  public void remove() {
    removeReturnedEntry(vertex, direction);
  }
}
