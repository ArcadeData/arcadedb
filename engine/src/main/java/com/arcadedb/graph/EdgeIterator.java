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
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;

import java.util.*;
import java.util.logging.*;

public class EdgeIterator extends ResettableIteratorBase<Edge> {
  private final RID              vertex;
  private final Vertex.DIRECTION direction;
  private       int              lastElementPosition = currentPosition.get();
  private       RID              nextEdgeRID;
  private       RID              nextVertexRID;

  public EdgeIterator(final EdgeSegment current, final RID vertex, final Vertex.DIRECTION direction) {
    super(null, current);
    this.vertex = vertex;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    if (currentContainer == null)
      return false;

    while (true) {
      if (currentPosition.get() < currentContainer.getUsed())
        return true;

      currentContainer = currentContainer.getPrevious();
      if (currentContainer == null)
        break;

      currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
    }

    return false;
  }

  @Override
  public Edge next() {
    while (true) {
      if (!hasNext())
        throw new NoSuchElementException();

      lastElementPosition = currentPosition.get();

      nextEdgeRID = currentContainer.getRID(currentPosition);
      nextVertexRID = currentContainer.getRID(currentPosition);

      if (nextEdgeRID.getPosition() < 0) {
        // CREATE LIGHTWEIGHT EDGE
        final DocumentType edgeType = currentContainer.getDatabase().getSchema().getTypeByBucketId(nextEdgeRID.getBucketId());

        if (direction == Vertex.DIRECTION.OUT)
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdgeRID, vertex, nextVertexRID);
        else
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdgeRID, nextVertexRID, vertex);
      }

      ++browsed;

      try {
        // LAZY LOAD THE CONTENT TO IMPROVE PERFORMANCE WITH TRAVERSAL. NOTE: THE RECORD NOT FOUND WILL NEVER BE TRIGGERED HERE ANYMORE
        return nextEdgeRID.asEdge(false);
      } catch (final RecordNotFoundException e) {
        // SKIP
      }
    }
  }

  @Override
  public void remove() {
    if (nextEdgeRID == null)
      throw new NoSuchElementException();

    try {
      if (nextEdgeRID.getPosition() < 0) {
        // CREATE LIGHTWEIGHT EDGE
        final DocumentType edgeType = currentContainer.getDatabase().getSchema().getTypeByBucketId(nextEdgeRID.getBucketId());

        if (direction == Vertex.DIRECTION.OUT)
          new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdgeRID, vertex, nextVertexRID).delete();
        else
          new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdgeRID, nextVertexRID, vertex).delete();
      } else
        nextEdgeRID.asEdge().delete();
    } catch (final RecordNotFoundException e) {
      // IGNORE IT
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on deleting edge record %s", e, nextEdgeRID);
    }

    currentContainer.removeEntry(lastElementPosition, currentPosition.get());
    ((DatabaseInternal) vertex.getDatabase()).updateRecord(currentContainer);

    currentPosition.set(lastElementPosition);
  }
}
