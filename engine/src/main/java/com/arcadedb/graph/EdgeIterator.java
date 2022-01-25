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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class EdgeIterator implements Iterator<Edge>, Iterable<Edge> {
  private final RID              vertex;
  private final Vertex.DIRECTION direction;
  private       EdgeSegment      currentContainer;
  private final AtomicInteger    currentPosition     = new AtomicInteger(MutableEdgeSegment.CONTENT_START_POSITION);
  private       int              lastElementPosition = currentPosition.get();
  private       RID              nextEdgeRID;
  private       RID              nextVertexRID;

  public EdgeIterator(final EdgeSegment current, final RID vertex, final Vertex.DIRECTION direction) {
    if (current == null)
      throw new IllegalArgumentException("Edge chunk is null");

    this.currentContainer = current;
    this.vertex = vertex;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    if (currentContainer == null)
      return false;

    if (currentPosition.get() < currentContainer.getUsed())
      return true;

    currentContainer = currentContainer.getNext();
    if (currentContainer != null) {
      currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
      return currentPosition.get() < currentContainer.getUsed();
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

      try {
        return nextEdgeRID.asEdge();
      } catch (RecordNotFoundException e) {
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
    } catch (RecordNotFoundException e) {
      // IGNORE IT
    } catch (Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on deleting edge record %s", e, nextEdgeRID);
    }

    currentContainer.removeEntry(lastElementPosition, currentPosition.get());
    ((DatabaseInternal) vertex.getDatabase()).updateRecord(currentContainer);

    currentPosition.set(lastElementPosition);
  }

  @Override
  public Iterator<Edge> iterator() {
    return this;
  }
}
