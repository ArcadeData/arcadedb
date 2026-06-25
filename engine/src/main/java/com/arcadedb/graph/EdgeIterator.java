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

import java.util.NoSuchElementException;
import java.util.logging.Level;

public class EdgeIterator extends ResettableIteratorBase<Edge> {
  private final RID              vertex;
  private final Vertex.DIRECTION direction;
  private       int              lastElementPosition = currentPosition.get();
  private       RID              nextEdgeRID;
  private       RID              nextVertexRID;
  private       boolean          pending             = false;

  public EdgeIterator(final EdgeSegment current, final RID vertex, final Vertex.DIRECTION direction) {
    super(null, current);
    this.vertex = vertex;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    if (pending)
      return true;

    if (currentContainer == null)
      return false;

    while (true) {
      if (currentPosition.get() < currentContainer.getUsed()) {
        lastElementPosition = currentPosition.get();

        nextEdgeRID = currentContainer.getRID(currentPosition);
        nextVertexRID = currentContainer.getRID(currentPosition);

        // VALIDATE A NON-LIGHTWEIGHT EDGE HERE SO A DANGLING POINTER (edge record removed but the link still in
        // the segment) IS SKIPPED WHILE PEEKING, KEEPING hasNext()/next() CONSISTENT WITH THE Iterator CONTRACT.
        // OTHERWISE next() WOULD SKIP THE RECORD AND THROW NoSuchElementException WHEN THE SEGMENT ENDS - WHICH
        // ONLY SURFACES WHEN THE CONTENT IS ACTUALLY LOADED, e.g. UNDER REPEATABLE_READ ISOLATION.
        if (nextEdgeRID.getPosition() > -1) {
          try {
            currentContainer.getDatabase().lookupByRID(nextEdgeRID, false);
          } catch (final RecordNotFoundException e) {
            // SKIP DANGLING EDGE
            nextEdgeRID = null;
            nextVertexRID = null;
            continue;
          }
        }

        pending = true;
        return true;
      }

      currentContainer = currentContainer.getPrevious();
      if (currentContainer == null)
        break;

      currentPosition.set(MutableEdgeSegment.CONTENT_START_POSITION);
    }

    return false;
  }

  @Override
  public Edge next() {
    if (!hasNext())
      throw new NoSuchElementException();

    pending = false;

    if (nextEdgeRID.getPosition() < 0) {
      // CREATE LIGHTWEIGHT EDGE
      final DocumentType edgeType = currentContainer.getDatabase().getSchema().getTypeByBucketId(nextEdgeRID.getBucketId());

      if (direction == Vertex.DIRECTION.OUT)
        return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdgeRID, vertex, nextVertexRID);
      else
        return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdgeRID, nextVertexRID, vertex);
    }

    ++browsed;

    // ALREADY VALIDATED IN hasNext(); LAZY LOAD THE CONTENT TO IMPROVE PERFORMANCE WITH TRAVERSAL.
    return (Edge) currentContainer.getDatabase().lookupByRID(nextEdgeRID, false);
  }

  @Override
  public void reset() {
    super.reset();
    pending = false;
    nextEdgeRID = null;
    nextVertexRID = null;
    lastElementPosition = currentPosition.get();
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
        ((Edge) currentContainer.getDatabase().lookupByRID(nextEdgeRID, false)).delete();
    } catch (final RecordNotFoundException e) {
      // IGNORE IT
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error on deleting edge record %s", e, nextEdgeRID);
    }

    currentContainer.removeEntry(lastElementPosition, currentPosition.get());
    ((DatabaseInternal) currentContainer.getDatabase()).updateRecord(currentContainer);

    currentPosition.set(lastElementPosition);
  }
}
