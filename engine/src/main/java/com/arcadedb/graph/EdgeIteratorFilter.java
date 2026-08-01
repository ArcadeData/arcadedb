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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;

import java.util.NoSuchElementException;
import java.util.logging.Level;

public class EdgeIteratorFilter extends IteratorFilterBase<Edge> {
  private final Vertex           vertex;
  private final Vertex.DIRECTION direction;

  public EdgeIteratorFilter(final DatabaseInternal database, final Vertex vertex, final Vertex.DIRECTION direction,
      final EdgeSegment current,
      final String[] edgeTypes) {
    super(database, current, edgeTypes);
    this.direction = direction;
    this.vertex = vertex;
  }

  @Override
  public boolean hasNext() {
    return super.hasNext(true);
  }

  @Override
  public Edge next() {
    hasNext();

    if (next == null)
      throw new NoSuchElementException();

    try {
      if (next.getPosition() < 0) {
        // LIGHTWEIGHT EDGE
        final DocumentType edgeType = currentContainer.getDatabase().getSchema().getTypeByBucketId(nextEdge.getBucketId());

        if (direction == Vertex.DIRECTION.OUT)
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdge.getBucketId(), vertex.getIdentity(), nextVertex);
        else
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdge.getBucketId(), nextVertex, vertex.getIdentity());
      }

      // LAZY LOAD THE CONTENT TO IMPROVE PERFORMANCE WITH TRAVERSAL. NOTE: THE RECORD NOT FOUND WILL NEVER BE TRIGGERED HERE ANYMORE
      return next.asEdge(false);

    } catch (final RecordNotFoundException e) {
      LogManager.instance()
          .log(this, Level.WARNING, "Error on loading edge %s from vertex %s direction %s", e, next, vertex, direction);

      next = null;
      if (hasNext())
        return next();

      throw e;

    } catch (final SchemaException e) {
      LogManager.instance()
          .log(this, Level.WARNING, "Error on loading edge %s from vertex %s direction %s", e, next, vertex, direction);
      throw e;
    } finally {
      next = null;
      ++browsed;
    }
  }

  @Override
  protected void handleCorruption(final Exception e, final RID edge, final RID vertex) {
    if ((e instanceof RecordNotFoundException || e instanceof SchemaException) &&//
        database.getMode() == ComponentFile.MODE.READ_WRITE) {

      if (fullStackTracePrinted < 10) {
        ++fullStackTracePrinted;
        LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s %s. Fixing it...", e, edge,
            vertex != null ? "vertex " + vertex : "");
      } else
        LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s %s. Fixing it. Error: %s", edge,
            vertex != null ? "vertex " + vertex : "", e.getMessage());

      try {
        database.transaction(() -> {
          final EdgeLinkedList outEdges = database.getGraphEngine().getEdgeHeadChunk((VertexInternal) this.vertex, direction);
          if (outEdges != null)
            outEdges.removeEdgeRID(edge);

        }, true);
      } catch (final NeedRetryException retryLater) {
        // #5670: the removal walk now reports an unreadable chunk as a retryable conflict, because a caller that
        // is DELETING an edge must not conclude there was nothing to remove. This caller is not deleting anything:
        // it is opportunistically pruning a reference whose edge is already gone, from inside a READ. A concurrent
        // commit reshaping the chain therefore costs the ghost one more pass, not the iteration.
        //
        // NeedRetryException, deliberately wider than the ConcurrentModificationException this change introduces:
        // the condition being absorbed is "retry later", not one specific cause. Its sibling LockTimeoutException
        // says the same thing about this prune - come back for it - and letting THAT escape would fail a read
        // because an optional repair could not get a lock, which is precisely the outcome this catch exists to
        // prevent. Narrowing it to the CME would re-open that door for the sake of matching a comment.
        LogManager.instance()
            .log(this, Level.FINE, "Cannot prune dangling edge %s from vertex %s now (concurrent change): %s", edge,
                vertex, retryLater.getMessage());
      }

    } else {
      if (fullStackTracePrinted < 10) {
        ++fullStackTracePrinted;
        LogManager.instance()
            .log(this, Level.WARNING, "Error on loading edge %s %s. Skip it.", e, edge, vertex != null ? "vertex " + vertex : "");
      } else
        LogManager.instance()
            .log(this, Level.WARNING, "Error on loading edge %s %s. Skip it. Error: %s", e, edge,
                vertex != null ? "vertex " + vertex : "", e.getMessage());
    }
  }
}
