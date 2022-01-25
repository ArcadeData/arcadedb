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
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;

import java.util.NoSuchElementException;
import java.util.logging.Level;

public class EdgeIteratorFilter extends IteratorFilterBase<Edge> {
  private final Vertex           vertex;
  private final Vertex.DIRECTION direction;

  public EdgeIteratorFilter(final DatabaseInternal database, final Vertex vertex, final Vertex.DIRECTION direction, final EdgeSegment current,
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
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdge, vertex.getIdentity(), nextVertex);
        else
          return new ImmutableLightEdge(currentContainer.getDatabase(), edgeType, nextEdge, nextVertex, vertex.getIdentity());
      }

      return next.asEdge();
    } catch (RecordNotFoundException e) {
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s from vertex %s direction %s", e, next, vertex, direction);

      next = null;
      if (hasNext())
        return next();

      throw e;

    } catch (SchemaException e) {
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s from vertex %s direction %s", e, next, vertex, direction);
      throw e;
    } finally {
      next = null;
    }
  }

  @Override
  protected void handleCorruption(final Exception e, final RID edge, final RID vertex) {
    if ((e instanceof RecordNotFoundException || e instanceof SchemaException) &&//
        database.getMode() == PaginatedFile.MODE.READ_WRITE) {

      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s %s. Fixing it...", e, edge, vertex != null ? "vertex " + vertex : "");

      database.transaction(() -> {
        final EdgeLinkedList outEdges = database.getGraphEngine().getEdgeHeadChunk((VertexInternal) this.vertex, direction);
        if (outEdges != null)
          outEdges.removeEdgeRID(edge);

      }, true);

    } else
      LogManager.instance().log(this, Level.WARNING, "Error on loading edge %s %s. Skip it.", e, edge, vertex != null ? "vertex " + vertex : "");
  }
}
