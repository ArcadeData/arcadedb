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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

import java.util.Iterator;

/**
 * Fetches edges of a given type from a specific vertex, using the vertex's edge linked list
 * instead of scanning the entire edge type. This is an optimization for queries like:
 * <pre>SELECT FROM EdgeType WHERE @out = #X:Y</pre>
 * which are rewritten to use vertex-centric edge access.
 */
public class FetchEdgesFromVertexStep extends AbstractExecutionStep {
  private final RID                vertexRid;
  private final Vertex.DIRECTION   direction;
  private final String             edgeTypeName;
  private       Iterator<Edge>     edgeIterator;
  private       boolean            inited = false;

  public FetchEdgesFromVertexStep(final RID vertexRid, final Vertex.DIRECTION direction, final String edgeTypeName,
      final CommandContext context) {
    super(context);
    this.vertexRid = vertexRid;
    this.direction = direction;
    this.edgeTypeName = edgeTypeName;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (!inited) {
      inited = true;
      final Vertex vertex = (Vertex) context.getDatabase().lookupByRID(vertexRid, true);
      edgeIterator = vertex.getEdges(direction, edgeTypeName).iterator();
    }

    return new ResultSet() {
      private int served = 0;

      @Override
      public boolean hasNext() {
        return served < nRecords && edgeIterator.hasNext();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new IllegalStateException();
        final Edge edge = edgeIterator.next();
        served++;
        final ResultInternal result = new ResultInternal(edge);
        context.setVariable("current", result);
        return result;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    return ExecutionStepInternal.getIndent(depth, indent) + "+ FETCH EDGES FROM VERTEX " + vertexRid
        + " (direction=" + direction + ", type=" + edgeTypeName + ")";
  }
}
