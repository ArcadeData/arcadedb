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

import com.arcadedb.database.Document;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.schema.EdgeType;

/**
 * Connects edges to their vertices after the edge has been saved.
 * This step retrieves the from/to vertices stored in the context during edge creation
 * and connects them using the GraphEngine.
 */
public class ConnectEdgeStep extends AbstractExecutionStep {

  public ConnectEdgeStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ResultSet upstream = getPrev().syncPull(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public Result next() {
        final Result result = upstream.next();
        if (result != null && result.isEdge()) {
          final Edge edge = result.getEdge().orElse(null);
          if (edge != null && edge.getIdentity() != null) {
            // Edge is now saved, connect it to vertices
            final int edgeHash = System.identityHashCode(edge);
            final Vertex fromVertex = (Vertex) context.getVariable("$__ARCADEDB_EDGE_FROM_" + edgeHash);
            final Vertex toVertex = (Vertex) context.getVariable("$__ARCADEDB_EDGE_TO_" + edgeHash);
            final EdgeType edgeType = (EdgeType) context.getVariable("$__ARCADEDB_EDGE_TYPE_" + edgeHash);

            if (fromVertex != null && toVertex != null && edgeType != null) {
              // Connect the edge using GraphEngine methods
              context.getDatabase().getGraphEngine().connectOutgoingEdge((VertexInternal) fromVertex, toVertex, edge);
              if (edgeType.isBidirectional()) {
                context.getDatabase().getGraphEngine()
                    .connectIncomingEdge(toVertex, fromVertex.getIdentity(), edge.getIdentity());
              }

              // Clean up context variables
              context.setVariable("$__ARCADEDB_EDGE_FROM_" + edgeHash, null);
              context.setVariable("$__ARCADEDB_EDGE_TO_" + edgeHash, null);
              context.setVariable("$__ARCADEDB_EDGE_TYPE_" + edgeHash, null);
            }
          }
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CONNECT EDGE TO VERTICES";
  }

  @Override
  public ExecutionStep copy(final CommandContext context) {
    return new ConnectEdgeStep(context);
  }
}
