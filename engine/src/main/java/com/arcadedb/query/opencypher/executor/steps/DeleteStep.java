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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Document;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.ast.DeleteClause;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for DELETE clause.
 * Deletes vertices and/or edges from the graph.
 * <p>
 * Examples:
 * - MATCH (n:Person {name: 'Alice'}) DELETE n
 * - MATCH (a)-[r:KNOWS]->(b) DELETE r
 * - MATCH (n:Person) WHERE n.age < 18 DETACH DELETE n
 * <p>
 * DETACH DELETE removes all relationships connected to a vertex before deleting it.
 * Regular DELETE will fail if the vertex still has relationships.
 */
public class DeleteStep extends AbstractExecutionStep {
  private final DeleteClause deleteClause;

  public DeleteStep(final DeleteClause deleteClause, final CommandContext context) {
    super(context);
    this.deleteClause = deleteClause;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("DeleteStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size()) {
          return true;
        }

        if (finished) {
          return false;
        }

        // Fetch more results
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;

        // Initialize prevResults on first call
        if (prevResults == null) {
          prevResults = prev.syncPull(context, nRecords);
        }

        // Process each input result
        while (buffer.size() < n && prevResults.hasNext()) {
          final Result inputResult = prevResults.next();

          // Apply DELETE operations to this result
          applyDeleteOperations(inputResult);

          // Pass through the result (elements are now deleted)
          buffer.add(inputResult);
        }

        if (!prevResults.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        DeleteStep.this.close();
      }
    };
  }

  /**
   * Applies all DELETE operations to a result.
   *
   * @param result the result containing variables to delete
   */
  private void applyDeleteOperations(final Result result) {
    if (deleteClause == null || deleteClause.isEmpty()) {
      return;
    }

    // Check if we're already in a transaction
    final boolean wasInTransaction = context.getDatabase().isTransactionActive();

    try {
      // Begin transaction if not already active
      if (!wasInTransaction) {
        context.getDatabase().begin();
      }

      for (final String variable : deleteClause.getVariables()) {
        // Get the object from the result
        final Object obj = result.getProperty(variable);
        if (obj == null) {
          // Variable not found in result - skip
          continue;
        }

        if (obj instanceof Vertex) {
          deleteVertex((Vertex) obj);
        } else if (obj instanceof Edge) {
          deleteEdge((Edge) obj);
        } else if (obj instanceof Document) {
          // Generic document - try to delete
          ((Document) obj).delete();
        }
      }

      // Commit transaction if we started it
      if (!wasInTransaction) {
        context.getDatabase().commit();
      }
    } catch (final Exception e) {
      // Rollback if we started the transaction
      if (!wasInTransaction && context.getDatabase().isTransactionActive()) {
        context.getDatabase().rollback();
      }
      throw e;
    }
  }

  /**
   * Deletes a vertex from the graph.
   * If detach is true, deletes all connected relationships first.
   *
   * @param vertex vertex to delete
   */
  private void deleteVertex(final Vertex vertex) {
    if (deleteClause.isDetach()) {
      // DETACH DELETE: Remove all connected relationships first
      deleteAllEdges(vertex);
    }

    // Delete the vertex
    vertex.delete();
  }

  /**
   * Deletes all edges connected to a vertex.
   *
   * @param vertex vertex whose edges should be deleted
   */
  private void deleteAllEdges(final Vertex vertex) {
    // Collect all edges to delete (both incoming and outgoing)
    // We collect first to avoid concurrent modification
    final List<Edge> edgesToDelete = new ArrayList<>();

    // Collect outgoing edges
    final Iterator<Edge> outgoing = vertex.getEdges(Vertex.DIRECTION.OUT).iterator();
    while (outgoing.hasNext()) {
      edgesToDelete.add(outgoing.next());
    }

    // Collect incoming edges
    final Iterator<Edge> incoming = vertex.getEdges(Vertex.DIRECTION.IN).iterator();
    while (incoming.hasNext()) {
      edgesToDelete.add(incoming.next());
    }

    // Delete all collected edges
    for (final Edge edge : edgesToDelete) {
      edge.delete();
    }
  }

  /**
   * Deletes an edge from the graph.
   *
   * @param edge edge to delete
   */
  private void deleteEdge(final Edge edge) {
    edge.delete();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    if (deleteClause != null && deleteClause.isDetach()) {
      builder.append("+ DETACH DELETE");
    } else {
      builder.append("+ DELETE");
    }
    if (deleteClause != null && !deleteClause.isEmpty()) {
      builder.append(" (").append(deleteClause.getVariables().size()).append(" items)");
    }
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
