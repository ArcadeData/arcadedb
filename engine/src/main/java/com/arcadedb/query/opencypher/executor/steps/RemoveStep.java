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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.ast.RemoveClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for REMOVE clause.
 * Removes properties from existing vertices and edges.
 * <p>
 * Examples:
 * - MATCH (n:Person {name: 'Alice'}) REMOVE n.temp - removes the temp property
 * - MATCH (n:Person) REMOVE n.prop1, n.prop2 - removes multiple properties
 * <p>
 * The REMOVE step modifies documents in place and passes them through to the next step.
 */
public class RemoveStep extends AbstractExecutionStep {
  private final RemoveClause removeClause;
  private final ExpressionEvaluator evaluator;

  public RemoveStep(final RemoveClause removeClause, final CommandContext context, final CypherFunctionFactory functionFactory) {
    super(context);
    this.removeClause = removeClause;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("RemoveStep requires a previous step");

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
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            // Apply REMOVE operations to this result
            applyRemoveOperations(inputResult);

            // Pass through the modified result
            buffer.add(inputResult);
          } finally {
            if (context.isProfiling())
              cost += System.nanoTime() - begin;
          }
        }

        if (!prevResults.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        RemoveStep.this.close();
      }
    };
  }

  /**
   * Applies all REMOVE operations to a result.
   *
   * @param result the result containing variables to update
   */
  private void applyRemoveOperations(final Result result) {
    if (removeClause == null || removeClause.isEmpty()) {
      return;
    }

    // Check if we're already in a transaction
    final boolean wasInTransaction = context.getDatabase().isTransactionActive();

    try {
      // Begin transaction if not already active
      if (!wasInTransaction) {
        context.getDatabase().begin();
      }

      for (final RemoveClause.RemoveItem item : removeClause.getItems()) {
        if (item.getType() == RemoveClause.RemoveItem.RemoveType.PROPERTY)
          removeProperty(item, result);
        else if (item.getType() == RemoveClause.RemoveItem.RemoveType.LABELS)
          removeLabels(item, result);
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
   * Removes a property from a document.
   *
   * @param item   the remove item specifying variable and property
   * @param result the result containing the variable
   */
  private void removeProperty(final RemoveClause.RemoveItem item, final Result result) {
    final String variable = item.getVariable();

    // Get the object from the result
    final Object obj = result.getProperty(variable);
    if (obj == null) {
      // Variable not found in result - skip this REMOVE item
      return;
    }

    if (!(obj instanceof Document)) {
      // Not a document - skip
      return;
    }

    // Resolve the property name. For dynamic bracket syntax (REMOVE n[keyExpr]) the name is
    // computed at runtime; otherwise it is the static dot-syntax property name.
    final String property;
    if (item.getKeyExpression() != null) {
      final Object keyValue = evaluator.evaluate(item.getKeyExpression(), result, context);
      if (keyValue == null)
        return; // null key is a no-op
      property = keyValue.toString();
    } else
      property = item.getProperty();

    final Document doc = (Document) obj;

    // Removing a property that isn't there is a no-op for Neo4j-compatible statistics: only
    // count it when the property actually existed before the removal.
    final boolean propertyExisted = doc.has(property);

    // Make document mutable
    final MutableDocument mutableDoc = doc.modify();

    // Remove the property (setting to null removes it)
    mutableDoc.remove(property);

    // Save the modified document
    mutableDoc.save();

    if (propertyExisted)
      context.getStatistics().addPropertiesSet(1);

    // Update the result with the modified document
    ((ResultInternal) result).setProperty(variable, mutableDoc);
  }

  /**
   * Removes labels from a vertex by changing its type.
   * Creates a new vertex with the reduced label set and migrates properties/edges.
   */
  private void removeLabels(final RemoveClause.RemoveItem item, final Result result) {
    final String variable = item.getVariable();
    final Object obj = result.getProperty(variable);
    if (!(obj instanceof Vertex vertex))
      return;

    final List<String> currentLabels = Labels.getLabels(vertex);
    final List<String> labelsToRemove = item.getLabels();

    // Check how many of the labels to remove actually exist on the vertex
    int removedLabelsCount = 0;
    for (final String label : labelsToRemove)
      if (currentLabels.contains(label))
        removedLabelsCount++;
    if (removedLabelsCount == 0)
      return;

    // Compute remaining labels
    final List<String> remainingLabels = new ArrayList<>(currentLabels);
    remainingLabels.removeAll(labelsToRemove);

    final String newTypeName;
    if (remainingLabels.isEmpty()) {
      newTypeName = "V";
      context.getDatabase().getSchema().getOrCreateVertexType("V");
    } else {
      newTypeName = Labels.ensureCompositeType(
          context.getDatabase().getSchema(), remainingLabels);
    }

    if (vertex.getTypeName().equals(newTypeName))
      return;

    // Create new vertex with the reduced type, copy properties
    final MutableVertex newVertex = context.getDatabase().newVertex(newTypeName);
    for (final String prop : vertex.getPropertyNames())
      newVertex.set(prop, vertex.get(prop));
    newVertex.save();

    final QueryStatistics stats = context.getStatistics();
    stats.addLabelsRemoved(removedLabelsCount);

    // Migrate edges
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT))
      newVertex.newEdge(edge.getTypeName(), edge.getVertex(Vertex.DIRECTION.IN));
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN))
      edge.getVertex(Vertex.DIRECTION.OUT).newEdge(edge.getTypeName(), newVertex);

    vertex.delete();

    ((ResultInternal) result).setProperty(variable, newVertex);
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ REMOVE");
    if (removeClause != null && !removeClause.isEmpty()) {
      builder.append(" (").append(removeClause.getItems().size()).append(" items)");
    }
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
