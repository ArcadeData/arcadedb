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
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.ast.SetClause;
import com.arcadedb.query.opencypher.executor.CypherFunctionFactory;
import com.arcadedb.query.opencypher.executor.ExpressionEvaluator;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Execution step for SET clause.
 * Supports: SET n.prop = value, SET n = {map}, SET n += {map}, SET n:Label
 */
public class SetStep extends AbstractExecutionStep {
  private final SetClause setClause;
  private final ExpressionEvaluator evaluator;

  public SetStep(final SetClause setClause, final CommandContext context,
                 final CypherFunctionFactory functionFactory) {
    super(context);
    this.setClause = setClause;
    this.evaluator = new ExpressionEvaluator(functionFactory);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("SetStep requires a previous step");

    return new ResultSet() {
      private ResultSet prevResults = null;
      private final List<Result> buffer = new ArrayList<>();
      private int bufferIndex = 0;
      private boolean finished = false;

      @Override
      public boolean hasNext() {
        if (bufferIndex < buffer.size())
          return true;
        if (finished)
          return false;
        fetchMore(nRecords);
        return bufferIndex < buffer.size();
      }

      @Override
      public Result next() {
        if (!hasNext())
          throw new NoSuchElementException();
        return buffer.get(bufferIndex++);
      }

      private void fetchMore(final int n) {
        buffer.clear();
        bufferIndex = 0;
        if (prevResults == null)
          prevResults = prev.syncPull(context, nRecords);
        while (buffer.size() < n && prevResults.hasNext()) {
          final Result inputResult = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            if (context.isProfiling())
              rowCount++;

            applySetOperations(inputResult);
            buffer.add(inputResult);
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }
        if (!prevResults.hasNext())
          finished = true;
      }

      @Override
      public void close() {
        SetStep.this.close();
      }
    };
  }

  private void applySetOperations(final Result result) {
    if (setClause == null || setClause.isEmpty())
      return;

    final boolean wasInTransaction = context.getDatabase().isTransactionActive();

    try {
      if (!wasInTransaction)
        context.getDatabase().begin();

      for (final SetClause.SetItem item : setClause.getItems()) {
        switch (item.getType()) {
          case PROPERTY:
            applyPropertySet(item, result);
            break;
          case REPLACE_MAP:
            applyReplaceMap(item, result);
            break;
          case MERGE_MAP:
            applyMergeMap(item, result);
            break;
          case LABELS:
            applyLabels(item, result);
            break;
        }
      }

      if (!wasInTransaction)
        context.getDatabase().commit();
    } catch (final Exception e) {
      if (!wasInTransaction && context.getDatabase().isTransactionActive())
        context.getDatabase().rollback();
      throw e;
    }
  }

  private void applyPropertySet(final SetClause.SetItem item, final Result result) {
    final Object obj = result.getProperty(item.getVariable());
    if (!(obj instanceof Document doc))
      return;

    final MutableDocument mutableDoc = doc.modify();
    final Object value = evaluator.evaluate(item.getValueExpression(), result, context);
    if (value == null)
      mutableDoc.remove(item.getProperty());
    else {
      validatePropertyValue(value);
      mutableDoc.set(item.getProperty(), value);
    }
    mutableDoc.save();
    ((ResultInternal) result).setProperty(item.getVariable(), mutableDoc);
  }

  @SuppressWarnings("unchecked")
  private void applyReplaceMap(final SetClause.SetItem item, final Result result) {
    final Object obj = result.getProperty(item.getVariable());
    if (!(obj instanceof Document doc))
      return;

    final Object mapValue = evaluator.evaluate(item.getValueExpression(), result, context);
    if (!(mapValue instanceof Map))
      return;

    final Map<String, Object> map = (Map<String, Object>) mapValue;
    final MutableDocument mutableDoc = doc.modify();

    // Remove all existing properties except internal ones
    final Set<String> existingProps = new java.util.HashSet<>(mutableDoc.getPropertyNames());
    for (final String prop : existingProps) {
      if (!prop.startsWith("@"))
        mutableDoc.remove(prop);
    }

    // Set new properties from map (skip null values - they mean "remove")
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() != null)
        mutableDoc.set(entry.getKey(), entry.getValue());
    }

    mutableDoc.save();
    ((ResultInternal) result).setProperty(item.getVariable(), mutableDoc);
  }

  @SuppressWarnings("unchecked")
  private void applyMergeMap(final SetClause.SetItem item, final Result result) {
    final Object obj = result.getProperty(item.getVariable());
    if (!(obj instanceof Document doc))
      return;

    final Object mapValue = evaluator.evaluate(item.getValueExpression(), result, context);
    if (!(mapValue instanceof Map))
      return;

    final Map<String, Object> map = (Map<String, Object>) mapValue;
    final MutableDocument mutableDoc = doc.modify();

    // Merge: add/update properties from map, null removes
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      if (entry.getValue() == null)
        mutableDoc.remove(entry.getKey());
      else
        mutableDoc.set(entry.getKey(), entry.getValue());
    }

    mutableDoc.save();
    ((ResultInternal) result).setProperty(item.getVariable(), mutableDoc);
  }

  private void applyLabels(final SetClause.SetItem item, final Result result) {
    final Object obj = result.getProperty(item.getVariable());
    if (!(obj instanceof Vertex vertex))
      return;

    // Get existing labels and add new ones
    final List<String> existingLabels = Labels.getLabels(vertex);
    final List<String> allLabels = new ArrayList<>(existingLabels);
    for (final String label : item.getLabels())
      if (!allLabels.contains(label))
        allLabels.add(label);

    // Create the composite type for the combined labels
    final String newTypeName = Labels.ensureCompositeType(
        context.getDatabase().getSchema(), allLabels);

    // If the type hasn't changed, nothing to do
    if (vertex.getTypeName().equals(newTypeName))
      return;

    // Create new vertex with the composite type, copy all properties
    final MutableVertex newVertex = context.getDatabase().newVertex(newTypeName);
    for (final String prop : vertex.getPropertyNames())
      newVertex.set(prop, vertex.get(prop));
    newVertex.save();

    // Copy edges from old vertex to new vertex
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT))
      newVertex.newEdge(edge.getTypeName(), edge.getVertex(Vertex.DIRECTION.IN));
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN))
      edge.getVertex(Vertex.DIRECTION.OUT).newEdge(edge.getTypeName(), newVertex);

    // Delete old vertex
    vertex.delete();

    ((ResultInternal) result).setProperty(item.getVariable(), newVertex);
  }

  private void validatePropertyValue(final Object value) {
    if (value instanceof List) {
      for (final Object element : (List<?>) value) {
        if (element instanceof Map)
          throw new IllegalArgumentException("TypeError: InvalidPropertyType - Property values can not contain map values");
        if (element instanceof List)
          validatePropertyValue(element);
      }
    } else if (value instanceof Map)
      throw new IllegalArgumentException("TypeError: InvalidPropertyType - Property values can not be maps");
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    builder.append(ind);
    builder.append("+ SET");
    if (setClause != null && !setClause.isEmpty())
      builder.append(" (").append(setClause.getItems().size()).append(" items)");
    if (context.isProfiling())
      builder.append(" (").append(getCostFormatted()).append(")");
    return builder.toString();
  }
}
