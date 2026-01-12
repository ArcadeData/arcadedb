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
package com.arcadedb.opencypher.executor.steps;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.opencypher.ast.SetClause;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Execution step for SET clause.
 * Updates properties on existing vertices and edges.
 * <p>
 * Examples:
 * - MATCH (n:Person {name: 'Alice'}) SET n.age = 31
 * - MATCH (n:Person) WHERE n.age > 30 SET n.senior = true
 * - MATCH (a)-[r:KNOWS]->(b) SET r.weight = 1.5
 * <p>
 * The SET step modifies documents in place and passes them through to the next step.
 */
public class SetStep extends AbstractExecutionStep {
  private final SetClause setClause;

  public SetStep(final SetClause setClause, final CommandContext context) {
    super(context);
    this.setClause = setClause;
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

          // Apply SET operations to this result
          applySetOperations(inputResult);

          // Pass through the modified result
          buffer.add(inputResult);
        }

        if (!prevResults.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        SetStep.this.close();
      }
    };
  }

  /**
   * Applies all SET operations to a result.
   *
   * @param result the result containing variables to update
   */
  private void applySetOperations(final Result result) {
    if (setClause == null || setClause.isEmpty()) {
      return;
    }

    for (final SetClause.SetItem item : setClause.getItems()) {
      final String variable = item.getVariable();
      final String property = item.getProperty();
      final String valueExpression = item.getValueExpression();

      // Get the object from the result
      final Object obj = result.getProperty(variable);
      if (obj == null) {
        // Variable not found in result - skip this SET item
        continue;
      }

      if (!(obj instanceof Document)) {
        // Not a document - skip
        continue;
      }

      final Document doc = (Document) obj;

      // Make document mutable
      final MutableDocument mutableDoc = doc.modify();

      // Evaluate the value expression and set the property
      final Object value = evaluateExpression(valueExpression, result);
      mutableDoc.set(property, value);

      // Save the modified document
      mutableDoc.save();

      // Update the result with the modified document
      ((ResultInternal) result).setProperty(variable, mutableDoc);
    }
  }

  /**
   * Evaluates a simple expression to get a value.
   * Currently supports:
   * - String literals: 'Alice', "Bob"
   * - Numbers: 42, 3.14
   * - Booleans: true, false
   * - null
   * <p>
   * TODO: Support variable references, property access, functions, arithmetic
   */
  private Object evaluateExpression(final String expression, final Result result) {
    if (expression == null || expression.trim().isEmpty()) {
      return null;
    }

    final String trimmed = expression.trim();

    // Null literal
    if (trimmed.equalsIgnoreCase("null")) {
      return null;
    }

    // Boolean literals
    if (trimmed.equalsIgnoreCase("true")) {
      return Boolean.TRUE;
    }
    if (trimmed.equalsIgnoreCase("false")) {
      return Boolean.FALSE;
    }

    // String literals (single or double quotes)
    if ((trimmed.startsWith("'") && trimmed.endsWith("'")) ||
        (trimmed.startsWith("\"") && trimmed.endsWith("\""))) {
      return trimmed.substring(1, trimmed.length() - 1);
    }

    // Numeric literals
    try {
      if (trimmed.contains(".")) {
        return Double.parseDouble(trimmed);
      } else {
        return Integer.parseInt(trimmed);
      }
    } catch (final NumberFormatException e) {
      // Not a number - might be a variable or property reference
    }

    // Variable reference: just the variable name (e.g., "age")
    // Check if it exists in the result
    if (result.getPropertyNames().contains(trimmed)) {
      return result.getProperty(trimmed);
    }

    // Property access: variable.property (e.g., "n.age")
    if (trimmed.contains(".")) {
      final String[] parts = trimmed.split("\\.", 2);
      if (parts.length == 2) {
        final Object varObj = result.getProperty(parts[0]);
        if (varObj instanceof Document) {
          return ((Document) varObj).get(parts[1]);
        }
      }
    }

    // If we can't evaluate it, return the expression as a string
    // This allows for future extension to support more complex expressions
    return trimmed;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ SET");
    if (setClause != null && !setClause.isEmpty()) {
      builder.append(" (").append(setClause.getItems().size()).append(" items)");
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
