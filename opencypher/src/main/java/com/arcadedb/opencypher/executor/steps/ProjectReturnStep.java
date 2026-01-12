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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.opencypher.ast.ReturnClause;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Execution step for projecting RETURN clause results.
 * Transforms results by selecting and projecting specified properties.
 * <p>
 * Examples:
 * - RETURN n -> returns the full vertex
 * - RETURN n.name -> returns just the name property
 * - RETURN n, r, b -> returns multiple variables
 */
public class ProjectReturnStep extends AbstractExecutionStep {
  private final ReturnClause returnClause;

  // Pattern for property access: variable.property
  private static final Pattern PROPERTY_PATTERN = Pattern.compile("(\\w+)\\.(\\w+)");

  public ProjectReturnStep(final ReturnClause returnClause, final CommandContext context) {
    super(context);
    this.returnClause = returnClause;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("ProjectReturnStep requires a previous step");

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

        // Fetch up to n results from previous step
        while (buffer.size() < n && prevResults.hasNext()) {
          final Result inputResult = prevResults.next();
          final ResultInternal projectedResult = projectResult(inputResult);
          buffer.add(projectedResult);
        }

        if (!prevResults.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        ProjectReturnStep.this.close();
      }
    };
  }

  /**
   * Projects a result according to the RETURN clause.
   * Note: Preserves original variables for ORDER BY access.
   */
  private ResultInternal projectResult(final Result inputResult) {
    final ResultInternal result = new ResultInternal();

    // IMPORTANT: Copy all original properties first
    // This ensures ORDER BY can access variables even after projection
    for (final String prop : inputResult.getPropertyNames()) {
      result.setProperty(prop, inputResult.getProperty(prop));
    }

    if (returnClause == null || returnClause.getItems().isEmpty()) {
      // No RETURN clause - return everything (already copied above)
      return result;
    }

    // Project each return item (adds projected properties alongside originals)
    for (final String item : returnClause.getItems()) {
      final String trimmedItem = item.trim();

      // Check if it's a property access: n.name
      final Matcher matcher = PROPERTY_PATTERN.matcher(trimmedItem);
      if (matcher.matches()) {
        final String variable = matcher.group(1);
        final String property = matcher.group(2);

        final Object obj = inputResult.getProperty(variable);
        if (obj instanceof Document) {
          final Document doc = (Document) obj;
          final Object value = doc.get(property);
          result.setProperty(variable + "." + property, value);
        }
      } else {
        // It's a simple variable: n (already copied above)
        // No need to set again
      }
    }

    return result;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ PROJECT RETURN ");
    if (returnClause != null) {
      builder.append(String.join(", ", returnClause.getItems()));
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
