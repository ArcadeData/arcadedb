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
import com.arcadedb.query.opencypher.ast.ReturnClause;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Final projection step that filters results to only include the requested RETURN properties.
 * This step runs AFTER ORDER BY/SKIP/LIMIT to ensure those steps have access to all variables,
 * but before returning results to the user.
 * <p>
 * Example: RETURN ID(n) AS id should only return the "id" property, not "n" or other intermediate variables.
 */
public class FinalProjectionStep extends AbstractExecutionStep {
  private final Set<String> requestedProperties;
  private final boolean returnAll;

  public FinalProjectionStep(final ReturnClause returnClause, final CommandContext context) {
    super(context);
    this.requestedProperties = new HashSet<>();
    this.returnAll = returnClause != null && returnClause.isReturnAll();

    // Collect the output names from the RETURN clause
    if (!returnAll && returnClause != null && returnClause.getReturnItems() != null) {
      for (final ReturnClause.ReturnItem item : returnClause.getReturnItems()) {
        requestedProperties.add(item.getOutputName());
      }
    }
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    checkForPrevious("FinalProjectionStep requires a previous step");

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

        if (prevResults == null) {
          prevResults = prev.syncPull(context, nRecords);
        }

        while (buffer.size() < n && prevResults.hasNext()) {
          final Result inputResult = prevResults.next();
          final long begin = context.isProfiling() ? System.nanoTime() : 0;
          try {
            final ResultInternal filteredResult = filterResult(inputResult);
            buffer.add(filteredResult);
          } finally {
            if (context.isProfiling())
              cost += (System.nanoTime() - begin);
          }
        }

        if (!prevResults.hasNext()) {
          finished = true;
        }
      }

      @Override
      public void close() {
        FinalProjectionStep.this.close();
      }
    };
  }

  /**
   * Metadata key for storing the projection name when unwrapping single-element results.
   * This allows wire protocols (like Bolt) to correctly format responses with field names.
   */
  public static final String PROJECTION_NAME_METADATA = "_projectionName";

  /**
   * Filters the result to only include the requested properties.
   * When the result contains a single property that is a Document (vertex/edge),
   * returns it as an element result directly, matching the behavior of the
   * Gremlin-based Cypher engine. The original projection name is stored in metadata
   * for wire protocols that need field names.
   */
  private ResultInternal filterResult(final Result inputResult) {
    final ResultInternal result = new ResultInternal();

    if (returnAll) {
      // RETURN * - pass through all properties
      Document singleDocument = null;
      int documentCount = 0;
      for (final String prop : inputResult.getPropertyNames()) {
        final Object value = inputResult.getProperty(prop);
        result.setProperty(prop, value);
        if (value instanceof Document doc) {
          result.setMetadata(PROJECTION_NAME_METADATA, prop);
          singleDocument = doc;
          documentCount++;
        }
      }
      if (documentCount == 1 && inputResult.getPropertyNames().size() == 1)
        result.setElement(singleDocument);
      return result;
    }

    // Only include properties that were explicitly requested in the RETURN clause
    Document singleDocument = null;
    int documentCount = 0;
    for (final String prop : requestedProperties) {
      if (inputResult.hasProperty(prop)) {
        final Object value = inputResult.getProperty(prop);
        result.setProperty(prop, value);
        if (value instanceof Document doc) {
          result.setMetadata(PROJECTION_NAME_METADATA, prop);
          singleDocument = doc;
          documentCount++;
        }
      }
    }

    // When returning a single document element, also set it as the element
    // so that toElement() and isElement() work for backward compatibility
    if (documentCount == 1 && requestedProperties.size() == 1)
      result.setElement(singleDocument);

    return result;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    final String ind = getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FINAL PROJECTION [");
    builder.append(String.join(", ", requestedProperties));
    builder.append("]");
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted()).append(")");
    }
    return builder.toString();
  }

  private static String getIndent(final int depth, final int indent) {
    return "  ".repeat(Math.max(0, depth * indent));
  }
}
