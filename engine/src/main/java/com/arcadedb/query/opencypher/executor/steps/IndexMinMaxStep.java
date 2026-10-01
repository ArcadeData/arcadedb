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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;

/**
 * Answers {@code MATCH (n:Label) RETURN min(n.prop)} and {@code max(n.prop)} from one end of the index on the property,
 * the way SQL's {@code MIN FROM INDEX} does, instead of reading every vertex of the label (issue #8666).
 * <p>
 * The planner only builds it where the index holds exactly the values the aggregate looks at: an ordered index on that
 * property alone, defined on the label itself, which skips the vertices with no value (what {@code min} and {@code max}
 * ignore anyway), and orders its keys the way Cypher orders the values. The answer is read from the vertex the first
 * entry points to, so it is the value the scan would have returned, not the index's own copy of the key. {@code DISTINCT} is irrelevant to a minimum or a maximum, so it does not stop the shortcut.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class IndexMinMaxStep extends AbstractExecutionStep {
  private final String  typeName;
  private final String  propertyName;
  private final boolean max;
  private final String  outputAlias;
  private       boolean executed = false;

  public IndexMinMaxStep(final String typeName, final String propertyName, final boolean max, final String outputAlias,
      final CommandContext context) {
    super(context);
    this.typeName = typeName;
    this.propertyName = propertyName;
    this.max = max;
    this.outputAlias = outputAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (executed)
      return new InternalResultSet();
    executed = true;

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    final Object value;
    try {
      value = readEnd(context);
      if (context.isProfiling())
        rowCount++;
    } finally {
      if (context.isProfiling())
        cost += System.nanoTime() - begin;
    }

    final ResultInternal result = new ResultInternal();
    result.setProperty(outputAlias, value);
    final InternalResultSet resultSet = new InternalResultSet();
    resultSet.add(result);
    return resultSet;
  }

  private Object readEnd(final CommandContext context) {
    final DocumentType type = context.getDatabase().getSchema().getType(typeName);
    // Read per execution, not at planning: the plan is cached, and the index can be dropped in between. An empty answer
    // would pass for a label with no values, so fail instead
    final TypeIndex index = type.getIndexByProperties(propertyName);
    if (!(index instanceof RangeIndex))
      throw new CommandExecutionException(
          "The index on '" + typeName + "." + propertyName + "' is no longer available: re-plan the query");
    // The planner refuses a case-insensitive index, whose key is the folded value (issue #8698): so must a plan that
    // predates one
    if (index.getMetadata() != null && index.getMetadata().isCaseInsensitive(0))
      throw new CommandExecutionException(
          "The index on '" + typeName + "." + propertyName + "' is case-insensitive now: re-plan the query");

    final IndexCursor cursor = index.iterator(!max);
    try {
      while (cursor.hasNext()) {
        try {
          final Object value = cursor.next().asVertex().get(propertyName);
          if (value != null)
            return value;
        } catch (final RecordNotFoundException e) {
          // deleted since the index answered: the next entry is the answer
        }
      }
      return null;
    } finally {
      cursor.close();
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final StringBuilder builder = new StringBuilder();
    builder.append("  ".repeat(Math.max(0, depth * indent)));
    builder.append(max ? "+ MAX" : "+ MIN").append(" FROM INDEX ").append(typeName).append('[').append(propertyName).append(']');
    if (context.isProfiling()) {
      builder.append(" (").append(getCostFormatted());
      if (rowCount > 0)
        builder.append(", ").append(getRowCountFormatted());
      builder.append(")");
    }
    return builder.toString();
  }
}
