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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.Json;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateMergeStep extends AbstractExecutionStep {
  private final Json       json;
  private final Expression expression;

  public UpdateMergeStep(final Json json, final CommandContext context) {
    super(context);
    this.json = json;
    this.expression = null;
  }

  public UpdateMergeStep(final Expression expression, final CommandContext context) {
    super(context);
    this.json = null;
    this.expression = expression;
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
        if (result instanceof ResultInternal internal) {
          if (!(result.getElement().orElse(null) instanceof Document)) {
            internal.setElement((Document) result.getElement().get().getRecord());
          }
          if (!(result.getElement().orElse(null) instanceof Document)) {
            return result;
          }
          handleMerge(result.getElement().orElse(null), context);
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
      }
    };
  }

  private void handleMerge(final Record record, final CommandContext context) {
    final MutableDocument doc = ((Document) record).modify();
    final Map<String, Object> map = json != null ? json.toMap(record, context) : resolveExpression(record, context);
    for (final Map.Entry<String, Object> entry : map.entrySet())
      doc.set(entry.getKey(), entry.getValue());
  }

  private Map<String, Object> resolveExpression(final Record record, final CommandContext context) {
    if (expression == null)
      throw new CommandExecutionException("Missing payload for UPDATE MERGE");

    Object value = expression.execute(record, context);

    // A sub-query payload evaluates to a collection: accept it only when it identifies a single record
    if (value instanceof Collection<?> collection) {
      if (collection.size() != 1)
        throw new CommandExecutionException(
            "Invalid value for UPDATE MERGE, expected a single map but found " + collection.size() + " items");
      value = collection.iterator().next();
    }

    // A user-supplied map is merged verbatim: its keys are an explicit intent, unlike the metadata a record-shaped
    // payload carries implicitly, which the branches below strip
    if (value instanceof Map<?, ?> map)
      return checkStringKeys(map);
    else if (value instanceof Document document)
      return document.toMap(false);
    else if (value instanceof Result result)
      return toPropertyMap(result);
    throw new CommandExecutionException("Invalid value for UPDATE MERGE, expected a map but found: " + value);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> checkStringKeys(final Map<?, ?> map) {
    for (final Object key : map.keySet())
      if (!(key instanceof String))
        throw new CommandExecutionException("Invalid value for UPDATE MERGE, expected a map with string keys but found: " + key);
    return (Map<String, Object>) map;
  }

  /**
   * Converts a result into the map of the properties to merge. Metadata ({@code @rid}, {@code @type}, ...) and computed
   * pseudo-properties ({@code $score}, ...) are excluded so they never end up stored on the target record.
   */
  private static Map<String, Object> toPropertyMap(final Result result) {
    final Map<String, Object> map = new LinkedHashMap<>();
    for (final String name : result.getPropertyNames()) {
      if (name.isEmpty())
        continue;
      final char prefix = name.charAt(0);
      if (prefix == '@' || prefix == '$')
        continue;
      map.put(name, result.getProperty(name));
    }
    return map;
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ UPDATE MERGE\n" + spaces + "  " + (json != null ? json : expression);
  }
}
