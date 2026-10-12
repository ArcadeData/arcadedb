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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The receiver dispatch {@code keys()} and {@code values()} have in common. They are an index-aligned pair - the n-th
 * key names the n-th value - and that only holds while both ask the receiver the same question. Two methods in two files
 * each dispatching over four receiver types is how the pair drifted twice (issues #6472 and #9843), so the dispatch,
 * including the flattening of a collection of receivers, lives here once and each method says only what it projects out
 * of a Map, a Document and a Result.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
abstract class AbstractSQLMethodKeysValues extends AbstractSQLMethod {

  protected AbstractSQLMethodKeysValues(final String name) {
    super(name);
  }

  /**
   * What this method projects out of a map receiver.
   */
  protected abstract Object fromMap(Map<?, ?> map);

  /**
   * What this method projects out of a document receiver. Implementations must ask the document for its
   * {@code getPropertyNames()}, which is what keeps the n-th key naming the n-th value.
   */
  protected abstract Object fromDocument(Document document);

  /**
   * What this method projects out of a result receiver (a sub-query row, a {@code LET} variable). Implementations must ask the
   * row for its {@code getPropertyNames()}, not for {@code toMap()}, which is element-first and carries the record metadata.
   */
  protected abstract Object fromResult(Result result);

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (value instanceof Map<?, ?> map)
      return fromMap(map);

    if (value instanceof Document document)
      return fromDocument(document);

    if (value instanceof Result result)
      return fromResult(result);

    // AN ARRAY-VALUED PARAMETER OR PROPERTY IS A COLLECTION RECEIVER TOO (ISSUE #7114)
    final List<Object> list = listReceiverOrNull(value);
    if (list != null) {
      final List<Object> result = new ArrayList<>();
      for (final Object o : list) {
        if (o instanceof Map || o instanceof Document || o instanceof Result || MultiValue.isMultiValue(o)) {
          final Object projected = execute(o, currentRecord, context, params);
          if (projected instanceof Collection<?> projectedCollection)
            result.addAll(projectedCollection);
        }
        // scalar/null elements project nothing: skip them rather than recursing into execute(), which returns null for
        // them
      }
      return result;
    }

    return null;
  }
}
