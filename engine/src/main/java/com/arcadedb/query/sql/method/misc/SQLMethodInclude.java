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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Filter the content by including only some fields. If the content is a document, a result or a map, answers a new map
 * with only the included properties and metadata fields - never a record, so the projection can not be saved over the
 * record it was read from (issue #9840). If it's a collection it acts on each single entry.
 *
 * <p>
 *
 * <p>Syntax:
 *
 * <blockquote>
 *
 * <p>
 *
 * <pre>
 * include(&lt;field|value|expression&gt; [,&lt;field-name&gt;]* )
 * </pre>
 *
 * <p>
 *
 * </blockquote>
 *
 * <p>
 *
 * <p>Examples:
 *
 * <blockquote>
 *
 * <p>
 *
 * <pre>
 * SELECT <b>include(roles, 'name')</b> FROM OUser
 * </pre>
 *
 * <p>
 *
 * </blockquote>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodInclude extends AbstractSQLMethod {

  public static final String NAME = "include";

  public SQLMethodInclude() {
    super(NAME, 1, -1);
  }

  @Override
  public String getSyntax() {
    return "Syntax error: include([<field-name>][,]*)";
  }

  @Override
  public Object execute(final Object current, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    // Gated on the RECEIVER, as exclude() is, and not on the first argument: a null field name is skipped wherever it
    // appears (copy() below), so `include(null, 'a')` and `include('a', null)` are the same projection. Gating on
    // params[0] threw the whole projection away for a leading null only (issue #9988, the shape of #7028 and #7889).
    if (current != null) {
      final boolean includeMetadata = referencesMetadata(params);
      if (current instanceof Identifiable identifiable && identifiable.getRecord() instanceof Document document)
        return copy(document, params, includeMetadata);

      final Map<String, Object> single = projectionSource(current, includeMetadata);
      if (single != null)
        return copy(single, params);

      if (MultiValue.isMultiValue(current)) {
        // ACT ON MULTIPLE DOCUMENTS
        final int size = MultiValue.getSizeIfAvailable(current);
        final List<Object> result = size > 0 ? new ArrayList<>(size) : new ArrayList<>();
        for (final Object o : MultiValue.getMultiValueIterable(current, false)) {
          if (o instanceof Identifiable identifiable && identifiable.getRecord() instanceof Document document) {
            result.add(copy(document, params, includeMetadata));
            continue;
          }
          final Map<String, Object> element = projectionSource(o, includeMetadata);
          if (element != null)
            result.add(copy(element, params));
        }
        return result;
      }
    }

    // INVALID, RETURN NULL
    return null;
  }

  /**
   * The same projection as {@link #copy(Map, Object[])} over {@link #projectionSource(Object, boolean)}, without
   * deserializing the whole record: a plain property name is read with {@link Document#get(String)}, and the full map
   * is built only when a wildcard or a metadata ({@code @}) name needs it.
   */
  private Object copy(final Document document, final Object[] iFieldNames, final boolean includeMetadata) {
    Map<String, Object> full = null;
    final Map<String, Object> doc = new LinkedHashMap<>(iFieldNames.length);
    for (final Object iFieldName : iFieldNames) {
      if (iFieldName != null) {
        final String fieldName = iFieldName.toString();

        if (fieldName.endsWith("*") || fieldName.startsWith("@")) {
          if (full == null)
            full = document.toMap(includeMetadata);
          if (fieldName.endsWith("*")) {
            final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
            for (final Map.Entry<String, Object> f : full.entrySet())
              if (f.getKey().startsWith(fieldPart))
                doc.put(f.getKey(), f.getValue());
          } else
            doc.put(fieldName, full.get(fieldName));

        } else
          doc.put(fieldName, document.get(fieldName));
      }
    }
    return doc;
  }

  private Object copy(final Map<String, Object> map, final Object[] iFieldNames) {
    final Map<String, Object> doc = new LinkedHashMap<>(iFieldNames.length);
    for (final Object iFieldName : iFieldNames) {
      if (iFieldName != null) {
        final String fieldName = iFieldName.toString();

        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          // A MAP RECEIVER MAY CARRY NON-STRING KEYS: MATCH ON THEIR TEXT, AS BEFORE
          for (final Map.Entry<?, ?> f : ((Map<?, ?>) map).entrySet()) {
            final String key = String.valueOf(f.getKey());
            if (key.startsWith(fieldPart))
              doc.put(key, f.getValue());
          }

        } else
          doc.put(fieldName, map.get(fieldName));
      }
    }
    return doc;
  }
}
