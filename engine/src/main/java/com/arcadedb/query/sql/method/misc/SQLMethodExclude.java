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

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Filter the content by excluding only some fields. If the content is a document, a result or a map, answers a new map
 * with every property except the excluded ones - plus the metadata fields ({@code @rid}, {@code @type}, ...) when one of
 * the names is a metadata field - never a record, so the projection can not be saved over the record it was read from
 * (issue #9840). If it's a collection it acts on each single entry.
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
 * exclude(&lt;field|value|expression&gt; [,&lt;field-name&gt;]* )
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
 * SELECT <b>exclude(roles, 'permissions')</b> FROM OUser
 * </pre>
 *
 * <p>
 *
 * </blockquote>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodExclude extends AbstractSQLMethod {

  public static final String NAME = "exclude";

  public SQLMethodExclude() {
    super(NAME, 1, -1);
  }

  @Override
  public String getSyntax() {
    return "Syntax error: exclude([<field-name>][,]*)";
  }

  @Override
  public Object execute(final Object current, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
    if (current != null) {
      final boolean includeMetadata = referencesMetadata(params);
      final Map<String, Object> single = projectionSource(current, includeMetadata);
      if (single != null)
        return copy(single, params);

      if (MultiValue.isMultiValue(current)) {
        // ACT ON MULTIPLE DOCUMENTS
        final int size = MultiValue.getSizeIfAvailable(current);
        final List<Object> result = size > 0 ? new ArrayList<>(size) : new ArrayList<>();
        for (final Object o : MultiValue.getMultiValueIterable(current, false)) {
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

  private Object copy(final Map<String, Object> map, final Object[] iFieldNames) {
    final Map<String, Object> doc = new LinkedHashMap<>(map);
    for (final Object iFieldName : iFieldNames) {
      if (iFieldName != null) {
        final String fieldName = iFieldName.toString();

        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          // A MAP RECEIVER MAY CARRY NON-STRING KEYS: MATCH ON THEIR TEXT, AS BEFORE
          ((Map<?, ?>) doc).keySet().removeIf(f -> String.valueOf(f).startsWith(fieldPart));

        } else
          doc.remove(fieldName);
      }
    }
    return doc;
  }
}
