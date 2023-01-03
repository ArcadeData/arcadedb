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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;

import java.util.*;

/**
 * Filter the content by including only some fields. If the content is a document, then creates a
 * copy with only the included fields. If it's a collection of documents it acts against on each
 * single entry.
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
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
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
  public Object execute(Object current, final Identifiable iCurrentRecord, final CommandContext iContext, final Object ioResult, final Object[] iParams) {
    if (iParams[0] != null) {
      if (current instanceof Identifiable)
        current = ((Identifiable) current).getRecord();
      else if (current instanceof Result)
        return copy(((Result) current).toMap(), iParams);

      if (current instanceof Document) {
        // ACT ON SINGLE DOCUMENT
        return copy((Document) current, iParams);
      } else if (current instanceof Map) {
        // ACT ON MAP
        return copy((Map) current, iParams);
      } else if (MultiValue.isMultiValue(current)) {
        // ACT ON MULTIPLE DOCUMENTS
        final int size = MultiValue.getSizeIfAvailable(current);
        final List<Object> result = size > 0 ? new ArrayList<>(size) : new ArrayList<>();
        for (final Object o : MultiValue.getMultiValueIterable(current, false)) {
          if (o instanceof Identifiable) {
            result.add(copy((Document) ((Identifiable) o).getRecord(), iParams));
          }
        }
        return result;
      }
    }

    // INVALID, RETURN NULL
    return null;
  }

  private Object copy(final Document document, final Object[] iFieldNames) {
    final DocumentType type = document.getDatabase().getSchema().getType(document.getTypeName());

    final MutableDocument doc;

    if (type instanceof VertexType)
      doc = document.getDatabase().newVertex(document.getTypeName());
    else if (type instanceof EdgeType)
      throw new IllegalArgumentException("Cannot copy an edge");
    else
      doc = document.getDatabase().newDocument(document.getTypeName());

    doc.setIdentity(document.getIdentity());

    for (int i = 0; i < iFieldNames.length; ++i) {
      if (iFieldNames[i] != null) {

        final String fieldName = iFieldNames[i].toString();

        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          final List<String> toInclude = new ArrayList<>();
          for (final String f : document.getPropertyNames()) {
            if (f.startsWith(fieldPart))
              toInclude.add(f);
          }

          for (final String f : toInclude)
            doc.set(fieldName, document.get(f));

        } else
          doc.set(fieldName, document.get(fieldName));
      }
    }
    return doc;
  }

  private Object copy(final Map map, final Object[] iFieldNames) {
    final Map<String, Object> doc = new HashMap<>(iFieldNames.length);
    for (int i = 0; i < iFieldNames.length; ++i) {
      if (iFieldNames[i] != null) {
        final String fieldName = iFieldNames[i].toString();

        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          final List<String> toInclude = new ArrayList<>();
          for (final Object f : map.keySet()) {
            if (f.toString().startsWith(fieldPart))
              toInclude.add(f.toString());
          }

          for (final String f : toInclude)
            doc.put(fieldName, map.get(f));

        } else
          doc.put(fieldName, map.get(fieldName));
      }
    }
    return doc;
  }
}
