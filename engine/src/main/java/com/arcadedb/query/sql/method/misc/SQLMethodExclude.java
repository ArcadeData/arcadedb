/*
 * Copyright 2022 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedEdgeType;
import com.arcadedb.schema.EmbeddedVertexType;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

import java.util.*;

/**
 * Filter the content by excluding only some fields. If the content is a document, then creates a
 * copy without the excluded fields. If it's a collection of documents it acts against on each
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
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
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
  public Object execute(Object current, final Identifiable iCurrentRecord, final CommandContext iContext, final Object ioResult, final Object[] iParams) {
    if (current != null) {
      if (current instanceof Identifiable)
        current = ((Identifiable) current).getRecord();
      else if (current instanceof Result)
        return copy(((Result) current).toMap(), iParams);

      if (current instanceof Document) {
        // ACT ON SINGLE DOCUMENT
        return copy((Document) current, iParams);
      } else if (current instanceof Map) {
        // ACT ON SINGLE MAP
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
    if (type instanceof EmbeddedVertexType)
      doc = document.getDatabase().newVertex(document.getTypeName());
    else if (type instanceof EmbeddedEdgeType)
      throw new IllegalArgumentException("Cannot copy an edge");
    else
      doc = document.getDatabase().newDocument(document.getTypeName());

    doc.set(document.toMap());
    doc.setIdentity(document.getIdentity());

    for (final Object iFieldName : iFieldNames) {
      if (iFieldName != null) {
        final String fieldName = iFieldName.toString();
        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          final List<String> toExclude = new ArrayList<>();
          for (final String f : doc.getPropertyNames()) {
            if (f.startsWith(fieldPart))
              toExclude.add(f);
          }

          for (final String f : toExclude)
            doc.remove(f);

        } else
          doc.remove(fieldName);
      }
    }
    return doc;
  }

  private Object copy(final Map map, final Object[] iFieldNames) {
    final Map<String, Object> doc = new HashMap<>(map);
    for (final Object iFieldName : iFieldNames) {
      if (iFieldName != null) {
        final String fieldName = iFieldName.toString();

        if (fieldName.endsWith("*")) {
          final String fieldPart = fieldName.substring(0, fieldName.length() - 1);
          final List<String> toExclude = new ArrayList<>();
          for (final String f : doc.keySet()) {
            if (f.startsWith(fieldPart))
              toExclude.add(f);
          }

          for (final String f : toExclude)
            doc.remove(f);

        } else
          doc.remove(fieldName);
      }
    }
    return doc;
  }
}
