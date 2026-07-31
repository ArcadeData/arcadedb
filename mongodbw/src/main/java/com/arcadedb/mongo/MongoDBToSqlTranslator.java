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
package com.arcadedb.mongo;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.parser.Identifier;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MongoDBToSqlTranslator {

  protected static void buildExpression(final StringBuilder buffer, final Map<String, Object> params, final Document query) {
    for (final Map.Entry<String, Object> entry : query.entrySet()) {
      final Object key = entry.getKey();
      final Object value = entry.getValue();

      if (key instanceof String string && string.startsWith("$"))
        buildExpression(buffer, params, string, value);
      else if (value instanceof Document) {
        buildAnd(buffer, params, key, value);
      } else if (value instanceof List list) {
        if ("$or".equals(key)) {
          buildOr(buffer, params, list);
        } else
          throw new IllegalArgumentException("Invalid operator " + key);
      } else {
        buffer.append(quoteFieldPath(entry.getKey()));
        buffer.append(" = ");
        buildValue(buffer, params, value);
      }
    }
  }

  protected static void buildAnd(final StringBuilder sql, final Map<String, Object> params, final Object key, final Object value) {
    int expressionCount = 0;

    sql.append("(");

    if (value instanceof List) {
      for (final Document o : (List<Document>) value) {
        if (expressionCount++ > 0)
          sql.append(" AND ");

        buildExpression(sql, params, o);
      }
    } else if (value instanceof Document document) {
      for (final Map.Entry<String, Object> subEntry : document.entrySet()) {
        final String subKey = subEntry.getKey();
        final Object subValue = subEntry.getValue();

        if (expressionCount++ > 0)
          sql.append(" AND ");

        if (key != null)
          sql.append(quoteFieldPath(key.toString()));

        buildExpression(sql, params, subKey, subValue);

      }
    }

    sql.append(")");
  }

  protected static void buildExpression(final StringBuilder sql, final Map<String, Object> params, final String key,
      final Object value) {
    if ("$in".equals(key)) {
      if (value instanceof Collection collection) {
        sql.append(" IN ");
        buildCollection(sql, params, collection);
      } else
        throw new IllegalArgumentException("Operator $in was expecting a collection");
    } else if ("$nin".equals(key)) {
      if (value instanceof Collection collection) {
        sql.append(" NOT IN ");
        buildCollection(sql, params, collection);
      } else
        throw new IllegalArgumentException("Operator $nin was expecting a collection");
    } else if ("$eq".equals(key)) {
      sql.append(" = ");
      buildValue(sql, params, value);
    } else if ("$ne".equals(key)) {
      sql.append(" <> ");
      buildValue(sql, params, value);
    } else if ("$lt".equals(key)) {
      sql.append(" < ");
      buildValue(sql, params, value);
    } else if ("$lte".equals(key)) {
      sql.append(" <= ");
      buildValue(sql, params, value);
    } else if ("$gt".equals(key)) {
      sql.append(" > ");
      buildValue(sql, params, value);
    } else if ("$gte".equals(key)) {
      sql.append(" >= ");
      buildValue(sql, params, value);
    } else if ("$exists".equals(key)) {
      sql.append(" IS DEFINED ");
    } else if ("$size".equals(key)) {
      sql.append(".size() = ");
      buildValue(sql, params, value);
    } else if ("$or".equals(key)) {
      buildOr(sql, params, (List) value);
    } else if ("$and".equals(key)) {
      buildAnd(sql, params, key, value);
    } else if ("$not".equals(key)) {
      sql.append(" NOT ");
      buildExpression(sql, params, (Document) value);
    } else
      throw new IllegalArgumentException("Unknown operator " + key);
  }

  protected static void buildOr(final StringBuilder buffer, final Map<String, Object> params, final List list) {
    buffer.append("(");

    int i = 0;
    for (final Object o : list) {
      if (i++ > 0)
        buffer.append(" OR ");

      if (o instanceof Document document) {
        buildExpression(buffer, params, document);
      }
    }

    buffer.append(")");
  }

  /**
   * Binds the whole collection to a single parameter. The SQL grammar accepts an input parameter between the parentheses of an
   * {@code IN} list, so there is no need to emit one placeholder per element.
   */
  protected static void buildCollection(final StringBuilder buffer, final Map<String, Object> params, final Collection coll) {
    buffer.append('(');
    buildValue(buffer, params, coll);
    buffer.append(')');
  }

  /**
   * Binds a value taken off the wire as a named parameter and appends only its placeholder. Nothing the client sent reaches the
   * statement text, so a value can no longer close a quoted literal and append clauses of its own - the injection is
   * unreachable by construction instead of by remembering to escape at each call site. Binding also preserves the value's Java
   * type: spelling it went through {@code String.valueOf}, which renders a {@code Date} or an {@code ObjectId} as text no SQL
   * parser accepts.
   * <p>
   * The parameter name is derived from the map's current size, so names are unique and assigned in the order the values are
   * met. That holds only while the map contains nothing but names this method generated: {@code params} must start empty and
   * carry no caller-supplied entries, otherwise a generated name can collide with one already there and silently overwrite it.
   */
  protected static void buildValue(final StringBuilder buffer, final Map<String, Object> params, final Object value) {
    final String name = "p" + params.size();
    params.put(name, value);
    buffer.append(':').append(name);
  }

  /**
   * Quotes a field reference for embedding in a statement. A MongoDB field name is a dot-separated path, so each segment is quoted
   * on its own: quoting the whole path would turn navigation into a single property whose name contains a dot.
   */
  protected static String quoteFieldPath(final String field) {
    final int dot = field.indexOf('.');
    if (dot < 0)
      return Identifier.quote(field);

    final StringBuilder buffer = new StringBuilder(field.length() + 8);
    int start = 0;
    for (int i = dot; i >= 0; i = field.indexOf('.', start)) {
      if (start > 0)
        buffer.append('.');
      buffer.append(Identifier.quote(field.substring(start, i)));
      start = i + 1;
    }
    return buffer.append('.').append(Identifier.quote(field.substring(start))).toString();
  }

  protected static void fillResultSet(final int numberToSkip, final int numberToReturn, final List<Document> result, final Iterator it) {
    for (int i = 0; it.hasNext(); ++i) {
      if (numberToSkip > 0 && i < numberToSkip - 1)
        continue;

      final Object next = it.next();

      if (next instanceof com.arcadedb.database.Document document)
        result.add(convertDocumentToMongoDB(document));
      else if (next instanceof Result result1)
        result.add(convertDocumentToMongoDB(result1));
      else
        throw new IllegalArgumentException("Object not supported");

      if (numberToReturn > 0 && result.size() >= numberToReturn)
        break;
    }
  }

  protected static Document convertDocumentToMongoDB(final com.arcadedb.database.Document doc) {
    return convertMapToMongoDB(doc.toMap());
  }

  protected static Document convertDocumentToMongoDB(final Result doc) {
    return convertMapToMongoDB(doc.toMap());
  }

  private static Document convertMapToMongoDB(final Map<String, Object> map) {
    final Document result = new Document();
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      final String p = entry.getKey();
      final Object value = entry.getValue();
      result.put(p, "_id".equals(p) ? getObjectId((String) value) : toBsonValue(value));
    }
    return result;
  }

  /**
   * Maps a stored value onto a type the BSON encoder accepts. A temporal property is held as a {@code java.time} value, and
   * the encoder handles exactly one of those, {@link Instant}, rejecting the rest outright and failing the whole response.
   * The engine anchors a stored date to UTC, so the conversion uses the same offset and is exact.
   */
  @SuppressWarnings("unchecked")
  private static Object toBsonValue(final Object value) {
    if (value instanceof Instant)
      return value;
    else if (value instanceof LocalDateTime dateTime)
      return dateTime.toInstant(ZoneOffset.UTC);
    else if (value instanceof LocalDate date)
      return date.atStartOfDay().toInstant(ZoneOffset.UTC);
    else if (value instanceof ZonedDateTime dateTime)
      return dateTime.toInstant();
    else if (value instanceof Date date)
      return date.toInstant();
    else if (value instanceof Map)
      // an embedded document can hold a temporal property of its own
      return convertMapToMongoDB((Map<String, Object>) value);
    else if (value instanceof List<?> list) {
      final List<Object> converted = new ArrayList<>(list.size());
      for (final Object item : list)
        converted.add(toBsonValue(item));
      return converted;
    }
    return value;
  }

  protected static ObjectId getObjectId(final String s) {
    final byte[] buffer = new byte[s.length() / 2];
    for (int i = 0; i < s.length(); i += 2) {
      buffer[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return new ObjectId(buffer);
  }

  protected static Document projectDocument(final Document document, final Document fields, final String idField) {
    if (document == null) {
      return null;
    } else {
      final Document newDocument = new Document();
      final Iterator var4;
      String key;
      if (onlyExclusions(fields)) {
        newDocument.putAll(document);
        var4 = fields.keySet().iterator();

        while (var4.hasNext()) {
          key = (String) var4.next();
          newDocument.remove(key);
        }
      } else {
        var4 = fields.keySet().iterator();

        while (var4.hasNext()) {
          key = (String) var4.next();
          if (Utils.isTrue(fields.get(key))) {
            projectField(document, newDocument, key);
          }
        }
      }

      if (!fields.containsKey(idField)) {
        newDocument.put(idField, document.get(idField));
      }

      return newDocument;
    }
  }

  protected static boolean onlyExclusions(final Document fields) {
    final Iterator var1 = fields.keySet().iterator();

    String key;
    do {
      if (!var1.hasNext()) {
        return true;
      }

      key = (String) var1.next();
    } while (!Utils.isTrue(fields.get(key)));

    return false;
  }

  protected static void projectField(final Document document, final Document newDocument, final String key) {
    if (document != null) {
      final int dotPos = key.indexOf(46);
      if (dotPos > 0) {
        final String mainKey = key.substring(0, dotPos);
        final String subKey = key.substring(dotPos + 1);
        final Object object = document.get(mainKey);
        if (object instanceof Document document1) {
          if (!newDocument.containsKey(mainKey)) {
            newDocument.put(mainKey, new Document());
          }

          projectField(document1, (Document) newDocument.get(mainKey), subKey);
        }
      } else {
        newDocument.put(key, document.get(key));
      }

    }
  }
}
