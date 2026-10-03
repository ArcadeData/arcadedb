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
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class MongoDBToSqlTranslator {

  protected static void buildExpression(final StringBuilder buffer, final Map<String, Object> params, final Document query) {
    int expressionCount = 0;
    for (final Map.Entry<String, Object> entry : query.entrySet()) {
      if (expressionCount++ > 0)
        buffer.append(" AND ");

      final Object key = entry.getKey();
      final Object value = entry.getValue();

      if (key instanceof String string && string.startsWith("$"))
        buildExpression(buffer, params, null, string, value);
      else if (value instanceof Document) {
        buildAnd(buffer, params, key, value);
      } else if (value instanceof List list) {
        if ("$or".equals(key)) {
          buildOr(buffer, params, list);
        } else
          throw new IllegalArgumentException("Invalid operator " + key);
      } else if (value instanceof BsonRegularExpression regex) {
        buildRegex(buffer, params, quoteFieldPath(entry.getKey()), regex.getPattern(), regex.getOptions());
      } else
        buildEquality(buffer, params, quoteFieldPath(entry.getKey()), true, value);
    }
  }

  protected static void buildAnd(final StringBuilder sql, final Map<String, Object> params, final Object key, final Object value) {
    sql.append("(");

    if (value instanceof List) {
      int expressionCount = 0;
      for (final Document o : (List<Document>) value) {
        if (expressionCount++ > 0)
          sql.append(" AND ");

        buildExpression(sql, params, o);
      }
    } else if (value instanceof Document document)
      appendOperators(sql, params, key != null ? quoteFieldPath(key.toString()) : null, document, true);

    sql.append(")");
  }

  /**
   * Appends the operators applied to one field, e.g. {@code {$gt: 1, $lt: 5}}, joined with AND. {@code $regex} and its sibling
   * {@code $options} form ONE condition, so {@code $options} is consumed together with {@code $regex}.
   * <p>
   * A field-scoped {@code $not} is built here, where the field is still known: MongoDB's {@code $not} also matches a document
   * whose field is null or missing, whereas SQL's {@code NOT (field > :p)} is unknown (so the row is dropped) when the field is
   * null. The negation is therefore {@code (field IS NULL OR NOT (...))} for every operator that can be unknown on a null.
   * A multi-operator operand needs the field re-emitted for EACH operator, joined with AND.
   */
  private static void appendOperators(final StringBuilder sql, final Map<String, Object> params, final String field,
      final Document operators, final boolean allowNot) {
    int expressionCount = 0;
    for (final Map.Entry<String, Object> subEntry : operators.entrySet()) {
      final String subKey = subEntry.getKey();
      final Object subValue = subEntry.getValue();

      if ("$options".equals(subKey)) {
        if (!operators.containsKey("$regex"))
          throw new IllegalArgumentException("$options needs a $regex");
        continue;
      }

      if (expressionCount++ > 0)
        sql.append(" AND ");

      if ("$not".equals(subKey)) {
        // real MongoDB does not accept $not nested inside $not; rejecting it here avoids silently falling
        // through to the top-level $not branch with no field in scope, which produces invalid SQL
        if (!allowNot)
          throw new IllegalArgumentException("Nested $not is not supported");

        final Document notOperand = subValue instanceof BsonRegularExpression regex ? regex.toDocument() :
            subValue instanceof Document document ? document : null;
        if (notOperand == null)
          throw new IllegalArgumentException("$not needs a regex or a document");
        if (notOperand.isEmpty())
          throw new IllegalArgumentException("$not requires a non-empty operator expression");

        boolean nullSensitive = false;
        for (final Map.Entry<String, Object> operator : notOperand.entrySet())
          if (!isTwoValued(operator.getKey(), operator.getValue()))
            nullSensitive = true;

        if (nullSensitive && field != null)
          sql.append("(").append(field).append(" IS NULL OR ");
        sql.append("NOT (");
        appendOperators(sql, params, field, notOperand, false);
        sql.append(")");
        if (nullSensitive && field != null)
          sql.append(")");
      } else if ("$regex".equals(subKey)) {
        if (subValue instanceof BsonRegularExpression regex)
          buildRegex(sql, params, field, regex.getPattern(), regex.getOptions());
        else
          buildRegex(sql, params, field, String.valueOf(subValue),
              operators.get("$options") != null ? operators.get("$options").toString() : null);
      } else
        buildExpression(sql, params, field, subKey, subValue);
    }
  }

  /**
   * @param field the already quoted field reference the operator applies to, or {@code null} for a top-level operator
   *              ({@code $or}, {@code $and}, {@code $not}) which has none
   */
  protected static void buildExpression(final StringBuilder sql, final Map<String, Object> params, final String field,
      final String key, final Object value) {
    if ("$in".equals(key)) {
      if (value instanceof Collection<?> collection) {
        // MongoDB's {$in: [.., null]} matches a null or missing field too, which SQL's IN never does (unknown)
        if (containsNull(collection)) {
          final Collection<?> withoutNull = withoutNull(collection);
          sql.append('(');
          appendField(sql, field);
          sql.append(" IS NULL");
          if (!withoutNull.isEmpty()) {
            sql.append(" OR ");
            appendField(sql, field);
            sql.append(" IN ");
            buildCollection(sql, params, withoutNull);
          }
          sql.append(')');
        } else {
          appendField(sql, field);
          sql.append(" IN ");
          buildCollection(sql, params, collection);
        }
      } else
        throw new IllegalArgumentException("Operator $in was expecting a collection");
    } else if ("$nin".equals(key)) {
      if (value instanceof Collection<?> collection) {
        // the exact complement of $in: a null or missing field is NOT in the list unless the list holds null itself
        if (containsNull(collection)) {
          final Collection<?> withoutNull = withoutNull(collection);
          sql.append('(');
          appendField(sql, field);
          sql.append(" IS NOT NULL");
          if (!withoutNull.isEmpty()) {
            sql.append(" AND ");
            appendField(sql, field);
            sql.append(" NOT IN ");
            buildCollection(sql, params, withoutNull);
          }
          sql.append(')');
        } else {
          sql.append('(');
          appendField(sql, field);
          sql.append(" IS NULL OR ");
          appendField(sql, field);
          sql.append(" NOT IN ");
          buildCollection(sql, params, collection);
          sql.append(')');
        }
      } else
        throw new IllegalArgumentException("Operator $nin was expecting a collection");
    } else if ("$eq".equals(key)) {
      buildEquality(sql, params, field, true, value);
    } else if ("$ne".equals(key)) {
      buildEquality(sql, params, field, false, value);
    } else if ("$lt".equals(key)) {
      appendField(sql, field);
      sql.append(" < ");
      buildValue(sql, params, value);
    } else if ("$lte".equals(key)) {
      appendField(sql, field);
      sql.append(" <= ");
      buildValue(sql, params, value);
    } else if ("$gt".equals(key)) {
      appendField(sql, field);
      sql.append(" > ");
      buildValue(sql, params, value);
    } else if ("$gte".equals(key)) {
      appendField(sql, field);
      sql.append(" >= ");
      buildValue(sql, params, value);
    } else if ("$exists".equals(key)) {
      appendField(sql, field);
      sql.append(Utils.isTrue(value) ? " IS DEFINED " : " IS NOT DEFINED ");
    } else if ("$size".equals(key)) {
      appendField(sql, field);
      sql.append(".size() = ");
      buildValue(sql, params, value);
    } else if ("$or".equals(key)) {
      if (!(value instanceof List list) || list.isEmpty())
        throw new IllegalArgumentException("Operator $or requires a non-empty array");
      buildOr(sql, params, list);
    } else if ("$and".equals(key)) {
      if (!(value instanceof List list) || list.isEmpty())
        throw new IllegalArgumentException("Operator $and requires a non-empty array");
      buildAnd(sql, params, key, list);
    } else if ("$not".equals(key)) {
      // Reached only for a top-level "$not" (no preceding field), whose operand is a nested {field: {...}} query
      // fragment - buildExpression(Document) below re-enters buildAnd for it and emits its own field name, so this
      // recursion is self-contained. The field-scoped "$not" is handled by appendOperators, where the field is known.
      sql.append(" NOT ");
      buildExpression(sql, params, (Document) value);
    } else
      throw new IllegalArgumentException("Unknown operator " + key);
  }

  /**
   * Whether the SQL emitted for an operator is never "unknown", so negating it with NOT needs no extra care for a null field:
   * {@code $exists}, the null-aware {@code $ne}/{@code $nin}, an equality with null and an {@code $in} that lists null.
   */
  private static boolean isTwoValued(final String operator, final Object operand) {
    return switch (operator) {
      case "$exists", "$ne", "$nin" -> true;
      case "$eq" -> operand == null;
      case "$in" -> operand instanceof Collection<?> collection && containsNull(collection);
      default -> false;
    };
  }

  private static void appendField(final StringBuilder sql, final String field) {
    if (field != null)
      sql.append(field);
  }

  private static boolean containsNull(final Collection<?> collection) {
    for (final Object element : collection)
      if (element == null)
        return true;
    return false;
  }

  private static Collection<?> withoutNull(final Collection<?> collection) {
    final List<Object> result = new ArrayList<>(collection.size());
    for (final Object element : collection)
      if (element != null)
        result.add(element);
    return result;
  }

  /**
   * Emits a regular-expression match. MongoDB's regex FINDS the pattern anywhere in the string, whereas SQL {@code MATCHES}
   * must match the whole string, so the pattern is surrounded by {@code (?s:.*)} and the options become an inline flag group
   * of their own, which applies to the user's pattern only. The engine's {@code MATCHES} caches the compiled pattern per
   * command and bounds the evaluation time, so a hostile expression cannot pin a thread.
   */
  protected static void buildRegex(final StringBuilder sql, final Map<String, Object> params, final String field,
      final String pattern, final String options) {
    final StringBuilder flags = new StringBuilder("u");
    if (options != null)
      for (int i = 0; i < options.length(); i++) {
        final char flag = options.charAt(i);
        switch (flag) {
        case 'i', 'm', 's', 'x' -> {
          if (flags.indexOf(String.valueOf(flag)) < 0)
            flags.append(flag);
        }
        case 'u' -> {
          // already always on
        }
        default -> throw new IllegalArgumentException("Unknown regular expression option '" + flag + "'");
        }
      }

    // in comments mode (x) a trailing "# comment" would swallow the closing parenthesis, hence the line break
    // the user's pattern alone must be valid: validating only the wrapped text would accept a pattern that closes the wrapper
    // itself (e.g. "a)|(b") and silently change its meaning
    try {
      Pattern.compile(pattern);
    } catch (final PatternSyntaxException e) {
      throw new IllegalArgumentException("Invalid regular expression '" + pattern + "': " + e.getDescription(), e);
    }

    final String wrapped = "(?s:.*)(?" + flags + ":" + pattern + (flags.indexOf("x") >= 0 ? "\n" : "") + ")(?s:.*)";
    appendField(sql, field);
    sql.append(" MATCHES ");
    buildValue(sql, params, wrapped);
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
   * <p>
   * Each element is normalized the same way a scalar value is in {@link #buildValue}: binding the collection as-is would leave
   * an {@code ObjectId} element comparing as its {@code toString()} rather than the stored hex string, so {@code $in}/{@code
   * $nin} on {@code _id} would never match even though the scalar (equality) case does.
   */
  protected static void buildCollection(final StringBuilder buffer, final Map<String, Object> params, final Collection coll) {
    // avoid the copy on the common case where nothing needs normalizing
    boolean hasObjectId = false;
    for (final Object element : coll)
      if (element instanceof ObjectId) {
        hasObjectId = true;
        break;
      }

    Collection<?> normalized = coll;
    if (hasObjectId) {
      final List<Object> converted = new ArrayList<>(coll.size());
      for (final Object element : coll)
        converted.add(element instanceof ObjectId objectId ? objectId.getHexData() : element);
      normalized = converted;
    }

    buffer.append('(');
    buildValue(buffer, params, normalized);
    buffer.append(')');
  }

  /**
   * Emits an (in)equality comparison, special-casing a {@code null} operand as {@code IS [NOT] NULL} rather than a
   * bound {@code = null} / {@code <> null} parameter. SQL equality against a bound {@code null} never matches a
   * stored {@code null} (or absent) property, whereas MongoDB's {@code {field: null}} does - it matches a missing
   * field as well as a stored {@code null} - so binding it as an ordinary parameter would silently match nothing.
   * <p>
   * {@code {field: {$ne: null}}} is not the exact negation of that: per MongoDB's own semantics it matches only a
   * field that exists and is not null, excluding a missing field too (rather than including it, the way negating
   * {@code {field: null}} might suggest). {@code IS NOT NULL} matches that: ArcadeDB also evaluates a missing
   * property as {@code null}, so it is excluded here exactly as MongoDB excludes it.
   */
  protected static void buildEquality(final StringBuilder buffer, final Map<String, Object> params, final String field,
      final boolean positive, final Object value) {
    if (value == null)
      buffer.append(field).append(positive ? " IS NULL" : " IS NOT NULL");
    else if (positive) {
      buffer.append(field).append(" = ");
      buildValue(buffer, params, value);
    } else {
      // MongoDB's $ne matches a document whose field is null or missing; SQL's "<>" is unknown for it, so the row would be lost
      buffer.append('(').append(field).append(" IS NULL OR ").append(field).append(" <> ");
      buildValue(buffer, params, value);
      buffer.append(')');
    }
  }

  /**
   * Binds a value taken off the wire as a named parameter and appends only its placeholder. Nothing the client sent reaches the
   * statement text, so a value can no longer close a quoted literal and append clauses of its own - the injection is
   * unreachable by construction instead of by remembering to escape at each call site. Binding also preserves the value's Java
   * type: spelling it went through {@code String.valueOf}, which renders a {@code Date} as text no SQL parser accepts.
   * <p>
   * An {@link ObjectId} is the one exception: it is converted to the same lowercase-hex string used to store it (see
   * {@link MongoDBCollectionWrapper#insertDocuments}), because ArcadeDB has no dedicated ObjectId type. Binding the object
   * itself would compare against its {@code toString()} ({@code "ObjectId[<hex>]"}), which never equals the stored hex text.
   * <p>
   * The parameter name is derived from the map's current size, so names are unique and assigned in the order the values are
   * met. That holds only while the map contains nothing but names this method generated: {@code params} must start empty and
   * carry no caller-supplied entries, otherwise a generated name can collide with one already there and silently overwrite it.
   */
  protected static void buildValue(final StringBuilder buffer, final Map<String, Object> params, final Object value) {
    final String name = "p" + params.size();
    params.put(name, value instanceof ObjectId objectId ? objectId.getHexData() : value);
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
      // consume the element before deciding whether to skip it - a "continue" that never calls next() burns loop
      // counter iterations without advancing the iterator, so skip has no effect for any numberToSkip >= 1
      final Object next = it.next();

      if (numberToSkip > 0 && i < numberToSkip)
        continue;

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
      // the record's own metadata is not part of a MongoDB document: a client would see fields it never wrote
      if (isRecordMetadata(p))
        continue;
      final Object value = entry.getValue();
      result.put(p, "_id".equals(p) ? convertIdToMongoDB(value) : toBsonValue(value));
    }
    return result;
  }

  static boolean isRecordMetadata(final String property) {
    return "@rid".equals(property) || "@type".equals(property) || "@cat".equals(property);
  }

  /**
   * MongoDB allows any BSON scalar as {@code _id}, not just an ObjectId. Only a value that actually looks like a
   * 24-char hex ObjectId string is decoded as one; anything else (an integer, an odd-length or non-hex string, ...)
   * is passed through unchanged instead of corrupting or throwing.
   * <p>
   * Known limitation (#6955): a client-supplied {@code String _id} that happens to be exactly 24 hex characters is,
   * once stored, indistinguishable from a real {@code ObjectId}'s hex encoding - both are the same bare hex string
   * on disk. A plain {@code insertOne} followed by {@code find} therefore round-trips such a {@code String _id} back
   * as an {@code ObjectId}. Unlike the upsert path (see {@code executeUpsert}'s {@code idIsObjectId} tracking), there
   * is no in-memory flag to bridge insert and a later, possibly separate, {@code find} call - fixing this for real
   * would mean persisting the original BSON type alongside {@code _id} (a marker byte/prefix, a side property, or a
   * schema-level type tag), a storage-format decision affecting every document with a hex-looking String {@code _id}
   * rather than a narrow code fix. Left as a documented limitation rather than guessed at unilaterally.
   */
  private static Object convertIdToMongoDB(final Object value) {
    if (value instanceof String s && isObjectIdHex(s))
      return getObjectId(s);
    return toBsonValue(value);
  }

  static boolean isObjectIdHex(final String s) {
    if (s.length() != 24)
      return false;
    for (int i = 0; i < s.length(); i++)
      if (Character.digit(s.charAt(i), 16) < 0)
        return false;
    return true;
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
    if (!isObjectIdHex(s))
      throw new IllegalArgumentException("'" + s + "' is not a 24-char hex ObjectId");

    final byte[] buffer = new byte[s.length() / 2];
    for (int i = 0; i < s.length(); i += 2) {
      buffer[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return new ObjectId(buffer);
  }

  /**
   * Validates a {@code find} projection, which is either an inclusion ({@code {a: 1}}) or an exclusion ({@code {a: 0}}) - the
   * two cannot be mixed, except that {@code _id} can always be excluded from an inclusion.
   *
   * @return {@code true} for an inclusion projection, {@code false} for an exclusion one
   */
  protected static boolean isInclusionProjection(final Document fields, final String idField) {
    boolean inclusion = false;
    boolean exclusion = false;
    for (final Map.Entry<String, Object> entry : fields.entrySet()) {
      final Object flag = entry.getValue();
      if (flag instanceof Document || flag instanceof List)
        throw new IllegalArgumentException("Projection operator on '" + entry.getKey() + "' is not supported");
      if (Utils.isTrue(flag))
        inclusion = true;
      else if (!idField.equals(entry.getKey()))
        exclusion = true;
    }
    if (inclusion && exclusion)
      throw new IllegalArgumentException("Cannot do exclusion on a field in an inclusion projection");
    return inclusion;
  }

  protected static Document projectDocument(final Document document, final Document fields, final String idField) {
    if (document == null)
      return null;

    final Document newDocument = new Document();
    if (isInclusionProjection(fields, idField)) {
      // the _id is part of the answer unless the projection excludes it
      if (!fields.containsKey(idField) || Utils.isTrue(fields.get(idField)))
        if (document.containsKey(idField))
          newDocument.put(idField, document.get(idField));

      for (final Map.Entry<String, Object> entry : fields.entrySet())
        if (Utils.isTrue(entry.getValue()))
          projectField(document, newDocument, entry.getKey());
    } else {
      newDocument.putAll(document);
      for (final String key : fields.keySet())
        removeField(newDocument, key);
    }
    return newDocument;
  }

  /**
   * Removes a possibly dotted field from a copy-on-write view of the document: an embedded document on the path is copied
   * first, so the source document is never modified.
   */
  private static void removeField(final Document document, final String key) {
    final int dotPos = key.indexOf('.');
    if (dotPos <= 0) {
      document.remove(key);
      return;
    }
    final String mainKey = key.substring(0, dotPos);
    if (document.get(mainKey) instanceof Document embedded) {
      final Document copy = new Document();
      copy.putAll(embedded);
      removeField(copy, key.substring(dotPos + 1));
      document.put(mainKey, copy);
    }
  }

  protected static void projectField(final Document document, final Document newDocument, final String key) {
    final int dotPos = key.indexOf('.');
    if (dotPos > 0) {
      final String mainKey = key.substring(0, dotPos);
      final String subKey = key.substring(dotPos + 1);
      final Object object = document.get(mainKey);
      if (object instanceof Document embedded) {
        if (!(newDocument.get(mainKey) instanceof Document))
          newDocument.put(mainKey, new Document());
        projectField(embedded, (Document) newDocument.get(mainKey), subKey);
      }
    } else if (document.containsKey(key))
      newDocument.put(key, document.get(key));
  }
}
