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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * The keys the unique indexes of a collection are built on (issue #12073). An engine index converts every key to the one type it
 * holds, so a unique index straight on {@code _id} (or on a field of a {@code createIndexes} with {@code unique: true}) made the
 * values 1 and "1", true and "true", or an ObjectId and its hex string the same key. Instead each such index is on a derived
 * property, {@code $unique:<field>[,<field>...]}, holding the value's BSON type and canonical form as text, written in the same
 * code path that writes the document: the engine enforces uniqueness on it, with no locking of its own, and two values collide
 * exactly when mongod's unique index makes them collide. Numbers compare by value whatever their type (1, 1L, 1.0 and
 * NumberDecimal("1.00") are one key), a missing field is a null, a document compares field by field in order.
 * <p>
 * Known differences: an array is one key, not one per element (mongod indexes each element of an array, a multikey index), and
 * only the documents this plugin writes carry the keys: a record written through SQL or another protocol has none, so no unique
 * index of the plugin covers it and, once the collection has the {@code _id} key index, a filter on {@code _id} (which selects its
 * candidates through that index) does not find it.
 */
final class MongoKeys {
  static final String PREFIX = "$unique:";
  static final String ID_KEY = PREFIX + "_id";

  private MongoKeys() {
  }

  static boolean isKeyProperty(final String name) {
    return name.startsWith(PREFIX);
  }

  /**
   * A client field may not take the name of a key property: it would stand for the key of another value.
   */
  static void checkNotKey(final String field) {
    if (isKeyProperty(field))
      throw new MongoServerError(ErrorCode.BadValue.getValue(), ErrorCode.BadValue.getName(),
          "The field name prefix '" + PREFIX + "' is reserved");
  }

  /**
   * The name of the key property of a field list: the fields joined with commas, a comma or a backslash in a field name escaped
   * with a backslash, so that {@code ["a,b"]} and {@code ["a", "b"]} are two properties and {@link #pathsOf} reads either back.
   */
  static String propertyOf(final String[] paths) {
    final StringBuilder buffer = new StringBuilder(PREFIX);
    for (int i = 0; i < paths.length; i++) {
      if (i > 0)
        buffer.append(',');
      for (int c = 0; c < paths[i].length(); c++) {
        final char ch = paths[i].charAt(c);
        if (ch == ',' || ch == '\\')
          buffer.append('\\');
        buffer.append(ch);
      }
    }
    return buffer.toString();
  }

  static String[] pathsOf(final String property) {
    final List<String> paths = new ArrayList<>();
    final StringBuilder path = new StringBuilder();
    for (int c = PREFIX.length(); c < property.length(); c++) {
      final char ch = property.charAt(c);
      if (ch == '\\' && c + 1 < property.length())
        path.append(property.charAt(++c));
      else if (ch == ',') {
        paths.add(path.toString());
        path.setLength(0);
      } else
        path.append(ch);
    }
    paths.add(path.toString());
    return paths.toArray(new String[0]);
  }

  /**
   * The field lists of the unique indexes on derived keys of a type, but the {@code _id} one.
   */
  static List<String[]> uniquePaths(final DocumentType type) {
    List<String[]> result = null;
    for (final TypeIndex index : type.getAllIndexes(false)) {
      if (!index.isUnique() || index.getPropertyNames().size() != 1)
        continue;
      final String property = index.getPropertyNames().getFirst();
      if (isKeyProperty(property) && !ID_KEY.equals(property)) {
        if (result == null)
          result = new ArrayList<>();
        result.add(pathsOf(property));
      }
    }
    return result != null ? result : List.of();
  }

  static boolean hasIdKeyIndex(final DocumentType type) {
    return findIdKeyIndex(type) != null;
  }

  static TypeIndex findIdKeyIndex(final DocumentType type) {
    for (final TypeIndex index : type.getAllIndexes(false))
      if (index.isUnique() && index.getPropertyNames().size() == 1 && ID_KEY.equals(index.getPropertyNames().getFirst()))
        return index;
    return null;
  }

  /**
   * Writes the {@code _id} key (for a document that has an {@code _id}) and the keys of {@code unique} on a record, from the
   * document as the client reads it.
   */
  static void setKeys(final MutableDocument record, final Document bson, final List<String[]> unique) {
    if (bson.containsKey("_id"))
      record.set(ID_KEY, key(bson.get("_id")));
    for (final String[] paths : unique)
      record.set(propertyOf(paths), key(bson, paths));
  }

  static String key(final Document document, final String[] paths) {
    if (paths.length == 1)
      return key(valueAt(document, paths[0]));
    final StringBuilder buffer = new StringBuilder().append('[');
    for (final String path : paths)
      append(buffer, valueAt(document, path));
    return buffer.append(']').toString();
  }

  static String key(final Object value) {
    final StringBuilder buffer = new StringBuilder();
    append(buffer, value);
    return buffer.toString();
  }

  /**
   * The value at a dotted path through embedded documents, {@code null} for a missing one (a unique index takes a missing field
   * as null).
   */
  private static Object valueAt(final Document document, final String path) {
    Object current = document;
    for (final String segment : path.split("\\.")) {
      if (!(current instanceof Document embedded) || !embedded.containsKey(segment))
        return null;
      current = embedded.get(segment);
    }
    return current;
  }

  /**
   * One letter for the BSON type, then the value; a string is prefixed with its length, so the text of a document or an array is
   * unambiguous.
   */
  private static void append(final StringBuilder buffer, final Object value) {
    if (value == null)
      buffer.append('z');
    else if (value instanceof Boolean bool)
      buffer.append('b').append(bool ? '1' : '0');
    else if (value instanceof Number number)
      buffer.append('n').append(numberText(number)).append(';');
    else if (value instanceof String string)
      appendText(buffer.append('s'), string);
    else if (value instanceof ObjectId objectId)
      buffer.append('o').append(objectId.getHexData());
    else if (value instanceof Date date)
      buffer.append('d').append(date.getTime()).append(';');
    else if (value instanceof Instant instant)
      // a BSON date: the decoder and the stored form both read it as an Instant, a client document may hold a Date
      buffer.append('d').append(instant.toEpochMilli()).append(';');
    else if (value instanceof Document document) {
      buffer.append('{');
      for (final Map.Entry<String, Object> entry : document.entrySet()) {
        appendText(buffer, entry.getKey());
        append(buffer, entry.getValue());
      }
      buffer.append('}');
    } else if (value instanceof List<?> list) {
      buffer.append('[');
      for (final Object element : list)
        append(buffer, element);
      buffer.append(']');
    } else if (value instanceof BinData binary)
      buffer.append('x').append(Base64.getEncoder().encodeToString(binary.getData())).append(';');
    else
      appendText(buffer.append('x').append(value.getClass().getSimpleName()).append(':'), value.toString());
  }

  private static void appendText(final StringBuilder buffer, final String text) {
    buffer.append(text.length()).append(':').append(text);
  }

  /**
   * A number as mongod compares it: by value, whatever its type. NaN equals NaN, -0 equals 0.
   */
  private static String numberText(final Number number) {
    final BigDecimal exact;
    if (number instanceof Double || number instanceof Float) {
      final double value = number.doubleValue();
      if (Double.isNaN(value))
        return "NaN";
      if (Double.isInfinite(value))
        return value > 0 ? "Inf" : "-Inf";
      exact = new BigDecimal(value);
    } else if (number instanceof Decimal128 decimal) {
      // the combination field of the high word: 11111 is NaN, 11110 an infinity
      final long combination = (decimal.getHigh() >>> 58) & 0x1F;
      if (combination == 0x1F)
        return "NaN";
      if (combination == 0x1E)
        return decimal.getHigh() < 0 ? "-Inf" : "Inf";
      try {
        exact = decimal.toBigDecimal();
      } catch (final ArithmeticException | IllegalArgumentException e) {
        // a negative zero, which BigDecimal has no form for
        return "0";
      }
    } else if (number instanceof BigDecimal decimal)
      exact = decimal;
    else
      exact = BigDecimal.valueOf(number.longValue());
    return exact.signum() == 0 ? "0" : exact.stripTrailingZeros().toString();
  }
}
