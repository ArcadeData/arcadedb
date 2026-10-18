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
package com.arcadedb.integration.misc;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.serializer.json.NonFiniteNumbers;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/**
 * The value types of a JSONL export that JSON cannot carry by itself (issue #10201).
 * <p>
 * A declared property is read back through its declaration, which restores the type JSON flattened. A value with no
 * declaration - an undeclared property, the elements of an untyped LIST or MAP, the properties of an embedded document - has
 * nothing to restore it from: {@code 3L} reads back as an Integer, {@code 1.50} as a Double without its scale, a date as epoch
 * milliseconds, a {@code byte[]} as a list, a link as a string. So the exporter writes, beside the {@code "p"} properties of a
 * record, a sparse tree of their types under {@link #KEY}, and the importer reads every value it names through it:
 * <ul>
 *   <li>a string token for a scalar: {@code "LONG"}, {@code "DECIMAL"}, {@code "DATETIME"}, {@code "LINK"}, ...
 *   ({@link #EMBEDDED} for an embedded document, which carries the tree of its own properties under the same key);</li>
 *   <li>an array for a list, holding the type of each element by position ({@code null} where the JSON value is exact);</li>
 *   <li>an object for a map, holding the type of each entry that needs one. Empty when no entry needs one but the map has a
 *   {@code "t"} key, which without it is read as an embedded document of the type that key names.</li>
 * </ul>
 * A value JSON reads back exactly (a string, a boolean, an Integer, a finite Double, null, and lists and maps of those) has no
 * entry, so a schema-full database exports what it did before. A file written before the types existed has no tree, and is read
 * as it always was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ValueTypeHints {
  /** The key of the type tree, beside a record's {@code "p"}. */
  public static final String KEY      = "y";
  public static final String EMBEDDED = "EMBEDDED";

  /** A JSON text with no type tree: for a format that holds it as text, the JSON read back as a file without a tree is. */
  public static final  String JSON              = "JSON";
  private static final String INTEGER           = "INTEGER";
  private static final String BOOLEAN           = "BOOLEAN";
  private static final String STRING            = "STRING";
  private static final String LONG              = "LONG";
  private static final String SHORT             = "SHORT";
  private static final String BYTE              = "BYTE";
  private static final String FLOAT             = "FLOAT";
  private static final String DOUBLE            = "DOUBLE";
  private static final String DECIMAL           = "DECIMAL";
  private static final String LINK              = "LINK";
  private static final String DATE              = "DATE";
  private static final String DATETIME          = "DATETIME";
  private static final String ZONED_DATETIME    = "ZONED_DATETIME";
  private static final String OFFSET_DATETIME   = "OFFSET_DATETIME";
  private static final String INSTANT           = "INSTANT";
  private static final String JAVA_DATE         = "JAVA_DATE";
  private static final String CALENDAR          = "CALENDAR";
  private static final String OFFSET_TIME       = "OFFSET_TIME";
  private static final String LOCAL_TIME        = "LOCAL_TIME";
  private static final String DURATION          = "DURATION";
  private static final String UUID_TOKEN        = "UUID";
  private static final String BINARY            = "BINARY";
  private static final String ARRAY_OF_SHORTS   = "ARRAY_OF_SHORTS";
  private static final String ARRAY_OF_INTEGERS = "ARRAY_OF_INTEGERS";
  private static final String ARRAY_OF_LONGS    = "ARRAY_OF_LONGS";
  private static final String ARRAY_OF_FLOATS   = "ARRAY_OF_FLOATS";
  private static final String ARRAY_OF_DOUBLES  = "ARRAY_OF_DOUBLES";

  private ValueTypeHints() {
  }

  /** A value written to JSON, and the type tree that reads it back (null when the JSON value is exact). */
  public record Encoded(Object json, Object hint) {
  }

  // ---------------------------------------------------------------------------------------------------------------------- EXPORT

  /**
   * Adds the type tree of {@code document}'s properties to {@code recordJson}, the record as the graph serializer wrote it,
   * and rewrites the {@code "p"} value of every property the tree names in the encoding the tree reads. Properties whose
   * declaration restores their type are left as the serializer wrote them.
   */
  public static void annotate(final Document document, final JSONObject recordJson) {
    final JSONObject hints = hintsOf(document, recordJson.getJSONObject("p"), true, false);
    if (hints != null)
      recordJson.put(KEY, hints);
  }

  private static JSONObject hintsOf(final Document document, final JSONObject properties, final boolean skipDeclared,
      final boolean linksAsText) {
    final DocumentType type = document.getType();
    JSONObject hints = null;
    for (final Map.Entry<String, Object> entry : document.toMap(false).entrySet()) {
      final Object value = entry.getValue();
      if (value == null)
        continue;
      if (skipDeclared && type != null && isRestoredByDeclaration(type.getPolymorphicPropertyIfExists(entry.getKey())))
        continue;

      final Encoded encoded = encode(value, linksAsText);
      if (encoded.hint() != null) {
        if (hints == null)
          hints = new JSONObject();
        hints.put(entry.getKey(), encoded.hint());
        properties.put(entry.getKey(), encoded.json());
      } else if (!skipDeclared)
        properties.put(entry.getKey(), encoded.json());
    }
    return hints;
  }

  /** Whether the declaration alone gives the value back its type: a scalar, a link, or a list or map of a declared scalar. */
  private static boolean isRestoredByDeclaration(final Property property) {
    if (property == null)
      return false;
    final Type type = property.getType();
    if (type == Type.EMBEDDED)
      return false;
    if (type == Type.LIST || type == Type.MAP) {
      final String ofType = property.getOfType();
      if (ofType == null)
        return false;
      final Type elementType = Type.getTypeByName(ofType);
      return elementType != null && elementType != Type.EMBEDDED && elementType != Type.LIST && elementType != Type.MAP;
    }
    return true;
  }

  /**
   * {@code value} as JSON, and the type tree that reads it back.
   *
   * @param linksAsText true to write a link as the plain text of its RID, with no type: for a format whose reader has no way
   *                    to re-point it at the record the import creates, and would otherwise hand back a link to whatever record
   *                    the target database holds at the source RID
   */
  public static Encoded encode(final Object value, final boolean linksAsText) {
    return switch (value) {
      case null -> new Encoded(null, null);
      case String s -> new Encoded(s, null);
      case Boolean b -> new Encoded(b, null);
      case Integer i -> new Encoded(i, null);
      case Double d -> Double.isFinite(d) ? new Encoded(d, null) : new Encoded(NonFiniteNumbers.encode(d), DOUBLE);
      case Float f -> new Encoded(NonFiniteNumbers.encode(f), FLOAT);
      case Long l -> new Encoded(l, LONG);
      case Short s -> new Encoded(s, SHORT);
      case Byte b -> new Encoded(b, BYTE);
      // AS TEXT: A JSON NUMBER IS READ BACK THROUGH A DOUBLE, WHICH DROPS THE SCALE AND EVERY DIGIT PAST THE 17TH
      case BigDecimal d -> new Encoded(d.toString(), DECIMAL);
      case LocalDateTime t -> new Encoded(t.toString(), DATETIME);
      case LocalDate t -> new Encoded(t.toString(), DATE);
      case ZonedDateTime t -> new Encoded(t.toString(), ZONED_DATETIME);
      case OffsetDateTime t -> new Encoded(t.toString(), OFFSET_DATETIME);
      case Instant t -> new Encoded(t.toString(), INSTANT);
      case Date d -> new Encoded(d.getTime(), JAVA_DATE);
      case Calendar c -> new Encoded(c.getTimeInMillis(), CALENDAR);
      case OffsetTime t -> new Encoded(t.toString(), OFFSET_TIME);
      case LocalTime t -> new Encoded(t.toString(), LOCAL_TIME);
      case CypherDuration d -> new Encoded(d.toString(), DURATION);
      case UUID u -> new Encoded(u.toString(), UUID_TOKEN);
      case byte[] b -> new Encoded(Base64.getEncoder().encodeToString(b), BINARY);
      case short[] a -> {
        final JSONArray json = new JSONArray();
        for (final short s : a)
          json.put(s);
        yield new Encoded(json, ARRAY_OF_SHORTS);
      }
      case int[] a -> {
        final JSONArray json = new JSONArray();
        for (final int i : a)
          json.put(i);
        yield new Encoded(json, ARRAY_OF_INTEGERS);
      }
      case long[] a -> {
        final JSONArray json = new JSONArray();
        for (final long l : a)
          json.put(l);
        yield new Encoded(json, ARRAY_OF_LONGS);
      }
      case float[] a -> {
        final JSONArray json = new JSONArray();
        for (final float f : a)
          json.put(NonFiniteNumbers.encode(f));
        yield new Encoded(json, ARRAY_OF_FLOATS);
      }
      case double[] a -> {
        final JSONArray json = new JSONArray();
        for (final double d : a)
          json.put(NonFiniteNumbers.encode(d));
        yield new Encoded(json, ARRAY_OF_DOUBLES);
      }
      case EmbeddedDocument embedded -> {
        final Document document = (Document) embedded;
        final JSONObject properties = new JSONObject();
        final JSONObject json = new JSONObject().put("t", document.getTypeName()).put("p", properties);
        final JSONObject hints = hintsOf(document, properties, false, linksAsText);
        if (hints != null)
          json.put(KEY, hints);
        yield new Encoded(json, EMBEDDED);
      }
      case Identifiable identifiable -> new Encoded(identifiable.getIdentity().toString(), linksAsText ? null : LINK);
      case Map<?, ?> map -> {
        final JSONObject json = new JSONObject();
        JSONObject hints = null;
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
          final String key = String.valueOf(entry.getKey());
          final Encoded encoded = encode(entry.getValue(), linksAsText);
          json.put(key, encoded.json());
          if (encoded.hint() != null) {
            if (hints == null)
              hints = new JSONObject();
            hints.put(key, encoded.hint());
          }
        }
        // WITHOUT A TREE, A MAP WITH A "t" KEY IS READ AS AN EMBEDDED DOCUMENT OF THE TYPE THE KEY NAMES
        if (hints == null && json.has("t"))
          hints = new JSONObject();
        yield new Encoded(json, hints);
      }
      case Collection<?> collection -> encodeElements(collection, linksAsText);
      case Object[] array -> encodeElements(Arrays.asList(array), linksAsText);
      default -> new Encoded(value, null);
    };
  }

  private static Encoded encodeElements(final Collection<?> elements, final boolean linksAsText) {
    final JSONArray json = new JSONArray();
    final Object[] elementHints = new Object[elements.size()];
    boolean hinted = false;
    int i = 0;
    for (final Object element : elements) {
      final Encoded encoded = encode(element, linksAsText);
      json.put(encoded.json());
      elementHints[i++] = encoded.hint();
      hinted |= encoded.hint() != null;
    }
    if (!hinted)
      return new Encoded(json, null);

    final JSONArray hints = new JSONArray();
    for (final Object hint : elementHints)
      hints.put(hint);
    return new Encoded(json, hints);
  }

  // ---------------------------------------------------------------------------------------------------------------------- IMPORT

  /**
   * Reads {@code json} back through its type tree. {@code untyped} reads a value the tree has no entry for, the way a file
   * without a tree is read.
   */
  public static Object decode(final DatabaseInternal database, final Object json, final Object hint,
      final Function<Object, Object> untyped) {
    if (hint == null || json == null)
      return untyped.apply(json);

    if (hint instanceof JSONArray elementHints) {
      if (!(json instanceof JSONArray array))
        return untyped.apply(json);
      final List<Object> list = new ArrayList<>(array.length());
      for (int i = 0; i < array.length(); i++)
        list.add(decode(database, array.get(i), i < elementHints.length() ? elementHints.get(i) : null, untyped));
      return list;
    }

    if (hint instanceof JSONObject entryHints) {
      if (!(json instanceof JSONObject object))
        return untyped.apply(json);
      final Map<String, Object> map = new LinkedHashMap<>();
      for (final String key : object.keySet())
        map.put(key, decode(database, object.get(key), entryHints.opt(key), untyped));
      return map;
    }

    return decodeScalar(database, json, hint.toString(), untyped);
  }

  private static Object decodeScalar(final DatabaseInternal database, final Object json, final String hint,
      final Function<Object, Object> untyped) {
    return switch (hint) {
      case INTEGER -> toNumber(json).intValue();
      case BOOLEAN -> json instanceof Boolean b ? b : Boolean.valueOf(json.toString());
      case STRING -> json.toString();
      case LONG -> toNumber(json).longValue();
      case SHORT -> toNumber(json).shortValue();
      case BYTE -> toNumber(json).byteValue();
      case FLOAT -> toDouble(json).floatValue();
      case DOUBLE -> toDouble(json);
      case DECIMAL -> new BigDecimal(json.toString());
      case LINK -> new RID(json.toString());
      case DATETIME -> LocalDateTime.parse(json.toString());
      case DATE -> LocalDate.parse(json.toString());
      case ZONED_DATETIME -> ZonedDateTime.parse(json.toString());
      case OFFSET_DATETIME -> OffsetDateTime.parse(json.toString());
      case INSTANT -> Instant.parse(json.toString());
      case JAVA_DATE -> new Date(toNumber(json).longValue());
      case CALENDAR -> {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(toNumber(json).longValue());
        yield calendar;
      }
      case OFFSET_TIME -> OffsetTime.parse(json.toString());
      case LOCAL_TIME -> LocalTime.parse(json.toString());
      case DURATION -> CypherDuration.parse(json.toString());
      case UUID_TOKEN -> UUID.fromString(json.toString());
      case BINARY -> Base64.getDecoder().decode(json.toString());
      case ARRAY_OF_SHORTS -> {
        final JSONArray array = (JSONArray) json;
        final short[] result = new short[array.length()];
        for (int i = 0; i < result.length; i++)
          result[i] = (short) array.getInt(i);
        yield result;
      }
      case ARRAY_OF_INTEGERS -> {
        final JSONArray array = (JSONArray) json;
        final int[] result = new int[array.length()];
        for (int i = 0; i < result.length; i++)
          result[i] = array.getInt(i);
        yield result;
      }
      case ARRAY_OF_LONGS -> {
        final JSONArray array = (JSONArray) json;
        final long[] result = new long[array.length()];
        for (int i = 0; i < result.length; i++)
          result[i] = array.getLong(i);
        yield result;
      }
      case ARRAY_OF_FLOATS -> {
        final JSONArray array = (JSONArray) json;
        final float[] result = new float[array.length()];
        for (int i = 0; i < result.length; i++)
          result[i] = toDouble(array.get(i)).floatValue();
        yield result;
      }
      case ARRAY_OF_DOUBLES -> {
        final JSONArray array = (JSONArray) json;
        final double[] result = new double[array.length()];
        for (int i = 0; i < result.length; i++)
          result[i] = toDouble(array.get(i));
        yield result;
      }
      case EMBEDDED -> decodeEmbedded(database, (JSONObject) json, untyped);
      // A TYPE THIS VERSION DOES NOT KNOW, FROM A NEWER EXPORT: READ AS A FILE WITHOUT A TREE IS
      default -> untyped.apply(json);
    };
  }

  /** A number as JSON holds it, or as a text format writes it. */
  private static Number toNumber(final Object json) {
    return json instanceof Number number ? number : new BigDecimal(json.toString());
  }

  private static Double toDouble(final Object json) {
    if (json instanceof String text) {
      final Double marker = NonFiniteNumbers.decode(text);
      if (marker != null)
        return marker;
      return Double.valueOf(text);
    }
    return ((Number) json).doubleValue();
  }

  private static Object decodeEmbedded(final DatabaseInternal database, final JSONObject json,
      final Function<Object, Object> untyped) {
    final String typeName = json.getString("t", null);
    final JSONObject properties = json.getJSONObject("p", new JSONObject());
    final JSONObject hints = json.getJSONObject(KEY, null);

    final Map<String, Object> values = new LinkedHashMap<>();
    for (final String key : properties.keySet())
      values.put(key, decode(database, properties.get(key), hints != null ? hints.opt(key) : null, untyped));

    if (typeName == null || !database.getSchema().existsType(typeName))
      return values;

    final DocumentType type = database.getSchema().getType(typeName);
    final MutableDocument document = type instanceof LocalVertexType ?
        database.newVertex(typeName) :
        database.newEmbeddedDocument(null, typeName);
    document.fromMap(values);
    return document;
  }
}
