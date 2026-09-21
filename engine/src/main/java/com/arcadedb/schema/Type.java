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
package com.arcadedb.schema;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.ImmutableEmbeddedDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.MultiIterator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Generic representation of a type.<br>
 * allowAssignmentFrom accepts any class, but Array.class means that the type accepts generic Arrays.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public enum Type {
  BOOLEAN("Boolean", 0, BinaryTypes.TYPE_BOOLEAN, Boolean.class, new Class<?>[] { Number.class }),

  INTEGER("Integer", 1, BinaryTypes.TYPE_INT, Integer.class, new Class<?>[] { Number.class }),

  SHORT("Short", 2, BinaryTypes.TYPE_SHORT, Short.class, new Class<?>[] { Number.class }),

  LONG("Long", 3, BinaryTypes.TYPE_LONG, Long.class, new Class<?>[] { Number.class, }),

  FLOAT("Float", 4, BinaryTypes.TYPE_FLOAT, Float.class, new Class<?>[] { Number.class }),

  DOUBLE("Double", 5, BinaryTypes.TYPE_DOUBLE, Double.class, new Class<?>[] { Number.class }),

  DATETIME("Datetime", 6, BinaryTypes.TYPE_DATETIME, Date.class,
      new Class<?>[] { Date.class, LocalDateTime.class, ZonedDateTime.class, OffsetDateTime.class, Instant.class, Number.class }),

  STRING("String", 7, BinaryTypes.TYPE_STRING, String.class, new Class<?>[] { Enum.class }),

  BINARY("Binary", 8, BinaryTypes.TYPE_BINARY, byte[].class, new Class<?>[] { byte[].class }),

  LIST("List", 9, BinaryTypes.TYPE_LIST, List.class, new Class<?>[] { List.class, MultiIterator.class }),

  MAP("Map", 10, BinaryTypes.TYPE_MAP, Map.class, new Class<?>[] { Map.class }),

  LINK("Link", 11, BinaryTypes.TYPE_RID, Identifiable.class, new Class<?>[] { Identifiable.class, RID.class }),

  BYTE("Byte", 12, BinaryTypes.TYPE_BYTE, Byte.class, new Class<?>[] { Number.class }),

  DATE("Date", 13, BinaryTypes.TYPE_DATE, Date.class, new Class<?>[] { LocalDate.class, Number.class }),

  DECIMAL("Decimal", 14, BinaryTypes.TYPE_DECIMAL, BigDecimal.class, new Class<?>[] { BigDecimal.class, Number.class }),

  EMBEDDED("Embedded", 15, BinaryTypes.TYPE_EMBEDDED, Document.class,
      new Class<?>[] { EmbeddedDocument.class, ImmutableEmbeddedDocument.class, MutableEmbeddedDocument.class }),

  DATETIME_MICROS("Datetime_micros", 16, BinaryTypes.TYPE_DATETIME_MICROS, LocalDateTime.class,
      new Class<?>[] { Date.class, LocalDateTime.class, ZonedDateTime.class, OffsetDateTime.class, Instant.class, Number.class }),

  DATETIME_NANOS("Datetime_nanos", 17, BinaryTypes.TYPE_DATETIME_NANOS, LocalDateTime.class,
      new Class<?>[] { Date.class, LocalDateTime.class, ZonedDateTime.class, OffsetDateTime.class, Instant.class, Number.class }),

  DATETIME_SECOND("Datetime_second", 18, BinaryTypes.TYPE_DATETIME_SECOND, LocalDateTime.class,
      new Class<?>[] { Date.class, LocalDateTime.class, ZonedDateTime.class, OffsetDateTime.class, Instant.class, Number.class }),

  ARRAY_OF_SHORTS("Short[]", 19, BinaryTypes.TYPE_ARRAY_OF_SHORTS, short[].class, new Class<?>[] { short[].class, Short[].class }),

  ARRAY_OF_INTEGERS("Integer[]", 20, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, int[].class,
      new Class<?>[] { int[].class, Integer[].class }),

  ARRAY_OF_LONGS("Long[]", 21, BinaryTypes.TYPE_ARRAY_OF_LONGS, long[].class, new Class<?>[] { long[].class, Long[].class }),

  ARRAY_OF_FLOATS("Float[]", 22, BinaryTypes.TYPE_ARRAY_OF_FLOATS, float[].class, new Class<?>[] { float[].class, Float[].class }),

  ARRAY_OF_DOUBLES("Double[]", 23, BinaryTypes.TYPE_ARRAY_OF_DOUBLES, double[].class,
      new Class<?>[] { double[].class, Double[].class }),
  ;

  public static final  String              DATE_FORMAT_DAYS    = "yyyy-MM-dd";
  public static final  String              DATE_FORMAT_SECONDS = "yyyy-MM-dd HH:mm:ss";
  public static final  String              DATE_FORMAT_MILLIS  = "yyyy-MM-dd HH:mm:ss.SSS";
  // Don't change the order, the type discover get broken if you change the order.
  private static final Type[]              TYPES               = new Type[] { LIST, MAP, LINK, STRING, DATETIME };
  private static final Type[]              TYPES_BY_ID         = new Type[24];
  // Values previously stored in javaTypes
  private static final Map<Class<?>, Type> TYPES_BY_USERTYPE   = new HashMap<Class<?>, Type>();
  private static final Map<String, Type>   TYPES_BY_NAME       = new HashMap<String, Type>();
  /**
   * The largest magnitude at which every {@code float} with an integral value is exactly the shortest decimal that
   * round-trips it: 2^24 itself qualifies, and above it consecutive floats are more than one apart. See
   * {@link #widenFloat}.
   */
  private static final float               EXACT_INTEGRAL_FLOAT = 1 << 24;
  /**
   * The largest magnitude at which every {@code long} is exactly representable as a {@code double}: 2^53 itself
   * qualifies, and above it consecutive doubles are more than one apart, so distinct longs collapse onto the same
   * double. See {@link #isExactAsDouble}.
   */
  private static final long                EXACT_INTEGRAL_DOUBLE = 1L << 53;

  static {
    for (final Type type : values()) {
      TYPES_BY_ID[type.id] = type;
      TYPES_BY_NAME.put(type.name.toLowerCase(Locale.ENGLISH), type);
    }

    // This is made by hand because not all types should be add.
    TYPES_BY_USERTYPE.put(Boolean.class, BOOLEAN);
    TYPES_BY_USERTYPE.put(Boolean.TYPE, BOOLEAN);
    TYPES_BY_USERTYPE.put(Integer.TYPE, INTEGER);
    TYPES_BY_USERTYPE.put(Integer.class, INTEGER);
    TYPES_BY_USERTYPE.put(BigInteger.class, INTEGER);
    TYPES_BY_USERTYPE.put(Short.class, SHORT);
    TYPES_BY_USERTYPE.put(Short.TYPE, SHORT);
    TYPES_BY_USERTYPE.put(Long.class, LONG);
    TYPES_BY_USERTYPE.put(Long.TYPE, LONG);
    TYPES_BY_USERTYPE.put(Float.TYPE, FLOAT);
    TYPES_BY_USERTYPE.put(Float.class, FLOAT);
    TYPES_BY_USERTYPE.put(Double.TYPE, DOUBLE);
    TYPES_BY_USERTYPE.put(Double.class, DOUBLE);
    TYPES_BY_USERTYPE.put(Date.class, DATETIME);
    TYPES_BY_USERTYPE.put(Calendar.class, DATETIME);
    TYPES_BY_USERTYPE.put(LocalDateTime.class, DATETIME);
    TYPES_BY_USERTYPE.put(ZonedDateTime.class, DATETIME);
    TYPES_BY_USERTYPE.put(OffsetDateTime.class, DATETIME);
    TYPES_BY_USERTYPE.put(Instant.class, DATETIME);
    TYPES_BY_USERTYPE.put(String.class, STRING);
    TYPES_BY_USERTYPE.put(Enum.class, STRING);
    TYPES_BY_USERTYPE.put(byte[].class, BINARY);
    TYPES_BY_USERTYPE.put(Byte.class, BYTE);
    TYPES_BY_USERTYPE.put(Byte.TYPE, BYTE);
    TYPES_BY_USERTYPE.put(Character.class, STRING);
    TYPES_BY_USERTYPE.put(Character.TYPE, STRING);
    TYPES_BY_USERTYPE.put(BigDecimal.class, DECIMAL);
    TYPES_BY_USERTYPE.put(List.class, LIST);
    TYPES_BY_USERTYPE.put(Map.class, MAP);
    TYPES_BY_USERTYPE.put(EmbeddedDocument.class, EMBEDDED);
    TYPES_BY_USERTYPE.put(ImmutableEmbeddedDocument.class, EMBEDDED);
    TYPES_BY_USERTYPE.put(MutableEmbeddedDocument.class, EMBEDDED);
    TYPES_BY_USERTYPE.put(short[].class, ARRAY_OF_SHORTS);
    TYPES_BY_USERTYPE.put(int[].class, ARRAY_OF_INTEGERS);
    TYPES_BY_USERTYPE.put(long[].class, ARRAY_OF_LONGS);
    TYPES_BY_USERTYPE.put(float[].class, ARRAY_OF_FLOATS);
    TYPES_BY_USERTYPE.put(double[].class, ARRAY_OF_DOUBLES);

    BYTE.castable.add(BOOLEAN);
    SHORT.castable.addAll(Arrays.asList(BOOLEAN, BYTE));
    INTEGER.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT));
    LONG.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER));
    FLOAT.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER));
    DOUBLE.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT));
    DECIMAL.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE));
  }

  final         String     name;
  private final int        id;
  private final byte       binaryType;
  final         Class<?>   javaDefaultType;
  private final Class<?>[] allowAssignmentFrom;
  private final Set<Type>  castable;

  Type(final String name, final int id, final byte binaryType, final Class<?> javaDefaultType,
      final Class<?>[] allowAssignmentBy) {
    this.name = name.toUpperCase(Locale.ENGLISH);
    this.id = id;
    this.binaryType = binaryType;
    this.javaDefaultType = javaDefaultType;
    this.allowAssignmentFrom = allowAssignmentBy;
    this.castable = new HashSet<>();
    this.castable.add(this);
  }

  /**
   * Return the type by ID.
   *
   * @param id The id to search
   *
   * @return The type if any, otherwise null
   */
  public static Type getById(final byte id) {
    if (id >= 0 && id < TYPES_BY_ID.length)
      return TYPES_BY_ID[id];
    return null;
  }

  /**
   * Return the type by binary type as byte.
   */
  public static Type getByBinaryType(final byte binaryType) {
    for (int i = 0; i < TYPES_BY_ID.length; i++) {
      if (TYPES_BY_ID[i].binaryType == binaryType)
        return TYPES_BY_ID[i];
    }
    return null;
  }

  public static void validateValue(final Object value) {
    if (value != null) {
      if (value instanceof String || value instanceof Number || value instanceof Map || value instanceof Collection)
        return;

      if (!TYPES_BY_USERTYPE.containsKey(value.getClass()))
        throw new IllegalArgumentException("Value '" + value + "' of class '" + value.getClass() + "' is not supported");
    }
  }

  /**
   * Get the identifier of the type. use this instead of {@link Enum#ordinal()} for guarantee a cross code version identifier.
   *
   * @return the identifier of the type.
   */
  public int getId() {
    return id;
  }

  /**
   * Return the correspondent type by checking the "assignability" of the class received as parameter.
   *
   * @param clazz Class to check
   *
   * @return OType instance if found, otherwise null
   */
  public static Type getTypeByClass(final Class<?> clazz) {
    if (clazz == null)
      return null;

    Type type = TYPES_BY_USERTYPE.get(clazz);
    if (type != null)
      return type;
    return getTypeByClassInherit(clazz);

  }

  private static Type getTypeByClassInherit(final Class<?> iClass) {
    if (iClass.isArray())
      return LIST;
    int priority = 0;
    boolean comparedAtLeastOnce;
    do {
      comparedAtLeastOnce = false;
      for (final Type type : TYPES) {
        if (type.allowAssignmentFrom.length > priority) {
          if (type.allowAssignmentFrom[priority].isAssignableFrom(iClass))
            return type;
          comparedAtLeastOnce = true;
        }
      }

      priority++;
    } while (comparedAtLeastOnce);
    return null;
  }

  public static Type getTypeByValue(final Object value) {
    if (value == null)
      return null;
    final Class<?> typez = value.getClass();
    final Type type = TYPES_BY_USERTYPE.get(typez);
    if (type != null)
      return type;

    final Type byType = getTypeByClassInherit(typez);

    return byType;
  }

  public static Type getTypeByName(final String name) {
    return TYPES_BY_NAME.get(name.toLowerCase(Locale.ENGLISH));
  }

  /**
   * Convert types based on the iTargetClass parameter.
   *
   * @param value       Value to convert
   * @param targetClass Expected class
   *
   * @return The converted value or the original if no conversion was applied
   */
  public static Object convert(final Database database, final Object value, final Class<?> targetClass) {
    return convert(database, value, targetClass, null);
  }

  /**
   * Same as {@link #convert(Database, Object, Class)}, but a failed conversion returns {@code null} instead of
   * throwing, for callers - typically a comparison across incompatible types - that need "no defined ordering"
   * rather than a raw parse exception (#5900).
   * <p>
   * This is now the ONLY way to get a {@code null} out of a failed conversion, and choosing between the two methods
   * is a real decision. {@code convert()} used to answer {@code null} for every exception that was not an
   * {@link IllegalArgumentException} - which is how a date literal it could not parse was silently stored as
   * {@code null} by an {@code INSERT} that reported success (issue #8090). It now refuses instead, so a WRITE path
   * must call {@code convert()} and a path that merely coerces values it did not write - a comparison, an index key
   * on a schemaless property where one heterogeneous row must not fail {@code CREATE INDEX} - calls this one.
   */
  public static Object convertOrNull(final Database database, final Object value, final Class<?> targetClass) {
    return convertOrNull(database, value, targetClass, null);
  }

  /**
   * {@link #convertOrNull(Database, Object, Class)} carrying the target {@link Property}, so a datetime keeps being
   * truncated to the precision the column declares while a value that cannot be converted still answers
   * {@code null} instead of throwing.
   */
  public static Object convertOrNull(final Database database, final Object value, final Class<?> targetClass,
      final Property property) {
    try {
      return convert(database, value, targetClass, property);
    } catch (final IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * When a property is declared as a collection ({@code LIST}/{@code MAP}) with a scalar {@code ofType} (e.g. {@code LIST OF LONG}),
   * returns a copy of the collection with every plain scalar entry converted to the declared {@code ofType}. Returns {@code null}
   * when no coercion applies, so the caller falls through to the normal conversion path. Nested documents, collections and links are
   * left untouched so validation can still report structural mismatches with its specific messages (issue #5261).
   */
  private static Object coerceCollectionOfType(final Database database, final Object value, final Class<?> targetClass,
      final Property property) {
    if (property == null)
      return null;

    final String ofTypeName = property.getOfType();
    if (ofTypeName == null)
      return null;

    final Type ofType = getTypeByName(ofTypeName);
    if (ofType == null)
      // The "ofType" refers to an embedded document type, not a scalar: nothing to coerce here.
      return null;

    final Class<?> ofClass = ofType.getDefaultJavaType();

    if (value instanceof Map<?, ?> sourceMap && Map.class.isAssignableFrom(targetClass)) {
      final Map<Object, Object> result = new LinkedHashMap<>(sourceMap.size());
      for (final Map.Entry<?, ?> entry : sourceMap.entrySet())
        result.put(entry.getKey(), coerceScalarItem(database, entry.getValue(), ofClass));
      return result;
    } else if (value instanceof Collection<?> sourceCollection && List.class.isAssignableFrom(targetClass)) {
      final List<Object> result = new ArrayList<>(sourceCollection.size());
      for (final Object item : sourceCollection)
        result.add(coerceScalarItem(database, item, ofClass));
      return result;
    }

    return null;
  }

  private static Object coerceScalarItem(final Database database, final Object item, final Class<?> ofClass) {
    if (item == null)
      return null;

    // Only coerce plain scalar values; leave nested documents/collections/links to the validation layer.
    if (item instanceof Number || item instanceof Boolean || item instanceof CharSequence || item instanceof Character)
      return convert(database, item, ofClass, null);

    return item;
  }

  public static Object convert(final Database database, final Object value, Class<?> targetClass, final Property property) {
    if (value == null)
      return null;

    if (targetClass == null)
      return value;

    // Coerce the nested scalar values of a collection declared with a scalar "ofType" (e.g. LIST OF LONG, MAP OF LONG) to the
    // declared type. JSON parsing on the write path (e.g. the remote client re-serializing a full record with UPDATE ... CONTENT)
    // loses the distinction between Long and Integer for values that fit the 32-bit range, so the container is assignable but its
    // entries carry the wrong scalar type and would be rejected by validation (issue #5261).
    final Object coerced = coerceCollectionOfType(database, value, targetClass, property);
    if (coerced != null)
      return coerced;

    final Class<?> valueClass = value.getClass();

    if (property == null ||
        !(value instanceof LocalDateTime) &&
            !(value instanceof ZonedDateTime) &&
            !(value instanceof Instant)) {
      if (valueClass.equals(targetClass))
        // SAME TYPE: DON'T CONVERT IT
        return value;

      if (targetClass.isAssignableFrom(valueClass))
        // COMPATIBLE TYPES: DON'T CONVERT IT
        return value;
    }

    try {
      if (targetClass.equals(String.class))
        return value.toString();
      else if (value instanceof Binary binary && targetClass.isAssignableFrom(byte[].class))
        return binary.toByteArray();
      else if (byte[].class.isAssignableFrom(valueClass)) {
        return value;
      } else if (value instanceof JSONArray jsonArray) {
        // JSONArray is an Iterable but not a java.util.Collection, so without this branch it would fall through to
        // the `List.of(value)` case below and get wrapped as a single element instead of having its items copied.
        // Normalize it to a real List and re-enter the conversion so the collection/array branches handle it (issue #5091).
        return convert(database, jsonArray.toList(), targetClass, property);
      } else if (targetClass.equals(float[].class) && value instanceof Collection<?> collection) {
        // Convert Collection to float[]
        final float[] array = new float[collection.size()];
        int i = 0;
        for (final Object item : collection)
          array[i++] = requireNonNullNumber(item, "FLOAT", property).floatValue();
        return array;
      } else if (targetClass.equals(float[].class) && value instanceof double[] src) {
        // Fast path: primitive narrowing copy
        final float[] array = new float[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = (float) src[i];
        return array;
      } else if (targetClass.equals(float[].class) && value instanceof long[] src) {
        final float[] array = new float[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = src[i];
        return array;
      } else if (targetClass.equals(double[].class) && value instanceof Collection<?> collection) {
        // Convert Collection to double[]
        final double[] array = new double[collection.size()];
        int i = 0;
        for (final Object item : collection)
          array[i++] = requireNonNullNumber(item, "DOUBLE", property).doubleValue();
        return array;
      } else if (targetClass.equals(double[].class) && value instanceof float[] src) {
        // Issue #3864 follow-up: HTTP vector params arrive as float[] from JSON parsing.
        final double[] array = new double[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = src[i];
        return array;
      } else if (targetClass.equals(double[].class) && value instanceof long[] src) {
        final double[] array = new double[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = src[i];
        return array;
      } else if (targetClass.equals(int[].class) && value instanceof Collection<?> collection) {
        // Convert Collection to int[]
        final int[] array = new int[collection.size()];
        int i = 0;
        for (final Object item : collection)
          array[i++] = narrowToIntegral((Number) item, Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER", property).intValue();
        return array;
      } else if (targetClass.equals(int[].class) && value instanceof long[] src) {
        final int[] array = new int[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = (int) narrowToIntegral(src[i], Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER", property);
        return array;
      } else if (targetClass.equals(int[].class) && value instanceof double[] src) {
        final int[] array = new int[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = (int) narrowToIntegral(src[i], Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER", property);
        return array;
      } else if (targetClass.equals(int[].class) && value instanceof float[] src) {
        final int[] array = new int[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = (int) narrowToIntegral(src[i], Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER", property);
        return array;
      } else if (targetClass.equals(long[].class) && value instanceof Collection<?> collection) {
        // Convert Collection to long[]. LONG is the widest integral type, so only the NaN guard applies (no
        // narrower range to check - see narrowToIntegral()).
        final long[] array = new long[collection.size()];
        int i = 0;
        for (final Object item : collection)
          array[i++] = narrowToIntegral((Number) item, Long.MIN_VALUE, Long.MAX_VALUE, "LONG", property).longValue();
        return array;
      } else if (targetClass.equals(long[].class) && value instanceof double[] src) {
        // LONG is the widest integral type, so only the NaN guard applies (no narrower range to check - see narrowToIntegral()).
        final long[] array = new long[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = narrowToIntegral(src[i], Long.MIN_VALUE, Long.MAX_VALUE, "LONG", property);
        return array;
      } else if (targetClass.equals(long[].class) && value instanceof float[] src) {
        final long[] array = new long[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = narrowToIntegral(src[i], Long.MIN_VALUE, Long.MAX_VALUE, "LONG", property);
        return array;
      } else if (targetClass.equals(short[].class) && value instanceof Collection<?> collection) {
        // Convert Collection to short[]
        final short[] array = new short[collection.size()];
        int i = 0;
        for (final Object item : collection)
          array[i++] = narrowToIntegral((Number) item, Short.MIN_VALUE, Short.MAX_VALUE, "SHORT", property).shortValue();
        return array;
      } else if (targetClass.equals(short[].class) && value instanceof long[] src) {
        final short[] array = new short[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = (short) narrowToIntegral(src[i], Short.MIN_VALUE, Short.MAX_VALUE, "SHORT", property);
        return array;
      } else if (targetClass.equals(short[].class) && value instanceof double[] src) {
        final short[] array = new short[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = (short) narrowToIntegral(src[i], Short.MIN_VALUE, Short.MAX_VALUE, "SHORT", property);
        return array;
      } else if (targetClass.equals(short[].class) && value instanceof float[] src) {
        final short[] array = new short[src.length];
        for (int i = 0; i < src.length; i++)
          array[i] = (short) narrowToIntegral(src[i], Short.MIN_VALUE, Short.MAX_VALUE, "SHORT", property);
        return array;
      } else if (targetClass.equals(byte[].class) && value instanceof Collection<?> collection) {
        // Convert Collection to byte[] for a BINARY property (issue #6061 follow-up): a JSON array
        // received over the wire (e.g. from RemoteGraphBatch) parses into a List<Number>, which had
        // no narrowing path back to byte[] and was silently stored as an untyped List instead.
        final byte[] array = new byte[collection.size()];
        int i = 0;
        for (final Object item : collection)
          array[i++] = narrowToIntegral((Number) item, Byte.MIN_VALUE, Byte.MAX_VALUE, "BYTE", property).byteValue();
        return array;
      } else if (targetClass.isEnum()) {
        if (value instanceof Number number)
          return ((Class<Enum>) targetClass).getEnumConstants()[number.intValue()];
        return Enum.valueOf((Class<Enum>) targetClass, value.toString());
      } else if (targetClass.equals(Byte.TYPE) || targetClass.equals(Byte.class)) {
        if (value instanceof Byte)
          return value;
        else if (value instanceof String string)
          return Byte.parseByte(string);
        else
          return narrowToIntegral((Number) value, Byte.MIN_VALUE, Byte.MAX_VALUE, "BYTE", property).byteValue();

      } else if (targetClass.equals(Short.TYPE) || targetClass.equals(Short.class)) {
        if (value instanceof Short)
          return value;
        else if (value instanceof String string)
          return string.isEmpty() ? 0 : Short.parseShort(string);
        else
          return narrowToIntegral((Number) value, Short.MIN_VALUE, Short.MAX_VALUE, "SHORT", property).shortValue();

      } else if (targetClass.equals(Integer.TYPE) || targetClass.equals(Integer.class)) {
        if (value instanceof Integer)
          return value;
        else if (value instanceof String string)
          return string.isEmpty() ? 0 : Integer.parseInt(string);
        else
          return narrowToIntegral((Number) value, Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER", property).intValue();

      } else if (targetClass.equals(Long.TYPE) || targetClass.equals(Long.class)) {
        if (value instanceof Long)
          return value;
        else if (value instanceof String string)
          return string.isEmpty() ? 0L : Long.parseLong(string);
        else if (DateUtils.isDate(value))
          return DateUtils.dateTimeToTimestamp(value, ChronoUnit.MILLIS);
        else if (isNaN((Number) value))
          // LONG never goes through narrowToIntegral() - there is no narrower range to check, it IS the widest
          // integral type - so it needs its own NaN guard (issue #5970).
          throw new IllegalArgumentException(
              "Value '" + value + "' is NaN and cannot be converted to type LONG" //
                  + (property != null ? " for property '" + property.getName() + "'" : ""));
        else
          return ((Number) value).longValue();

      } else if (targetClass.equals(Float.TYPE) || targetClass.equals(Float.class)) {
        if (value instanceof Float)
          return value;
        else if (value instanceof String string)
          return string.isEmpty() ? 0f : Float.parseFloat(string);
        else
          return ((Number) value).floatValue();

      } else if (targetClass.equals(BigDecimal.class)) {
        if (value instanceof String string)
          return new BigDecimal(string);
        else if (value instanceof Number)
          return new BigDecimal(value.toString());

      } else if (targetClass.equals(Double.TYPE) || targetClass.equals(Double.class)) {
        if (value instanceof Double)
          return value;
        else if (value instanceof String string)
          return string.isEmpty() ? 0d : Double.parseDouble(string);
        else if (value instanceof Float float1)
          // The primitive widening would carry the float's rounding error into the double; widenFloat re-reads its
          // decimal instead, and skips the round-trip where it provably cannot matter (issue #7609).
          return widenFloat(float1);
        else
          return ((Number) value).doubleValue();

      } else if (targetClass.equals(Boolean.TYPE) || targetClass.equals(Boolean.class)) {
        if (value instanceof Boolean)
          return value;
        else if (value instanceof String string) {
          if ("true".equalsIgnoreCase(string))
            return Boolean.TRUE;
          else if ("false".equalsIgnoreCase(string))
            return Boolean.FALSE;
          throw new IllegalArgumentException("Value is not boolean. Expected true or false but received '" + value + "'");
        } else if (value instanceof Number number)
          return number.intValue() != 0;

      } else if (Set.class.isAssignableFrom(targetClass)) {
        // The caller specifically wants a Set.  If the value is a collection
        // we will add all of the items in the collection to a set.  Otherwise
        // we will create a singleton set with only the value in it.
        if (value instanceof Collection<?> collection) {
          final Set<Object> set = new HashSet<Object>(collection);
          return set;
        } else {
          return Set.of(value);
        }

      } else if (List.class.isAssignableFrom(targetClass)) {
        // The caller specifically wants a List.  If the value is a collection
        // we will add all of the items in the collection to a List.  Otherwise
        // we will create a singleton List with only the value in it.
        if (value instanceof Collection<?> collection) {
          final List<Object> list = new ArrayList<Object>(collection);
          return list;
        } else {
          return List.of(value);
        }

      } else if (targetClass.equals(Collection.class)) {
        // The caller specifically wants a Collection of any type.
        // we will return a list if the value is a collection or
        // a singleton set if the value is not a collection.
        if (value instanceof Collection<?> collection) {
          final List<Object> set = new ArrayList<Object>(collection);
          return set;
        } else {
          return Set.of(value);
        }

      } else if (targetClass.equals(EmbeddedDocument.class)) {
        if (value instanceof Map map) {
          final DocumentType embeddedType = database.getSchema().getType((String) map.get("@type"));
          return new MutableEmbeddedDocument(database, embeddedType, null);
        } else
          throw new IllegalArgumentException(
              "Cannot convert object of type '" + value.getClass().getName() + "' into an EmbeddedDocument");

      } else if (targetClass.equals(Date.class)) {
        return convertToDate(database, value);
      } else if (targetClass.equals(Calendar.class)) {
        final Calendar cal = Calendar.getInstance();
        cal.setTime(convertToDate(database, value));
        return cal;
      } else if (targetClass.equals(LocalDate.class)) {
        if (value instanceof LocalDateTime time)
          return time.toLocalDate();
        else if (value instanceof Instant instant)
          return instant.atOffset(ZoneOffset.UTC).toLocalDate();
        else if (value instanceof Number number)
          return DateUtils.date(database, number.longValue(), LocalDate.class);
        else if (value instanceof Date date)
          // floorDiv, not '/': see DateUtils.dateToEpochDays. This is the DEFAULT coercion for a DATE column
          // (getJavaImplementation answers LocalDate), so a pre-epoch java.util.Date assigned to an ordinary DATE
          // property used to round to the following day (#7638, found in review).
          return DateUtils.date(database, Math.floorDiv(date.getTime(), DateUtils.MS_IN_A_DAY), LocalDate.class);
        else if (value instanceof Calendar calendar)
          return DateUtils.date(database, Math.floorDiv(calendar.getTimeInMillis(), DateUtils.MS_IN_A_DAY),
              LocalDate.class);
        else if (value instanceof String valueAsString) {
          if (FileUtils.isLong(valueAsString))
            return DateUtils.date(database, Long.parseLong(value.toString()), LocalDate.class);
          else if (database != null)
            try {
              return LocalDate.parse(valueAsString, DateUtils.getFormatter(database.getSchema().getDateTimeFormat()));
            } catch (final DateTimeParseException ignore) {
              try {
                return LocalDate.parse(valueAsString, DateUtils.getFormatter(database.getSchema().getDateFormat()));
              } catch (final DateTimeParseException ignore2) {
                // A DATE column fed a full timestamp keeps the date part rather than being emptied (issue #8090).
                return DateUtils.parseDateTimeKeepingWallClock(database, valueAsString).toLocalDate();
              }
            }
          else
            return DateUtils.parseDateTimeKeepingWallClock(null, valueAsString).toLocalDate();
        }
      } else if (targetClass.equals(LocalDateTime.class)) {
        if (value instanceof LocalDateTime time) {
          if (property != null)
            return time.truncatedTo(DateUtils.getPrecisionFromType(property.getType()));
        } else if (value instanceof Number number) {
          return DateUtils.date(database, number.longValue(), LocalDateTime.class);
        } else if (value instanceof Date date)
          return DateUtils.dateTime(database, date.getTime(), ChronoUnit.MILLIS, LocalDateTime.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        else if (value instanceof Calendar calendar)
          return DateUtils.dateTime(database, calendar.getTimeInMillis(), ChronoUnit.MILLIS, LocalDateTime.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        else if (value instanceof String valueAsString) {
          if (!FileUtils.isLong(valueAsString))
            // DateUtils.parseDateTime(), not a private copy of its fallback chain: this branch used to carry its own
            // and the two drifted apart, so a literal the bulk GraphBatch path accepted was rejected here (and vice
            // versa). The shared chain also brings the SQL-timestamp spelling with a fractional second, which no
            // format on either side accepted - and, because the exception it threw was not an IllegalArgumentException,
            // the blanket handler below turned that rejection into a silently stored NULL (issue #8090).
            //
            // Truncated to the declared precision exactly as the LocalDateTime branch above truncates, so that a
            // literal carrying more digits than the column holds reads back the same before and after a reload
            // instead of keeping digits the serializer is about to drop.
            //
            // ...KeepingWallClock: this branch has always dropped the offset of an offset-bearing string rather than
            // rebasing it onto the database's zone, because Cypher's datetime() renders itself with a Z and
            // `SET n.t = datetime('2026-01-01T00:00:00')` must read back 00:00 (issue #4125). Only the ACCEPTED
            // FORMATS are unified here; the zone disagreement with the bulk path is its own change.
            return truncateToPropertyPrecision(DateUtils.parseDateTimeKeepingWallClock(database, valueAsString),
                property);
        }
      } else if (targetClass.equals(ZonedDateTime.class)) {
        if (value instanceof ZonedDateTime time) {
          if (property != null)
            return time.truncatedTo(DateUtils.getPrecisionFromType(property.getType()));
        } else if (value instanceof Number number)
          return DateUtils.dateTime(database, number.longValue(), ChronoUnit.MILLIS, ZonedDateTime.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        else if (value instanceof Date date)
          return DateUtils.dateTime(database, date.getTime(), ChronoUnit.MILLIS, ZonedDateTime.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        else if (value instanceof Calendar calendar)
          return DateUtils.dateTime(database, calendar.getTimeInMillis(), ChronoUnit.MILLIS, ZonedDateTime.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        if (value instanceof String valueAsString) {
          if (!FileUtils.isLong(valueAsString)) {
            try {
              return truncateToPropertyPrecision(ZonedDateTime.parse(valueAsString), property);
            } catch (final DateTimeParseException ignore) {
              // No zone in the string: parse it as a local datetime through the shared chain - which accepts the
              // SQL-timestamp spelling with a fraction (issue #8090) - and anchor it to the database's zone, rather
              // than failing and letting the blanket handler below answer NULL.
              return truncateToPropertyPrecision(
                  DateUtils.parseDateTimeKeepingWallClock(database, valueAsString).atZone(zoneIdOf(database)), property);
            }
          }
        }
      } else if (targetClass.equals(Instant.class)) {
        switch (value) {
        case Instant instant -> {
          if (property != null)
            return instant.truncatedTo(DateUtils.getPrecisionFromType(property.getType()));
        }
        case Number number -> {
          return DateUtils.dateTime(database, number.longValue(), ChronoUnit.MILLIS, Instant.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        }
        case Date date -> {
          return DateUtils.dateTime(database, date.getTime(), ChronoUnit.MILLIS, Instant.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        }
        case Calendar calendar -> {
          return DateUtils.dateTime(database, calendar.getTimeInMillis(), ChronoUnit.MILLIS, Instant.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        }
        case String valueAsString -> {
          // This branch had no String case at all, so `arcadedb.dateTimeImplementation=java.time.Instant` left a
          // datetime literal in the record as the raw String it arrived as. It now goes through the same shared
          // chain as every other datetime target (issue #8090).
          if (!FileUtils.isLong(valueAsString)) {
            final Instant parsed = DateUtils.parseDateTime(database, valueAsString).atZone(zoneIdOf(database))
                .toInstant();
            return property != null ? parsed.truncatedTo(DateUtils.getPrecisionFromType(property.getType())) : parsed;
          }
        }
        default -> {
        }
        }
      } else if (targetClass.equals(Identifiable.class) || targetClass.equals(RID.class)) {
        if (MultiValue.isMultiValue(value)) {
          final List<Identifiable> result = new ArrayList<>();
          for (final Object o : MultiValue.getMultiValueIterable(value)) {
            if (o instanceof Identifiable identifiable) {
              result.add(identifiable);
            } else if (o instanceof Result resultObj && resultObj.isElement()) {
              // Extract the document from Result object
              result.add((Identifiable) resultObj.getElement().get());
            } else if (o instanceof String) {
              try {
                result.add(RID.create(database, value.toString()));
              } catch (final Exception e) {
                LogManager.instance()
                    .log(Type.class, Level.FINE, "Error in conversion of value '%s' to type '%s'", e, value, targetClass);
              }
            }
          }
          // If the property type is LINK (not LIST) and we have a single-element list, unwrap it
          if (property != null && property.getType() == LINK && result.size() == 1) {
            return result.get(0);
          }
          return result;
        } else if (value instanceof String string) {
          try {
            return RID.create(database, string);
          } catch (final Exception e) {
            LogManager.instance()
                .log(Type.class, Level.FINE, "Error in conversion of value '%s' to type '%s'", e, value, targetClass);
          }
        }
      }
    } catch (final IllegalArgumentException e) {
      // PASS THROUGH
      throw e;
    } catch (final DateTimeException | ParseException e) {
      // A date/time value that cannot be parsed must fail the write, not empty the column. These two are the
      // date/time family's equivalent of the NumberFormatException the arm above already lets through, and they were
      // the only reason a well-formed INSERT could report success while storing NULL: DateTimeParseException extends
      // DateTimeException -> RuntimeException, ParseException is checked, so neither reached the pass-through arm and
      // both landed here, where the only trace left was a Level.FINE line that is off by default (issue #8090).
      throw new IllegalArgumentException(
          "Error in conversion of value '" + value + "' to type '" + targetClass.getSimpleName() + "': " + e.getMessage(), e);
    } catch (final Exception e) {
      LogManager.instance().log(Type.class, Level.FINE, "Error in conversion of value '%s' to type '%s'", e, value, targetClass);
      return null;
    }

    return value;
  }

  /**
   * True when {@code value} is a {@link Double} or {@link Float} holding {@code NaN}. Per the JLS narrowing-
   * conversion rules {@code Double.NaN.longValue()}/{@code Float.NaN.longValue()} return {@code 0}, which is
   * in-range for every integral target type, so every narrowing path (not just {@link #narrowToIntegral}) must
   * check this explicitly instead of trusting the range check to catch it (issue #5970).
   */
  private static boolean isNaN(final Number value) {
    return (value instanceof Double doubleValue && doubleValue.isNaN()) || (value instanceof Float floatValue && floatValue.isNaN());
  }

  /**
   * Casts {@code item} to {@link Number}, rejecting a {@code null} element with a clear message
   * instead of letting it NPE deep inside a {@code float}/{@code double} narrowing conversion
   * (issue #6061 code review follow-up, sibling of the {@code null} guard added to
   * {@link #narrowToIntegral}: a {@code List} containing a {@code null} element, e.g. sent for an
   * {@code ARRAY_OF_FLOATS}/{@code ARRAY_OF_DOUBLES} property, previously NPE'd on {@code
   * ((Number) item).floatValue()} instead of raising a clean validation error).
   */
  private static Number requireNonNullNumber(final Object item, final String targetType, final Property property) {
    if (item == null)
      throw new IllegalArgumentException(
          "A null element cannot be converted to " + targetType //
              + (property != null ? " for property '" + property.getName() + "'" : ""));
    return (Number) item;
  }

  /**
   * Range-checks {@code value} before narrowing it to a smaller integral type ({@code BYTE}, {@code SHORT} or
   * {@code INTEGER}). Narrowing with a plain {@code .intValue()}/{@code .shortValue()}/{@code .byteValue()} wraps
   * two's-complement on overflow instead of rejecting - {@code 3000000000L} silently became {@code -1294967296} -
   * which corrupts the stored value without any error (issue #5905).
   * <p>
   * The comparison is done in {@code long}, so a {@link Double}/{@link Float} outside the long range (including
   * {@code Infinity}) is caught via the saturating conversion {@link Number#longValue()} already performs. A
   * {@link BigDecimal}/{@link BigInteger} can exceed even the long range, where {@code longValue()} truncates bits
   * instead of saturating, so those two types are range-checked directly against {@code long} bounds first.
   * <p>
   * {@code NaN} needs its own guard: per the JLS narrowing-conversion rules {@code Double.NaN.longValue()}/
   * {@code Float.NaN.longValue()} return {@code 0}, which is in-range for every target type and would otherwise
   * slip through the range check as a silent {@code 0} (issue #5970).
   * <p>
   * {@code value} can be {@code null} here: every {@code Collection -> byte[]/short[]/int[]/long[]} branch in
   * {@link #convert} passes a raw {@code (Number) item} cast from the source collection, and a {@code null}
   * element casts cleanly, so without this guard a {@code List} containing {@code null} (e.g. sent for a
   * BINARY/ARRAY_OF_* property) would NPE on {@code value.longValue()} below instead of raising a clean
   * validation error (issue #6061 code review follow-up).
   */
  private static Number narrowToIntegral(final Number value, final long min, final long max, final String targetType,
      final Property property) {
    if (value == null)
      throw new IllegalArgumentException(
          "A null element cannot be converted to " + targetType //
              + (property != null ? " for property '" + property.getName() + "'" : ""));

    if (isNaN(value))
      throw new IllegalArgumentException(
          "Value '" + value + "' is NaN and cannot be converted to type " + targetType //
              + (property != null ? " for property '" + property.getName() + "'" : ""));

    final long longValue;
    if (value instanceof BigDecimal bigDecimal)
      longValue = bigDecimal.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0 ? Long.MAX_VALUE
          : bigDecimal.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0 ? Long.MIN_VALUE : bigDecimal.longValue();
    else if (value instanceof BigInteger bigInteger)
      longValue = bigInteger.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0 ? Long.MAX_VALUE
          : bigInteger.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0 ? Long.MIN_VALUE : bigInteger.longValue();
    else
      longValue = value.longValue();

    if (longValue < min || longValue > max)
      throw new IllegalArgumentException(
          "Value '" + value + "' is out of range for type " + targetType + " (" + min + " to " + max + ")" //
              + (property != null ? " for property '" + property.getName() + "'" : ""));

    return longValue;
  }

  /**
   * Primitive-typed sibling of {@link #narrowToIntegral(Number, long, long, String, Property)}, used by the
   * {@code double[]}/{@code float[]}/{@code long[]} array-narrowing branches of {@link #convert} so a large
   * source array is range/NaN-checked without boxing every element into a {@link Double}/{@link Float}/{@link Long}
   * just to unbox it back out again. {@code BigDecimal}/{@code BigInteger} cannot appear in a primitive array, so
   * unlike the {@code Number} overload this needs no special-casing for either.
   */
  private static long narrowToIntegral(final double value, final long min, final long max, final String targetType, final Property property) {
    if (Double.isNaN(value))
      throw new IllegalArgumentException(
          "Value '" + value + "' is NaN and cannot be converted to type " + targetType //
              + (property != null ? " for property '" + property.getName() + "'" : ""));

    final long longValue = (long) value;
    if (longValue < min || longValue > max)
      throw new IllegalArgumentException(
          "Value '" + value + "' is out of range for type " + targetType + " (" + min + " to " + max + ")" //
              + (property != null ? " for property '" + property.getName() + "'" : ""));

    return longValue;
  }

  /** See {@link #narrowToIntegral(double, long, long, String, Property)}. */
  private static long narrowToIntegral(final float value, final long min, final long max, final String targetType, final Property property) {
    if (Float.isNaN(value))
      throw new IllegalArgumentException(
          "Value '" + value + "' is NaN and cannot be converted to type " + targetType //
              + (property != null ? " for property '" + property.getName() + "'" : ""));

    final long longValue = (long) value;
    if (longValue < min || longValue > max)
      throw new IllegalArgumentException(
          "Value '" + value + "' is out of range for type " + targetType + " (" + min + " to " + max + ")" //
              + (property != null ? " for property '" + property.getName() + "'" : ""));

    return longValue;
  }

  /**
   * See {@link #narrowToIntegral(double, long, long, String, Property)}. No NaN check: a {@code long} cannot hold
   * one.
   */
  private static long narrowToIntegral(final long value, final long min, final long max, final String targetType, final Property property) {
    if (value < min || value > max)
      throw new IllegalArgumentException(
          "Value '" + value + "' is out of range for type " + targetType + " (" + min + " to " + max + ")" //
              + (property != null ? " for property '" + property.getName() + "'" : ""));

    return value;
  }

  public static Number increment(final Number a, final Number b) {
    if (a == null || b == null)
      throw new IllegalArgumentException("Cannot increment a null value");

    switch (a) {
    case Integer i -> {
      switch (b) {
      case Integer integer -> {
        try {
          return Math.addExact(a.intValue(), b.intValue());
        } catch (final ArithmeticException e) {
          // SPECIAL CASE: UPGRADE TO LONG
          return (long) a.intValue() + (long) b.intValue();
        }
      }
      case Long l -> {
        return a.intValue() + b.longValue();
      }
      case Short aShort -> {
        try {
          return Math.addExact(a.intValue(), b.shortValue());
        } catch (final ArithmeticException e) {
          // SPECIAL CASE: UPGRADE TO LONG
          return (long) a.intValue() + (long) b.shortValue();
        }
      }
      case Float v -> {
        return a.intValue() + b.floatValue();
      }
      case Double v -> {
        return a.intValue() + b.doubleValue();
      }
      case BigDecimal decimal -> {
        return new BigDecimal(a.intValue()).add(decimal);
      }
      default -> {
      }
      }
    }
    case Long l -> {
      switch (b) {
      case Integer i -> {
        return a.longValue() + b.intValue();
      }
      case Long aLong -> {
        return a.longValue() + b.longValue();
      }
      case Short i -> {
        return a.longValue() + b.shortValue();
      }
      case Float v -> {
        return a.longValue() + b.floatValue();
      }
      case Double v -> {
        return a.longValue() + b.doubleValue();
      }
      case BigDecimal decimal -> {
        return new BigDecimal(a.longValue()).add(decimal);
      }
      default -> {
      }
      }
    }
    case Short i -> {
      switch (b) {
      case Integer integer -> {
        try {
          return Math.addExact(a.shortValue(), b.intValue());
        } catch (final ArithmeticException e) {
          // SPECIAL CASE: UPGRADE TO LONG
          return (long) a.shortValue() + (long) b.intValue();
        }
      }
      case Long l -> {
        return Long.valueOf(a.shortValue() + b.longValue());
      }
      case Short aShort -> {
        // A SHORT + SHORT SUM CAN NEVER OVERFLOW int (MAGNITUDE <= 2 * 32768), SO int ARITHMETIC IS ALWAYS EXACT HERE
        return a.shortValue() + b.shortValue();
      }
      case Float v -> {
        return a.shortValue() + b.floatValue();
      }
      case Double v -> {
        return a.shortValue() + b.doubleValue();
      }
      case BigDecimal decimal -> {
        return new BigDecimal(a.shortValue()).add(decimal);
      }
      default -> {
      }
      }
    }
    case Float v -> {
      switch (b) {
      case Integer i -> {
        return a.floatValue() + b.intValue();
      }
      case Long l -> {
        return a.floatValue() + b.longValue();
      }
      case Short i -> {
        return a.floatValue() + b.shortValue();
      }
      case Float aFloat -> {
        return a.floatValue() + b.floatValue();
      }
      case Double aDouble -> {
        return widenFloat(a.floatValue()) + b.doubleValue();
      }
      case BigDecimal decimal -> {
        return floatToBigDecimal(a.floatValue()).add(decimal);
      }
      default -> {
      }
      }
    }
    case Double v -> {
      switch (b) {
      case Integer i -> {
        return a.doubleValue() + b.intValue();
      }
      case Long l -> {
        return a.doubleValue() + b.longValue();
      }
      case Short i -> {
        return a.doubleValue() + b.shortValue();
      }
      case Float aFloat -> {
        return a.doubleValue() + widenFloat(b.floatValue());
      }
      case Double aDouble -> {
        return a.doubleValue() + b.doubleValue();
      }
      case BigDecimal decimal -> {
        return BigDecimal.valueOf(a.doubleValue()).add(decimal);
      }
      default -> {
      }
      }
    }
    case BigDecimal bigDecimal -> {
      switch (b) {
      case Integer i -> {
        return ((BigDecimal) a).add(new BigDecimal(b.intValue()));
      }
      case Long l -> {
        return ((BigDecimal) a).add(new BigDecimal(b.longValue()));
      }
      case Short i -> {
        return ((BigDecimal) a).add(new BigDecimal(b.shortValue()));
      }
      case Float v -> {
        return ((BigDecimal) a).add(floatToBigDecimal(b.floatValue()));
      }
      case Double v -> {
        return ((BigDecimal) a).add(BigDecimal.valueOf(b.doubleValue()));
      }
      case BigDecimal decimal -> {
        return ((BigDecimal) a).add(decimal);
      }
      default -> {
      }
      }
    }
    default -> {
    }
    }

    throw new IllegalArgumentException(
        "Cannot increment value '" + a + "' (" + a.getClass() + ") with '" + b + "' (" + b.getClass() + ")");
  }

  public static Number decrement(final Number a, final Number b) {
    if (a == null || b == null)
      throw new IllegalArgumentException("Cannot decrement a null value");

    switch (a) {
    case Integer i -> {
      switch (b) {
      case Integer integer -> {
        try {
          return Math.subtractExact(a.intValue(), b.intValue());
        } catch (final ArithmeticException e) {
          // SPECIAL CASE: UPGRADE TO LONG
          return (long) a.intValue() - (long) b.intValue();
        }
      }
      case Long l -> {
        return a.intValue() - b.longValue();
      }
      case Short aShort -> {
        try {
          return Math.subtractExact(a.intValue(), b.shortValue());
        } catch (final ArithmeticException e) {
          // SPECIAL CASE: UPGRADE TO LONG
          return (long) a.intValue() - (long) b.shortValue();
        }
      }
      case Float v -> {
        return a.intValue() - b.floatValue();
      }
      case Double v -> {
        return a.intValue() - b.doubleValue();
      }
      case BigDecimal decimal -> {
        return new BigDecimal(a.intValue()).subtract(decimal);
      }
      default -> {
      }
      }
    }
    case Long l -> {
      switch (b) {
      case Integer i -> {
        return a.longValue() - b.intValue();
      }
      case Long aLong -> {
        return a.longValue() - b.longValue();
      }
      case Short i -> {
        return a.longValue() - b.shortValue();
      }
      case Float v -> {
        return a.longValue() - b.floatValue();
      }
      case Double v -> {
        return a.longValue() - b.doubleValue();
      }
      case BigDecimal decimal -> {
        return new BigDecimal(a.longValue()).subtract(decimal);
      }
      default -> {
      }
      }
    }
    case Short i -> {
      switch (b) {
      case Integer integer -> {
        try {
          return Math.subtractExact(a.shortValue(), b.intValue());
        } catch (final ArithmeticException e) {
          // SPECIAL CASE: UPGRADE TO LONG
          return (long) a.shortValue() - (long) b.intValue();
        }
      }
      case Long l -> {
        return a.shortValue() - b.longValue();
      }
      case Short aShort -> {
        // A SHORT - SHORT DIFFERENCE CAN NEVER OVERFLOW int (MAGNITUDE <= 2 * 32768), SO int ARITHMETIC IS ALWAYS EXACT HERE
        return a.shortValue() - b.shortValue();
      }
      case Float v -> {
        return a.shortValue() - b.floatValue();
      }
      case Double v -> {
        return a.shortValue() - b.doubleValue();
      }
      case BigDecimal decimal -> {
        return new BigDecimal(a.shortValue()).subtract(decimal);
      }
      default -> {
      }
      }
    }
    case Float v -> {
      if (b instanceof Integer)
        return a.floatValue() - b.intValue();
      else if (b instanceof Long)
        return a.floatValue() - b.longValue();
      else if (b instanceof Short)
        return a.floatValue() - b.shortValue();
      else if (b instanceof Float)
        return a.floatValue() - b.floatValue();
      else if (b instanceof Double)
        return widenFloat(a.floatValue()) - b.doubleValue();
      else if (b instanceof BigDecimal decimal)
        return floatToBigDecimal(a.floatValue()).subtract(decimal);
    }
    case Double v -> {
      switch (b) {
      case Integer i -> {
        return a.doubleValue() - b.intValue();
      }
      case Long l -> {
        return a.doubleValue() - b.longValue();
      }
      case Short i -> {
        return a.doubleValue() - b.shortValue();
      }
      case Float aFloat -> {
        return a.doubleValue() - widenFloat(b.floatValue());
      }
      case Double aDouble -> {
        return a.doubleValue() - b.doubleValue();
      }
      case BigDecimal decimal -> {
        return BigDecimal.valueOf(a.doubleValue()).subtract(decimal);
      }
      default -> {
      }
      }
    }
    case BigDecimal bigDecimal -> {
      switch (b) {
      case Integer i -> {
        return ((BigDecimal) a).subtract(new BigDecimal(b.intValue()));
      }
      case Long l -> {
        return ((BigDecimal) a).subtract(new BigDecimal(b.longValue()));
      }
      case Short i -> {
        return ((BigDecimal) a).subtract(new BigDecimal(b.shortValue()));
      }
      case Float v -> {
        return ((BigDecimal) a).subtract(floatToBigDecimal(b.floatValue()));
      }
      case Double v -> {
        return ((BigDecimal) a).subtract(BigDecimal.valueOf(b.doubleValue()));
      }
      case BigDecimal decimal -> {
        return ((BigDecimal) a).subtract(decimal);
      }
      default -> {
      }
      }
    }
    default -> {
    }
    }

    throw new IllegalArgumentException(
        "Cannot decrement value '" + a + "' (" + a.getClass() + ") with '" + b + "' (" + b.getClass() + ")");
  }

  /**
   * Widens a {@link Float} to a {@code double} through its decimal form rather than through
   * {@link Float#doubleValue()}. The primitive widening is exact on the BITS, which means it faithfully
   * reproduces the single precision rounding error as a double ({@code (double) 0.05f} is
   * 0.05000000074505806), so a float that reads as 0.05 would not compare equal to the double 0.05. Re-reading
   * the shortest decimal that round-trips the float removes the error instead of preserving it, which is what
   * {@link #convert} already does for the index path - the two have to agree or the same query answers
   * differently depending on whether an index happens to exist. The mapping is strictly monotonic, so the
   * resulting comparison is still a total order.
   * <p>
   * The decimal round-trip costs a string per call, so it is kept off the hot paths that do not need it: it is
   * reached only when a {@code Float} actually meets a {@code Double} or a {@link BigDecimal}, never when both
   * operands already share a type, and never for an integral float at or below 2^24, which widens exactly.
   *
   * @param f the float to widen
   *
   * @return the double that reads the same in decimal
   */
  public static double widenFloat(final float f) {
    // NaN and the infinities have no shorter decimal form: widen them directly and skip the parse.
    if (Float.isNaN(f) || Float.isInfinite(f))
      return f;
    // An integral float at or below 2^24 is the only integer inside its own rounding interval (the ulp is at most 1
    // there), so its shortest decimal is that integer and the primitive widening is already exact. The bound is not
    // conservative and must not be raised: above it the ulp exceeds 1 and a shorter decimal fits in the same interval,
    // so the two diverge - 33554448f widens to 33554448 but reads as 3.355445E7, which is 33554450.
    if (f == (long) f && Math.abs(f) <= EXACT_INTEGRAL_FLOAT)
      return f;
    return Double.parseDouble(Float.toString(f));
  }

  /**
   * Builds the {@link BigDecimal} that reads the same in decimal as the given {@link Float}. {@code
   * BigDecimal.valueOf(float)} has no float overload, so the argument widens through {@code double} first and
   * the single precision rounding error is carried into the decimal. See {@link #widenFloat}.
   *
   * @param value the float to convert
   *
   * @return the decimal that reads the same
   *
   * @throws NumberFormatException if the float is NaN or infinite - {@link BigDecimal} cannot represent those at
   *                               all, so there is nothing to return. This is what {@code BigDecimal.valueOf(float)}
   *                               did before it, so callers that already reached it are unaffected, but unlike
   *                               {@link #widenFloat} this one has no non-finite path to fall back on
   */
  public static BigDecimal floatToBigDecimal(final float value) {
    return new BigDecimal(Float.toString(value));
  }

  /**
   * Answers whether the given {@code long} survives a round trip through {@code double}. Above 2^53 it does not:
   * the mantissa runs out and consecutive doubles are two or more apart, so a whole band of distinct longs share
   * one double and a comparison performed in {@code double} reports them equal (issue #7628). Callers that need an
   * exact answer for such a long promote both operands to {@link BigDecimal} instead of to {@code double}.
   *
   * @param value the long to test
   *
   * @return {@code true} when {@code (long) (double) value == value} for every long of this magnitude
   */
  public static boolean isExactAsDouble(final long value) {
    return value >= -EXACT_INTEGRAL_DOUBLE && value <= EXACT_INTEGRAL_DOUBLE;
  }

  /**
   * Converts a {@link BigInteger} to the closest {@code double}, clamped to a finite value. {@code
   * BigInteger.doubleValue()} returns {@link Double#POSITIVE_INFINITY}/{@link Double#NEGATIVE_INFINITY} once the
   * magnitude exceeds a double's range, and this method exists for exactly the caller that would otherwise compare
   * that infinity against a genuinely infinite {@code Float}/{@code Double} operand as equal - {@code
   * BigInteger.TEN.pow(400)} is finite and enormous, not infinite, and must keep comparing less than
   * {@link Double#POSITIVE_INFINITY} (issue #7669 review, CodeRabbit).
   *
   * @param value the operand to convert
   *
   * @return the closest finite {@code double}, or {@code +-Double.MAX_VALUE} for a magnitude {@code double} cannot
   * represent at all
   */
  public static double finiteDoubleValue(final BigInteger value) {
    final double converted = value.doubleValue();
    return Double.isFinite(converted) ? converted : value.signum() < 0 ? -Double.MAX_VALUE : Double.MAX_VALUE;
  }

  /**
   * Builds the {@link BigDecimal} that reads the same in decimal as the given floating point operand, so an
   * integral operand too large for {@code double} can be compared against it exactly. A {@code Float} goes through
   * {@link #floatToBigDecimal} and everything else through {@code BigDecimal.valueOf(double)}, both of which read
   * the shortest decimal that round-trips the value rather than its binary expansion - the same decimal
   * {@link #widenFloat} reads, so the exact path and the {@code double} path order the same pairs the same way.
   *
   * @param value the operand, which must be finite
   *
   * @return the decimal that reads the same
   */
  public static BigDecimal floatingToBigDecimal(final Number value) {
    if (value instanceof Float float1)
      return floatToBigDecimal(float1);
    if (value instanceof BigDecimal bigDecimal)
      return bigDecimal;
    return BigDecimal.valueOf(value.doubleValue());
  }

  /**
   * Answers whether the given floating point operand has an exact {@link BigDecimal} form. NaN and the infinities
   * do not, so a comparison involving one of them stays in {@code double}, where {@code Double.compare} already
   * orders them totally.
   * <p>
   * FOR AN OPERAND WHOSE STATIC TYPE IS {@link Number} - which is what {@code BinaryComparator} holds, having read
   * the value out of a record. A caller holding a primitive {@code float}/{@code double}, as two of the
   * {@link #castComparableNumber} branches below do, calls {@code Float.isFinite}/{@code Double.isFinite} directly
   * instead: routing those through here would box the operand on a comparison path that is otherwise
   * allocation-free. The apparent inconsistency is that trade, not an oversight (raised in review).
   *
   * @param value the operand
   *
   * @return {@code true} when the value is finite
   */
  public static boolean isFinite(final Number value) {
    if (value instanceof Float float1)
      return !float1.isNaN() && !float1.isInfinite();
    if (value instanceof Double double1)
      return !double1.isNaN() && !double1.isInfinite();
    return true;
  }

  public static Number[] castComparableNumber(Number left, Number right) {
    // CHECK FOR CONVERSION
    if (left instanceof Short) {
      // SHORT
      if (right instanceof Integer)
        left = left.intValue();
      else if (right instanceof Long)
        left = left.longValue();
      else if (right instanceof Float)
        left = left.floatValue();
      else if (right instanceof Double)
        left = left.doubleValue();
      else if (right instanceof BigDecimal)
        left = new BigDecimal(left.intValue());
      else if (right instanceof Byte)
        left = left.byteValue();
      else if (right instanceof BigInteger bigInteger1) {
        // Mirrors the BigDecimal arm above: a BigInteger on the right has no narrower common type, so both
        // operands promote to BigDecimal (issue #7669).
        left = new BigDecimal(left.intValue());
        right = new BigDecimal(bigInteger1);
      }

    } else if (left instanceof Integer) {
      // INTEGER
      if (right instanceof Long)
        left = left.longValue();
      else if (right instanceof Float float1) {
        // Narrowing an int to float loses precision above 2^24 (issue #7614), e.g. Integer.MAX_VALUE would
        // become 2.1474836E9. Both operands meet at double instead, which holds every int exactly.
        left = left.doubleValue();
        right = widenFloat(float1);
      } else if (right instanceof Double)
        left = left.doubleValue();
      else if (right instanceof BigDecimal)
        left = new BigDecimal(left.intValue());
      else if (right instanceof Short)
        right = right.intValue();
      else if (right instanceof Byte)
        right = right.intValue();
      else if (right instanceof BigInteger bigInteger1) {
        left = new BigDecimal(left.intValue());
        right = new BigDecimal(bigInteger1);
      }

    } else if (left instanceof Long) {
      // LONG
      if (right instanceof Float float1) {
        // Narrowing a long to float loses precision above 2^24 (issue #7614), e.g. 16777217L would collapse
        // onto the same float as 16777216L. Both operands meet at double instead, which holds every long
        // exactly up to 2^53 - the same promotion BinaryComparator.compareWideningLong already applies. Past
        // that bound double runs out of mantissa too and the same collapse returns one band higher, so a long
        // that big meets the float in BigDecimal instead (issue #7628).
        if (isExactAsDouble(left.longValue()) || !Float.isFinite(float1)) {
          left = left.doubleValue();
          right = widenFloat(float1);
        } else {
          left = BigDecimal.valueOf(left.longValue());
          right = floatToBigDecimal(float1);
        }
      } else if (right instanceof Double double1) {
        // Same 2^53 bound as the Float branch above (issue #7628)
        if (isExactAsDouble(left.longValue()) || !Double.isFinite(double1))
          left = left.doubleValue();
        else {
          left = BigDecimal.valueOf(left.longValue());
          right = BigDecimal.valueOf(double1);
        }
      } else if (right instanceof BigDecimal)
        left = new BigDecimal(left.longValue());
      else if (right instanceof Integer || right instanceof Byte || right instanceof Short)
        right = right.longValue();
      else if (right instanceof BigInteger bigInteger1) {
        left = new BigDecimal(left.longValue());
        right = new BigDecimal(bigInteger1);
      }

    } else if (left instanceof Float) {
      // FLOAT
      if (right instanceof Double)
        left = widenFloat(left.floatValue());
      else if (right instanceof BigDecimal)
        left = floatToBigDecimal(left.floatValue());
      else if (right instanceof Byte || right instanceof Short)
        right = right.floatValue();
      else if (right instanceof Integer) {
        // Symmetric case of the INTEGER branch above: narrowing the integral operand to float would lose
        // precision above 2^24 (issue #7614), so both meet at double instead, which holds every int exactly.
        left = widenFloat(left.floatValue());
        right = right.doubleValue();
      } else if (right instanceof Long) {
        // Symmetric case of the LONG branch above: double for a long inside 2^53, BigDecimal past it (#7614/#7628)
        final float float1 = left.floatValue();
        if (isExactAsDouble(right.longValue()) || !Float.isFinite(float1)) {
          left = widenFloat(float1);
          right = right.doubleValue();
        } else {
          left = floatToBigDecimal(float1);
          right = BigDecimal.valueOf(right.longValue());
        }
      } else if (right instanceof BigInteger bigInteger1) {
        // Same non-finite guard as the Long branch above: floatToBigDecimal() throws NumberFormatException on
        // NaN/Infinity, which BigDecimal cannot represent at all, so a non-finite float meets the BigInteger in
        // double instead - Double.compare() already orders NaN/Infinity totally (issue #7669 review, CodeRabbit).
        final float float1 = left.floatValue();
        if (Float.isFinite(float1)) {
          left = floatToBigDecimal(float1);
          right = new BigDecimal(bigInteger1);
        } else {
          left = widenFloat(float1);
          right = finiteDoubleValue(bigInteger1);
        }
      }

    } else if (left instanceof Double) {
      // DOUBLE
      if (right instanceof BigDecimal)
        left = BigDecimal.valueOf(left.doubleValue());
      else if (right instanceof Float float1)
        right = widenFloat(float1);
      else if (right instanceof Long long1) {
        // Symmetric case of the LONG branch above (issue #7628)
        final double double1 = left.doubleValue();
        if (isExactAsDouble(long1) || !Double.isFinite(double1))
          right = right.doubleValue();
        else {
          left = BigDecimal.valueOf(double1);
          right = BigDecimal.valueOf(long1);
        }
      } else if (right instanceof Byte || right instanceof Short || right instanceof Integer)
        right = right.doubleValue();
      else if (right instanceof BigInteger bigInteger1) {
        // Same guard as the Long branch above: BigDecimal.valueOf(double) throws NumberFormatException on
        // NaN/Infinity (issue #7669 review, CodeRabbit).
        final double double1 = left.doubleValue();
        if (Double.isFinite(double1)) {
          left = BigDecimal.valueOf(double1);
          right = new BigDecimal(bigInteger1);
        } else
          right = finiteDoubleValue(bigInteger1);
      }

    } else if (left instanceof BigInteger bigInteger) {
      // Mirrors the BigDecimal branch below: a BigInteger operand has no narrower common type with any other
      // Number, so both sides promote to BigDecimal (issue #7669 - the missing left-hand counterpart of #7623).
      if (right instanceof Integer integer) {
        left = new BigDecimal(bigInteger);
        right = new BigDecimal(integer);
      } else if (right instanceof Long long1) {
        left = new BigDecimal(bigInteger);
        right = new BigDecimal(long1);
      } else if (right instanceof Float float1) {
        // Non-finite guard, symmetric with the Float branch's own BigInteger arm above (issue #7669 review,
        // CodeRabbit): floatToBigDecimal() throws NumberFormatException on NaN/Infinity.
        if (Float.isFinite(float1)) {
          left = new BigDecimal(bigInteger);
          right = floatToBigDecimal(float1);
        } else {
          left = finiteDoubleValue(bigInteger);
          right = widenFloat(float1);
        }
      } else if (right instanceof Double double1) {
        // Non-finite guard, symmetric with the Double branch's own BigInteger arm above (issue #7669 review,
        // CodeRabbit): BigDecimal.valueOf(double) throws NumberFormatException on NaN/Infinity.
        if (Double.isFinite(double1)) {
          left = new BigDecimal(bigInteger);
          right = BigDecimal.valueOf(double1);
        } else
          left = finiteDoubleValue(bigInteger);
      } else if (right instanceof Short short1) {
        left = new BigDecimal(bigInteger);
        right = new BigDecimal(short1);
      } else if (right instanceof Byte byte1) {
        left = new BigDecimal(bigInteger);
        right = new BigDecimal(byte1);
      } else if (right instanceof BigDecimal)
        left = new BigDecimal(bigInteger);

    } else if (left instanceof BigDecimal) {
      // DOUBLE
      if (right instanceof Integer integer)
        right = new BigDecimal(integer);
      else if (right instanceof Long long1)
        // The Long case was missing, so the couple came back as (BigDecimal, Long) and the caller's compareTo()
        // threw ClassCastException - `WHERE decimalProperty > 3000000000` crashed rather than answered (issue #7609).
        right = new BigDecimal(long1);
      else if (right instanceof Float float1)
        right = floatToBigDecimal(float1);
      else if (right instanceof Double double1)
        right = BigDecimal.valueOf(double1);
      else if (right instanceof Short short1)
        right = new BigDecimal(short1);
      else if (right instanceof Byte byte1)
        right = new BigDecimal(byte1);
      else if (right instanceof BigInteger bigInteger1)
        // Same hole the missing Long arm left before #7609: without this, the couple comes back as
        // (BigDecimal, BigInteger) and the caller's compareTo()/equals() throws ClassCastException.
        right = new BigDecimal(bigInteger1);
    } else if (left instanceof Byte) {
      if (right instanceof Short)
        left = left.shortValue();
      else if (right instanceof Integer)
        left = left.intValue();
      else if (right instanceof Long)
        left = left.longValue();
      else if (right instanceof Float)
        left = left.floatValue();
      else if (right instanceof Double)
        left = left.doubleValue();
      else if (right instanceof BigDecimal)
        left = new BigDecimal(left.intValue());
      else if (right instanceof BigInteger bigInteger1) {
        left = new BigDecimal(left.intValue());
        right = new BigDecimal(bigInteger1);
      }
    }

    if (left instanceof BigDecimal bigDecimal && right instanceof BigDecimal bigDecimal1) {
      // BigDecimal.equals() is scale-sensitive (BigDecimal("5").equals(BigDecimal("5.0")) is false, even though
      // compareTo() answers 0), so a couple that lands here with different scales makes every equals()-based
      // caller (QueryOperatorEquals, BinaryComparator.equals()) disagree with the compareTo()-based operators
      // on the identical pair of values (issue #7613). Stripping both to their canonical scale, the same
      // treatment normalizeNumberForKey already applies for GROUP BY/DISTINCT keys, makes equals() agree with
      // compareTo() by construction for every caller at once.
      left = bigDecimal.stripTrailingZeros();
      right = bigDecimal1.stripTrailingZeros();
    }

    return new Number[] { left, right };
  }

  /**
   * Returns a canonical representation of a value to be used as a hash/equality key (e.g. GROUP BY or DISTINCT keys).
   * <p>
   * Java boxed numbers do not equate across types or scales: {@code Integer(1).equals(Long(1))} is {@code false} and
   * {@code BigDecimal("1").equals(BigDecimal("1.0"))} is {@code false} (different scale). When the same logical value
   * reaches a grouping step represented with different numeric types (e.g. an Integer from an index scan and a
   * BigDecimal from arithmetic), this would split a single logical group into several. To avoid that, every finite
   * {@link Number} is normalised to a {@link BigDecimal} with trailing zeros stripped, so numerically-equal values
   * share the same {@code equals}/{@code hashCode}. Non-finite floating point values (NaN, +/-Infinity) cannot be
   * represented as BigDecimal and are returned unchanged; all non-numeric values are returned unchanged.
   *
   * @param value the value to normalise (may be {@code null})
   *
   * @return the canonical key for the value
   */
  public static Object normalizeNumberForKey(final Object value) {
    if (value instanceof Number) {
      if (value instanceof BigDecimal bigDecimal)
        return bigDecimal.stripTrailingZeros();
      if (value instanceof BigInteger bigInteger)
        // Stripped like every other arm, so a BigInteger 100 keys the same as an Integer 100 or a Double 100.0
        // (issue #7623) rather than landing at scale 0 while the decimal paths land at scale -2.
        return new BigDecimal(bigInteger).stripTrailingZeros();
      if (value instanceof Double || value instanceof Float) {
        // A Float reaches its key through the decimal form, as it does everywhere else a Float meets a wider type:
        // .doubleValue() would key 0.05f as 0.05000000074505806 while the Double 0.05 keys as 0.05, splitting one
        // logical group in two - which is the very thing this method exists to prevent (issue #7609).
        final double d = value instanceof Float float1 ? widenFloat(float1) : ((Number) value).doubleValue();
        if (Double.isNaN(d) || Double.isInfinite(d))
          return value;
        return BigDecimal.valueOf(d).stripTrailingZeros();
      }
      // Integer/Long/Short/Byte/AtomicInteger/AtomicLong and other integral numbers. Stripped like the decimal
      // paths above, so an Integer 100 and a Double 100.0 land on the same scale -2 key instead of disagreeing
      // (unscaled 100 at scale 0 vs unscaled 1 at scale -2) and splitting one GROUP BY/DISTINCT group in two
      // (issue #7623).
      return BigDecimal.valueOf(((Number) value).longValue()).stripTrailingZeros();
    }
    return value;
  }

  /**
   * Convert the input object to an integer.
   *
   * @param value Any type supported
   *
   * @return The integer value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public int asInt(final Object value) {
    if (value instanceof Number number)
      return number.intValue();
    else if (value instanceof String string)
      return Integer.parseInt(string);
    else if (value instanceof Boolean boolean1)
      return boolean1 ? 1 : 0;

    throw new IllegalArgumentException("Cannot convert value " + value + " to int for type: " + name);
  }

  /**
   * Convert the input object to a long.
   *
   * @param value Any type supported
   *
   * @return The long value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public long asLong(final Object value) {
    if (value instanceof Number number)
      return number.longValue();
    else if (value instanceof String string)
      return Long.parseLong(string);
    else if (value instanceof Boolean boolean1)
      return boolean1 ? 1 : 0;

    throw new IllegalArgumentException("Cannot convert value " + value + " to long for type: " + name);
  }

  /**
   * Convert the input object to a Float.
   *
   * @param value Any type supported
   *
   * @return The float value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public float asFloat(final Object value) {
    if (value instanceof Number number)
      return number.floatValue();
    else if (value instanceof String string)
      return Float.parseFloat(string);

    throw new IllegalArgumentException("Cannot convert value " + value + " to float for type: " + name);
  }

  /**
   * Convert the input object to a Double.
   *
   * @param value Any type supported
   *
   * @return The double value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public double asDouble(final Object value) {
    if (value instanceof Number number)
      return number.doubleValue();
    else if (value instanceof String string)
      return Double.parseDouble(string);

    throw new IllegalArgumentException("Cannot convert value " + value + " to double for type: " + name);
  }

  public byte getBinaryType() {
    return binaryType;
  }

  /**
   * Convert the input object to a string.
   *
   * @param value Any type supported
   *
   * @return The string if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  @Deprecated
  public String asString(final Object value) {
    return value.toString();
  }

  public boolean isMultiValue() {
    return this == LIST || this == MAP;
  }

  public boolean isLink() {
    return this == LINK;
  }

  public boolean isEmbedded() {
    return this == LIST || this == MAP;
  }

  public Class<?> getDefaultJavaType() {
    return javaDefaultType;
  }

  /**
   * Returns the Java class a value of this type is materialised as once the schema has coerced it, i.e. the target
   * {@link #convert(Database, Object, Class, Property)} uses when a property declares this type.
   * <p>
   * This is {@link #getDefaultJavaType()} for every type except {@code DATE} and {@code DATETIME}, whose runtime
   * representation is configurable per database. Callers that need to reproduce the stored form of a value - the
   * write path in {@code MutableDocument}, and the partitioned bucket strategy that has to hash a lookup key the way
   * placement hashed the stored one (issue #5595) - must agree on this mapping, so it lives in one place.
   *
   * @param database database whose {@code DATE}/{@code DATETIME} settings apply, or {@code null} to fall back to the
   *                 default Java type
   */
  public Class<?> getJavaImplementation(final Database database) {
    if (database instanceof DatabaseInternal internal) {
      if (this == DATE)
        return internal.getSerializer().getDateImplementation();
      if (this == DATETIME)
        return internal.getSerializer().getDateTimeImplementation();
    }
    return javaDefaultType;
  }

  public Set<Type> getCastable() {
    return castable;
  }

  @Deprecated
  public Class<?>[] getJavaTypes() {
    return null;
  }

  public Object newInstance(final Object value) {
    return convert(null, value, javaDefaultType);
  }

  private static Date convertToDate(final Database database, final Object value) throws ParseException {
    if (value instanceof Date date)
      return date;
    if (value instanceof Number number)
      return new Date(number.longValue());
    else if (value instanceof Calendar calendar)
      return calendar.getTime();
    else if (value instanceof LocalDateTime time)
      return new Date(TimeUnit.MILLISECONDS.convert(time.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) +//
          time.getLong(ChronoField.MILLI_OF_SECOND));
    else if (value instanceof Instant instant)
      return new Date(instant.toEpochMilli());
    else if (value instanceof ZonedDateTime time)
      return new Date(TimeUnit.MILLISECONDS.convert(time.toEpochSecond(), TimeUnit.SECONDS) +//
          time.getLong(ChronoField.MILLI_OF_SECOND));
    else if (value instanceof LocalDate date)
      return new Date(date.toEpochDay() * DateUtils.MS_IN_A_DAY);
    else if (value instanceof String valueAsString) {
      if (FileUtils.isLong(valueAsString))
        return new Date(Long.parseLong(value.toString()));
      else if (database != null) {
        final Date fromDateTimeFormat = parseFully(database.getSchema().getDateTimeFormat(), valueAsString);
        if (fromDateTimeFormat != null)
          return fromDateTimeFormat;
        final Date fromDateFormat = parseFully(database.getSchema().getDateFormat(), valueAsString);
        if (fromDateFormat != null)
          return fromDateFormat;
        return dateFromSharedChain(database, valueAsString);
      } else
        return dateFromSharedChain(null, valueAsString);
    }
    throw new IllegalArgumentException("Object of class " + value.getClass() + " cannot be converted to Date");
  }

  /**
   * Parses {@code valueAsString} with a schema pattern, answering {@code null} unless the pattern consumed the WHOLE
   * string. {@code SimpleDateFormat.parse(String)} stops at the first character it cannot use and reports success on
   * what it read, so the schema's default {@code yyyy-MM-dd HH:mm:ss} "successfully" parsed
   * {@code '2024-02-29 13:45:10.123456'} by throwing the fraction away - a silent precision loss that hid the same
   * defect issue #8090 reported as a silent NULL on the {@link LocalDateTime} path. A partial match now yields to the
   * shared {@link DateUtils#parseDateTime} chain, which does understand the fraction.
   * <p>
   * Locale.ENGLISH, not the JVM default: a schema pattern with a textual field (MMM, EEE) has to parse the same on
   * every server, exactly as DateUtils.getFormatter() pins it for the java.time paths (issues #7112, #7144).
   * SimpleDateFormat rather than a DateTimeFormatter here because this branch keeps SimpleDateFormat's lenient
   * resolution and its default-time-zone anchoring, which java.time does not reproduce.
   */
  private static Date parseFully(final String pattern, final String valueAsString) {
    final ParsePosition position = new ParsePosition(0);
    final Date parsed = new SimpleDateFormat(pattern, Locale.ENGLISH).parse(valueAsString, position);
    return parsed != null && position.getIndex() == valueAsString.length() ? parsed : null;
  }

  /**
   * Last resort of {@link #convertToDate}: the shared {@link DateUtils#parseDateTime} chain, which accepts the ISO
   * forms and the SQL-timestamp spelling with a fractional second that {@code SimpleDateFormat} leaves unparsed.
   * This branch used to guess the format from the string's length and then, on a miss, fall off the end of the
   * method and let the caller's blanket handler answer {@code null} (issue #8090).
   * <p>
   * The result is anchored to the database's zone so it denotes the same instant {@code SimpleDateFormat} would
   * have produced for the same wall-clock, and its sub-millisecond digits are dropped because {@link Date} cannot
   * hold them.
   */
  private static Date dateFromSharedChain(final Database database, final String valueAsString) {
    return Date.from(DateUtils.parseDateTime(database, valueAsString).atZone(zoneIdOf(database)).toInstant());
  }

  /**
   * The database's configured zone, falling back to the JVM's when there is no database in scope - the same anchor
   * {@code SimpleDateFormat} and {@link java.util.Calendar} use by default.
   */
  private static ZoneId zoneIdOf(final Database database) {
    if (database != null) {
      final ZoneId zoneId = database.getSchema().getZoneId();
      if (zoneId != null)
        return zoneId;
    }
    return ZoneId.systemDefault();
  }

  /**
   * Truncates a parsed datetime to the precision the target property declares, so a literal carrying more digits
   * than the column can hold reads back identically before and after a reload instead of briefly keeping digits the
   * serializer is about to drop. This mirrors what the {@link LocalDateTime}/{@link ZonedDateTime} value branches
   * already do; only the string branches were missing it. A no-op when the value is not bound to a property.
   */
  private static LocalDateTime truncateToPropertyPrecision(final LocalDateTime value, final Property property) {
    return property == null ? value : value.truncatedTo(DateUtils.getPrecisionFromType(property.getType()));
  }

  private static ZonedDateTime truncateToPropertyPrecision(final ZonedDateTime value, final Property property) {
    return property == null ? value : value.truncatedTo(DateUtils.getPrecisionFromType(property.getType()));
  }
}
