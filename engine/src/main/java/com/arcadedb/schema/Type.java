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
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.ImmutableEmbeddedDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.MultiIterator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Generic representation of a type.<br>
 * allowAssignmentFrom accepts any class, but Array.class means that the type accepts generic Arrays.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public enum Type {
  BOOLEAN("Boolean", 0, BinaryTypes.TYPE_BOOLEAN, Boolean.class, new Class<?>[] { Number.class }),

  INTEGER("Integer", 1, BinaryTypes.TYPE_INT, Integer.class, new Class<?>[] { Number.class }),

  SHORT("Short", 2, BinaryTypes.TYPE_SHORT, Short.class, new Class<?>[] { Number.class }),

  LONG("Long", 3, BinaryTypes.TYPE_LONG, Long.class, new Class<?>[] { Number.class, }),

  FLOAT("Float", 4, BinaryTypes.TYPE_FLOAT, Float.class, new Class<?>[] { Number.class }),

  DOUBLE("Double", 5, BinaryTypes.TYPE_DOUBLE, Double.class, new Class<?>[] { Number.class }),

  DATETIME("Datetime", 6, BinaryTypes.TYPE_DATETIME, Date.class,
      new Class<?>[] { Date.class, LocalDateTime.class, ZonedDateTime.class, Instant.class, Number.class }),

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
      new Class<?>[] { Date.class, LocalDateTime.class, ZonedDateTime.class, Instant.class, Number.class }),

  DATETIME_NANOS("Datetime_nanos", 17, BinaryTypes.TYPE_DATETIME_NANOS, LocalDateTime.class,
      new Class<?>[] { Date.class, LocalDateTime.class, ZonedDateTime.class, Instant.class, Number.class }),

  DATETIME_SECOND("Datetime_second", 18, BinaryTypes.TYPE_DATETIME_SECOND, LocalDateTime.class,
      new Class<?>[] { Date.class, LocalDateTime.class, ZonedDateTime.class, Instant.class, Number.class }),

  ARRAY_OF_SHORTS("Short[]", 19, BinaryTypes.TYPE_ARRAY_OF_SHORTS, short[].class, new Class<?>[] { short[].class, Short[].class }),

  ARRAY_OF_INTEGERS("Integer[]", 20, BinaryTypes.TYPE_ARRAY_OF_INTEGERS, int[].class,
      new Class<?>[] { int[].class, Integer[].class }),

  ARRAY_OF_LONGS("Long[]", 21, BinaryTypes.TYPE_ARRAY_OF_LONGS, long[].class, new Class<?>[] { long[].class, Long[].class }),

  ARRAY_OF_FLOATS("Float[]", 22, BinaryTypes.TYPE_ARRAY_OF_FLOATS, float[].class, new Class<?>[] { float[].class, Float[].class }),

  ARRAY_OF_DOUBLES("Double[]", 23, BinaryTypes.TYPE_ARRAY_OF_DOUBLES, double[].class,
      new Class<?>[] { double[].class, Double[].class }),
  ;

  // Don't change the order, the type discover get broken if you change the order.
  private static final Type[] TYPES = new Type[] { LIST, MAP, LINK, STRING, DATETIME };

  private static final Type[]              TYPES_BY_ID       = new Type[24];
  // Values previously stored in javaTypes
  private static final Map<Class<?>, Type> TYPES_BY_USERTYPE = new HashMap<Class<?>, Type>();
  private static final Map<String, Type>   TYPES_BY_NAME     = new HashMap<String, Type>();

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

  public static Object convert(final Database database, final Object value, Class<?> targetClass, final Property property) {
    if (value == null)
      return null;

    if (targetClass == null)
      return value;

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
          return ((Number) value).byteValue();

      } else if (targetClass.equals(Short.TYPE) || targetClass.equals(Short.class)) {
        if (value instanceof Short)
          return value;
        else if (value instanceof String string)
          return string.isEmpty() ? 0 : Short.parseShort(string);
        else
          return ((Number) value).shortValue();

      } else if (targetClass.equals(Integer.TYPE) || targetClass.equals(Integer.class)) {
        if (value instanceof Integer)
          return value;
        else if (value instanceof String string)
          return string.isEmpty() ? 0 : Integer.parseInt(string);
        else
          return ((Number) value).intValue();

      } else if (targetClass.equals(Long.TYPE) || targetClass.equals(Long.class)) {
        if (value instanceof Long)
          return value;
        else if (value instanceof String string)
          return string.isEmpty() ? 0L : Long.parseLong(string);
        else if (DateUtils.isDate(value))
          return DateUtils.dateTimeToTimestamp(value, ChronoUnit.MILLIS);
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
        else if (value instanceof Float)
          // THIS IS NECESSARY DUE TO A BUG/STRANGE BEHAVIOR OF JAVA BY LOSING PRECISION
          return Double.parseDouble(value.toString());
        else
          return ((Number) value).doubleValue();

      } else if (targetClass.equals(Boolean.TYPE) || targetClass.equals(Boolean.class)) {
        if (value instanceof Boolean)
          return value;
        else if (value instanceof String string) {
          if (string.equalsIgnoreCase("true"))
            return Boolean.TRUE;
          else if (string.equalsIgnoreCase("false"))
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

      } else if (Collection.class.equals(targetClass)) {
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
        else if (value instanceof Number number)
          return DateUtils.date(database, number.longValue(), LocalDate.class);
        else if (value instanceof Date date)
          return DateUtils.date(database, date.getTime() / DateUtils.MS_IN_A_DAY, LocalDate.class);
        else if (value instanceof Calendar calendar)
          return DateUtils.date(database, calendar.getTimeInMillis() / DateUtils.MS_IN_A_DAY, LocalDate.class);
        else if (value instanceof String valueAsString) {
          if (FileUtils.isLong(valueAsString))
            return DateUtils.date(database, Long.parseLong(value.toString()), LocalDate.class);
          else if (database != null)
            try {
              return LocalDate.parse(valueAsString, DateTimeFormatter.ofPattern((database.getSchema().getDateTimeFormat())));
            } catch (final DateTimeParseException ignore) {
              return LocalDate.parse(valueAsString, DateTimeFormatter.ofPattern((database.getSchema().getDateFormat())));
            }
          else {
            // GUESS FORMAT BY STRING LENGTH
            if (valueAsString.length() == "yyyy-MM-dd".length())
              return LocalDateTime.parse(valueAsString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
          }
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
          if (!FileUtils.isLong(valueAsString)) {
            if (database != null)
              try {
                return LocalDateTime.parse(valueAsString);
              } catch (Exception e) {
                try {
                  return LocalDateTime.parse(valueAsString,
                      DateTimeFormatter.ofPattern((database.getSchema().getDateTimeFormat())));
                } catch (final DateTimeParseException ignore) {
                  return LocalDateTime.parse(valueAsString, DateTimeFormatter.ofPattern((database.getSchema().getDateFormat())));
                }
              }
            else {
              // GUESS FORMAT BY STRING LENGTH
              if (valueAsString.length() == "yyyy-MM-dd".length())
                return LocalDateTime.parse(valueAsString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
              else if (valueAsString.length() == "yyyy-MM-dd HH:mm:ss".length())
                return LocalDateTime.parse(valueAsString, DateTimeFormatter.ofPattern("yyyyyyyy-MM-dd HH:mm:ss"));
              else if (valueAsString.length() == "yyyy-MM-dd HH:mm:ss.SSS".length())
                return LocalDateTime.parse(valueAsString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
            }
          }
        }
      } else if (targetClass.equals(ZonedDateTime.class)) {
        if (value instanceof ZonedDateTime time) {
          if (property != null)
            return time.truncatedTo(DateUtils.getPrecisionFromType(property.getType()));
        } else if (value instanceof Date date)
          return DateUtils.dateTime(database, date.getTime(), ChronoUnit.MILLIS, LocalDateTime.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        else if (value instanceof Calendar calendar)
          return DateUtils.dateTime(database, calendar.getTimeInMillis(), ChronoUnit.MILLIS, ZonedDateTime.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        if (value instanceof String valueAsString) {
          if (!FileUtils.isLong(valueAsString)) {
            if (database != null)
              try {
                return ZonedDateTime.parse(valueAsString, DateTimeFormatter.ofPattern((database.getSchema().getDateTimeFormat())));
              } catch (final DateTimeParseException ignore) {
                return ZonedDateTime.parse(valueAsString, DateTimeFormatter.ofPattern((database.getSchema().getDateFormat())));
              }
            else {
              // GUESS FORMAT BY STRING LENGTH
              if (valueAsString.length() == "yyyy-MM-dd".length())
                return ZonedDateTime.parse(valueAsString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
              else if (valueAsString.length() == "yyyy-MM-dd HH:mm:ss".length())
                return ZonedDateTime.parse(valueAsString, DateTimeFormatter.ofPattern("yyyyyyyy-MM-dd HH:mm:ss"));
              else if (valueAsString.length() == "yyyy-MM-dd HH:mm:ss.SSS".length())
                return ZonedDateTime.parse(valueAsString, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
            }
          }
        }
      } else if (targetClass.equals(Instant.class)) {
        if (value instanceof Instant instant) {
          if (property != null)
            return instant.truncatedTo(DateUtils.getPrecisionFromType(property.getType()));
        } else if (value instanceof Date date)
          return DateUtils.dateTime(database, date.getTime(), ChronoUnit.MILLIS, LocalDateTime.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
        else if (value instanceof Calendar calendar)
          return DateUtils.dateTime(database, calendar.getTimeInMillis(), ChronoUnit.MILLIS, Instant.class,
              property != null ? DateUtils.getPrecisionFromType(property.getType()) : ChronoUnit.MILLIS);
      } else if (targetClass.equals(Identifiable.class) || targetClass.equals(RID.class)) {
        if (MultiValue.isMultiValue(value)) {
          final List<Identifiable> result = new ArrayList<>();
          for (final Object o : MultiValue.getMultiValueIterable(value)) {
            if (o instanceof Identifiable identifiable) {
              result.add(identifiable);
            } else if (o instanceof String) {
              try {
                result.add(new RID(database, value.toString()));
              } catch (final Exception e) {
                LogManager.instance()
                    .log(Type.class, Level.FINE, "Error in conversion of value '%s' to type '%s'", e, value, targetClass);
              }
            }
          }
          return result;
        } else if (value instanceof String string) {
          try {
            return new RID(database, string);
          } catch (final Exception e) {
            LogManager.instance()
                .log(Type.class, Level.FINE, "Error in conversion of value '%s' to type '%s'", e, value, targetClass);
          }
        }
      }
    } catch (final IllegalArgumentException e) {
      // PASS THROUGH
      throw e;
    } catch (final Exception e) {
      LogManager.instance().log(Type.class, Level.FINE, "Error in conversion of value '%s' to type '%s'", e, value, targetClass);
      return null;
    }

    return value;
  }

  public static Number increment(final Number a, final Number b) {
    if (a == null || b == null)
      throw new IllegalArgumentException("Cannot increment a null value");

    if (a instanceof Integer) {
      if (b instanceof Integer) {
        final int sum = a.intValue() + b.intValue();
        if (sum < 0 && a.intValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.intValue() + b.intValue());
        return sum;
      } else if (b instanceof Long)
        return Long.valueOf(a.intValue() + b.longValue());
      else if (b instanceof Short) {
        final int sum = a.intValue() + b.shortValue();
        if (sum < 0 && a.intValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.intValue() + b.shortValue());
        return sum;
      } else if (b instanceof Float)
        return Float.valueOf(a.intValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.intValue() + b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return new BigDecimal(a.intValue()).add(decimal);

    } else if (a instanceof Long) {
      if (b instanceof Integer)
        return Long.valueOf(a.longValue() + b.intValue());
      else if (b instanceof Long)
        return Long.valueOf(a.longValue() + b.longValue());
      else if (b instanceof Short)
        return Long.valueOf(a.longValue() + b.shortValue());
      else if (b instanceof Float)
        return Float.valueOf(a.longValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.longValue() + b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return new BigDecimal(a.longValue()).add(decimal);

    } else if (a instanceof Short) {
      if (b instanceof Integer) {
        final int sum = a.shortValue() + b.intValue();
        if (sum < 0 && a.shortValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.shortValue() + b.intValue());
        return sum;
      } else if (b instanceof Long)
        return Long.valueOf(a.shortValue() + b.longValue());
      else if (b instanceof Short) {
        final int sum = a.shortValue() + b.shortValue();
        if (sum < 0 && a.shortValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO INTEGER
          return Integer.valueOf(a.intValue() + b.intValue());
        return sum;
      } else if (b instanceof Float)
        return Float.valueOf(a.shortValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.shortValue() + b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return new BigDecimal(a.shortValue()).add(decimal);

    } else if (a instanceof Float) {
      if (b instanceof Integer)
        return Float.valueOf(a.floatValue() + b.intValue());
      else if (b instanceof Long)
        return Float.valueOf(a.floatValue() + b.longValue());
      else if (b instanceof Short)
        return Float.valueOf(a.floatValue() + b.shortValue());
      else if (b instanceof Float)
        return Float.valueOf(a.floatValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.floatValue() + b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return BigDecimal.valueOf(a.floatValue()).add(decimal);

    } else if (a instanceof Double) {
      if (b instanceof Integer)
        return Double.valueOf(a.doubleValue() + b.intValue());
      else if (b instanceof Long)
        return Double.valueOf(a.doubleValue() + b.longValue());
      else if (b instanceof Short)
        return Double.valueOf(a.doubleValue() + b.shortValue());
      else if (b instanceof Float)
        return Double.valueOf(a.doubleValue() + b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.doubleValue() + b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return BigDecimal.valueOf(a.doubleValue()).add(decimal);

    } else if (a instanceof BigDecimal) {
      if (b instanceof Integer)
        return ((BigDecimal) a).add(new BigDecimal(b.intValue()));
      else if (b instanceof Long)
        return ((BigDecimal) a).add(new BigDecimal(b.longValue()));
      else if (b instanceof Short)
        return ((BigDecimal) a).add(new BigDecimal(b.shortValue()));
      else if (b instanceof Float)
        return ((BigDecimal) a).add(BigDecimal.valueOf(b.floatValue()));
      else if (b instanceof Double)
        return ((BigDecimal) a).add(BigDecimal.valueOf(b.doubleValue()));
      else if (b instanceof BigDecimal decimal)
        return ((BigDecimal) a).add(decimal);

    }

    throw new IllegalArgumentException(
        "Cannot increment value '" + a + "' (" + a.getClass() + ") with '" + b + "' (" + b.getClass() + ")");
  }

  public static Number decrement(final Number a, final Number b) {
    if (a == null || b == null)
      throw new IllegalArgumentException("Cannot decrement a null value");

    if (a instanceof Integer) {
      if (b instanceof Integer) {
        final int sum = a.intValue() - b.intValue();
        if (sum < 0 && a.intValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.intValue() - b.intValue());
        return sum;
      } else if (b instanceof Long)
        return Long.valueOf(a.intValue() - b.longValue());
      else if (b instanceof Short) {
        final int sum = a.intValue() - b.shortValue();
        if (sum < 0 && a.intValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.intValue() - b.shortValue());
        return sum;
      } else if (b instanceof Float)
        return Float.valueOf(a.intValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.intValue() - b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return new BigDecimal(a.intValue()).subtract(decimal);

    } else if (a instanceof Long) {
      if (b instanceof Integer)
        return Long.valueOf(a.longValue() - b.intValue());
      else if (b instanceof Long)
        return Long.valueOf(a.longValue() - b.longValue());
      else if (b instanceof Short)
        return Long.valueOf(a.longValue() - b.shortValue());
      else if (b instanceof Float)
        return Float.valueOf(a.longValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.longValue() - b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return new BigDecimal(a.longValue()).subtract(decimal);

    } else if (a instanceof Short) {
      if (b instanceof Integer) {
        final int sum = a.shortValue() - b.intValue();
        if (sum < 0 && a.shortValue() > 0 && b.intValue() > 0)
          // SPECIAL CASE: UPGRADE TO LONG
          return Long.valueOf(a.shortValue() - b.intValue());
        return sum;
      } else if (b instanceof Long)
        return Long.valueOf(a.shortValue() - b.longValue());
      else if (b instanceof Short) {
        final int sum = a.shortValue() - b.shortValue();
        if (sum < 0 && a.shortValue() > 0 && b.shortValue() > 0)
          // SPECIAL CASE: UPGRADE TO INTEGER
          return Integer.valueOf(a.intValue() - b.intValue());
        return sum;
      } else if (b instanceof Float)
        return Float.valueOf(a.shortValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.shortValue() - b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return new BigDecimal(a.shortValue()).subtract(decimal);

    } else if (a instanceof Float) {
      if (b instanceof Integer)
        return Float.valueOf(a.floatValue() - b.intValue());
      else if (b instanceof Long)
        return Float.valueOf(a.floatValue() - b.longValue());
      else if (b instanceof Short)
        return Float.valueOf(a.floatValue() - b.shortValue());
      else if (b instanceof Float)
        return Float.valueOf(a.floatValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.floatValue() - b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return BigDecimal.valueOf(a.floatValue()).subtract(decimal);

    } else if (a instanceof Double) {
      if (b instanceof Integer)
        return Double.valueOf(a.doubleValue() - b.intValue());
      else if (b instanceof Long)
        return Double.valueOf(a.doubleValue() - b.longValue());
      else if (b instanceof Short)
        return Double.valueOf(a.doubleValue() - b.shortValue());
      else if (b instanceof Float)
        return Double.valueOf(a.doubleValue() - b.floatValue());
      else if (b instanceof Double)
        return Double.valueOf(a.doubleValue() - b.doubleValue());
      else if (b instanceof BigDecimal decimal)
        return BigDecimal.valueOf(a.doubleValue()).subtract(decimal);

    } else if (a instanceof BigDecimal) {
      if (b instanceof Integer)
        return ((BigDecimal) a).subtract(new BigDecimal(b.intValue()));
      else if (b instanceof Long)
        return ((BigDecimal) a).subtract(new BigDecimal(b.longValue()));
      else if (b instanceof Short)
        return ((BigDecimal) a).subtract(new BigDecimal(b.shortValue()));
      else if (b instanceof Float)
        return ((BigDecimal) a).subtract(BigDecimal.valueOf(b.floatValue()));
      else if (b instanceof Double)
        return ((BigDecimal) a).subtract(BigDecimal.valueOf(b.doubleValue()));
      else if (b instanceof BigDecimal decimal)
        return ((BigDecimal) a).subtract(decimal);

    }

    throw new IllegalArgumentException(
        "Cannot decrement value '" + a + "' (" + a.getClass() + ") with '" + b + "' (" + b.getClass() + ")");
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

    } else if (left instanceof Integer) {
      // INTEGER
      if (right instanceof Long)
        left = left.longValue();
      else if (right instanceof Float)
        left = left.floatValue();
      else if (right instanceof Double)
        left = left.doubleValue();
      else if (right instanceof BigDecimal)
        left = new BigDecimal(left.intValue());
      else if (right instanceof Short)
        right = right.intValue();
      else if (right instanceof Byte)
        right = right.intValue();

    } else if (left instanceof Long) {
      // LONG
      if (right instanceof Float)
        left = left.floatValue();
      else if (right instanceof Double)
        left = left.doubleValue();
      else if (right instanceof BigDecimal)
        left = new BigDecimal(left.longValue());
      else if (right instanceof Integer || right instanceof Byte || right instanceof Short)
        right = right.longValue();

    } else if (left instanceof Float) {
      // FLOAT
      if (right instanceof Double)
        left = left.doubleValue();
      else if (right instanceof BigDecimal)
        left = BigDecimal.valueOf(left.floatValue());
      else if (right instanceof Byte || right instanceof Short || right instanceof Integer || right instanceof Long)
        right = right.floatValue();

    } else if (left instanceof Double) {
      // DOUBLE
      if (right instanceof BigDecimal)
        left = BigDecimal.valueOf(left.doubleValue());
      else if (right instanceof Byte || right instanceof Short || right instanceof Integer || right instanceof Long
          || right instanceof Float)
        right = right.doubleValue();

    } else if (left instanceof BigDecimal) {
      // DOUBLE
      if (right instanceof Integer integer)
        right = new BigDecimal(integer);
      else if (right instanceof Float float1)
        right = BigDecimal.valueOf(float1);
      else if (right instanceof Double double1)
        right = BigDecimal.valueOf(double1);
      else if (right instanceof Short short1)
        right = new BigDecimal(short1);
      else if (right instanceof Byte byte1)
        right = new BigDecimal(byte1);
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
    }

    return new Number[] { left, right };
  }

  /**
   * Convert the input object to an integer.
   *
   * @param iValue Any type supported
   *
   * @return The integer value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public int asInt(final Object iValue) {
    if (iValue instanceof Number number)
      return number.intValue();
    else if (iValue instanceof String string)
      return Integer.parseInt(string);
    else if (iValue instanceof Boolean boolean1)
      return boolean1 ? 1 : 0;

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to int for type: " + name);
  }

  /**
   * Convert the input object to a long.
   *
   * @param iValue Any type supported
   *
   * @return The long value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public long asLong(final Object iValue) {
    if (iValue instanceof Number number)
      return number.longValue();
    else if (iValue instanceof String string)
      return Long.parseLong(string);
    else if (iValue instanceof Boolean boolean1)
      return boolean1 ? 1 : 0;

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to long for type: " + name);
  }

  /**
   * Convert the input object to a Float.
   *
   * @param iValue Any type supported
   *
   * @return The float value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public float asFloat(final Object iValue) {
    if (iValue instanceof Number number)
      return number.floatValue();
    else if (iValue instanceof String string)
      return Float.parseFloat(string);

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to float for type: " + name);
  }

  /**
   * Convert the input object to a Double.
   *
   * @param iValue Any type supported
   *
   * @return The double value if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  public double asDouble(final Object iValue) {
    if (iValue instanceof Number number)
      return number.doubleValue();
    else if (iValue instanceof String string)
      return Double.parseDouble(string);

    throw new IllegalArgumentException("Cannot convert value " + iValue + " to double for type: " + name);
  }

  public byte getBinaryType() {
    return binaryType;
  }

  /**
   * Convert the input object to a string.
   *
   * @param iValue Any type supported
   *
   * @return The string if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  @Deprecated
  public String asString(final Object iValue) {
    return iValue.toString();
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

  private static Date convertToDate(final Database database, final Object iValue) throws ParseException {
    if (iValue instanceof Date date)
      return date;
    if (iValue instanceof Number number)
      return new Date(number.longValue());
    else if (iValue instanceof Calendar calendar)
      return calendar.getTime();
    else if (iValue instanceof LocalDateTime time)
      return new Date(TimeUnit.MILLISECONDS.convert(time.toEpochSecond(ZoneOffset.UTC), TimeUnit.SECONDS) +//
          time.getLong(ChronoField.MILLI_OF_SECOND));
    else if (iValue instanceof Instant instant)
      return new Date(instant.toEpochMilli());
    else if (iValue instanceof ZonedDateTime time)
      return new Date(TimeUnit.MILLISECONDS.convert(time.toEpochSecond(), TimeUnit.SECONDS) +//
          time.getLong(ChronoField.MILLI_OF_SECOND));
    else if (iValue instanceof LocalDate date)
      return new Date(date.toEpochDay() * DateUtils.MS_IN_A_DAY);
    else if (iValue instanceof String valueAsString) {
      if (FileUtils.isLong(valueAsString))
        return new Date(Long.parseLong(iValue.toString()));
      else if (database != null)
        try {
          return new SimpleDateFormat(database.getSchema().getDateTimeFormat()).parse(valueAsString);
        } catch (final ParseException ignore) {
          return new SimpleDateFormat(database.getSchema().getDateFormat()).parse(valueAsString);
        }
      else {
        // GUESS FORMAT BY STRING LENGTH
        if (valueAsString.length() == "yyyy-MM-dd".length())
          return new SimpleDateFormat("yyyy-MM-dd").parse(valueAsString);
        else if (valueAsString.length() == "yyyy-MM-dd HH:mm:ss".length())
          return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(valueAsString);
        else if (valueAsString.length() == "yyyy-MM-dd HH:mm:ss.SSS".length())
          return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(valueAsString);
      }
    }
    throw new IllegalArgumentException("Object of class " + iValue.getClass() + " cannot be converted to Date");
  }
}
