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
package com.arcadedb.postgres;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Represents PostgreSQL data types and provides serialization/deserialization functionality.
 */
public enum PostgresType {
  SMALLINT(21, Short.class, 2, value -> Short.parseShort(value)),
  INTEGER(23, Integer.class, 4, value -> Integer.parseInt(value)),
  LONG(20, Long.class, 8, value -> Long.parseLong(value)),
  REAL(700, Float.class, 4, value -> Float.parseFloat(value)),
  DOUBLE(701, Double.class, 8, value -> Double.parseDouble(value)),
  CHAR(18, Character.class, 1, value -> value.charAt(0)),
  BOOLEAN(16, Boolean.class, 1, value -> parseBooleanText(value)),
  DATE(1082, Date.class, 4, value -> parseDateText(value)),
  TIMESTAMP(1114, LocalDateTime.class, 8, value -> parseTimestampText(value)),
  VARCHAR(1043, String.class, -1, value -> value),
  TEXT(25, String.class, -1, value -> value),
  BPCHAR(1042, String.class, -1, value -> value),
  JSON(114, JSONObject.class, -1, JSONObject::new),
  // Adding array types with PostgreSQL array type codes
  ARRAY_INT(1007, Collection.class, -1, value -> parseArrayFromString(value, Integer::parseInt)),
  ARRAY_CHAR(1003, Collection.class, -1, value -> parseArrayFromString(value, s -> s.charAt(0))),
  ARRAY_LONG(1016, Collection.class, -1, value -> parseArrayFromString(value, Long::parseLong)),
  ARRAY_REAL(1021, Collection.class, -1, value -> parseArrayFromString(value, Float::parseFloat)),
  ARRAY_DOUBLE(1022, Collection.class, -1, value -> parseArrayFromString(value, Double::parseDouble)),
  ARRAY_TEXT(1009, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_JSON(199, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_BOOLEAN(1000, Collection.class, -1, value -> parseArrayFromString(value, Boolean::parseBoolean));

  private static final Map<Integer, PostgresType> CODE_MAP = Arrays.stream(values())
      .collect(Collectors.toMap(type -> type.code, type -> type));

  // PostgreSQL-compatible datetime format (ISO 8601 without 'T' separator)
  private static final String            POSTGRES_TIMESTAMP_FORMAT   = "yyyy-MM-dd HH:mm:ss.SSSSSS";
  private static final DateTimeFormatter POSTGRES_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(POSTGRES_TIMESTAMP_FORMAT);
  // PostgreSQL caps array dimensions at 6 (MAXDIM in pg_config_manual.h).
  private static final int               MAX_ARRAY_DIMENSIONS        = 6;
  // PostgreSQL's epoch for DATE/TIMESTAMP binary formats is 2000-01-01T00:00:00 UTC.
  private static final long              POSTGRES_EPOCH_SECONDS      = 946684800L;
  private static final long              POSTGRES_EPOCH_DAYS         = 10957L; // LocalDate.of(2000, 1, 1).toEpochDay()

  private static Boolean parseBooleanText(final String value) {
    if (value == null)
      throw new PostgresProtocolException("Cannot parse null BOOLEAN text value");
    return switch (value.toLowerCase()) {
      case "t", "true", "1", "y", "yes", "on" -> Boolean.TRUE;
      case "f", "false", "0", "n", "no", "off" -> Boolean.FALSE;
      default -> throw new PostgresProtocolException("Cannot parse BOOLEAN text value: " + value);
    };
  }

  private static Date parseDateText(final String value) {
    if (value == null)
      throw new PostgresProtocolException("Cannot parse null DATE text value");
    return Date.from(LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC).toInstant());
  }

  private static LocalDateTime parseTimestampText(final String value) {
    if (value == null)
      throw new PostgresProtocolException("Cannot parse null TIMESTAMP text value");
    final String iso = value.replace(' ', 'T');
    try {
      return LocalDateTime.parse(iso);
    } catch (DateTimeParseException e) {
      return OffsetDateTime.parse(iso).toLocalDateTime();
    }
  }

  private static Number toNumber(final Object value) {
    if (value instanceof Number n)
      return n;
    if (value instanceof Boolean b)
      return b ? 1 : 0;
    if (value instanceof Character c)
      return (int) c;
    return new BigDecimal(value.toString());
  }

  private static boolean toBooleanValue(final Object value) {
    if (value instanceof Boolean b)
      return b;
    if (value instanceof Number n)
      return n.intValue() != 0;
    return parseBooleanText(value.toString());
  }

  private static LocalDate toLocalDateValue(final Object value) {
    if (value instanceof LocalDate ld)
      return ld;
    if (value instanceof LocalDateTime ldt)
      return ldt.toLocalDate();
    if (value instanceof Date d)
      return d.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
    if (value instanceof String s)
      return LocalDate.parse(s);
    throw new PostgresProtocolException("Unsupported DATE binary value type: " + value.getClass());
  }

  private static LocalDateTime toLocalDateTimeValue(final Object value) {
    if (value instanceof LocalDateTime l)
      return l;
    if (value instanceof Date d)
      return LocalDateTime.ofInstant(d.toInstant(), ZoneOffset.UTC);
    if (value instanceof LocalDate ld)
      return ld.atStartOfDay();
    if (value instanceof String s)
      return parseTimestampText(s);
    throw new PostgresProtocolException("Unsupported TIMESTAMP binary value type: " + value.getClass());
  }

  public final  int                      code;
  public final  Class<?>                 cls;
  public final  int                      size;
  private final Function<String, Object> textParser;

  PostgresType(final int code, final Class<?> cls, final int size, Function<String, Object> textParser) {
    this.code = code;
    this.cls = cls;
    this.size = size;
    this.textParser = textParser;
  }

  /**
   * Parses an array string representation into an ArrayList.
   * Handles PostgreSQL array format like '{1,2,3}' or '{\"value1\",\"value2\"}'
   */
  private static <T> ArrayList<T> parseArrayFromString(String arrayStr, Function<String, T> elementParser) {
    if (arrayStr == null || arrayStr.isEmpty())
      return new ArrayList<>();

    // Handle PostgreSQL array format: remove curly braces and split by comma
    String content = arrayStr.trim();
    if (content.startsWith("{") && content.endsWith("}")) {
      content = content.substring(1, content.length() - 1);
    }

    // Split by comma, but handle quoted strings properly
    List<String> elements = new ArrayList<>();
    StringBuilder currentElement = new StringBuilder();
    boolean inQuotes = false;
    for (int i = 0; i < content.length(); i++) {
      char c = content.charAt(i);
      if (c == '"') {
        inQuotes = !inQuotes;
        // Skip the quote character itself for parsing
        continue;
      } else if (c == ',' && !inQuotes) {
        elements.add(currentElement.toString().trim());
        currentElement = new StringBuilder();
        continue;
      }
      currentElement.append(c);
    }
    // Add the last element
    if (currentElement.length() > 0) {
      elements.add(currentElement.toString().trim());
    }

    // Parse each element
    ArrayList<T> result = new ArrayList<>();
    for (String element : elements) {
      if (!element.isEmpty()) {
        result.add(elementParser.apply(element));
      }
    }
    return result;
  }

  public static PostgresType getTypeForValue(Object val) {
    if (val == null) {
      return PostgresType.VARCHAR;
    } else if (val instanceof Float) {
      return PostgresType.REAL;
    } else if (val instanceof Double) {
      return PostgresType.DOUBLE;
    } else if (val instanceof Integer || val instanceof Short || val instanceof Byte) {
      return PostgresType.INTEGER;
    } else if (val instanceof Long) {
      return PostgresType.LONG;
    } else if (val instanceof Boolean) {
      return PostgresType.BOOLEAN;
    } else if (val instanceof String) {
      return PostgresType.VARCHAR;
    } else if (val instanceof Character) {
      return PostgresType.CHAR;
    } else if (val instanceof JSONObject) {
      return PostgresType.JSON;
    } else if (val instanceof Result) {
      return PostgresType.JSON;
    } else if (val instanceof EmbeddedDocument) {
      return PostgresType.JSON;
    } else if (val instanceof Map) {
      return PostgresType.JSON;
    } else if (val instanceof Record) {
      return PostgresType.JSON;
    } else if (val instanceof Collection<?> collection) {
      // Determine element type from the first non-null element
      return collection.stream()
          .filter(Objects::nonNull)
          .findFirst()
          .map(PostgresType::getArrayTypeForElementType)
          .orElse(PostgresType.ARRAY_TEXT);
    } else if (val instanceof Iterable<?> iterable) {
      return StreamSupport.stream(iterable.spliterator(), false)
          .filter(Objects::nonNull)
          .findFirst()
          .map(PostgresType::getArrayTypeForElementType)
          .orElse(PostgresType.ARRAY_TEXT);

    } else if (val instanceof Iterator<?> iterator) {
      while (iterator.hasNext()) {
        Object next = iterator.next();
        if (next != null) {
          return getArrayTypeForElementType(next);
        }
      }
      return PostgresType.ARRAY_TEXT;
    } else if (val instanceof byte[]) {
      return PostgresType.ARRAY_CHAR;
    } else if (val.getClass().isArray()) {
      // Handle Java arrays
      return switch (val) {
        // Shorts and boxed bytes widen to int4[]: getArrayTypeForElementType answers ARRAY_INT for a Short or a
        // Byte element, and there is no int2[] entry to pair with a narrower answer. Only the primitive byte[]
        // means BINARY (handled above); a Byte[] is an array of small integers.
        case short[] shorts -> PostgresType.ARRAY_INT;
        case Short[] shorts -> PostgresType.ARRAY_INT;
        case Byte[] bytes -> PostgresType.ARRAY_INT;
        case int[] ints -> PostgresType.ARRAY_INT;
        case Integer[] ints -> PostgresType.ARRAY_INT;
        case long[] longs -> PostgresType.ARRAY_LONG;
        case Long[] longs -> PostgresType.ARRAY_LONG;
        case double[] doubles -> PostgresType.ARRAY_DOUBLE;
        case Double[] doubles -> PostgresType.ARRAY_DOUBLE;
        case float[] floats -> PostgresType.ARRAY_REAL;
        case Float[] floats -> PostgresType.ARRAY_REAL;
        case boolean[] booleans -> PostgresType.ARRAY_BOOLEAN;
        case Boolean[] booleans -> PostgresType.ARRAY_BOOLEAN;
        case char[] chars -> PostgresType.ARRAY_CHAR;
        case Character[] chars -> PostgresType.ARRAY_CHAR;
        case String[] strings -> PostgresType.ARRAY_TEXT;
        default -> throw new IllegalStateException("Unexpected value: " + val);
      };
    } else if (val instanceof Date) {
      return PostgresType.DATE;
    } else if (val instanceof LocalDateTime) {
      return PostgresType.TIMESTAMP;
    }

    return PostgresType.VARCHAR;

  }

  /**
   * Maps an ArcadeDB schema Type to a PostgreSQL type, resolving the element type of a LIST from the
   * property's declared "OF" clause.
   *
   * @param arcadeType The ArcadeDB schema type
   * @param ofType     The declared element type name of a LIST property, or null when undeclared. Ignored for
   *                   any other type.
   *
   * @return The corresponding PostgreSQL type
   */
  public static PostgresType getTypeFromArcade(final Type arcadeType, final String ofType) {
    if (arcadeType == Type.LIST)
      return getArrayTypeForOfType(ofType);
    return getTypeFromArcade(arcadeType);
  }

  /**
   * Resolves the array type of a "LIST OF &lt;ofType&gt;" property. An ofType that does not name a scalar
   * {@link Type} refers to an embedded document type, so the list is advertised as json[]; this mirrors the
   * convention used by Type.coerceCollectionOfType. An undeclared ofType stays text[].
   */
  private static PostgresType getArrayTypeForOfType(final String ofType) {
    if (ofType == null || ofType.isBlank())
      return ARRAY_TEXT;

    final Type elementType = Type.getTypeByName(ofType);
    if (elementType == null)
      // Not a scalar: the list holds embedded documents of a schema type.
      return ARRAY_JSON;

    // Every branch must agree with getArrayTypeForElementType, which types a populated list from its first
    // element: a mismatch would make a column's OID depend on whether the list is empty. DECIMAL therefore
    // falls through to ARRAY_TEXT, because a list of BigDecimal has no match there either.
    return switch (elementType) {
      case BOOLEAN -> ARRAY_BOOLEAN;
      case INTEGER, SHORT, BYTE -> ARRAY_INT;
      case LONG -> ARRAY_LONG;
      case FLOAT -> ARRAY_REAL;
      case DOUBLE -> ARRAY_DOUBLE;
      case MAP, EMBEDDED -> ARRAY_JSON;
      default -> ARRAY_TEXT;
    };
  }

  /**
   * Maps an ArcadeDB schema Type to a PostgreSQL type.
   *
   * @param arcadeType The ArcadeDB schema type
   *
   * @return The corresponding PostgreSQL type
   */
  public static PostgresType getTypeFromArcade(Type arcadeType) {
    if (arcadeType == null) {
      return PostgresType.VARCHAR;
    }

    // Every branch must agree with getTypeForValue, which types a column from a sample row: a mismatch would make
    // the column's OID depend on whether the result set happens to be empty.
    return switch (arcadeType) {
      case BOOLEAN -> PostgresType.BOOLEAN;
      case INTEGER -> PostgresType.INTEGER;
      case SHORT -> PostgresType.SMALLINT;
      case LONG -> PostgresType.LONG;
      case FLOAT -> PostgresType.REAL;
      case DOUBLE -> PostgresType.DOUBLE;
      case BYTE -> PostgresType.SMALLINT;
      case STRING -> PostgresType.VARCHAR;
      case DATETIME, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND -> PostgresType.TIMESTAMP;
      case DATE -> PostgresType.DATE;
      case BINARY -> PostgresType.VARCHAR; // No direct binary type, use VARCHAR
      case LIST -> PostgresType.ARRAY_TEXT;
      case ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS -> PostgresType.ARRAY_INT;
      case ARRAY_OF_LONGS -> PostgresType.ARRAY_LONG;
      case ARRAY_OF_FLOATS -> PostgresType.ARRAY_REAL;
      case ARRAY_OF_DOUBLES -> PostgresType.ARRAY_DOUBLE;
      case MAP, EMBEDDED -> PostgresType.JSON;
      case LINK -> PostgresType.VARCHAR;
      case DECIMAL -> PostgresType.DOUBLE;
      default -> PostgresType.VARCHAR;
    };
  }

  /**
   * Serializes a value as text format into the provided Binary buffer.
   *
   * @param pgType     The PostgreSQL type
   * @param typeBuffer The buffer to write to
   * @param value      The value to serialize
   */
  @SuppressWarnings("unchecked")
  public void serializeAsText(final PostgresType pgType, final Binary typeBuffer, final Object value) {
    String serializedValue = null;
    if (value == null && pgType.code == BOOLEAN.code) {
      serializedValue = "0";
    } else if (value instanceof Collection<?> collection) {
      // Handle array serialization
      serializedValue = serializeArrayToString(collection, pgType);
    } else if (value != null && value.getClass().isArray()) {
      // Handle primitive arrays by converting them to Collections
      Collection<?> collection = convertPrimitiveArrayToCollection(value);
      serializedValue = serializeArrayToString(collection, pgType);
    } else if (value instanceof Boolean b) {
      // PostgreSQL BOOL text format is "t"/"f"
      serializedValue = b ? "t" : "f";
    } else if (value instanceof Date date) {
      // DATE (OID 1082) expects "YYYY-MM-DD" in text format
      serializedValue = date.toInstant().atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE);
    } else if (value instanceof LocalDateTime ldt) {
      // TIMESTAMP (OID 1114) expects "yyyy-MM-dd HH:mm:ss.SSSSSS" in text format
      serializedValue = ldt.format(POSTGRES_DATETIME_FORMATTER);
    } else if (value instanceof JSONObject json) {
      serializedValue = json.toString();
    } else if (value instanceof Map<?, ?> map) {
      serializedValue = new JSONObject((Map<String, ?>) map).toString();
    } else if (value instanceof Record record) {
      serializedValue = record.toJSON(true).toString();
    } else if (value instanceof Result result) {
      serializedValue = result.toJSON().toString();
    } else if (value instanceof EmbeddedDocument embeddedDocument) {
      serializedValue = embeddedDocument.toJSON(true).toString();
    } else if (value != null) {
      serializedValue = value.toString();
    }
    writeString(typeBuffer, serializedValue);
  }

  /**
   * Serializes a value as PostgreSQL binary format into the provided Binary buffer.
   * Used when a client requests binary results via Bind message format codes. Types without a
   * binary mapping fall back to {@link #serializeAsText}.
   */
  public void serializeAsBinary(final PostgresType pgType, final Binary typeBuffer, final Object value) {
    if (value == null) {
      typeBuffer.putInt(-1);
      return;
    }
    switch (pgType) {
    case SMALLINT -> {
      typeBuffer.putInt(2);
      typeBuffer.putShort(toNumber(value).shortValue());
    }
    case INTEGER -> {
      typeBuffer.putInt(4);
      typeBuffer.putInt(toNumber(value).intValue());
    }
    case LONG -> {
      typeBuffer.putInt(8);
      typeBuffer.putLong(toNumber(value).longValue());
    }
    case REAL -> {
      typeBuffer.putInt(4);
      typeBuffer.putInt(Float.floatToRawIntBits(toNumber(value).floatValue()));
    }
    case DOUBLE -> {
      typeBuffer.putInt(8);
      typeBuffer.putLong(Double.doubleToRawLongBits(toNumber(value).doubleValue()));
    }
    case BOOLEAN -> {
      typeBuffer.putInt(1);
      typeBuffer.putByte((byte) (toBooleanValue(value) ? 1 : 0));
    }
    case CHAR -> {
      // Postgres "char" (OID 18) is a single byte on the wire.
      typeBuffer.putInt(1);
      final char c = value instanceof Character ch ? ch : value.toString().charAt(0);
      typeBuffer.putByte((byte) c);
    }
    case DATE -> {
      typeBuffer.putInt(4);
      typeBuffer.putInt((int) (toLocalDateValue(value).toEpochDay() - POSTGRES_EPOCH_DAYS));
    }
    case TIMESTAMP -> {
      typeBuffer.putInt(8);
      final LocalDateTime ldt = toLocalDateTimeValue(value);
      final long secsFromPgEpoch = ldt.toEpochSecond(ZoneOffset.UTC) - POSTGRES_EPOCH_SECONDS;
      typeBuffer.putLong(secsFromPgEpoch * 1_000_000L + ldt.getNano() / 1000L);
    }
    // Strings, JSON, and arrays do not have a separate binary representation: their wire format
    // is identical to text for our purposes (length-prefixed UTF-8 bytes / array literal text).
    default -> serializeAsText(pgType, typeBuffer, value);
    }
  }

  private void writeString(final Binary typeBuffer, final String value) {
    if (value == null) {
      typeBuffer.putInt(-1);
      return;
    }

    final byte[] str = value.getBytes(DatabaseFactory.getDefaultCharset());
    typeBuffer.putInt(str.length);
    typeBuffer.putByteArray(str);
  }

  /**
   * Serializes a Collection into a PostgreSQL array string format.
   */
  @SuppressWarnings("unchecked")
  private String serializeArrayToString(Collection<?> collection, PostgresType pgType) {
    if (collection.isEmpty())
      return "{}";

    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Object element : collection) {
      if (!first) {
        sb.append(",");
      }
      first = false;
      if (element instanceof Float || element.getClass() == float.class) {
        sb.append(((Number) element).floatValue());
      } else if (element instanceof Double || element.getClass() == double.class) {
        sb.append(((Number) element).doubleValue());
      } else if (element instanceof Number || element instanceof Boolean) {
        sb.append(element);
      } else if (element instanceof Character) {
        sb.append("'").append(element).append("'");
      } else if (element instanceof Date date) {
        // Format Date as PostgreSQL-compatible timestamp in arrays
        LocalDateTime ldt = LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC);
        sb.append("\"").append(ldt.format(POSTGRES_DATETIME_FORMATTER)).append("\"");
      } else if (element instanceof LocalDateTime ldt) {
        // Format LocalDateTime as PostgreSQL-compatible timestamp in arrays
        sb.append("\"").append(ldt.format(POSTGRES_DATETIME_FORMATTER)).append("\"");
      } else if (element instanceof Binary binary) {
        sb.append(binary.getString());
      } else if (element instanceof byte[] bytes) {
        sb.append(Arrays.toString(bytes));
      } else if (element instanceof Collection<?> subCollection) {
        sb.append(serializeArrayToString(subCollection, pgType));
      } else if (element instanceof Result result) {
        sb.append("\"").append(result.toJSON().toString().replace("\"", "\\\"")).append("\"");
      } else if (element instanceof JSONObject json) {
        sb.append("\"").append(json.toString().replace("\"", "\\\"")).append("\"");
      } else if (element instanceof Map<?, ?> map) {
        sb.append("\"").append(new JSONObject((Map<String, ?>) map).toString().replace("\"", "\\\"")).append("\"");
      } else if (element instanceof Record record) {
        sb.append("\"").append(record.toJSON(true).toString().replace("\"", "\\\"")).append("\"");
      } else if (element instanceof EmbeddedDocument embeddedDocument) {
        sb.append("\"").append(embeddedDocument.toJSON(true).toString().replace("\"", "\\\"")).append("\"");
      } else if (element instanceof String str) {
        sb.append("\"").append(str.replace("\"", "\\\"")).append("\"");
      } else {
        sb.append(element == null ? "NULL" : element.toString());
      }
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Converts a primitive array to a Collection for serialization.
   * Handles all primitive array types: int[], long[], float[], double[], short[], boolean[], char[], byte[]
   * and object arrays like String[].
   */
  private Collection<?> convertPrimitiveArrayToCollection(Object array) {
    if (array instanceof int[] intArray) {
      List<Integer> list = new ArrayList<>(intArray.length);
      for (int val : intArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof long[] longArray) {
      List<Long> list = new ArrayList<>(longArray.length);
      for (long val : longArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof float[] floatArray) {
      List<Float> list = new ArrayList<>(floatArray.length);
      for (float val : floatArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof double[] doubleArray) {
      List<Double> list = new ArrayList<>(doubleArray.length);
      for (double val : doubleArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof short[] shortArray) {
      List<Short> list = new ArrayList<>(shortArray.length);
      for (short val : shortArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof boolean[] booleanArray) {
      List<Boolean> list = new ArrayList<>(booleanArray.length);
      for (boolean val : booleanArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof char[] charArray) {
      List<Character> list = new ArrayList<>(charArray.length);
      for (char val : charArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof byte[] byteArray) {
      List<Byte> list = new ArrayList<>(byteArray.length);
      for (byte val : byteArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof Object[] objectArray) {
      // Handle object arrays like String[]
      return Arrays.asList(objectArray);
    } else {
      // Fallback: should not happen, but return empty list
      return new ArrayList<>();
    }
  }

  /**
   * Determines the appropriate array type based on the element type.
   */
  public static PostgresType getArrayTypeForElementType(Object element) {
    if (element instanceof Integer ||
        element instanceof Short ||
        element instanceof Byte)
      return ARRAY_INT;
    if (element instanceof Float ||
        element.getClass() == float.class)
      return ARRAY_REAL;
    if (element instanceof Double ||
        element.getClass() == double.class)
      return ARRAY_DOUBLE;
    if (element instanceof Long)
      return ARRAY_LONG;
    if (element instanceof Boolean)
      return ARRAY_BOOLEAN;
    if (element instanceof String)
      return ARRAY_TEXT;
    if (element instanceof JSONObject ||
        element instanceof Map ||
        element instanceof Result ||
        element instanceof EmbeddedDocument ||
        element instanceof Record)
      return ARRAY_JSON;
    // Default to text array for all other types
    return ARRAY_TEXT;
  }

  /**
   * Deserializes a value based on the PostgreSQL type code and format code.
   *
   * @param code         The PostgreSQL type code
   * @param formatCode   The format code (0 for text, 1 for binary)
   * @param valueAsBytes The raw byte array to deserialize
   *
   * @return The deserialized object
   *
   * @throws PostgresProtocolException if the type or format is not supported
   */
  public static Object deserialize(final long code, final int formatCode, final byte[] valueAsBytes) {
    return switch (formatCode) {
      case 0 -> deserializeText(code, valueAsBytes);
      case 1 -> deserializeBinary(code, valueAsBytes);
      default -> throw new PostgresProtocolException("Invalid format code " + formatCode);
    };
  }

  private static Object deserializeText(final long code, final byte[] valueAsBytes) {
    String str = new String(valueAsBytes, DatabaseFactory.getDefaultCharset());
    if (code == 0) { // UNSPECIFIED
      // Try to detect if this is a PostgreSQL array format
      if (str.startsWith("{") && str.endsWith("}")) {
        // Parse as an array using the TEXT array parser
        return parseArrayFromString(str, s -> s);
      }
      return str;
    }

    PostgresType type = CODE_MAP.get((int) code);
    if (type == null) {
      throw new PostgresProtocolException("Type with code " + code + " not supported for deserializing");
    }

    return type.textParser.apply(str);
  }

  private static Object deserializeBinary(final long code, final byte[] valueAsBytes) {
    ByteBuffer buffer = ByteBuffer.wrap(valueAsBytes);
    PostgresType type = CODE_MAP.get((int) code);

    if (type == null) {
      throw new PostgresProtocolException("Type with code " + code + " not supported for deserializing");
    }

    return switch (type) {
      case VARCHAR, TEXT, BPCHAR -> {
        // In PostgreSQL binary format, VARCHAR/TEXT/BPCHAR is just the raw bytes
        // The length is already provided in the Bind message's parameter size
        yield new String(valueAsBytes, DatabaseFactory.getDefaultCharset());
      }
      case SMALLINT -> buffer.getShort();
      case INTEGER -> buffer.getInt();
      case LONG -> buffer.getLong();
      case REAL -> buffer.getFloat();
      case DOUBLE -> buffer.getDouble();
      case DATE -> {
        // PostgreSQL binary DATE: int32 days since 2000-01-01
        final int days = buffer.getInt();
        yield Date.from(LocalDate.ofEpochDay(POSTGRES_EPOCH_DAYS + days).atStartOfDay(ZoneOffset.UTC).toInstant());
      }
      case TIMESTAMP -> {
        // PostgreSQL binary TIMESTAMP: int64 microseconds since 2000-01-01T00:00:00Z
        final long microseconds = buffer.getLong();
        long secs = POSTGRES_EPOCH_SECONDS + microseconds / 1_000_000L;
        int nanos = (int) (microseconds % 1_000_000L) * 1000;
        if (nanos < 0) {
          secs -= 1;
          nanos += 1_000_000_000;
        }
        yield LocalDateTime.ofEpochSecond(secs, nanos, ZoneOffset.UTC);
      }
      case CHAR -> (char) buffer.get();
      case BOOLEAN -> buffer.get() == 1;
      case JSON -> {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield new JSONObject(new String(bytes));
      }
      case ARRAY_INT, ARRAY_LONG, ARRAY_DOUBLE, ARRAY_REAL, ARRAY_TEXT, ARRAY_BOOLEAN, ARRAY_CHAR, ARRAY_JSON ->
          deserializeBinaryArray(buffer);
    };
  }

  private static List<Object> deserializeBinaryArray(ByteBuffer buffer) {
    final int ndim = buffer.getInt();    // number of dimensions
    buffer.getInt();                      // hasnull flag (unused)
    final int elemOid = buffer.getInt(); // element type OID

    if (ndim == 0)
      return new ArrayList<>();

    if (ndim < 0 || ndim > MAX_ARRAY_DIMENSIONS)
      throw new PostgresProtocolException("Invalid array dimension count: " + ndim);

    long totalElements = 1L;
    for (int d = 0; d < ndim; d++) {
      final int dimSize = buffer.getInt();
      buffer.getInt();                   // lower bound (unused)
      if (dimSize < 0)
        throw new PostgresProtocolException("Negative array dimension size: " + dimSize);
      totalElements *= dimSize;
      if (totalElements > Integer.MAX_VALUE)
        throw new PostgresProtocolException("Array element count exceeds Integer.MAX_VALUE");
    }

    // Each element occupies at least 4 bytes (the length prefix), so an element count
    // exceeding remaining/4 cannot fit and signals a malformed or malicious payload.
    if (totalElements > buffer.remaining() / 4L)
      throw new PostgresProtocolException("Array element count exceeds remaining buffer bytes");

    final int count = (int) totalElements;
    final ArrayList<Object> result = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final int elemLen = buffer.getInt();
      if (elemLen == -1) {
        result.add(null);
      } else {
        final byte[] elemBytes = new byte[elemLen];
        buffer.get(elemBytes);
        result.add(deserializeBinaryElement(elemOid, elemBytes));
      }
    }
    return result;
  }

  private static Object deserializeBinaryElement(final int elemOid, final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    if (elemOid == BOOLEAN.code)
      return buf.get() != 0;
    if (elemOid == LONG.code)
      return buf.getLong();
    // int2[] (OID 1005) is not in the ARRAY_* enum, but int2 elements can appear inside
    // any array (e.g. composite types), so we still decode them as Short here.
    if (elemOid == SMALLINT.code)
      return buf.getShort();
    if (elemOid == INTEGER.code)
      return buf.getInt();
    if (elemOid == REAL.code)
      return buf.getFloat();
    if (elemOid == DOUBLE.code)
      return buf.getDouble();
    // text/varchar/bpchar/json/unknown - raw bytes are already the UTF-8 string content.
    return new String(bytes, DatabaseFactory.getDefaultCharset());
  }

  /**
   * Checks if this type is an array type.
   */
  public boolean isArrayType() {
    return this == ARRAY_INT ||
        this == ARRAY_CHAR ||
        this == ARRAY_LONG ||
        this == ARRAY_DOUBLE ||
        this == ARRAY_REAL ||
        this == ARRAY_TEXT ||
        this == ARRAY_JSON ||
        this == ARRAY_BOOLEAN;
  }

  /**
   * Returns true for scalar types that should be advertised with their native Postgres OID rather
   * than collapsing to VARCHAR. Clients use the announced OID to choose a deserializer, so
   * advertising VARCHAR for numeric/boolean/temporal columns causes values to round-trip as
   * strings and breaks typed parameter comparisons.
   */
  public boolean isNativeScalarType() {
    return this == SMALLINT ||
        this == INTEGER ||
        this == LONG ||
        this == REAL ||
        this == DOUBLE ||
        this == CHAR ||
        this == BOOLEAN ||
        this == DATE ||
        this == TIMESTAMP;
  }

  /**
   * Returns true when {@link #serializeAsBinary} produces a wire payload that matches the type's
   * binary protocol specification. Array types currently lack a binary encoder, so they MUST be
   * advertised as text (0) in RowDescription regardless of what the client requested in Bind -
   * otherwise the RowDescription format code and the DataRow bytes disagree and clients misparse.
   * String/JSON types are safe because their text and binary wire formats are identical raw bytes.
   */
  public boolean hasBinaryEncoding() {
    return !isArrayType();
  }
}
