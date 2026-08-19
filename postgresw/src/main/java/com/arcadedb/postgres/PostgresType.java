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
import com.arcadedb.serializer.json.JSONArray;
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
  // The three catalog columns after the OID are pg_type's own: typname, typelem (the element type of an
  // array, 0 for a scalar) and typarray (the array type built on a scalar, 0 for an array). They are the
  // answer this protocol gives a client that enumerates or looks up pg_type (issue #5290), so they hold
  // PostgreSQL's real names and OIDs rather than anything ArcadeDB-specific: a client resolves the OID it
  // was handed in RowDescription against them, and a name that is not Postgres' name resolves to nothing.
  SMALLINT(21, "int2", 0, 1005, Short.class, 2, value -> Short.parseShort(value)),
  INTEGER(23, "int4", 0, 1007, Integer.class, 4, value -> Integer.parseInt(value)),
  LONG(20, "int8", 0, 1016, Long.class, 8, value -> Long.parseLong(value)),
  REAL(700, "float4", 0, 1021, Float.class, 4, value -> Float.parseFloat(value)),
  DOUBLE(701, "float8", 0, 1022, Double.class, 8, value -> Double.parseDouble(value)),
  CHAR(18, "char", 0, 1002, Character.class, 1, value -> value.charAt(0)),
  BOOLEAN(16, "bool", 0, 1000, Boolean.class, 1, value -> parseBooleanText(value)),
  DATE(1082, "date", 0, 1182, Date.class, 4, value -> parseDateText(value)),
  TIMESTAMP(1114, "timestamp", 0, 1115, LocalDateTime.class, 8, value -> parseTimestampText(value)),
  VARCHAR(1043, "varchar", 0, 1015, String.class, -1, value -> value),
  TEXT(25, "text", 0, 1009, String.class, -1, value -> value),
  BPCHAR(1042, "bpchar", 0, 1014, String.class, -1, value -> value),
  JSON(114, "json", 0, 199, JSONObject.class, -1, PostgresType::parseJsonText),
  // The type PostgreSQL has for a blob, and the only honest answer for a byte[] (issue #6411). A byte[] used
  // to be typed as "char"[] (OID 1002) by getTypeForValue and as varchar (OID 1043) by getTypeFromArcade, so
  // a BINARY column's OID depended on whether the result set happened to be empty - and neither answer was
  // right: an array of one-byte characters makes the client decode arbitrary bytes as text, which for any
  // non-UTF-8 payload loses data rather than merely looking odd.
  BYTEA(17, "bytea", 0, 1001, byte[].class, -1, PostgresType::parseByteaText),
  // Adding array types with PostgreSQL array type codes
  ARRAY_INT(1007, "_int4", 23, 0, Collection.class, -1, value -> parseArrayFromString(value, Integer::parseInt)),
  // 1002 is "char"[], the array of the single-byte CHAR (OID 18) this enum pairs it with. It used to be
  // declared as 1003, which in PostgreSQL is name[] - an unrelated type - so a client resolving the OID
  // this protocol announced for a list of characters was told a type ArcadeDB never produces (issue #5290).
  ARRAY_CHAR(1002, "_char", 18, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s.charAt(0))),
  ARRAY_LONG(1016, "_int8", 20, 0, Collection.class, -1, value -> parseArrayFromString(value, Long::parseLong)),
  ARRAY_REAL(1021, "_float4", 700, 0, Collection.class, -1, value -> parseArrayFromString(value, Float::parseFloat)),
  ARRAY_DOUBLE(1022, "_float8", 701, 0, Collection.class, -1, value -> parseArrayFromString(value, Double::parseDouble)),
  ARRAY_TEXT(1009, "_text", 25, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_JSON(199, "_json", 114, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_BOOLEAN(1000, "_bool", 16, 0, Collection.class, -1, value -> parseArrayFromString(value, Boolean::parseBoolean));

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

  /**
   * Parses an inbound json value. A list of documents is sent to the client as a json array (issue #5366), so
   * the same payload must be accepted back: an array yields a List, anything else a {@link JSONObject}.
   */
  private static Object parseJsonText(final String value) {
    if (value == null)
      return null;
    final String trimmed = value.trim();
    if (!trimmed.isEmpty() && trimmed.charAt(0) == '[')
      return new JSONArray(trimmed).toList();
    return new JSONObject(value);
  }

  /**
   * Parses the text representation of a bytea. PostgreSQL emits the hex format {@code \x<hexdigits>} for any
   * server version from 9.0 on - which is what this protocol announces - but still accepts the older escape
   * format on input, and some clients still send it, so both are read here.
   */
  private static byte[] parseByteaText(final String value) {
    if (value == null)
      throw new PostgresProtocolException("Cannot parse null BYTEA text value");

    if (value.length() >= 2 && value.charAt(0) == '\\' && (value.charAt(1) == 'x' || value.charAt(1) == 'X')) {
      final int digits = value.length() - 2;
      if (digits % 2 != 0)
        throw new PostgresProtocolException("Invalid hex BYTEA text value: odd number of digits");

      final byte[] bytes = new byte[digits / 2];
      for (int i = 0; i < bytes.length; i++) {
        final int high = Character.digit(value.charAt(2 + i * 2), 16);
        final int low = Character.digit(value.charAt(3 + i * 2), 16);
        if (high < 0 || low < 0)
          throw new PostgresProtocolException("Invalid hex BYTEA text value: not a hex digit");
        bytes[i] = (byte) ((high << 4) | low);
      }
      return bytes;
    }

    return parseByteaEscapeText(value);
  }

  /**
   * Decodes the pre-9.0 bytea escape format: a backslash introduces either another backslash or a three-digit
   * octal byte, and everything else stands for itself.
   */
  private static byte[] parseByteaEscapeText(final String value) {
    final byte[] bytes = new byte[value.length()];
    int len = 0;
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      if (c != '\\') {
        bytes[len++] = (byte) c;
        continue;
      }

      if (i + 1 >= value.length())
        throw new PostgresProtocolException("Invalid escaped BYTEA text value: trailing backslash");

      final char next = value.charAt(++i);
      if (next == '\\') {
        bytes[len++] = (byte) '\\';
        continue;
      }

      if (i + 2 >= value.length())
        throw new PostgresProtocolException("Invalid escaped BYTEA text value: truncated octal escape");

      int octal = 0;
      for (int digit = 0; digit < 3; digit++) {
        final int parsed = Character.digit(value.charAt(i + digit), 8);
        if (parsed < 0)
          throw new PostgresProtocolException("Invalid escaped BYTEA text value: not an octal digit");
        octal = octal * 8 + parsed;
      }
      i += 2;
      bytes[len++] = (byte) octal;
    }
    return Arrays.copyOf(bytes, len);
  }

  /** Renders bytes in the {@code \x<hexdigits>} form PostgreSQL 9.0 and later emit. */
  private static String toByteaText(final byte[] bytes) {
    final StringBuilder buffer = new StringBuilder(2 + bytes.length * 2);
    buffer.append("\\x");
    for (final byte b : bytes) {
      buffer.append(Character.forDigit((b >> 4) & 0xF, 16));
      buffer.append(Character.forDigit(b & 0xF, 16));
    }
    return buffer.toString();
  }

  /** The bytes behind a value announced as bytea, or null when the value is not one. */
  private static byte[] byteaValueOf(final Object value) {
    if (value instanceof byte[] bytes)
      return bytes;
    if (value instanceof Binary binary)
      return binary.toByteArray();
    return null;
  }

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
  /** pg_type.typname, the name PostgreSQL itself gives {@link #code}. */
  public final  String                   typeName;
  /** pg_type.typelem: the OID of the element type for an array type, 0 for a scalar. */
  public final  int                      elementCode;
  /** pg_type.typarray: the OID of the array type built on a scalar, 0 for an array type. */
  public final  int                      arrayCode;
  public final  Class<?>                 cls;
  public final  int                      size;
  private final Function<String, Object> textParser;

  PostgresType(final int code, final String typeName, final int elementCode, final int arrayCode, final Class<?> cls,
      final int size, Function<String, Object> textParser) {
    this.code = code;
    this.typeName = typeName;
    this.elementCode = elementCode;
    this.arrayCode = arrayCode;
    this.cls = cls;
    this.size = size;
    this.textParser = textParser;
  }

  /**
   * Returns the type carrying the given OID, or null when this protocol has no such type. Unlike
   * {@code deserializeAsText}/{@code deserializeAsBinary}, which reject an unknown OID, a catalog lookup
   * for a type ArcadeDB cannot produce is a legitimate answer of "no row" rather than an error.
   */
  public static PostgresType byCode(final int code) {
    return CODE_MAP.get(code);
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
      if (c == '\\' && i + 1 < content.length()) {
        // Inside an array literal a backslash escapes the next character, typically a quote or another
        // backslash (issue #5366). Without this the escaped quote was read as the end of the element.
        currentElement.append(content.charAt(++i));
        continue;
      } else if (c == '"') {
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
    } else if (val instanceof byte[] || val instanceof Binary) {
      return PostgresType.BYTEA;
    } else if (val.getClass().isArray()) {
      // Handle Java arrays
      // Shorts and boxed bytes widen to int4[]: getArrayTypeForElementType answers ARRAY_INT for a Short or a
      // Byte element, and there is no int2[] entry to pair with a narrower answer. Only the primitive byte[]
      // means BINARY (handled above); a Byte[] is an array of small integers.
      if (val instanceof short[])
        return PostgresType.ARRAY_INT;
      else if (val instanceof Short[])
        return PostgresType.ARRAY_INT;
      else if (val instanceof Byte[])
        return PostgresType.ARRAY_INT;
      else if (val instanceof int[])
        return PostgresType.ARRAY_INT;
      else if (val instanceof Integer[])
        return PostgresType.ARRAY_INT;
      else if (val instanceof long[])
        return PostgresType.ARRAY_LONG;
      else if (val instanceof Long[])
        return PostgresType.ARRAY_LONG;
      else if (val instanceof double[])
        return PostgresType.ARRAY_DOUBLE;
      else if (val instanceof Double[])
        return PostgresType.ARRAY_DOUBLE;
      else if (val instanceof float[])
        return PostgresType.ARRAY_REAL;
      else if (val instanceof Float[])
        return PostgresType.ARRAY_REAL;
      else if (val instanceof boolean[])
        return PostgresType.ARRAY_BOOLEAN;
      else if (val instanceof Boolean[])
        return PostgresType.ARRAY_BOOLEAN;
      else if (val instanceof char[])
        return PostgresType.ARRAY_CHAR;
      else if (val instanceof Character[])
        return PostgresType.ARRAY_CHAR;
      else if (val instanceof String[])
        return PostgresType.ARRAY_TEXT;
      else
        throw new IllegalStateException("Unexpected value: " + val);
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
   * {@link Type} refers to an embedded document type, so the list is advertised as a single json document
   * holding a JSON array; this mirrors the convention used by Type.coerceCollectionOfType. An undeclared
   * ofType stays text[].
   */
  private static PostgresType getArrayTypeForOfType(final String ofType) {
    if (ofType == null || ofType.isBlank())
      return ARRAY_TEXT;

    final Type elementType = Type.getTypeByName(ofType);
    if (elementType == null)
      // Not a scalar: the list holds embedded documents of a schema type.
      return JSON;

    // Every branch must agree with getArrayTypeForElementType, which types a populated list from its first
    // element: a mismatch would make a column's OID depend on whether the list is empty. DECIMAL therefore
    // falls through to ARRAY_TEXT, because a list of BigDecimal has no match there either.
    return switch (elementType) {
      case BOOLEAN -> ARRAY_BOOLEAN;
      case INTEGER, SHORT, BYTE -> ARRAY_INT;
      case LONG -> ARRAY_LONG;
      case FLOAT -> ARRAY_REAL;
      case DOUBLE -> ARRAY_DOUBLE;
      // Nested collections (issue #5365) join maps and embedded documents in being carried as a JSON document:
      // a Postgres array is rectangular and homogeneous, an ArcadeDB nested list is neither.
      case MAP, EMBEDDED, LIST, ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS, ARRAY_OF_LONGS, ARRAY_OF_FLOATS, ARRAY_OF_DOUBLES -> JSON;
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
      case BINARY -> PostgresType.BYTEA;
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
    final byte[] byteaValue = pgType == BYTEA ? byteaValueOf(value) : null;
    if (value == null && pgType.code == BOOLEAN.code) {
      serializedValue = "0";
    } else if (pgType == JSON && value != null && (value instanceof Collection<?> || value.getClass().isArray())) {
      // The column was announced as a json document holding an array (issue #5366): emit a real JSON array
      // instead of a Postgres array literal, so the payload matches the announced OID.
      serializedValue = serializeCollectionAsJson(
          value instanceof Collection<?> collection ? collection : convertPrimitiveArrayToCollection(value));
    } else if (byteaValue != null) {
      // A blob announced as bytea travels as \x<hex> (issue #6411), not as the {1,2,3} array literal the
      // generic byte[] handling below would produce for it.
      serializedValue = toByteaText(byteaValue);
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
    case BYTEA -> {
      // bytea binary format is the raw bytes, with no framing of its own beyond the length prefix.
      final byte[] bytes = byteaValueOf(value);
      if (bytes == null) {
        serializeAsText(pgType, typeBuffer, value);
        return;
      }
      typeBuffer.putInt(bytes.length);
      typeBuffer.putByteArray(bytes);
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
        appendQuoted(sb, binary.getString());
      } else if (element instanceof Collection<?> subCollection) {
        // A nested list is carried as a JSON document (issue #5365): the column is advertised as json[], so the
        // element must be a quoted JSON array. Emitting a nested "{...}" literal instead made the announced OID
        // and the payload disagree, and clients re-parsed the inner braces as a second array dimension.
        appendQuoted(sb, new JSONArray(subCollection).toString());
      } else if (element.getClass().isArray()) {
        appendQuoted(sb, new JSONArray(convertPrimitiveArrayToCollection(element)).toString());
      } else if (element instanceof Result result) {
        appendQuoted(sb, result.toJSON().toString());
      } else if (element instanceof JSONObject json) {
        appendQuoted(sb, json.toString());
      } else if (element instanceof Map<?, ?> map) {
        appendQuoted(sb, new JSONObject((Map<String, ?>) map).toString());
      } else if (element instanceof Record record) {
        appendQuoted(sb, record.toJSON(true).toString());
      } else if (element instanceof EmbeddedDocument embeddedDocument) {
        appendQuoted(sb, embeddedDocument.toJSON(true).toString());
      } else if (element instanceof String str) {
        appendQuoted(sb, str);
      } else {
        sb.append(element == null ? "NULL" : element.toString());
      }
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Appends a value as a double-quoted element of a Postgres array literal. Inside the quotes both the
   * backslash and the double quote must be escaped, in that order (issue #5366): escaping only the quote left a
   * backslash in the data acting as an escape character, so "C:\temp" reached the client as "C:temp" and a
   * nested JSON document could be truncated in the middle.
   */
  private static void appendQuoted(final StringBuilder sb, final String value) {
    sb.append('"').append(value.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
  }

  /**
   * Serializes a collection of documents, maps or nested collections as a single JSON array (issue #5366).
   * The column is advertised as json, so the value is plain JSON: no Postgres array literal, no quoting and no
   * escaping of the elements, which is what made JDBC clients display the documents as escaped text.
   */
  private String serializeCollectionAsJson(final Collection<?> collection) {
    return serializeNestedAsJsonArray(collection).toString();
  }

  /**
   * Converts a single collection element into a value the JSON serializer renders natively. Documents keep
   * their "@type" attribute, temporal values use the Postgres text format, and nested collections recurse.
   */
  @SuppressWarnings("unchecked")
  private Object toJsonElement(final Object element) {
    if (element == null)
      return null;
    else if (element instanceof JSONObject json)
      return json;
    else if (element instanceof JSONArray json)
      return json;
    else if (element instanceof Result result)
      return result.toJSON();
    else if (element instanceof EmbeddedDocument embeddedDocument)
      return embeddedDocument.toJSON(true);
    else if (element instanceof Record record)
      return record.toJSON(true);
    else if (element instanceof Map<?, ?> map)
      return new JSONObject((Map<String, ?>) map);
    else if (element instanceof Collection<?> nested)
      return serializeNestedAsJsonArray(nested);
    else if (element instanceof Binary binary)
      return binary.getString();
    else if (element instanceof Date date)
      return LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC).format(POSTGRES_DATETIME_FORMATTER);
    else if (element instanceof LocalDateTime ldt)
      return ldt.format(POSTGRES_DATETIME_FORMATTER);
    else
      return element.getClass().isArray() ? serializeNestedAsJsonArray(convertPrimitiveArrayToCollection(element)) : element;
  }

  private JSONArray serializeNestedAsJsonArray(final Collection<?> collection) {
    final JSONArray array = new JSONArray();
    for (final Object element : collection)
      array.put(toJsonElement(element));
    return array;
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
    // A list of documents is advertised as a single json value holding a JSON array, not as json[] (issue
    // #5366). A json[] forces every element to travel as a quoted string inside a Postgres array literal, and
    // JDBC-based clients (DBeaver, DataGrip/PhpStorm, DbVisualizer) then show the escaped text instead of the
    // documents. One json document renders as the plain [{...},{...}] the data actually is, everywhere.
    if (element instanceof JSONObject ||
        element instanceof Map ||
        element instanceof Result ||
        element instanceof EmbeddedDocument ||
        element instanceof Record)
      return JSON;
    // A nested collection or array (issue #5365) cannot be announced as a flat array of scalars either: a
    // Postgres array is rectangular and homogeneous while an ArcadeDB nested list can be ragged and mixed.
    if (element instanceof Iterable ||
        element.getClass().isArray())
      return JSON;
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
      case BYTEA -> valueAsBytes.clone();
      case CHAR -> (char) buffer.get();
      case BOOLEAN -> buffer.get() == 1;
      case JSON -> {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield parseJsonText(new String(bytes));
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
    return this == BYTEA ||
        this == SMALLINT ||
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
