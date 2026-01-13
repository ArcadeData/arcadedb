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
import com.arcadedb.serializer.json.JSONObject;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
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
  BOOLEAN(16, Boolean.class, 1, value -> value.equalsIgnoreCase("true")),
  DATE(1082, Date.class, 8, value -> new Date(Long.parseLong(value))),
  VARCHAR(1043, String.class, -1, value -> value),
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
  @SuppressWarnings("unchecked")
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
      if (val instanceof int[]) {
        return PostgresType.ARRAY_INT;
      } else if (val instanceof long[]) {
        return PostgresType.ARRAY_LONG;
      } else if (val instanceof double[]) {
        return PostgresType.ARRAY_DOUBLE;
      } else if (val instanceof float[]) {
        return PostgresType.ARRAY_REAL;
      } else if (val instanceof boolean[]) {
        return PostgresType.ARRAY_BOOLEAN;
      } else if (val instanceof char[]) {
        return PostgresType.ARRAY_CHAR;
      } else if (val instanceof String[]) {
        return PostgresType.ARRAY_TEXT;
      }
    } else if (val instanceof Date) {
      return PostgresType.DATE;
    } else if (val instanceof LocalDateTime) {
      return PostgresType.DATE;
    }

    return PostgresType.VARCHAR;

  }

  /**
   * Serializes a value as text format into the provided Binary buffer.
   *
   * @param pgType     The PostgreSQL type
   * @param typeBuffer The buffer to write to
   * @param value      The value to serialize
   */
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
    } else if (value instanceof JSONObject json) {
      serializedValue = json.toString();
    } else if (value instanceof Map map) {
      serializedValue = new JSONObject(map).toString();
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
        sb.append(date.getTime());
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
      } else if (element instanceof Map map) {
        sb.append("\"").append(new JSONObject(map).toString().replace("\"", "\\\"")).append("\"");
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
      case VARCHAR -> {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield new String(bytes);
      }
      case SMALLINT -> buffer.getShort();
      case INTEGER -> buffer.getInt();
      case LONG -> buffer.getLong();
      case REAL -> buffer.getFloat();
      case DOUBLE -> buffer.getDouble();
      case DATE -> new Date(buffer.getLong());
      case CHAR -> buffer.getChar();
      case BOOLEAN -> buffer.get() == 1;
      case JSON -> {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        yield new JSONObject(new String(bytes));
      }
      case ARRAY_INT, ARRAY_LONG, ARRAY_DOUBLE, ARRAY_REAL, ARRAY_TEXT, ARRAY_BOOLEAN, ARRAY_CHAR, ARRAY_JSON -> {
        // For binary format, would need to implement proper array binary deserialization
        // This is a simplified placeholder - proper implementation would need to handle
        // array dimensions and element deserialization according to PostgreSQL protocol
        throw new PostgresProtocolException("Binary deserialization for arrays not yet implemented");
      }
    };
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
}
