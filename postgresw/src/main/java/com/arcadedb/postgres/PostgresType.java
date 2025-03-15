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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.serializer.json.JSONFactory;

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
  // Adding array types with PostgreSQL array type codes
  ARRAY_INT(1007, ArrayList.class, -1, value -> parseArrayFromString(value, Integer::parseInt)),
  ARRAY_LONG(1016, ArrayList.class, -1, value -> parseArrayFromString(value, Long::parseLong)),
  ARRAY_DOUBLE(1022, ArrayList.class, -1, value -> parseArrayFromString(value, Double::parseDouble)),
  ARRAY_TEXT(1009, ArrayList.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_BOOLEAN(1000, ArrayList.class, -1, value -> parseArrayFromString(value, Boolean::parseBoolean));

  private static final Map<Integer, PostgresType> CODE_MAP = Arrays.stream(values())
      .collect(Collectors.toMap(type -> type.code, type -> type));

  public final int code;
  public final Class<?> cls;
  public final int size;
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

  /**
   * Serializes a value as text format into the provided Binary buffer.
   * Uses JSONObject for JSON conversion of complex types.
   *
   * @param code       The PostgreSQL type code
   * @param typeBuffer The buffer to write to
   * @param value      The value to serialize
   */
  public void serializeAsText(final long code, final Binary typeBuffer, Object value) {
    if (value == null) {
      if (code == BOOLEAN.code) {
        value = "0";
      } else {
        typeBuffer.putInt(-1);
        return;
      }
    }

    // Handle collections (including arrays)
    if (value instanceof Collection<?>) {
      try {
        // For PostgreSQL arrays, convert using the array format
        String pgArray = convertToPgArray((Collection<?>) value);
        byte[] bytes = pgArray.getBytes(DatabaseFactory.getDefaultCharset());
        typeBuffer.putInt(bytes.length);
        typeBuffer.putByteArray(bytes);
      } catch (Exception e) {
        // Fallback to simple string representation
        String str = value.toString();
        final byte[] bytes = str.getBytes(DatabaseFactory.getDefaultCharset());
        typeBuffer.putInt(bytes.length);
        typeBuffer.putByteArray(bytes);
      }
      return;
    }

    // Standard handling for non-collection types
    final byte[] str = value.toString().getBytes(DatabaseFactory.getDefaultCharset());
    typeBuffer.putInt(str.length);
    typeBuffer.putByteArray(str);
  }

  /**
   * Converts a collection to a PostgreSQL array format using JSONObject.
   */
  private String convertToPgArray(Collection<?> collection) {
    if (collection == null || collection.isEmpty())
      return "{}";
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Object item : collection) {
      if (!first) {
        sb.append(",");
      }
      first = false;

      if (item == null) {
        sb.append("NULL");
      } else if (item instanceof String) {
        // For strings, we need PostgreSQL's quoted string format
        sb.append("\"").append(((String) item).replace("\"", "\\\"")).append("\"");
      } else if (item instanceof Number || item instanceof Boolean) {
        sb.append(item);
      } else {
        try {
          // Complex objects get converted to JSON using JSONFactory
          String json = JSONFactory.INSTANCE.getGson().toJson(item);
          sb.append("\"").append(json.replace("\"", "\\\"")).append("\"");
        } catch (Exception e) {
          // Fallback
          sb.append("\"").append(item.toString().replace("\"", "\\\"")).append("\"");
        }
      }
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Determines the appropriate array type based on the element type.
   */
  public static PostgresType getArrayTypeForElementType(Class<?> elementType) {
    if (elementType == Integer.class || elementType == int.class)
      return ARRAY_INT;
    if (elementType == Long.class || elementType == long.class)
      return ARRAY_LONG;
    if (elementType == Double.class || elementType == double.class ||
        elementType == Float.class || elementType == float.class)
      return ARRAY_DOUBLE;
    if (elementType == Boolean.class || elementType == boolean.class)
      return ARRAY_BOOLEAN;
    // Default to text array for all other types
    return ARRAY_TEXT;
  }

  /**
   * Deserializes a value based on the PostgreSQL type code and format code.
   *
   * @param code         The PostgreSQL type code
   * @param formatCode   The format code (0 for text, 1 for binary)
   * @param valueAsBytes The raw byte array to deserialize
   * @return The deserialized object
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
      case ARRAY_INT, ARRAY_LONG, ARRAY_DOUBLE, ARRAY_TEXT, ARRAY_BOOLEAN -> {
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
    return this == ARRAY_INT || this == ARRAY_LONG || this == ARRAY_DOUBLE
        || this == ARRAY_TEXT || this == ARRAY_BOOLEAN;
  }
}
