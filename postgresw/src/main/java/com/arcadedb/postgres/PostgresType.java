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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
  VARCHAR(1043, String.class, -1, value -> value);

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
   * Serializes a value as text format into the provided Binary buffer.
   *
   * @param code       The PostgreSQL type code
   * @param typeBuffer The buffer to write to
   * @param value      The value to serialize
   */
  public void serializeAsText(final long code,final Binary typeBuffer, Object value) {
    if (value == null) {
      if (code == BOOLEAN.code) {
        value = "0";
      } else {
        typeBuffer.putInt(-1);
        return;
      }
    }

    final byte[] str = value.toString().getBytes(DatabaseFactory.getDefaultCharset());
    typeBuffer.putInt(str.length);
    typeBuffer.putByteArray(str);
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
    };
  }
}
