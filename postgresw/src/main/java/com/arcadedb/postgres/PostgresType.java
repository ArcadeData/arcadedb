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
import java.util.Date;

public enum PostgresType {
  SMALLINT(21, Short.class, 2, -1), //
  INTEGER(23, Integer.class, 4, -1), //
  LONG(20, Long.class, 8, -1), //
  REAL(700, Float.class, 4, -1), //
  DOUBLE(701, Double.class, 8, -1), //
  CHAR(18, Character.class, 1, -1), //
  BOOLEAN(16, Boolean.class, 1, -1),  //
  DATE(1082, Date.class, 8, -1), //
  VARCHAR(1043, String.class, -1, -1), //
  ;

  public final int      code;
  public final Class<?> cls;
  public final int      size;
  public final int      modifier;

  PostgresType(final int code, final Class<?> cls, final int size, final int modifier) {
    this.code = code;
    this.cls = cls;
    this.size = size;
    this.modifier = modifier;
  }

  public void serialize(final ByteBuffer typeBuffer, final Object value) {
    if (value == null) {
      typeBuffer.putInt(-1);
      return;
    }

    switch (this) {
    case VARCHAR:
      final byte[] str = value.toString().getBytes(DatabaseFactory.getDefaultCharset());
      typeBuffer.putInt(str.length);
      typeBuffer.put(str);
      break;

    case SMALLINT:
      typeBuffer.putInt(Binary.SHORT_SERIALIZED_SIZE);
      typeBuffer.putShort(((Number) value).shortValue());
      break;

    case INTEGER:
      typeBuffer.putInt(Binary.INT_SERIALIZED_SIZE);
      typeBuffer.putInt(((Number) value).intValue());
      break;

    case LONG:
      typeBuffer.putInt(Binary.LONG_SERIALIZED_SIZE);
      typeBuffer.putLong(((Number) value).longValue());
      break;

    case REAL:
      typeBuffer.putInt(Binary.INT_SERIALIZED_SIZE);
      typeBuffer.putFloat(((Number) value).floatValue());
      break;

    case DOUBLE:
      typeBuffer.putInt(Binary.LONG_SERIALIZED_SIZE);
      typeBuffer.putDouble(((Number) value).doubleValue());
      break;

    case DATE:
      typeBuffer.putInt(Binary.LONG_SERIALIZED_SIZE);
      typeBuffer.putLong(((Date) value).getTime());
      break;

    case CHAR:
      typeBuffer.putInt(Binary.BYTE_SERIALIZED_SIZE);
      typeBuffer.put((byte) ((Character) value).charValue());
      break;

    case BOOLEAN:
      typeBuffer.putInt(Binary.BYTE_SERIALIZED_SIZE);
      typeBuffer.put((byte) (((Boolean) value) ? 1 : 0));
      break;
//
//    case ANY:
//      typeBuffer.putInt(Binary.INT_SERIALIZED_SIZE);
//      typeBuffer.putInt(((Number) value).intValue());
//      break;

    default:
      throw new PostgresProtocolException("Type " + this + " not supported for serializing");
    }
  }

  public static Object deserialize(final long code, final int formatCode, final byte[] valueAsBytes) {
    switch (formatCode) {
    case 0:
      final String str = new String(valueAsBytes, DatabaseFactory.getDefaultCharset());
      if (code == 0) // UNSPECIFIED
        return str;
      else if (code == VARCHAR.code)
        return str;
      else if (code == SMALLINT.code)
        return Short.parseShort(str);
      else if (code == INTEGER.code)
        return Integer.parseInt(str);
      else if (code == LONG.code)
        return Long.parseLong(str);
      else if (code == REAL.code)
        return Float.parseFloat(str);
      else if (code == DOUBLE.code)
        return Double.parseDouble(str);
      else if (code == DATE.code)
        return new Date(Long.parseLong(str));
      else if (code == CHAR.code)
        return str.charAt(0);
      else if (code == BOOLEAN.code)
        return str.equalsIgnoreCase("true");
//      else if (code == ANY.code)
//        return Integer.parseInt(str);
      else
        throw new PostgresProtocolException("Type with code " + code + " not supported for deserializing");

    case 1:
      final ByteBuffer typeBuffer = ByteBuffer.wrap(valueAsBytes);

      if (code == VARCHAR.code) {
        final int length = typeBuffer.getInt();
        final byte[] buffer = new byte[length];
        typeBuffer.get(buffer);
        return new String(buffer);
      } else if (code == SMALLINT.code)
        return typeBuffer.getShort();
      else if (code == INTEGER.code)
        return typeBuffer.getInt();
      else if (code == LONG.code)
        return typeBuffer.getLong();
      else if (code == REAL.code)
        return typeBuffer.getFloat();
      else if (code == DOUBLE.code)
        return typeBuffer.getDouble();
      else if (code == DATE.code)
        return new Date(typeBuffer.getLong());
      else if (code == CHAR.code)
        return typeBuffer.getChar();
      else if (code == BOOLEAN.code)
        return typeBuffer.get() == 1;
//      else if (code == ANY.code)
//        return typeBuffer.getInt();
      else
        throw new PostgresProtocolException("Type with code " + code + " not supported for deserializing");

    default:
      throw new PostgresProtocolException("Invalid format code " + formatCode);
    }
  }
}
