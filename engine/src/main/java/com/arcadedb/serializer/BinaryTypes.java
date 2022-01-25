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
package com.arcadedb.serializer;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class BinaryTypes {
  public final static byte TYPE_NULL              = 0;
  public final static byte TYPE_STRING            = 1;
  public final static byte TYPE_BYTE              = 2;
  public final static byte TYPE_SHORT             = 3;
  public final static byte TYPE_INT               = 4;
  public final static byte TYPE_LONG              = 5;
  public final static byte TYPE_FLOAT             = 6;
  public final static byte TYPE_DOUBLE            = 7;
  public final static byte TYPE_DATE              = 8;
  public final static byte TYPE_DATETIME          = 9;
  public final static byte TYPE_DECIMAL           = 10;
  public final static byte TYPE_BOOLEAN           = 11;
  public final static byte TYPE_BINARY            = 12;
  public final static byte TYPE_COMPRESSED_RID    = 13;
  public final static byte TYPE_RID               = 14;
  public final static byte TYPE_UUID              = 15;
  public final static byte TYPE_LIST              = 16;
  public final static byte TYPE_MAP               = 17;
  public final static byte TYPE_COMPRESSED_STRING = 18;
  public final static byte TYPE_EMBEDDED          = 19;

  public static byte getTypeFromValue(final Object value) {
    final byte type;

    if (value == null)
      type = TYPE_NULL;
    else if (value instanceof String)
      type = TYPE_STRING;
    else if (value instanceof Byte)
      type = TYPE_BYTE;
    else if (value instanceof Short)
      type = TYPE_SHORT;
    else if (value instanceof Integer)
      type = TYPE_INT;
    else if (value instanceof Long)
      type = TYPE_LONG;
    else if (value instanceof Float)
      type = TYPE_FLOAT;
    else if (value instanceof Double)
      type = TYPE_DOUBLE;
    else if (value instanceof Date) // CAN'T DETERMINE IF DATE OR DATETIME, USE DATETIME
      type = TYPE_DATETIME;
    else if (value instanceof BigDecimal)
      type = TYPE_DECIMAL;
    else if (value instanceof Boolean)
      type = TYPE_BOOLEAN;
    else if (value instanceof byte[])
      type = TYPE_BINARY;
    else if (value instanceof RID)
      type = TYPE_COMPRESSED_RID;
    else if (value instanceof UUID)
      type = TYPE_UUID;
    else if (value instanceof Map)
      type = TYPE_MAP;
    else if (value instanceof Document)
      type = ((Document) value).getIdentity() != null ? TYPE_RID : TYPE_EMBEDDED;
    else if (value instanceof Result) {
      // COMING FROM A QUERY
      if (((Result) value).isElement()) {
        final Document document = ((Result) value).getElement().get();
        type = document.getIdentity() != null ? TYPE_RID : TYPE_EMBEDDED;
      } else
        // SERIALIZE THE RESULT AS A MAP
        type = TYPE_MAP;
    } else if (value instanceof Iterable || value.getClass().isArray())
      // TODO: SUPPORT SET SEMANTIC TOO
      type = TYPE_LIST;
    else
      throw new IllegalArgumentException("Cannot serialize value '" + value + "' of type " + value.getClass());

    return type;
  }

  public static int getTypeSize(final byte type) {
    switch (type) {
    case BinaryTypes.TYPE_INT:
      return Binary.INT_SERIALIZED_SIZE;

    case BinaryTypes.TYPE_SHORT:
      return Binary.SHORT_SERIALIZED_SIZE;

    case BinaryTypes.TYPE_LONG:
    case BinaryTypes.TYPE_DATETIME:
    case BinaryTypes.TYPE_DATE:
      return Binary.LONG_SERIALIZED_SIZE;

    case BinaryTypes.TYPE_BYTE:
      return Binary.BYTE_SERIALIZED_SIZE;

    case BinaryTypes.TYPE_DECIMAL:

    case BinaryTypes.TYPE_FLOAT:
      return Binary.FLOAT_SERIALIZED_SIZE;

    case BinaryTypes.TYPE_DOUBLE:
      return Binary.DOUBLE_SERIALIZED_SIZE;

    case BinaryTypes.TYPE_RID:
      return Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

    case BinaryTypes.TYPE_UUID:
      return Binary.LONG_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;

    default:
      // VARIABLE SIZE
      return -1;
    }
  }

  public static byte getTypeFromClass(final Class typez) {
    final byte type;

    if (typez == String.class)
      type = TYPE_STRING;
    else if (typez == Byte.class)
      type = TYPE_BYTE;
    else if (typez == Short.class)
      type = TYPE_SHORT;
    else if (typez == Integer.class)
      type = TYPE_INT;
    else if (typez == Long.class)
      type = TYPE_LONG;
    else if (typez == Float.class)
      type = TYPE_FLOAT;
    else if (typez == Double.class)
      type = TYPE_DOUBLE;
    else if (typez == Date.class) // CAN'T DETERMINE IF DATE OR DATETIME, USE DATETIME
      type = TYPE_DATETIME;
    else if (typez == BigDecimal.class)
      type = TYPE_DECIMAL;
    else if (typez == Boolean.class)
      type = TYPE_BOOLEAN;
    else if (typez == byte[].class)
      type = TYPE_BINARY;
    else if (typez == RID.class)
      type = TYPE_COMPRESSED_RID;
    else if (typez == UUID.class)
      type = TYPE_UUID;
    else if (Collection.class.isAssignableFrom(typez) || typez.isArray())
      // TODO: SUPPORT SET SEMANTIC TOO
      type = TYPE_LIST;
    else if (Map.class.isAssignableFrom(typez))
      type = TYPE_MAP;
    else if (Document.class.isAssignableFrom(typez))
      type = TYPE_EMBEDDED;
    else
      throw new DatabaseMetadataException("Cannot find type for class '" + typez + "'");

    return type;
  }

  public static byte getType(final Type inputType) {
    return inputType.getBinaryType();
  }

  public static Class<?> getClassFromType(final byte type) {
    switch (type) {
    case BinaryTypes.TYPE_STRING:
    case BinaryTypes.TYPE_COMPRESSED_STRING:
      return String.class;

    case BinaryTypes.TYPE_INT:
      return Integer.class;

    case BinaryTypes.TYPE_SHORT:
      return Short.class;

    case BinaryTypes.TYPE_LONG:
      return Long.class;

    case BinaryTypes.TYPE_BYTE:
      return Byte.class;

    case BinaryTypes.TYPE_DECIMAL:
      return BigDecimal.class;

    case BinaryTypes.TYPE_FLOAT:
      return Float.class;

    case BinaryTypes.TYPE_DOUBLE:
      return Double.class;

    case BinaryTypes.TYPE_DATETIME:
    case BinaryTypes.TYPE_DATE:
      return Date.class;

    case BinaryTypes.TYPE_RID:
    case BinaryTypes.TYPE_UUID:
      return RID.class;

    case BinaryTypes.TYPE_EMBEDDED:
      return Document.class;

    default:
      // UNKNOWN
      return null;
    }
  }
}
