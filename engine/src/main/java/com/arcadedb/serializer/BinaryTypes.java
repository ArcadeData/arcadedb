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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import org.locationtech.spatial4j.shape.Shape;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

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
  public final static byte TYPE_DATETIME_MICROS   = 20; // @SINCE 23.1.1
  public final static byte TYPE_DATETIME_NANOS    = 21; // @SINCE 23.1.1
  public final static byte TYPE_DATETIME_SECOND   = 22; // @SINCE 23.1.1
  public final static byte TYPE_ARRAY_OF_SHORTS   = 23; // @SINCE 23.6.1
  public final static byte TYPE_ARRAY_OF_INTEGERS = 24; // @SINCE 23.6.1
  public final static byte TYPE_ARRAY_OF_LONGS    = 25; // @SINCE 23.6.1
  public final static byte TYPE_ARRAY_OF_FLOATS   = 26; // @SINCE 23.6.1
  public final static byte TYPE_ARRAY_OF_DOUBLES  = 27; // @SINCE 23.6.1
  public final static byte TYPE_COMPRESSED_GEOMETRY = 28; // @SINCE 26.2.1 - Binary geometry storage (Point, Circle, Rectangle, etc.)
  public final static byte TYPE_EXTERNAL                 = 29; // @SINCE 26.5.1 - Property value stored uncompressed in a paired external bucket. Followed by [bucketIdVarint][positionVarint].
  public final static byte TYPE_EXTERNAL_COMPRESSED_FAST = 30; // @SINCE 26.5.1 - Same as TYPE_EXTERNAL but the value bytes in the external blob are LZ4-fast-compressed; the type byte is the dispatcher (no per-blob algo marker needed).
  public final static byte TYPE_EXTERNAL_COMPRESSED_MAX  = 31; // @SINCE 26.5.1 - Same as TYPE_EXTERNAL_COMPRESSED_FAST but compressed with LZ4 HC (high compression, ~10pp smaller output, 8-20x slower compress; decompression is identical to FAST since LZ4 HC uses the same format).

  // Geometry subtypes for TYPE_COMPRESSED_GEOMETRY
  public final static byte GEOMETRY_SUBTYPE_POINT     = 1; // Point: x(double), y(double)
  public final static byte GEOMETRY_SUBTYPE_CIRCLE    = 2; // Circle: x(double), y(double), radius(double)
  public final static byte GEOMETRY_SUBTYPE_RECTANGLE = 3; // Rectangle: minX, minY, maxX, maxY (4 doubles)
  public final static byte GEOMETRY_SUBTYPE_LINESTRING = 4; // LineString: numPoints(int), [x,y]* (n*2 doubles)
  public final static byte GEOMETRY_SUBTYPE_POLYGON   = 5; // Polygon: numPoints(int), [x,y]* (n*2 doubles)

  /**
   * Maps the binary type an index key column DECLARES (what the schema says, what the index reports outward and what
   * a caller's key is narrowed to) to the binary type the index actually writes on its pages.
   * <p>
   * The only remapping is {@link #TYPE_RID} - the encoding of {@code Type.LINK}, and therefore of an edge type's
   * {@code @out}/{@code @in} endpoints - to {@link #TYPE_COMPRESSED_RID}: the fixed {@code putInt(bucketId)} +
   * {@code putLong(position)} form costs 12 bytes per column against the 2-7 of the varint form that indexes already
   * use for every entry <i>value</i>. Both encodings are deterministic and injective, and both deserialize to the
   * same {@code RID}, so neither hashing (HASH, #5677) nor ordered comparison (LSM, #5703) is affected.
   */
  public static byte getIndexStorageType(final byte declaredType) {
    return declaredType == TYPE_RID ? TYPE_COMPRESSED_RID : declaredType;
  }

  /**
   * Inverse of {@link #getIndexStorageType(byte)}, for reading back a storage type an index persisted. The mapping is
   * unambiguous because no {@code Type} declares {@link #TYPE_COMPRESSED_RID}, so that byte can only mean "a LINK
   * column stored compressed".
   */
  public static byte getIndexDeclaredType(final byte storageType) {
    return storageType == TYPE_COMPRESSED_RID ? TYPE_RID : storageType;
  }

  public static byte getTypeFromValue(final Object value, final Property propertyType) {
    final byte type;

    // ORDERED BY THE MOST COMMON FIRST
    if (value == null)
      type = TYPE_NULL;
    else if (value instanceof String)
      type = TYPE_STRING;
    else if (value instanceof Integer)
      type = TYPE_INT;
    else if (value instanceof Long)
      type = TYPE_LONG;
    else if (value instanceof RID)
      type = TYPE_COMPRESSED_RID;
    else if (value instanceof Byte)
      type = TYPE_BYTE;
    else if (value instanceof Short)
      type = TYPE_SHORT;
    else if (value instanceof Float)
      type = TYPE_FLOAT;
    else if (value instanceof Double)
      type = TYPE_DOUBLE;
    else if (value instanceof LocalDateTime time) {
      if (propertyType != null)
        type = propertyType.getType().getBinaryType();
      else
        type = DateUtils.getBestBinaryTypeForPrecision(DateUtils.getPrecision(time.getNano()));
    } else if (value instanceof ZonedDateTime time) {
      if (propertyType != null)
        type = propertyType.getType().getBinaryType();
      else
        type = DateUtils.getBestBinaryTypeForPrecision(DateUtils.getPrecision(time.getNano()));
    } else if (value instanceof OffsetDateTime time) {
      if (propertyType != null)
        type = propertyType.getType().getBinaryType();
      else
        type = DateUtils.getBestBinaryTypeForPrecision(DateUtils.getPrecision(time.getNano()));
    } else if (value instanceof Instant instant) {
      if (propertyType != null)
        type = propertyType.getType().getBinaryType();
      else
        type = DateUtils.getBestBinaryTypeForPrecision(DateUtils.getPrecision(instant.getNano()));
    } else if (value instanceof LocalDate)
      type = TYPE_DATE;
    else if (value instanceof Calendar) // CAN'T DETERMINE IF DATE OR DATETIME, USE DATETIME
      type = TYPE_DATETIME;
    else if (value instanceof Date) // CAN'T DETERMINE IF DATE OR DATETIME, USE DATETIME
      type = TYPE_DATETIME;
    else if (value instanceof BigDecimal)
      type = TYPE_DECIMAL;
    else if (value instanceof Boolean)
      type = TYPE_BOOLEAN;
    else if (value instanceof byte[])
      type = TYPE_BINARY;
    else if (value instanceof UUID)
      type = TYPE_UUID;
    else if (value instanceof Map || value instanceof JSONObject)
      type = TYPE_MAP;
    else if (value instanceof Document document)
      type = document.getIdentity() != null ? TYPE_RID : TYPE_EMBEDDED;
    else if (value instanceof Result result) {
      // COMING FROM A QUERY
      if (result.isElement()) {
        final Document document = result.getElement().get();
        type = document.getIdentity() != null ? TYPE_RID : TYPE_EMBEDDED;
      } else
        // SERIALIZE THE RESULT AS A MAP
        type = TYPE_MAP;
    } else if (value.getClass().isArray()) {
      // DECIDE THE BINARY TYPE FROM THE ARRAY'S DECLARED COMPONENT TYPE, NOT FROM ITS FIRST ELEMENT: SNIFFING THE
      // FIRST ELEMENT MADE AN EMPTY PRIMITIVE ARRAY (WHOSE "FIRST ELEMENT" IS null) FALL BACK TO TYPE_LIST, SO THE
      // SAME PROPERTY'S RUNTIME TYPE DEPENDED ON WHETHER IT HAPPENED TO BE EMPTY (ISSUE #6464). THE COMPONENT TYPE
      // ALSO COVERS A BOXED WRAPPER ARRAY (Short[]/Integer[]/Long[]/Float[]/Double[]) SUPPLIED FOR A DECLARED
      // ARRAY_OF_* PROPERTY, WHICH WAS PREVIOUSLY ALWAYS DOWNGRADED TO TYPE_LIST REGARDLESS OF CONTENT.
      //
      // A WRAPPER ARRAY CAN HOLD A null ELEMENT (A PRIMITIVE ARRAY CANNOT), AND BinarySerializer's
      // TYPE_ARRAY_OF_* BRANCHES UNBOX EACH ELEMENT WITH AN ENHANCED FOR-LOOP, WHICH WOULD THROW AN UNCAUGHT NPE ON
      // A null ENTRY. FALL BACK TO TYPE_LIST WHEN A WRAPPER ARRAY CONTAINS ANY null ELEMENT: TYPE_LIST SERIALIZES
      // EACH ELEMENT INDIVIDUALLY WITH ITS OWN TYPE_NULL TAG, WHICH IS EXACTLY HOW SUCH AN ARRAY WAS HANDLED
      // BEFORE #6464 AND KEEPS THAT CASE BACKWARD COMPATIBLE.
      final Class<?> componentType = value.getClass().getComponentType();
      if (componentType == short.class)
        type = TYPE_ARRAY_OF_SHORTS;
      else if (componentType == Short.class)
        type = arrayHasNullElement((Object[]) value) ? TYPE_LIST : TYPE_ARRAY_OF_SHORTS;
      else if (componentType == int.class)
        type = TYPE_ARRAY_OF_INTEGERS;
      else if (componentType == Integer.class)
        type = arrayHasNullElement((Object[]) value) ? TYPE_LIST : TYPE_ARRAY_OF_INTEGERS;
      else if (componentType == long.class)
        type = TYPE_ARRAY_OF_LONGS;
      else if (componentType == Long.class)
        type = arrayHasNullElement((Object[]) value) ? TYPE_LIST : TYPE_ARRAY_OF_LONGS;
      else if (componentType == float.class)
        type = TYPE_ARRAY_OF_FLOATS;
      else if (componentType == Float.class)
        type = arrayHasNullElement((Object[]) value) ? TYPE_LIST : TYPE_ARRAY_OF_FLOATS;
      else if (componentType == double.class)
        type = TYPE_ARRAY_OF_DOUBLES;
      else if (componentType == Double.class)
        type = arrayHasNullElement((Object[]) value) ? TYPE_LIST : TYPE_ARRAY_OF_DOUBLES;
      else
        type = TYPE_LIST;

    } else if (value instanceof Iterable)
      type = TYPE_LIST;
    else if (isGeoSpatialShape(value))
      type = TYPE_COMPRESSED_GEOMETRY; // Shapes are serialized as binary geometry
    else if (value instanceof Number) {
      // GENERIC NUMBER IMPLEMENTATION. THIS HAPPENS WITH JSON NUMBERS
      byte t;

      try {
        Integer.parseInt(value.toString());
        t = TYPE_INT;
      } catch (NumberFormatException e) {
        try {
          Long.parseLong(value.toString());
          t = TYPE_LONG;
        } catch (NumberFormatException e2) {
          Double.parseDouble(value.toString());
          t = TYPE_DOUBLE;
        }
      }
      type = t;
    } else {
      LogManager.instance()
          .log(BinaryTypes.class, Level.WARNING, "Cannot serialize value '%s' of type %s. The value will be ignored", value,
              value.getClass());
      type = -1; // UNKNOWN TYPE
    }

    return type;
  }

  /**
   * Scans a boxed wrapper array (Short[]/Integer[]/Long[]/Float[]/Double[]) for a null element. Used by
   * {@link #getTypeFromValue(Object, Property)} to decide whether the array is safe to classify as one of the
   * TYPE_ARRAY_OF_* binary types, whose serializer unboxes every element and would NPE on a null one.
   */
  private static boolean arrayHasNullElement(final Object[] array) {
    for (final Object element : array)
      if (element == null)
        return true;
    return false;
  }

  public static int getTypeSize(final byte type) {
    return switch (type) {
      case BinaryTypes.TYPE_INT -> Binary.INT_SERIALIZED_SIZE;
      case BinaryTypes.TYPE_SHORT -> Binary.SHORT_SERIALIZED_SIZE;
      case BinaryTypes.TYPE_LONG, BinaryTypes.TYPE_DATETIME, BinaryTypes.TYPE_DATE -> Binary.LONG_SERIALIZED_SIZE;
      case BinaryTypes.TYPE_BYTE -> Binary.BYTE_SERIALIZED_SIZE;
      case BinaryTypes.TYPE_DECIMAL, BinaryTypes.TYPE_FLOAT -> Binary.FLOAT_SERIALIZED_SIZE;
      case BinaryTypes.TYPE_DOUBLE -> Binary.DOUBLE_SERIALIZED_SIZE;
      case BinaryTypes.TYPE_RID -> Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;
      case BinaryTypes.TYPE_UUID -> Binary.LONG_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;
      // VARIABLE SIZE
      default -> -1;
    };
  }

  public static Class<?> getClassFromType(final byte type) {
    return switch (type) {
      case BinaryTypes.TYPE_STRING, BinaryTypes.TYPE_COMPRESSED_STRING -> String.class;
      case BinaryTypes.TYPE_INT -> Integer.class;
      case BinaryTypes.TYPE_SHORT -> Short.class;
      case BinaryTypes.TYPE_LONG -> Long.class;
      case BinaryTypes.TYPE_BYTE -> Byte.class;
      case BinaryTypes.TYPE_DECIMAL -> BigDecimal.class;
      case BinaryTypes.TYPE_FLOAT -> Float.class;
      case BinaryTypes.TYPE_DOUBLE -> Double.class;
      case BinaryTypes.TYPE_DATETIME,
           BinaryTypes.TYPE_DATETIME_MICROS,
           BinaryTypes.TYPE_DATETIME_NANOS,
           BinaryTypes.TYPE_DATETIME_SECOND -> GlobalConfiguration.DATE_TIME_IMPLEMENTATION.getValue();
      case BinaryTypes.TYPE_DATE -> GlobalConfiguration.DATE_IMPLEMENTATION.getValue();
      case BinaryTypes.TYPE_RID, BinaryTypes.TYPE_UUID -> RID.class;
      case BinaryTypes.TYPE_EMBEDDED -> Document.class;
      case BinaryTypes.TYPE_ARRAY_OF_SHORTS -> short[].class;
      case BinaryTypes.TYPE_ARRAY_OF_INTEGERS -> int[].class;
      case BinaryTypes.TYPE_ARRAY_OF_LONGS -> long[].class;
      case BinaryTypes.TYPE_ARRAY_OF_FLOATS -> float[].class;
      case BinaryTypes.TYPE_ARRAY_OF_DOUBLES -> double[].class;
      case BinaryTypes.TYPE_COMPRESSED_GEOMETRY -> Shape.class;
      case BinaryTypes.TYPE_BINARY -> byte[].class;
      // UNKNOWN
      default -> null;
    };
  }

  /**
   * Checks if the value is a geospatial Shape from spatial4j library.
   * Direct instanceof check (no reflection) - spatial4j is Apache 2.0 licensed.
   */
  public static boolean isGeoSpatialShape(final Object value) {
    return value instanceof Shape;
  }
}
