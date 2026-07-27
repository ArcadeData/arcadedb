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
package com.arcadedb.engine.timeseries;

import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.Type;

/**
 * Defines a column in a TimeSeries type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ColumnDefinition {

  public enum ColumnRole {
    TIMESTAMP, TAG, FIELD
  }

  private final String          name;
  private final Type            dataType;
  private final ColumnRole      role;
  private final TimeSeriesCodec compressionHint;

  public ColumnDefinition(final String name, final Type dataType, final ColumnRole role) {
    this(name, dataType, role, defaultCodecFor(dataType, role));
  }

  public ColumnDefinition(final String name, final Type dataType, final ColumnRole role, final TimeSeriesCodec compressionHint) {
    this.name = name;
    this.dataType = dataType;
    this.role = role;
    this.compressionHint = compressionHint;
  }

  public String getName() {
    return name;
  }

  public Type getDataType() {
    return dataType;
  }

  public ColumnRole getRole() {
    return role;
  }

  public TimeSeriesCodec getCompressionHint() {
    return compressionHint;
  }

  /**
   * Returns the fixed byte size for this column's data type in the mutable row format, or -1 for a
   * variable-length column (STRING, and any type not storable in a fixed-stride row).
   * <p>
   * This is the single width table of the row format: the writer must advance the row cursor by
   * exactly this many bytes, and {@code calculateRowSize} reserves exactly this many. A type missing
   * from it and yet written as a fixed-width value overruns its neighbours in the row - the defect
   * behind issue #5475.
   */
  public int getFixedSize() {
    return fixedSizeOf(dataType);
  }

  /**
   * Whether this data type can be stored in a TimeSeries row.
   * <p>
   * A TimeSeries row is a fixed-stride record and a sealed block column is one of three primitive
   * codecs, so a type needs either a fixed width or a bounded text form. Containers, RIDs, binaries
   * and arbitrary-precision decimals have neither; declaring one used to be accepted and then
   * corrupted the row, so {@code CREATE TIMESERIES TYPE} now refuses it (issue #5475).
   */
  public static boolean isStorableType(final Type type) {
    return type == Type.STRING || fixedSizeOf(type) > 0;
  }

  /**
   * The data types a TimeSeries column may declare, for error messages.
   */
  public static String storableTypeNames() {
    return "BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, STRING, DATE, DATETIME, "
        + "DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS";
  }

  /**
   * Converts the raw bits carried by {@link TimeSeriesRowSource#getRawValue(int, int)} back into the
   * Java type this column declares.
   * <p>
   * Both storage layers box through this method, so the type a query returns cannot depend on whether
   * the sample has been compacted yet.
   */
  public Object boxRaw(final long raw) {
    return switch (dataType) {
      case DOUBLE -> Double.longBitsToDouble(raw);
      case FLOAT -> Float.intBitsToFloat((int) raw);
      case LONG, DATETIME, DATE, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> raw;
      case INTEGER -> (int) raw;
      case SHORT -> (short) raw;
      case BYTE -> (byte) raw;
      case BOOLEAN -> raw != 0;
      default -> throw new IllegalStateException(
          "Column '" + name + "' of type " + dataType + " is not stored as a fixed-width value");
    };
  }

  /**
   * Boxes a value decoded from a {@code GORILLA_XOR} sealed column into this column's declared type.
   */
  public Object boxDouble(final double value) {
    return dataType == Type.FLOAT ? (Object) (float) value : (Object) value;
  }

  /**
   * Boxes a value decoded from a {@code DICTIONARY} sealed column into this column's declared type.
   * <p>
   * Dictionary columns are stored as text, so a non-STRING column sealed with this codec (a datetime
   * or boolean tag, and any pre-#5475 field that defaulted to it) has to be parsed back. An
   * unparseable entry is handed back as the raw text rather than failing the scan.
   */
  public Object boxString(final String value) {
    if (dataType == Type.STRING || value == null)
      return value;
    if (dataType == Type.BOOLEAN)
      return Boolean.parseBoolean(value);
    if (value.isEmpty())
      return null;
    try {
      return switch (dataType) {
        case DOUBLE -> Double.valueOf(value);
        case FLOAT -> Float.valueOf(value);
        case LONG, DATETIME, DATE, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> Long.valueOf(value);
        case INTEGER -> Integer.valueOf(value);
        case SHORT -> Short.valueOf(value);
        case BYTE -> Byte.valueOf(value);
        default -> value;
      };
    } catch (final NumberFormatException e) {
      return value;
    }
  }

  /**
   * Coerces a filter literal supplied by a query - a SQL literal, a JSON tag value, a PromQL matcher -
   * into this column's declared type, so a tag filter compares like with like against what the storage
   * layers hand back.
   * <p>
   * Before issue #5475 the SQL planner stringified every literal, which happened to match because the
   * sealed layer also handed back the dictionary text; with the declared type restored on both layers,
   * the literal has to be coerced instead.
   */
  public Object coerceValue(final Object value) {
    if (value == null)
      return null;
    if (dataType == Type.STRING)
      return value instanceof String s ? s : value.toString();
    if (dataType == Type.BOOLEAN)
      return value instanceof Boolean b ? b : Boolean.parseBoolean(value.toString());
    if (value instanceof Number n)
      return switch (dataType) {
        case DOUBLE -> n.doubleValue();
        case FLOAT -> n.floatValue();
        case INTEGER -> n.intValue();
        case SHORT -> n.shortValue();
        case BYTE -> n.byteValue();
        case LONG, DATETIME, DATE, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> n.longValue();
        default -> value;
      };
    return boxString(value.toString());
  }

  /**
   * Integer view of a stored value, for {@code SIMPLE8B} encoding.
   * <p>
   * {@code BOOLEAN} is an integer column since issue #5475 and {@link Boolean} is not a
   * {@link Number}, so every site that packs a column into longs has to go through here.
   */
  public static long integerValueOf(final Object value) {
    if (value == null)
      return 0L;
    if (value instanceof Boolean b)
      return b ? 1L : 0L;
    return ((Number) value).longValue();
  }

  /**
   * Numeric view of a stored value, for the min/max/sum block statistics used by aggregation
   * push-down. {@code null} counts as zero, matching what the column stores for it.
   */
  public static double numericValueOf(final Object value) {
    if (value == null)
      return 0.0;
    if (value instanceof Boolean b)
      return b ? 1.0 : 0.0;
    return ((Number) value).doubleValue();
  }

  @Override
  public String toString() {
    return name + " " + dataType + " (" + role + ")";
  }

  private static int fixedSizeOf(final Type type) {
    return switch (type) {
      case LONG, DATETIME, DATE, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS, DOUBLE -> 8;
      case INTEGER, FLOAT -> 4;
      case SHORT -> 2;
      case BYTE, BOOLEAN -> 1;
      default -> -1;
    };
  }

  private static TimeSeriesCodec defaultCodecFor(final Type dataType, final ColumnRole role) {
    if (role == ColumnRole.TIMESTAMP)
      return TimeSeriesCodec.DELTA_OF_DELTA;
    if (role == ColumnRole.TAG)
      return TimeSeriesCodec.DICTIONARY;
    return switch (dataType) {
      case DOUBLE, FLOAT -> TimeSeriesCodec.GORILLA_XOR;
      // Datetimes and booleans are integers, not text: a dictionary would write every instant out as
      // a string and split the block every MAX_DICTIONARY_SIZE distinct values (issue #5475).
      case LONG, INTEGER, SHORT, BYTE, BOOLEAN, DATETIME, DATE, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS ->
          TimeSeriesCodec.SIMPLE8B;
      default -> TimeSeriesCodec.DICTIONARY;
    };
  }

  /**
   * The codec table as it was before issue #5475, used for TimeSeries types whose schema predates the
   * change.
   * <p>
   * The codec is not recorded in a sealed block: it is resolved from the schema when the store is
   * opened, so the table that produced a block's bytes must be the table used to decode them.
   * {@code LocalTimeSeriesType.fromJSON} now persists the codec per column and falls back to this
   * table when the entry is absent, which is what keeps blocks written by an older build readable.
   */
  public static TimeSeriesCodec legacyCodecFor(final Type dataType, final ColumnRole role) {
    if (role == ColumnRole.TIMESTAMP)
      return TimeSeriesCodec.DELTA_OF_DELTA;
    if (role == ColumnRole.TAG)
      return TimeSeriesCodec.DICTIONARY;
    return switch (dataType) {
      case DOUBLE, FLOAT -> TimeSeriesCodec.GORILLA_XOR;
      case LONG, INTEGER, SHORT, BYTE -> TimeSeriesCodec.SIMPLE8B;
      default -> TimeSeriesCodec.DICTIONARY;
    };
  }
}
