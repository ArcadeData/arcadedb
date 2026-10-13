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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;

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
  private final boolean         explicitCodec;
  // The kind of the storage row this column's type is encoded as, resolved once: storableValue runs for every value of every sample
  private final byte            kind;

  /**
   * A column whose codec is DERIVED from its data type and role, through {@link #defaultCodecFor}.
   * <p>
   * This is what {@code withTimestamp}/{@code withTag}/{@code withField} build, and what a {@code CREATE
   * TIMESERIES TYPE} column with no {@code CODEC} clause parses to. A renderer must NOT name the resolved codec
   * back, or it would freeze today's default table into the statement - see {@link #isExplicitCodec()}.
   */
  public ColumnDefinition(final String name, final Type dataType, final ColumnRole role) {
    this.name = name;
    this.dataType = dataType;
    this.role = role;
    this.compressionHint = defaultCodecFor(dataType, role);
    this.explicitCodec = false;
    this.kind = TimeSeriesBatch.kindOf(dataType);
  }

  /**
   * A column whose codec was NAMED by the caller: a {@code CODEC} clause, a {@code "compression"} entry in an
   * export or in {@code schema.json}, or an application handing a {@link ColumnDefinition} to
   * {@code withColumn}. The codec travels with the column from here on, default table or not.
   * <p>
   * {@code null} is read as "none named" rather than stored: the codec is resolved from the default table and the
   * column is indistinguishable from the 3-argument one. Nothing in {@code src/main} passes it, and the
   * alternative is a column whose codec is {@code null} on every read path that asks for it.
   */
  public ColumnDefinition(final String name, final Type dataType, final ColumnRole role, final TimeSeriesCodec compressionHint) {
    this.name = name;
    this.dataType = dataType;
    this.role = role;
    this.compressionHint = compressionHint != null ? compressionHint : defaultCodecFor(dataType, role);
    this.explicitCodec = compressionHint != null;
    this.kind = TimeSeriesBatch.kindOf(dataType);
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
   * Whether this column's codec was NAMED by whoever built it, rather than derived from the default table
   * (issue #7703).
   * <p>
   * The codec is always populated, so before this flag existed the only way to ask was "does it differ from
   * {@link #defaultCodecFor}?" - which answers NO for a column that named the very codec the table happens to
   * return today. A renderer that omits the clause on that answer emits DDL the receiving build re-derives, and
   * a build whose default table has since moved resolves a DIFFERENT codec: the restored type is not the
   * exported one, and nothing says so. That is exactly the failure issue #5475 fixed, re-entered through the
   * DDL door, and {@link #legacyCodecFor} exists because the table has already changed once.
   * <p>
   * The distinction is a property of the COLUMN rather than a mode on the renderer, so it survives every hop a
   * column definition makes - export to import, {@code schema.json} to builder, builder to SQL - without a
   * caller having to remember to turn it on.
   */
  public boolean isExplicitCodec() {
    return explicitCodec;
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
   * Whether an aggregate other than COUNT can read this column as a number on BOTH storage layers (issue #7725).
   * <p>
   * Asked of the CODEC rather than of the data type, because the codec is exactly what decides the answer: the
   * sealed layer reads an aggregation column through {@code decompressDoubleColumnFromBytes}, which decodes
   * {@code GORILLA_XOR} and {@code SIMPLE8B} and throws on everything else. So a {@code STRING} field, a TAG of
   * any declared type (a tag is always {@code DICTIONARY}, even a {@code LONG} one) and the timestamp column
   * itself are all unreadable there, whatever their {@link Type} suggests.
   * <p>
   * Before this existed the two layers disagreed about such a column instead of refusing it: the mutable path
   * answered a real {@code 0.0} per sample and the sealed path threw, so the same request answered 200 with
   * zeros until compaction ran and 500 afterwards.
   */
  public boolean isNumericallyAggregatable() {
    return switch (getCompressionHint()) {
      case GORILLA_XOR, SIMPLE8B -> true;
      default -> false;
    };
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
    return coerceValue(null, value);
  }

  /**
   * {@link #coerceValue(Object)}, reading a date or an instant given as text through {@code database}, as an {@code INSERT} reads
   * it, so a literal finds the row an {@code INSERT} with the same literal wrote (issue #9833). With {@code null} the text is read
   * with the defaults into a {@code java.util.Date}, in the JVM's default time zone, which names another instant whenever that
   * zone is not UTC.
   */
  public Object coerceValue(final Database database, final Object value) {
    if (value == null)
      return null;
    if (dataType == Type.STRING)
      return value instanceof String s ? s : value.toString();
    if (dataType == Type.BOOLEAN)
      return value instanceof Boolean b ? b : Boolean.parseBoolean(value.toString());
    // A date or an instant in any form the engine reads - a temporal object, or text that is not a bare number - names the
    // epoch count this column stores for it (issue #9724). It used to reach Long.valueOf, fail, and come back unchanged, so
    // a tag filter compared a String or a LocalDateTime against the stored Long and matched nothing. A NUMBER is already a
    // count in the column's unit at this layer, the unit the engine API, the line protocol and every HTTP surface speak.
    if (isTemporal() && !(value instanceof Number) && !(value instanceof String text && isIntegerText(text)))
      try {
        return epochOf(database, value);
      } catch (final IllegalArgumentException e) {
        return value;
      }
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
   * Whether this column holds a {@code DATE} or a {@code DATETIME} of some precision, stored as an epoch count in its own unit:
   * days for a DATE, the precision the type names for the others.
   */
  public boolean isTemporal() {
    return switch (dataType) {
      case DATE, DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> true;
      default -> false;
    };
  }

  /**
   * The epoch count this DATE/DATETIME column stores for a value given at the SQL boundary, read exactly as a document property
   * of the same declared type reads it (issues #9643, #9724, #9725): the value is converted to the Java type that property holds
   * and then encoded the way the binary serializer encodes it. A number is therefore epoch MILLIS for every DATETIME precision and
   * epoch DAYS for a DATE (issue #9324), text is parsed by the same chain, and an instant the precision cannot represent is
   * refused instead of saturated.
   * <p>
   * {@code INSERT}, the tag push-down and the field filter all go through here, so the literal that wrote a row is the literal
   * that finds it.
   *
   * @param database may be {@code null}, which reads text with the default formats and a {@code java.util.Date} for a DATETIME
   *
   * @throws IllegalArgumentException when the value is not a date or an instant, or is outside the range the column can store
   */
  public long epochOf(final Database database, final Object value) {
    if (!isTemporal())
      throw new IllegalArgumentException("Column '" + name + "' of type " + dataType + " is not a date or a datetime");
    final Object converted = Type.convert(database, value, database != null ? dataType.getJavaImplementation(database) :
        dataType.getDefaultJavaType());
    if (converted == null)
      throw new IllegalArgumentException("Cannot convert a value of type " + value.getClass().getName() + " to the " + dataType
          + " column '" + name + "'");
    final Long epoch = dataType == Type.DATE ? DateUtils.dateToEpochDays(converted)
        : DateUtils.dateTimeToTimestampExact(database, converted, DateUtils.getPrecisionFromType(dataType));
    if (epoch == null)
      throw new IllegalArgumentException("Cannot convert a value of type " + value.getClass().getName() + " to the " + dataType
          + " column '" + name + "'");
    return epoch;
  }

  /**
   * The value the append path can encode for this column, for a writer that hands over what it parsed without converting it: the
   * line protocol and gRPC give a quoted datetime as text, a typed tag as text, whatever the client sent (issue #9809). The storage
   * layer casts a value to the {@code Number} (or {@code String}) its column kind names, so anything else used to surface as a bare
   * {@code ClassCastException} from the middle of the append.
   * <ul>
   * <li>a DATE/DATETIME column keeps a number as it is, a count in the column's unit, the contract of every writer that is not SQL; text
   * of digits is that same count, the rule {@link #coerceValue} applies to a tag filter, and any other date or instant is read by
   * {@link #epochOf}, the conversion {@code INSERT} uses, so the same text names the same instant on every surface</li>
   * <li>any other numeric column keeps a number that its type can hold (a {@code 300} is refused by a BYTE column, where the append
   * would store {@code 44}) and parses text, as {@code INSERT} does</li>
   * <li>a BOOLEAN column keeps a boolean and parses {@code true} / {@code false} (any case), so a typed tag sent as {@code "true"} is
   * not stored as {@code false}; any other text, and a number, is refused rather than stored as {@code false}</li>
   * <li>a STRING column keeps a string of at most {@link TimeSeriesBucket#MAX_STRING_BYTES} UTF-8 bytes, the limit the row refuses
   * a longer one at, after the rest of the batch is assembled</li>
   * </ul>
   *
   * @param database may be {@code null}, which reads text with the default formats (see {@link #epochOf})
   *
   * @throws IllegalArgumentException naming this column, when the value cannot be stored in it
   */
  public Object storableValue(final Database database, final Object value) {
    if (value == null)
      return null;
    // The common case, a number into a numeric column (every kind up to BYTE, the datetimes among them), answers before any dispatch
    if (kind <= TimeSeriesBatch.KIND_BYTE && value instanceof Number number) {
      if (fits(number))
        return value;
      throw new IllegalArgumentException("Cannot convert a value of type " + value.getClass().getSimpleName() + " to the " + dataType
          + " column '" + name + "': the number is out of range");
    }
    return switch (kind) {
      case TimeSeriesBatch.KIND_BOOLEAN -> booleanOf(value);
      case TimeSeriesBatch.KIND_STRING -> {
        if (value instanceof String text)
          yield withinStringLimit(text);
        throw unstorable(value, null);
      }
      case TimeSeriesBatch.KIND_TEXT -> withinStringLimit(value instanceof String text ? text : value.toString());
      default -> {
        if (value instanceof Number)
          yield value;
        if (!(value instanceof String) && !isTemporal())
          throw unstorable(value, null);
        try {
          yield isTemporal() ? temporalCount(database, value) : parseNumericText((String) value);
        } catch (final RuntimeException e) {
          throw unstorable(value, e);
        }
      }
    };
  }

  /**
   * Whether the number is one the column's type can hold, for the kinds up to BYTE (the numeric ones). A floating point value for an
   * integral column is range-checked as a double, so NaN and the infinities are refused too. Only the RANGE is checked: a fractional
   * value in range keeps its integral part (1.5 is stored as 1 in an INTEGER column), as the append has always done.
   */
  private boolean fits(final Number number) {
    return switch (kind) {
      case TimeSeriesBatch.KIND_INTEGER -> within(number, Integer.MIN_VALUE, Integer.MAX_VALUE);
      case TimeSeriesBatch.KIND_SHORT -> within(number, Short.MIN_VALUE, Short.MAX_VALUE);
      case TimeSeriesBatch.KIND_BYTE -> within(number, Byte.MIN_VALUE, Byte.MAX_VALUE);
      case TimeSeriesBatch.KIND_LONG -> fitsLong(number);
      case TimeSeriesBatch.KIND_FLOAT -> floatingFits(number, Float.MAX_VALUE);
      default -> floatingFits(number, Double.MAX_VALUE); // DOUBLE
    };
  }

  /**
   * Whether a floating point column holds the number. NaN and the infinities are values of such a column (a sensor reading can be
   * NaN, and the engine keeps it), so a Java floating point number is refused only when it is finite and larger than the column's
   * type, which would otherwise be stored as an infinity nobody sent. An arbitrary-precision number must convert to a finite one.
   */
  private static boolean floatingFits(final Number number, final double max) {
    if (number instanceof BigDecimal || number instanceof BigInteger) {
      final double value = number.doubleValue();
      return Double.isFinite(value) && Math.abs(value) <= max;
    }
    if (max == Double.MAX_VALUE)
      return true; // A Double, a Float or a Java integer always fits a DOUBLE
    final double value = number.doubleValue();
    return !Double.isFinite(value) || Math.abs(value) <= max;
  }

  /**
   * Whether the number is one a LONG holds. A Java integer always is; an arbitrary-precision one is asked exactly, because its
   * {@code doubleValue()} rounds a value just past the bound back onto it; anything else is a floating point value, whose integral
   * part must be in range (NaN and the infinities are not).
   */
  private static boolean fitsLong(final Number number) {
    if (isWholeJavaInteger(number))
      return true;
    if (number instanceof BigInteger integer)
      return integer.bitLength() < 64;
    if (number instanceof BigDecimal decimal)
      return decimal.toBigInteger().bitLength() < 64;
    final double value = number.doubleValue();
    return value >= -0x1p63 && value < 0x1p63;
  }

  private static boolean isWholeJavaInteger(final Number number) {
    return number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte;
  }

  private static boolean within(final Number number, final long min, final long max) {
    if (isWholeJavaInteger(number)) {
      final long value = number.longValue();
      return value >= min && value <= max;
    }
    // The bounds of the narrow kinds are far below 2^53, so a double (even of a big number) compares exactly
    final double value = number.doubleValue();
    return value >= min && value <= max;
  }

  private String withinStringLimit(final String text) {
    // At most 3 bytes per char, so a short text needs no count
    if (text.length() * 3 > TimeSeriesBucket.MAX_STRING_BYTES) {
      final int bytes = utf8Length(text);
      if (bytes > TimeSeriesBucket.MAX_STRING_BYTES)
        throw new IllegalArgumentException("Cannot store a text of " + bytes + " UTF-8 bytes in the " + dataType + " column '" + name
            + "': the limit is " + TimeSeriesBucket.MAX_STRING_BYTES + " bytes, so the text is longer than a row can hold");
    }
    return text;
  }

  private static int utf8Length(final String text) {
    int bytes = 0;
    for (int i = 0; i < text.length(); i++) {
      final char c = text.charAt(i);
      if (c < 0x80)
        bytes += 1;
      else if (c < 0x800)
        bytes += 2;
      else if (Character.isHighSurrogate(c) && i + 1 < text.length() && Character.isLowSurrogate(text.charAt(i + 1))) {
        bytes += 4;
        ++i;
      } else
        bytes += 3;
    }
    return bytes;
  }

  private Boolean booleanOf(final Object value) {
    if (value instanceof Boolean flag)
      return flag;
    if (value instanceof String text) {
      if ("true".equalsIgnoreCase(text))
        return Boolean.TRUE;
      if ("false".equalsIgnoreCase(text))
        return Boolean.FALSE;
    }
    throw unstorable(value, null);
  }

  private Long temporalCount(final Database database, final Object value) {
    return value instanceof String text && isIntegerText(text) ? Long.parseLong(text) : epochOf(database, value);
  }

  private Object parseNumericText(final String text) {
    return switch (dataType) {
      case DOUBLE -> Double.parseDouble(text);
      case FLOAT -> Float.parseFloat(text);
      case INTEGER -> Integer.parseInt(text);
      case SHORT -> Short.parseShort(text);
      case BYTE -> Byte.parseByte(text);
      default -> Long.parseLong(text);
    };
  }

  /**
   * The refusal of a value this column cannot store: it names the column and the type, which the {@code ClassCastException} it
   * replaces never did.
   */
  IllegalArgumentException unstorable(final Object value, final Throwable cause) {
    // The simple name: this text goes back to the client, which has no use for a package
    return new IllegalArgumentException("Cannot convert a value of type " + value.getClass().getSimpleName() + " to the " + dataType
        + " column '" + name + "'", cause);
  }

  /**
   * The object a document property of this DATE/DATETIME type hands back for the epoch count the column stores, which is what
   * the SQL boundary returns for it (issue #9725): the same instant, in the same Java class, as the twin document property, so a
   * value read from a row can be written back as it is and compared against the literal that wrote it.
   * <p>
   * A count no date can stand for - which only the engine API, the line protocol or gRPC can have written, since they store the
   * number they are given - is handed back as that number rather than failing the read of every row around it.
   */
  public Object temporalOf(final Database database, final long epoch) {
    try {
      return ((DatabaseInternal) database).getSerializer().deserializeTemporal(database, epoch, dataType.getBinaryType());
    } catch (final DateTimeException | ArithmeticException e) {
      return epoch;
    }
  }

  private static boolean isIntegerText(final String text) {
    final int length = text.length();
    if (length == 0)
      return false;
    // ASCII digits only: the count Long.valueOf reads, never a digit of another script
    final int start = text.charAt(0) == '-' || text.charAt(0) == '+' ? 1 : 0;
    if (start == length)
      return false;
    for (int i = start; i < length; i++) {
      final char c = text.charAt(i);
      if (c < '0' || c > '9')
        return false;
    }
    return true;
  }

  /**
   * Whether this column stores its values as exact integers ({@code SIMPLE8B}): the integral types, BOOLEAN and the
   * datetimes. Their block statistics and their aggregates are kept as longs (issue #9720), so a value past 2^53 is answered
   * exactly rather than through a double.
   */
  public boolean storesExactIntegers() {
    return compressionHint == TimeSeriesCodec.SIMPLE8B;
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
   * Numeric view of a value as THIS column stores it, for the {@code GORILLA_XOR} encoder and for the
   * min/max/sum block statistics the aggregation push-down answers from (issue #7743).
   * <p>
   * The one thing it adds over {@link #numericValueOf(Object)} is the null, which is {@link TimeSeriesNaN#ABSENT} on every
   * column: a floating-point column stores it as the NaN itself, and an integral one as a zero its block's null bitmap marks
   * absent (issue #9721). Spelled here rather than at each call site, because the statistics describe the samples and would
   * otherwise declare a minimum of zero for a block whose only zero is a missing measurement.
   *
   * @see TimeSeriesBatch#rawNull(byte) the same rule on the mutable side
   */
  public double storedNumericValueOf(final Object value) {
    // An integral column carries absence in the null bitmap of its block since issue #9721, so a null is absent on every
    // numeric column: the statistics describe the samples that have a value
    if (value == null)
      return TimeSeriesNaN.ABSENT;
    return numericValueOf(value);
  }

  /**
   * Whether "no measurement" has a representation in this column's stored form: a floating-point column has
   * NaN, and nothing else has anything (issue #7743).
   */
  public boolean storesAbsentMarker() {
    return dataType == Type.DOUBLE || dataType == Type.FLOAT;
  }

  /**
   * Numeric view of a stored value, for the min/max/sum block statistics used by aggregation
   * push-down. {@code null} counts as zero, which is what a column with no absent marker stores for it;
   * a floating-point column goes through {@link #storedNumericValueOf(Object)} instead.
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

  /**
   * The codec a column of this data type and role gets when none is named. Public because a caller that has to
   * decide whether a {@link ColumnDefinition} carries an EXPLICIT codec has no other way to ask: the codec is always
   * populated, so "explicit" is only visible as "differs from this" (issue #7399).
   */
  public static TimeSeriesCodec defaultCodecFor(final Type dataType, final ColumnRole role) {
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
   * Why this column's codec cannot be honoured by the storage layer, or {@code null} when it can (issue #9310).
   * <p>
   * The codec is first consulted when a shard seals, and {@code TimeSeriesSealedStore.compressColumn} only knows
   * three encoders, so a codec it cannot run - or can run only on a numeric column - used to be accepted at
   * {@code CREATE} and then fail in every later compaction, permanently and type-wide. The rules mirror that switch:
   * <ul>
   * <li>TIMESTAMP: {@code DELTA_OF_DELTA} only. The timestamp column is always encoded with it, so any other name
   * would be stored, rendered back and ignored.</li>
   * <li>TAG and FIELD: {@code DICTIONARY} for any type; {@code GORILLA_XOR} only for DOUBLE/FLOAT and
   * {@code SIMPLE8B} only for the integer-like types (integers, boolean, datetimes), i.e. the codec
   * {@link #defaultCodecFor} picks, because those are the ones whose values round-trip through the encoder.</li>
   * <li>{@code NONE} and {@code DELTA_OF_DELTA} on a non-timestamp column have no encoder.</li>
   * </ul>
   * Keep this in step with {@code TimeSeriesSealedStore.compressColumn}; {@code Issue9310TimeSeriesCodecValidationTest}
   * drives every accepted combination through a compaction and checks the values read back, so a drift fails there. A new
   * data type must be classified in {@link #defaultCodecFor}, which this method defers to for the numeric codecs.
   */
  public static String codecRefusal(final String name, final Type dataType, final ColumnRole role, final TimeSeriesCodec codec) {
    if (role == ColumnRole.TIMESTAMP)
      return codec == TimeSeriesCodec.DELTA_OF_DELTA ? null
          : "Column '" + name + "' is the TIMESTAMP column, which is always encoded with DELTA_OF_DELTA, so it cannot declare codec " + codec;
    return switch (codec) {
      case DICTIONARY -> null;
      // Each numeric codec only for the types its encoder AND decoder round-trip: SIMPLE8B stores integers, so a DOUBLE
      // would read back as raw long bits; GORILLA_XOR stores doubles, so a LONG would lose precision past 2^53 and read
      // back as a Double. A TAG defaults to DICTIONARY whatever its type, so the type's numeric codec is its FIELD default.
      case GORILLA_XOR, SIMPLE8B -> {
        final TimeSeriesCodec numeric = defaultCodecFor(dataType, ColumnRole.FIELD);
        yield numeric == codec ? null
            : "Column '" + name + "' of type " + dataType + " cannot use codec " + codec
                + ", which would not read the values back unchanged. Use DICTIONARY" + (isNumericCodec(numeric) ? " or " + numeric : "");
      }
      default -> "Column '" + name + "' cannot use codec " + codec + ", which has no encoder for a " + role
          + " column. Supported codecs: GORILLA_XOR, SIMPLE8B, DICTIONARY";
    };
  }

  private static boolean isNumericCodec(final TimeSeriesCodec codec) {
    return codec == TimeSeriesCodec.GORILLA_XOR || codec == TimeSeriesCodec.SIMPLE8B;
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
