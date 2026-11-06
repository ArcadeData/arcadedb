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
package com.arcadedb.query.facet;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Converts the literals of a range or histogram facet (bounds, BOUNDS) into the encoding of the column they are compared with, by the
 * rules stored values are converted with, and the epoch counts of a date column into epoch milliseconds.
 */
final class FacetBounds {
  private static final BigDecimal LONG_MIN = BigDecimal.valueOf(Long.MIN_VALUE);
  private static final BigDecimal LONG_MAX = BigDecimal.valueOf(Long.MAX_VALUE);

  private FacetBounds() {
  }

  /**
   * A literal converted to the property's type as a WHERE comparing the property with it converts it ({@link Type#convertOrNull}), so
   * {@code '2020-01-01'} on a DATE property is the date WHERE compares with. The literal itself when it does not convert.
   */
  static Object schemaConverted(final DatabaseInternal database, final Type propertyType, final Object value) {
    if (value == null || propertyType == null || database == null)
      return value;
    final Object converted = Type.convertOrNull(database, value, propertyType.getJavaImplementation(database));
    return converted != null ? converted : value;
  }

  /**
   * A literal as the lower bound {@code v >= bound} of a LONG column: a number of a column that is not a date is rounded up (for an
   * integral value {@code v >= 1.5} is {@code v >= 2}, and {@code v < 1.5} is {@code v < 2}, so the ceiling serves both ends of a
   * half-open range), saturating at the long limits; anything else (a date, a date string) is converted as a stored value is
   * ({@link ColumnConversion#toColumnLong}), after the conversion WHERE applies.
   *
   * @throws CommandExecutionException when the literal is not a value of the column
   */
  static long toColumnLong(final String alias, final DatabaseInternal database, final ColumnConversion extractor,
      final ColumnDefinition column, final Object literal) {
    if (column.kind() != ColumnKind.LONG)
      throw new IllegalStateException("Column '" + column.property() + "' is not a LONG column");
    if (literal instanceof Number number && !column.propertyType().isDateOrDateTime())
      return ceilToLong(alias, number);
    try {
      return extractor.toColumnLong(column.property(), schemaConverted(database, column.propertyType(), literal));
    } catch (final IllegalArgumentException e) {
      throw notAValue(alias, column, literal);
    }
  }

  /**
   * A literal as a bound of a DOUBLE column.
   *
   * @throws CommandExecutionException when the literal is not a number
   */
  static double toColumnDouble(final String alias, final ColumnDefinition column, final Object literal) {
    if (literal instanceof Number number && !Double.isNaN(number.doubleValue()))
      return number.doubleValue();
    throw notAValue(alias, column, literal);
  }

  /** +1 for a literal that is positive infinity, -1 for negative infinity, 0 for anything else: an infinite range end is unbounded. */
  static int infinity(final Object literal) {
    if ((literal instanceof Double || literal instanceof Float) && Double.isInfinite(((Number) literal).doubleValue()))
      return ((Number) literal).doubleValue() > 0 ? 1 : -1;
    return 0;
  }

  /**
   * A BOUNDS literal of a time histogram no column converts (the match set holds no bucket): a number as epoch milliseconds, a date
   * or a date string (read as a UTC wall clock when it carries no offset, as a DATETIME reads it) as its epoch milliseconds.
   *
   * @throws CommandExecutionException when the literal is no instant
   */
  static long toEpochMs(final String alias, final Object literal) {
    if (literal instanceof Number number)
      return ceilToLong(alias, number);
    final Long epochMs;
    try {
      epochMs = DateUtils.dateTimeToTimestamp(literal, ChronoUnit.MILLIS);
    } catch (final RuntimeException e) {
      throw new CommandExecutionException("FACETS " + alias + ": the bound " + literal + " is not a date");
    }
    if (epochMs == null)
      throw new CommandExecutionException("FACETS " + alias + ": the bound " + literal + " is not a date");
    return epochMs;
  }

  /** A BOUNDS literal of a numeric histogram on an integral grid that no column converts, rounded up as {@link #toColumnLong}. */
  static long toLong(final String alias, final Object literal) {
    if (literal instanceof Number number)
      return ceilToLong(alias, number);
    throw new CommandExecutionException("FACETS " + alias + ": the bound " + literal + " is not a number");
  }

  /** A BOUNDS literal of a numeric histogram on a double grid that no column converts. */
  static double toDouble(final String alias, final Object literal) {
    if (literal instanceof Number number && !Double.isNaN(number.doubleValue()))
      return number.doubleValue();
    throw new CommandExecutionException("FACETS " + alias + ": the bound " + literal + " is not a number");
  }

  /** An epoch count of a date column in {@code unit} as epoch milliseconds, rounded down; a column that is not a date holds ms. */
  static long toEpochMs(final long value, final TimeUnit unit) {
    if (unit == null)
      return value;
    return switch (unit) {
      case DAYS -> Math.multiplyExact(value, 86_400_000L);
      case HOURS -> Math.multiplyExact(value, 3_600_000L);
      case MINUTES -> Math.multiplyExact(value, 60_000L);
      case SECONDS -> Math.multiplyExact(value, 1_000L);
      case MILLISECONDS -> value;
      case MICROSECONDS -> Math.floorDiv(value, 1_000L);
      case NANOSECONDS -> Math.floorDiv(value, 1_000_000L);
    };
  }

  private static long ceilToLong(final String alias, final Number number) {
    if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte)
      return number.longValue();
    if (number instanceof BigInteger integer)
      return saturate(new BigDecimal(integer));
    if (number instanceof BigDecimal decimal)
      return saturate(decimal.setScale(0, RoundingMode.CEILING));
    final double value = number.doubleValue();
    if (Double.isNaN(value))
      throw new CommandExecutionException("FACETS " + alias + ": NaN is not a bound");
    // the cast saturates at the long limits
    return (long) Math.ceil(value);
  }

  private static long saturate(final BigDecimal integral) {
    if (integral.compareTo(LONG_MAX) > 0)
      return Long.MAX_VALUE;
    if (integral.compareTo(LONG_MIN) < 0)
      return Long.MIN_VALUE;
    return integral.longValueExact();
  }

  private static CommandExecutionException notAValue(final String alias, final ColumnDefinition column, final Object literal) {
    return new CommandExecutionException(
        "FACETS " + alias + ": the bound " + literal + " is not a value of '" + column.property() + "' (" + column.propertyType() + ")");
  }
}
