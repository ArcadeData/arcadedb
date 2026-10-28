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
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnarValueExtractor;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.DateUtils;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * How a collector turns a value (a literal bound, an INCLUDE value, a scanned raw value) into the 64-bit encoding of its LONG column:
 * the declaring type's {@link ColumnarValueExtractor} for a real column, or a raw conversion for the synthetic column of a property the
 * fallback scan reads by value ({@link #integers()}, {@link #epochMillis}).
 */
interface ColumnConversion {
  /**
   * @throws IllegalArgumentException when the value is not a value of the column
   */
  long toColumnLong(String property, Object value);

  /** The unit of the epoch counts of a date column, null for a column that is not a date. */
  TimeUnit dateUnit(String property);

  /**
   * The conversion of a real column as a collector counts it: the extractor's for literals, the epoch unit of {@code column} (the
   * definition the counted segments were written with), not of whatever the extractor holds now; null for no extractor.
   */
  static ColumnConversion of(final ColumnarValueExtractor extractor, final ColumnDefinition column) {
    if (extractor == null)
      return null;
    final TimeUnit dateUnit = ColumnarValueExtractor.dateUnit(column.propertyType());
    return new ColumnConversion() {
      @Override
      public long toColumnLong(final String property, final Object value) {
        return extractor.toColumnLong(property, value);
      }

      @Override
      public TimeUnit dateUnit(final String property) {
        return dateUnit;
      }
    };
  }

  /** The raw values of an integral property: an integral number as itself (normalized as GROUP BY keys it), anything else refused. */
  static ColumnConversion integers() {
    return new ColumnConversion() {
      @Override
      public long toColumnLong(final String property, final Object value) {
        if (value instanceof Number && Type.normalizeNumberForKey(value) instanceof Long integral)
          return integral;
        throw new IllegalArgumentException("Not an integral number: " + value);
      }

      @Override
      public TimeUnit dateUnit(final String property) {
        return null;
      }
    };
  }

  /**
   * The raw values of a date or datetime property as epoch milliseconds: {@code Date}, {@code Calendar}, {@code Instant},
   * {@code LocalDate} (its UTC midnight), {@code LocalDateTime} (a UTC wall clock, as a DATETIME stores it), {@code ZonedDateTime},
   * {@code OffsetDateTime}, a number as epoch milliseconds, a date string in the database's formats ({@link DateUtils#dateTimeToTimestamp}).
   */
  static ColumnConversion epochMillis(final DatabaseInternal database) {
    return new ColumnConversion() {
      @Override
      public long toColumnLong(final String property, final Object value) {
        final Long epochMs;
        try {
          epochMs = DateUtils.dateTimeToTimestamp(database, value, ChronoUnit.MILLIS);
        } catch (final RuntimeException e) {
          throw new IllegalArgumentException("Not a date: " + value, e);
        }
        if (epochMs == null)
          throw new IllegalArgumentException("Not a date: " + value);
        return epochMs;
      }

      @Override
      public TimeUnit dateUnit(final String property) {
        return TimeUnit.MILLISECONDS;
      }
    };
  }
}
