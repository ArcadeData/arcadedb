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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DocumentValidator;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.TimeBoundRegex;

import java.util.List;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * The MANDATORY, NOTNULL, MIN, MAX and REGEXP constraints declared on the columns of a TIMESERIES type, held to every value an
 * ingestion surface is about to append (issue #10238). {@code ALTER PROPERTY} accepts them on a column like on any property, and
 * no write path used to look at them: SQL hands a row to the engine without the document validation, and the line protocol, gRPC,
 * Prometheus remote write and the JSONL import never build a document at all. Every one of those paths converts each value with
 * {@link ColumnDefinition#storableValue} first, and this check runs on what that conversion returns, so a constraint means the same
 * thing whichever surface the sample arrives through: the value the column will STORE is checked, read the way SQL reads it back.
 * <p>
 * The rules are the document ones, measured on the stored value:
 * <ul>
 * <li>MIN / MAX bound the value of a numeric column, the length of a STRING one and the instant of a DATE/DATETIME one. A temporal
 * bound is read as SQL reads a literal ({@link ColumnDefinition#epochOf}) and compared to the stored count in the column's own
 * unit, so a DATETIME_NANOS column is compared exactly, not through milliseconds</li>
 * <li>REGEXP is matched against the text of the value as SQL returns it (a temporal count as the date it stands for,
 * {@link ColumnDefinition#temporalOf}), the text {@link DocumentValidator#regexpText} defines for a document property, under one
 * {@code arcadedb.command.regexTimeout} budget per sample, shared by the sample's columns</li>
 * <li>MANDATORY and NOTNULL both require a value. A document can carry a property set to null, a sample cannot: the storage keeps
 * no difference between a column a writer left out and one it set to null (a line protocol point has no null at all), so a
 * MANDATORY column that took a null would store the very row MANDATORY exists to refuse. A NaN in a FLOAT/DOUBLE column is no value
 * either: it is the marker that column stores for an absent value ({@link TimeSeriesNaN})</li>
 * <li>A STRING column, tag or field, stores an absent value as the empty text and reads it back so: MIN and REGEXP judge that
 * text, which is what keeps a constraint declared before the samples and one declared after them in agreement (issue #10336)</li>
 * </ul>
 * A refusal is a reason that names the constraint and never the value, because on the gateway paths the value comes from the
 * client and the reason goes back to it and to the log; SQL prefixes the value itself.
 * <p>
 * {@link #of} answers {@code null} for a type with no constraint on any column, which is nearly every type: the write paths then
 * pay one null check per batch or statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeSeriesColumnConstraints {
  private final ColumnCheck   timestamp;
  private final ColumnCheck[] values;
  private final boolean       matchesPatterns;
  private final long          regexTimeoutMs;

  private TimeSeriesColumnConstraints(final ColumnCheck timestamp, final ColumnCheck[] values, final long regexTimeoutMs) {
    this.timestamp = timestamp;
    this.values = values;
    boolean patterns = timestamp != null && timestamp.pattern != null;
    for (final ColumnCheck check : values)
      patterns |= check != null && check.pattern != null;
    this.matchesPatterns = patterns;
    this.regexTimeoutMs = regexTimeoutMs;
  }

  /**
   * The constraints {@code type} declares now, or {@code null} when no column of it carries any. Read once per statement or batch:
   * the schema is not consulted again for the rows the result checks.
   */
  public static TimeSeriesColumnConstraints of(final Database database, final LocalTimeSeriesType type) {
    final List<ColumnDefinition> columns = type.getTsColumns();
    ColumnCheck timestamp = null;
    ColumnCheck[] values = null;
    boolean any = false;
    int valueColumn = 0;
    for (int i = 0; i < columns.size(); i++) {
      final ColumnDefinition column = columns.get(i);
      final Property property = type.getPolymorphicPropertyIfExists(column.getName());
      final ColumnCheck check = property == null ? null : ColumnCheck.of(database, column, property);
      if (column.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        timestamp = check;
      else {
        if (check != null) {
          if (values == null)
            values = new ColumnCheck[columns.size() - 1];
          values[valueColumn] = check;
        }
        valueColumn++;
      }
      any |= check != null;
    }
    if (!any)
      return null;
    return new TimeSeriesColumnConstraints(timestamp, values != null ? values : new ColumnCheck[columns.size() - 1],
        GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(database));
  }

  /**
   * The check of ONE column for constraints it does not carry yet, so {@code ALTER PROPERTY} can ask whether a stored sample would
   * violate them before declaring them, the time series twin of {@code ExistingRecordsCheck}. {@code null} when there is nothing
   * to check.
   */
  public static ColumnCheck columnCheck(final Database database, final ColumnDefinition column, final Type type,
      final boolean required, final String requiredBy, final String min, final String max, final String regexp) {
    if (!required && min == null && max == null && regexp == null)
      return null;
    Pattern pattern = null;
    String unusable = null;
    if (regexp != null)
      try {
        pattern = Pattern.compile(regexp);
      } catch (final PatternSyntaxException e) {
        unusable = invalidRegexp(regexp, e);
      }
    return new ColumnCheck(database, column, type, required, requiredBy, min, max, pattern, unusable);
  }

  private static String invalidRegexp(final String regexp, final PatternSyntaxException e) {
    return "has the REGEXP '" + regexp + "', which is not a valid regular expression: " + e.getDescription()
        + ". Remove it with ALTER PROPERTY ... REGEXP null";
  }

  /**
   * The deadline the REGEXP checks of one sample share, so a sample with several pattern-constrained columns costs one
   * {@code arcadedb.command.regexTimeout} at most. {@code 0} when no column has a pattern, which reads no clock.
   */
  public long sampleDeadline() {
    return matchesPatterns ? TimeBoundRegex.newDeadline(regexTimeoutMs) : 0L;
  }

  /**
   * @return why the TIMESTAMP column refuses {@code timestamp}, or {@code null} when it takes it
   */
  public String checkTimestamp(final long timestamp, final long deadline) {
    return this.timestamp == null ? null : this.timestamp.check(timestamp, deadline);
  }

  /**
   * @param valueColumn the column's ordinal among the non-timestamp columns, in schema order
   * @param value       the value as {@link ColumnDefinition#storableValue} returned it, {@code null} for no value
   *
   * @return why the column refuses {@code value}, or {@code null} when it takes it
   */
  public String check(final int valueColumn, final Object value, final long deadline) {
    final ColumnCheck check = values[valueColumn];
    return check == null ? null : check.check(value, deadline);
  }

  /** The constraints of one column, compiled once: the bounds parsed, the pattern compiled. */
  public static final class ColumnCheck {
    private enum Measure {NONE, INTEGRAL, FLOAT, DOUBLE, LENGTH, TEMPORAL}

    private final Database         database;
    private final ColumnDefinition column;
    private final Type             type;
    private final boolean          required;
    private final String           requiredBy;
    private final Measure          measure;
    private final String           min;
    private final String           max;
    private final long             minLong;
    private final long             maxLong;
    private final double           minDouble;
    private final double           maxDouble;
    // a bound or a pattern the stored schema carries and the write path cannot use: a database written before such a constraint
    // was refused at declaration only loads with a warning, and its writes are refused naming it, as a document's are
    private final String           unusable;
    private final Pattern          pattern;

    private ColumnCheck(final Database database, final ColumnDefinition column, final Type type, final boolean required,
        final String requiredBy, final String min, final String max, final Pattern pattern, final String unusablePattern) {
      this.database = database;
      this.column = column;
      this.type = type;
      this.required = required;
      this.requiredBy = requiredBy;
      this.min = min;
      this.max = max;

      String unusable = unusablePattern;
      Measure measure = Measure.NONE;
      long minLong = Long.MIN_VALUE, maxLong = Long.MAX_VALUE;
      double minDouble = Double.NEGATIVE_INFINITY, maxDouble = Double.POSITIVE_INFINITY;
      if (min != null || max != null) {
        try {
          switch (type) {
          case BYTE, SHORT, INTEGER, LONG -> {
            measure = Measure.INTEGRAL;
            if (min != null)
              minLong = Long.parseLong(min);
            if (max != null)
              maxLong = Long.parseLong(max);
          }
          case FLOAT -> {
            measure = Measure.FLOAT;
            if (min != null)
              minDouble = Float.parseFloat(min);
            if (max != null)
              maxDouble = Float.parseFloat(max);
          }
          case DOUBLE -> {
            measure = Measure.DOUBLE;
            if (min != null)
              minDouble = Double.parseDouble(min);
            if (max != null)
              maxDouble = Double.parseDouble(max);
          }
          case STRING -> {
            measure = Measure.LENGTH;
            if (min != null)
              minLong = Integer.parseInt(min);
            if (max != null)
              maxLong = Integer.parseInt(max);
          }
          case DATE, DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> {
            measure = Measure.TEMPORAL;
            if (min != null)
              minLong = column.epochOf(database, min);
            if (max != null)
              maxLong = column.epochOf(database, max);
          }
          default -> unusable = "has a MIN or MAX, which is not applicable to type " + type;
          }
        } catch (final RuntimeException e) {
          LogManager.instance().log(this, Level.FINE, "Bound '%s'/'%s' of TIMESERIES column '%s' is not usable", e, min, max,
              column.getName());
          unusable = "has the bound '" + (min != null ? min : "") + (min != null && max != null ? "'/'" : "") + (max != null ? max : "")
              + "', which cannot be read as " + type + ". Remove it with ALTER PROPERTY ... MIN null / MAX null";
        }
      }

      this.measure = measure;
      this.minLong = minLong;
      this.maxLong = maxLong;
      this.minDouble = minDouble;
      this.maxDouble = maxDouble;
      this.unusable = unusable;
      this.pattern = pattern;
    }

    /** The check of the constraints {@code property} declares now, the pattern being the one the property keeps compiled. */
    static ColumnCheck of(final Database database, final ColumnDefinition column, final Property property) {
      final boolean required = property.isMandatory() || property.isNotNull();
      if (!required && property.getMin() == null && property.getMax() == null && property.getRegexp() == null)
        return null;
      Pattern pattern = null;
      String unusable = null;
      if (property.getRegexp() != null)
        try {
          pattern = property.getRegexpPattern();
        } catch (final PatternSyntaxException e) {
          unusable = invalidRegexp(property.getRegexp(), e);
        }
      return new ColumnCheck(database, column, property.getType(), required, property.isMandatory() ? "MANDATORY" : "NOTNULL",
          property.getMin(), property.getMax(), pattern, unusable);
    }

    /**
     * A column whose stored bound or pattern cannot be used refuses every VALUE, naming the constraint, as a document property does;
     * a sample with no value for it is refused by MANDATORY/NOTNULL, and otherwise only where the column stores something in its
     * place: a STRING column stores the empty text (issue #10336), so its MIN and REGEXP judge that text, as {@code ALTER PROPERTY}
     * judges it on the samples already stored. A numeric or temporal column keeps no value as no value, so no bound applies.
     *
     * @param value the value as the column stores it (a temporal one as its count), {@code null} for no value
     *
     * @return why the column refuses {@code value}, or {@code null} when it takes it
     */
    public String check(final Object value, final long deadline) {
      // NaN is how a FLOAT/DOUBLE column stores no value, the absent marker every aggregate skips (TimeSeriesNaN): it is no value
      // here either, so MANDATORY/NOTNULL refuse it and every other constraint lets it through, as a null
      if (value == null || value instanceof Double d && d.isNaN() || value instanceof Float f && f.isNaN()) {
        if (required)
          return "the column is " + requiredBy + " and the sample carries no value for it";
        if (value != null || column.getDataType() != Type.STRING)
          return null;
        final String reason = checkValue("", deadline);
        return reason == null ? null : "the sample carries no value for it, which the column stores as the empty text, and " + reason;
      }
      return checkValue(value, deadline);
    }

    private String checkValue(final Object value, final long deadline) {
      if (unusable != null)
        return "the column " + unusable;
      // storableValue hands a numeric or temporal column a Number on every path; a refusal rather than a ClassCastException in the
      // middle of a batch if that ever stopped holding
      if (measure != Measure.NONE && measure != Measure.LENGTH && !(value instanceof Number))
        return "the value is not a number";

      switch (measure) {
      case INTEGRAL, TEMPORAL -> {
        final long v = ((Number) value).longValue();
        if (v < minLong)
          return measure == Measure.TEMPORAL ? "the date is before the MIN '" + min + "'" : "the value is less than the MIN " + min;
        if (v > maxLong)
          return measure == Measure.TEMPORAL ? "the date is after the MAX '" + max + "'" : "the value is greater than the MAX " + max;
      }
      case FLOAT, DOUBLE -> {
        final double v = measure == Measure.FLOAT ? ((Number) value).floatValue() : ((Number) value).doubleValue();
        if (v < minDouble)
          return "the value is less than the MIN " + min;
        if (v > maxDouble)
          return "the value is greater than the MAX " + max;
      }
      case LENGTH -> {
        final int length = value.toString().length();
        if (length < minLong)
          return "the text has fewer characters than the MIN " + min;
        if (length > maxLong)
          return "the text has more characters than the MAX " + max;
      }
      default -> {
      }
      }

      if (pattern != null) {
        final String text = DocumentValidator.regexpText(type, readBack(value));
        try {
          if (text == null || !TimeBoundRegex.matchesUntil(pattern, text, deadline))
            return "the value does not match the REGEXP '" + pattern.pattern() + "'";
        } catch (final TimeoutException e) {
          return "the value could not be matched against the REGEXP '" + pattern.pattern() + "' within "
              + GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getKey();
        }
      }
      return null;
    }

    /** The value as SQL returns it, the one a pattern written for the property is meant to describe. */
    private Object readBack(final Object value) {
      if (type.isDateOrDateTime())
        return column.temporalOf(database, ((Number) value).longValue());
      if (value instanceof Number)
        return Type.convert(database, value, type.getJavaImplementation(database));
      return value;
    }
  }
}
