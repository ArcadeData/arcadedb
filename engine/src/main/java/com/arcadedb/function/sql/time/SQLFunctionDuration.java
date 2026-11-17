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
package com.arcadedb.function.sql.time;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.utility.DateUtils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * Returns a java.time.Duration, from an amount and a time unit ({@code duration(1, 'hour')}) or, as openCypher writes it, from one
 * ISO-8601 text ({@code duration('PT1H')}) or one map of components ({@code duration({hours: 1, minutes: 30})}) (issue #12362). The
 * one-argument forms share the openCypher parser, so both languages read the same text the same way.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 * @see {@link SQLFunctionSysdate}, {@link SQLFunctionDate}
 */
public class SQLFunctionDuration extends SQLFunctionAbstract {
  public static final String NAME = "duration";

  /**
   * Get the date at construction to have the same date for all the iteration.
   */
  public SQLFunctionDuration() {
    super(NAME);
  }

  public Object execute(final Object thisObject, final Identifiable currentRecord, final Object currentResult,
      final Object[] params, final CommandContext context) {
    if (params.length == 1)
      return params[0] == null ? null : fromOneArgument(params[0]);

    if (params.length != 2)
      throw new InvalidArgumentException(NAME + "() expects an ISO-8601 duration ('PT1H'), a map of components ({hours: 1}) or an "
          + "amount and a time unit (1, 'hour'), but received " + params.length + " arguments");

    if (params[0] == null || params[1] == null)
      return null;

    final long amount = getAmount(params[0]);
    final String unitName = params[1].toString();
    final ChronoUnit unit;
    try {
      unit = DateUtils.parsePrecision(unitName);
    } catch (final SerializationException e) {
      // issue #11589: a unit nothing names is the caller's mistake
      throw new InvalidArgumentException("duration() does not know the time unit '" + InvalidArgumentException.quote(unitName) + "'", e);
    }

    // A java.time.Duration is an exact amount of time, so it accepts only units with an exact length. Weeks have one
    // (7 days) and are converted; years and months do not - their length depends on which year or month - and used to
    // reach Duration.of() as an unsupported estimated unit, which threw UnsupportedTemporalTypeException (issue #6388).
    if (unit == ChronoUnit.WEEKS) {
      try {
        return Duration.ofDays(Math.multiplyExact(amount, 7));
      } catch (final ArithmeticException e) {
        // The conversion is the only place an amount can overflow, and a bare ArithmeticException here would be the
        // one error path in this family still answering 500 for a mistake in the call.
        throw new IllegalArgumentException(
            NAME + "() cannot build a duration of " + amount + " weeks: the amount overflows the range of a duration", e);
      }
    }

    // DAYS is the one estimated unit Duration.of() accepts (as exactly 24 hours); every other one - YEARS, MONTHS -
    // it rejects, which is precisely the set to refuse here with an explanation rather than let through.
    if (unit.isDurationEstimated() && unit != ChronoUnit.DAYS)
      throw new IllegalArgumentException(
          NAME + "() cannot build a duration of '" + unitName + "': a duration is an exact amount of time and a "
              + unitName + " has no fixed length. Use days, hours, minutes, seconds or smaller");

    return Duration.of(amount, unit);
  }

  /**
   * The exact duration an ISO-8601 text or a map of components spells. A {@link Duration} is an exact amount of time, so a text or a
   * map with years or months, whose length depends on the calendar, is refused as the two-argument form refuses those units.
   */
  @SuppressWarnings("unchecked")
  private static Duration fromOneArgument(final Object value) {
    final CypherDuration parsed;
    try {
      if (value instanceof Duration duration)
        return duration;
      else if (value instanceof CypherDuration cypherDuration)
        parsed = cypherDuration;
      else if (value instanceof String text)
        parsed = CypherDuration.parse(text.trim());
      else if (value instanceof Map<?, ?> map)
        parsed = CypherDuration.fromMap((Map<String, Object>) map);
      else
        throw new InvalidArgumentException(
            NAME + "() expects an ISO-8601 duration text or a map of components as its only argument, but received a "
                + value.getClass().getSimpleName());
    } catch (final IllegalArgumentException e) {
      throw new InvalidArgumentException(NAME + "() cannot read '" + InvalidArgumentException.quote(value)
          + "' as a duration: use an ISO-8601 duration such as 'PT1H30M' or 'P2DT3H', or a map such as {hours: 1, minutes: 30}", e);
    }

    if (parsed.getMonths() != 0)
      throw new InvalidArgumentException(NAME + "() cannot build a duration of '" + InvalidArgumentException.quote(value)
          + "': a duration is an exact amount of time and years and months have no fixed length. Use days, hours, minutes, "
          + "seconds or smaller");

    try {
      return Duration.ofDays(parsed.getDays()).plusSeconds(parsed.getSeconds()).plusNanos(parsed.getNanosAdjustment());
    } catch (final ArithmeticException e) {
      throw new IllegalArgumentException(
          NAME + "() cannot build a duration of '" + value + "': the amount overflows the range of a duration", e);
    }
  }

  private static long getAmount(final Object param) {
    if (param instanceof Number number)
      // issue #10850: longValue() read duration(2.5, 'hours') as 2 hours
      return requireWholeLongArgument(NAME, number, "amount");
    else if (param instanceof String string) {
      try {
        return Long.parseLong(string.trim());
      } catch (final NumberFormatException e) {
        throw new IllegalArgumentException(NAME + "() received an amount '" + string + "' that is not a number", e);
      }
    } else
      throw new IllegalArgumentException("amount '" + param + "' not a number or a string");
  }

  public String getSyntax() {
    return "duration(<amount>, <time-unit>) | duration(<iso-8601-text>) | duration(<map>)";
  }
}
