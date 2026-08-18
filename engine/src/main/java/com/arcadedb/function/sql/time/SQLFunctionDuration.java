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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.utility.DateUtils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Returns a java.time.Duration.
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
    if (params.length != 2)
      throw new IllegalArgumentException("duration() function expected 2 parameters: amount and time-unit");

    if (params[0] == null || params[1] == null)
      return null;

    final long amount = getAmount(params[0]);
    final String unitName = params[1].toString();
    final ChronoUnit unit = DateUtils.parsePrecision(unitName);

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

  private static long getAmount(final Object param) {
    if (param instanceof Number number)
      return number.longValue();
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
    return "duration(<amount>, <time-unit>)";
  }
}
