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
package com.arcadedb.function.date;

import com.arcadedb.query.sql.executor.CommandContext;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.WeekFields;

/**
 * date.field(timestamp, field) - Extract a field from timestamp.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DateField extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "field";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  @Override
  public String getDescription() {
    return "Extract a specific field (year, month, day, etc.) from a timestamp";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final long timestamp = toMillis(args[0]);
    final String field = args[1] != null ? args[1].toString().toLowerCase() : "year";

    final ZonedDateTime dateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault());

    return switch (field) {
      case "year", "years" -> (long) dateTime.getYear();
      case "month", "months" -> (long) dateTime.getMonthValue();
      case "day", "days", "dayofmonth" -> (long) dateTime.getDayOfMonth();
      case "hour", "hours" -> (long) dateTime.getHour();
      case "minute", "minutes" -> (long) dateTime.getMinute();
      case "second", "seconds" -> (long) dateTime.getSecond();
      case "millisecond", "milliseconds", "millis" -> (long) (dateTime.getNano() / 1_000_000);
      case "dayofweek", "weekday" -> (long) dateTime.getDayOfWeek().getValue();
      case "dayofyear" -> (long) dateTime.getDayOfYear();
      case "week", "weekofyear" -> (long) dateTime.get(WeekFields.ISO.weekOfYear());
      default -> throw new IllegalArgumentException("Unknown date field: " + field);
    };
  }
}
