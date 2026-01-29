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
package com.arcadedb.query.opencypher.functions.date;

import com.arcadedb.query.sql.executor.CommandContext;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * date.fields(dateStr, format, timezone) - Extract all fields from date string.
 *
 * <p><b>Parameters:</b></p>
 * <ul>
 *   <li>dateStr: The date string to parse</li>
 *   <li>format: Optional date format pattern (defaults to ISO format)</li>
 *   <li>timezone: Optional timezone ID (e.g., "UTC", "America/New_York", defaults to system timezone)</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DateFields extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "fields";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Parse a date string and extract all fields as a map (supports optional timezone parameter)";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final String dateStr = args[0].toString();
    final String format = args.length > 1 && args[1] != null ? args[1].toString() : null;
    final String timezoneStr = args.length > 2 && args[2] != null ? args[2].toString() : null;

    // Validate and parse timezone if provided
    final ZoneId zoneId;
    if (timezoneStr != null) {
      try {
        zoneId = ZoneId.of(timezoneStr);
      } catch (final java.time.DateTimeException e) {
        throw new IllegalArgumentException("Invalid timezone ID: " + timezoneStr, e);
      }
    } else {
      zoneId = ZoneId.systemDefault();
    }

    final DateTimeFormatter formatter = getFormatter(format);
    final LocalDateTime localDateTime = LocalDateTime.parse(dateStr, formatter);
    final ZonedDateTime dateTime = localDateTime.atZone(zoneId);

    final Map<String, Object> fields = new HashMap<>();
    fields.put("year", (long) dateTime.getYear());
    fields.put("month", (long) dateTime.getMonthValue());
    fields.put("day", (long) dateTime.getDayOfMonth());
    fields.put("hour", (long) dateTime.getHour());
    fields.put("minute", (long) dateTime.getMinute());
    fields.put("second", (long) dateTime.getSecond());
    fields.put("millisecond", (long) (dateTime.getNano() / 1_000_000));
    fields.put("dayOfWeek", (long) dateTime.getDayOfWeek().getValue());
    fields.put("dayOfYear", (long) dateTime.getDayOfYear());
    fields.put("weekOfYear", (long) dateTime.get(java.time.temporal.WeekFields.ISO.weekOfYear()));
    fields.put("timezone", dateTime.getZone().getId());

    return fields;
  }
}
