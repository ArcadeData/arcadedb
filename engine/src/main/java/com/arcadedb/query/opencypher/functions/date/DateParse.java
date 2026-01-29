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
import java.time.format.DateTimeParseException;

/**
 * date.parse(dateStr, unit, format) - Parse date string to timestamp.
 *
 * @author ArcadeDB Team
 */
public class DateParse extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "parse";
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
    return "Parse a date string to a timestamp using the specified format pattern";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final String dateStr = args[0].toString();
    final String unit = args.length > 1 && args[1] != null ? args[1].toString() : UNIT_MS;
    final String format = args.length > 2 && args[2] != null ? args[2].toString() : null;

    try {
      final DateTimeFormatter formatter = getFormatter(format);
      final LocalDateTime localDateTime = LocalDateTime.parse(dateStr, formatter);
      final ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
      final long millis = zonedDateTime.toInstant().toEpochMilli();

      // Convert to requested unit
      return millis / unitToMillis(unit);
    } catch (final DateTimeParseException e) {
      throw new IllegalArgumentException("Cannot parse date: " + dateStr + " with format: " + format, e);
    }
  }
}
