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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * date.fromISO8601(dateStr) - Parse ISO 8601 string to timestamp.
 *
 * @author ArcadeDB Team
 */
public class DateFromISO8601 extends AbstractDateFunction {
  @Override
  protected String getSimpleName() {
    return "fromISO8601";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Parse an ISO 8601 format string to a timestamp";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final String dateStr = args[0].toString();

    try {
      final ZonedDateTime dateTime = ZonedDateTime.parse(dateStr, DateTimeFormatter.ISO_DATE_TIME);
      return dateTime.toInstant().toEpochMilli();
    } catch (final DateTimeParseException e) {
      // Try without timezone
      try {
        final ZonedDateTime dateTime = ZonedDateTime.parse(dateStr + "Z", DateTimeFormatter.ISO_DATE_TIME);
        return dateTime.toInstant().toEpochMilli();
      } catch (final DateTimeParseException e2) {
        throw new IllegalArgumentException("Cannot parse ISO 8601 date: " + dateStr, e);
      }
    }
  }
}
