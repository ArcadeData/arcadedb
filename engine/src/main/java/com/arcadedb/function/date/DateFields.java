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

import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * date.fields(text, pattern = 'yyyy-MM-dd HH:mm:ss' [, timezone]) - The fields of a date string, as APOC's
 * apoc.date.fields answers them (issue #12106).
 * <p>
 * The text is parsed whole by a {@link DateTimeFormatter} pattern, and the map holds the fields the parsed text supports
 * under APOC's keys: {@code years}, {@code months}, {@code weekdays} (1 = Monday), {@code days}, {@code hours},
 * {@code minutes}, {@code seconds}, plus {@code zoneid}, the short name of the zone the text names or, when it names none
 * (an offset is not a zone), UTC. The fields are the ones written in the text.
 * <p>
 * The {@code timezone} argument is an ArcadeDB extension APOC does not have: a text that names an instant (a date, a time
 * and an offset or a zone) is expressed in that zone, one that names none is read in it, and {@code zoneid} is its name.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class DateFields extends AbstractDateFunction {
  private static final String[]      KEYS   = { "years", "months", "weekdays", "days", "hours", "minutes", "seconds" };
  private static final ChronoField[] FIELDS = { ChronoField.YEAR, ChronoField.MONTH_OF_YEAR, ChronoField.DAY_OF_WEEK,
      ChronoField.DAY_OF_MONTH, ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE };

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
    return "Parse a date string and return its fields as a map - years, months, weekdays, days, hours, minutes, seconds "
        + "and zoneid: date.fields(text, pattern = 'yyyy-MM-dd HH:mm:ss' [, timezone])";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Map<String, Object> fields = new LinkedHashMap<>();
    if (args[0] == null)
      return fields;

    final String text = args[0].toString();
    final String pattern = patternArgument(args, 1);
    final String timezone = args.length > 2 && args[2] != null ? args[2].toString() : null;

    final ZoneId zoneId;
    try {
      zoneId = ZoneId.of(timezone != null ? timezone : UTC);
    } catch (final DateTimeException e) {
      throw new IllegalArgumentException("Invalid timezone ID: " + timezone, e);
    }

    final DateTimeFormatter formatter;
    try {
      formatter = DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH).withZone(zoneId);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("The pattern: " + pattern + " is not correct", e);
    }

    final TemporalAccessor read;
    try {
      read = formatter.parse(text);
    } catch (final DateTimeException e) {
      // issue #11589: a text the format cannot read is the caller's mistake, not a raw DateTimeParseException
      throw new InvalidArgumentException(getName() + "(): '" + InvalidArgumentException.quote(text) + "' cannot be parsed with the format '"
          + InvalidArgumentException.quote(pattern) + "'", e);
    }
    // the instant the text names, its own offset or zone first (issue #10596), expressed in the zone asked for
    final TemporalAccessor parsed =
        timezone != null && read.isSupported(ChronoField.INSTANT_SECONDS) ? Instant.from(read).atZone(zoneId) : read;

    for (int i = 0; i < FIELDS.length; i++)
      if (parsed.isSupported(FIELDS[i]))
        fields.put(KEYS[i], parsed.getLong(FIELDS[i]));

    final ZoneId zone = parsed.query(TemporalQueries.zone());
    if (zone != null)
      fields.put("zoneid", zone.getDisplayName(TextStyle.SHORT, Locale.ROOT));

    return fields;
  }
}
