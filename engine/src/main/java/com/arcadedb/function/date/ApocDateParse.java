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

import java.text.ParseException;
import java.util.concurrent.TimeUnit;

/**
 * apoc.date.parse(text, unit = 'ms', format = 'yyyy-MM-dd HH:mm:ss', timezone = 'UTC') - Parse a date string to a timestamp in
 * {@code unit}, as APOC's apoc.date.parse does (issue #12106): a {@link java.text.SimpleDateFormat} pattern parsed leniently
 * from the start of the text (a month 13 rolls over into the next year), a text naming no zone read in {@code timezone}, a
 * blank text is null. ArcadeDB's own date.parse, strict and {@code java.time} based (issue #10206), is {@link DateParse}.
 * <p>
 * Deviations from APOC: an unknown time zone is an error, where APOC reads in GMT; without a time zone a pattern that names
 * the zone reads a zone-less text in UTC, where APOC uses the JVM's zone.
 */
public class ApocDateParse extends AbstractApocDateFunction {
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
    return 4;
  }

  @Override
  public String getDescription() {
    return "Parse a date string to a timestamp as APOC does: apoc.date.parse(text, unit = 'ms', format = 'yyyy-MM-dd HH:mm:ss', timezone = 'UTC')";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args[0] == null)
      return null;

    final String text = args[0].toString();
    if (text.isBlank())
      return null;

    final TimeUnit unit = timeUnit(args.length > 1 ? args[1] : null);
    final String pattern = patternArgument(args, 2);
    final long millis;
    try {
      millis = simpleDateFormat(pattern, args.length > 3 ? args[3] : null).parse(text).getTime();
    } catch (final ParseException e) {
      throw new InvalidArgumentException(getName() + "(): '" + InvalidArgumentException.quote(text) + "' cannot be parsed with the format '"
          + InvalidArgumentException.quote(pattern) + "'", e);
    }
    return unit.convert(millis, TimeUnit.MILLISECONDS);
  }
}
