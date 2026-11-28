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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * apoc.date.convertFormat(temporal, currentFormat, convertTo = 'iso_local_date') - Converts a string holding a date or a time in
 * one format to a string in another, as APOC's apoc.date.convertFormat does: {@code temporal} is parsed with
 * {@code currentFormat} and written with {@code convertTo}. A format is the name of one of the {@link DateTimeFormatter} constants
 * ({@code iso_zoned_date_time}, {@code iso_offset_date_time}, {@code basic_iso_date}, ..., case-insensitive) or a
 * {@link DateTimeFormatter#ofPattern(String)} pattern. A null or empty {@code temporal} is null. {@code @neo4j/graphql} turns a
 * {@code DateTime} aggregate into its string with it ({@code iso_zoned_date_time} to {@code iso_offset_date_time}).
 * <p>
 * Deviation from APOC: an input the format cannot read, or an unknown pattern, is an {@link InvalidArgumentException} (a client
 * error on every protocol, issue #11589) carrying the JDK's message, where APOC wraps the JDK exception in a failed function call.
 */
public class ApocDateConvertFormat extends AbstractApocDateFunction {
  /** The default of the third argument: what APOC answers for an omitted one, observed on Neo4j 5.26 with APOC 5.26. */
  private static final String DEFAULT_CONVERT_TO = "iso_local_date";

  /** The named formats: every public static {@link DateTimeFormatter} constant of the JDK, by lower-case name. */
  private static final Map<String, DateTimeFormatter> NAMED = new ConcurrentHashMap<>();
  private static final Map<String, DateTimeFormatter> PATTERNS = new ConcurrentHashMap<>();
  /** A bound on the patterns kept, so a caller that sends a new pattern every row cannot grow the cache without limit. */
  private static final int MAX_CACHED_PATTERNS = 1_000;

  static {
    for (final Field field : DateTimeFormatter.class.getFields())
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == DateTimeFormatter.class)
        try {
          NAMED.put(field.getName().toLowerCase(Locale.ROOT), (DateTimeFormatter) field.get(null));
        } catch (final IllegalAccessException e) {
          // a public constant: cannot happen
        }
  }

  @Override
  protected String getSimpleName() {
    return "convertFormat";
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 3;
  }

  @Override
  public String getDescription() {
    return "Convert a date or time string from one format to another as APOC does: apoc.date.convertFormat(temporal, currentFormat, convertTo = 'iso_local_date'); a format is a DateTimeFormatter constant name (iso_zoned_date_time) or a pattern";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final Object temporal = args[0];
    if (temporal == null || temporal.toString().isEmpty())
      return null;

    final DateTimeFormatter from = formatter(args[1]);
    final DateTimeFormatter to = formatter(args.length > 2 ? args[2] : null);
    try {
      return to.format(from.parse(temporal.toString()));
    } catch (final DateTimeException e) {
      throw new InvalidArgumentException(e.getMessage());
    }
  }

  private static DateTimeFormatter formatter(final Object format) {
    final String text = format == null || format.toString().isEmpty() ? DEFAULT_CONVERT_TO : format.toString();
    final DateTimeFormatter named = NAMED.get(text.toLowerCase(Locale.ROOT));
    if (named != null)
      return named;

    DateTimeFormatter pattern = PATTERNS.get(text);
    if (pattern == null) {
      try {
        pattern = DateTimeFormatter.ofPattern(text);
      } catch (final IllegalArgumentException e) {
        throw new InvalidArgumentException(e.getMessage());
      }
      if (PATTERNS.size() < MAX_CACHED_PATTERNS)
        PATTERNS.put(text, pattern);
    }
    return pattern;
  }
}
