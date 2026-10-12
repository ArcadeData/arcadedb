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
package com.arcadedb.database;

import com.arcadedb.query.opencypher.temporal.CypherDuration;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Every text the {@code toString()} of a value of some class can be: the characters it is written with, and a pattern of its
 * shape (issue #9853). Both may describe MORE than the class writes, never less: a REGEXP is refused only when no string over
 * the characters, of the shape, matches it, so a text left out would refuse a pattern that value satisfies.
 *
 * @param characters every character a text of the class can contain, in the order a search tries them
 * @param shape      a pattern every text of the class matches
 */
record TextForm(String characters, Pattern shape) {
  private static final String DIGITS  = "0123456789";
  private static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  private static final String LOCAL_DATE_SHAPE = "[+-]?[0-9]{4,}-[0-9]{2}-[0-9]{2}";
  // seconds and their fraction are left out when zero, the fraction is written with 3, 6 or 9 digits
  private static final String LOCAL_TIME_SHAPE = "[0-9]{2}:[0-9]{2}(:[0-9]{2}(\\.[0-9]{1,9})?)?";
  private static final String OFFSET_SHAPE     = "(Z|[+-][0-9]{2}:[0-9]{2}(:[0-9]{2})?)";

  // the most digits each width writes: 127, 32767, 2147483647, 9223372036854775807
  private static final TextForm BYTE_FORM        = new TextForm(DIGITS + "-", "-?[0-9]{1,3}");
  private static final TextForm SHORT_FORM       = new TextForm(DIGITS + "-", "-?[0-9]{1,5}");
  private static final TextForm INTEGER_FORM     = new TextForm(DIGITS + "-", "-?[0-9]{1,10}");
  private static final TextForm LONG_FORM        = new TextForm(DIGITS + "-", "-?[0-9]{1,19}");
  // 1.0E10, -Infinity, NaN
  private static final TextForm FLOATING_FORM    = new TextForm(DIGITS + "-.EINafinty",
      "-?([0-9]+\\.[0-9]+(E-?[0-9]+)?|Infinity)|NaN");
  // 1E+3, -0.5, 1.2E-7
  private static final TextForm DECIMAL_FORM     = new TextForm(DIGITS + "-+.E", "-?[0-9]+(\\.[0-9]+)?(E[+-][0-9]+)?");
  private static final TextForm LOCAL_DATE_FORM  = new TextForm(DIGITS + "-+", LOCAL_DATE_SHAPE);
  private static final TextForm LOCAL_DT_FORM    = new TextForm(DIGITS + "-+:T.", LOCAL_DATE_SHAPE + "T" + LOCAL_TIME_SHAPE);
  private static final TextForm OFFSET_DT_FORM   = new TextForm(DIGITS + "-+:T.Z",
      LOCAL_DATE_SHAPE + "T" + LOCAL_TIME_SHAPE + OFFSET_SHAPE);
  // the zone id: Europe/Rome, America/Port-au-Prince, Etc/GMT+5
  private static final TextForm ZONED_DT_FORM    = new TextForm(DIGITS + "-+:T.Z[]/_" + LETTERS,
      LOCAL_DATE_SHAPE + "T" + LOCAL_TIME_SHAPE + OFFSET_SHAPE + "(\\[[^\\]]+\\])?");
  private static final TextForm LOCAL_TIME_FORM  = new TextForm(DIGITS + ":.", LOCAL_TIME_SHAPE);
  private static final TextForm OFFSET_TIME_FORM = new TextForm(DIGITS + "-+:.Z", LOCAL_TIME_SHAPE + OFFSET_SHAPE);
  // Mon Jan 15 10:30:00 GMT+01:00 2024
  private static final TextForm DATE_FORM        = new TextForm(DIGITS + " -+:" + LETTERS,
      "[A-Z][a-z]{2} [A-Z][a-z]{2} [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} .+");
  // P1Y2M3DT4H5M6.5S, PT-0.5S
  private static final TextForm DURATION_FORM    = new TextForm(DIGITS + "-.PYMWDTHS", "P[0-9YMWDTHS.-]*");

  private TextForm(final String characters, final String shape) {
    this(characters, Pattern.compile(shape));
  }

  /**
   * The texts a value held as {@code held} can be written as, or null when the class is not one a fixed set of characters
   * describes (a Calendar is written as a dump of its fields, a String as anything).
   */
  static TextForm of(final Class<?> held) {
    if (held == Long.class)
      return LONG_FORM;
    if (held == Integer.class)
      return INTEGER_FORM;
    if (held == Short.class)
      return SHORT_FORM;
    if (held == Byte.class)
      return BYTE_FORM;
    if (held == Double.class || held == Float.class)
      return FLOATING_FORM;
    if (held == BigDecimal.class)
      return DECIMAL_FORM;
    if (held == LocalDate.class)
      return LOCAL_DATE_FORM;
    if (held == LocalDateTime.class)
      return LOCAL_DT_FORM;
    if (held == Instant.class || held == OffsetDateTime.class)
      return OFFSET_DT_FORM;
    if (held == ZonedDateTime.class)
      return ZONED_DT_FORM;
    if (held == LocalTime.class)
      return LOCAL_TIME_FORM;
    if (held == OffsetTime.class)
      return OFFSET_TIME_FORM;
    if (held == Date.class)
      return DATE_FORM;
    if (held == CypherDuration.class)
      return DURATION_FORM;
    return null;
  }
}
