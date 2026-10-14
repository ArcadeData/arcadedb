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

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.utility.RegexAutomaton;
import com.arcadedb.utility.RegexOverAlphabet;
import com.arcadedb.utility.RegexOverAlphabet.Verdict;
import com.arcadedb.utility.TimeBoundRegex;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Every text the {@code toString()} of a value of some class can be: the characters it is written with, and a pattern of its
 * shape (issue #9853). Both may describe MORE than the class writes, never less: a REGEXP is refused only when no string over
 * the characters, of the shape, matches it, so a text left out would refuse a pattern that value satisfies.
 * <p>
 * Each form also carries actual values of its class: the edge values of its range and ordinary ones. They are what pins the
 * form against the class ({@code Issue9853TextFormCoversEveryValueTest}), and a pattern one of them matches is satisfied by a
 * real value, whatever the shape says (issue #9990).
 *
 * @param characters every character a text of the class can contain, in the order a search tries them
 * @param shape      a pattern every text of the class matches
 * @param samples    values of the class
 */
record TextForm(String characters, Pattern shape, List<Object> samples) {
  // the samples are a handful of values per form, held for the life of the JVM on purpose: the declaration-time check and
  // the test that pins every form read the same ones
  private static final String DIGITS          = "0123456789";
  private static final String LETTERS         = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  private static final String PRINTABLE_ASCII = printableAscii();

  private static final String LOCAL_DATE_SHAPE = "[+-]?[0-9]{4,}-[0-9]{2}-[0-9]{2}";
  // seconds and their fraction are left out when zero, the fraction is written with 3, 6 or 9 digits
  private static final String LOCAL_TIME_SHAPE = "[0-9]{2}:[0-9]{2}(:[0-9]{2}(\\.[0-9]{1,9})?)?";
  private static final String OFFSET_SHAPE     = "(Z|[+-][0-9]{2}:[0-9]{2}(:[0-9]{2})?)";

  // the most digits each width writes: 127, 32767, 2147483647, 9223372036854775807
  private static final TextForm BYTE_FORM        = new TextForm(DIGITS + "-", "-?[0-9]{1,3}",
      List.of(Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0));
  private static final TextForm SHORT_FORM       = new TextForm(DIGITS + "-", "-?[0-9]{1,5}",
      List.of(Short.MIN_VALUE, Short.MAX_VALUE, (short) 0));
  private static final TextForm INTEGER_FORM     = new TextForm(DIGITS + "-", "-?[0-9]{1,10}",
      List.of(Integer.MIN_VALUE, Integer.MAX_VALUE, 0));
  private static final TextForm LONG_FORM        = new TextForm(DIGITS + "-", "-?[0-9]{1,19}",
      List.of(Long.MIN_VALUE, Long.MAX_VALUE, 0L));
  // 1.0E10, -Infinity, NaN
  private static final TextForm FLOATING_FORM    = new TextForm(DIGITS + "-.EINafinty",
      "-?([0-9]+\\.[0-9]+(E-?[0-9]+)?|Infinity)|NaN",
      List.of(Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.MIN_VALUE, Float.MAX_VALUE, -0.0f, 1e-5f, 1.5f,
          Double.NaN, Double.NEGATIVE_INFINITY, Double.MIN_VALUE, Double.MAX_VALUE, -1.0e300, 1e7, 0.001));
  // 1E+3, -0.5, 1.2E-7
  private static final TextForm DECIMAL_FORM     = new TextForm(DIGITS + "-+.E", "-?[0-9]+(\\.[0-9]+)?(E[+-][0-9]+)?",
      List.of(new BigDecimal("1E+3"), new BigDecimal("-1.2E-7"), new BigDecimal("0E-10"), new BigDecimal("-123.450"),
          BigDecimal.ZERO));
  private static final TextForm LOCAL_DATE_FORM  = new TextForm(DIGITS + "-+", LOCAL_DATE_SHAPE,
      List.of(LocalDate.MIN, LocalDate.MAX, LocalDate.of(2024, 1, 15), LocalDate.of(-1, 1, 1)));
  private static final TextForm LOCAL_DT_FORM    = new TextForm(DIGITS + "-+:T.", LOCAL_DATE_SHAPE + "T" + LOCAL_TIME_SHAPE,
      List.of(LocalDateTime.MIN, LocalDateTime.MAX, LocalDateTime.of(2024, 1, 15, 0, 0),
          LocalDateTime.of(2024, 1, 15, 10, 30, 5, 1_000), LocalDateTime.of(2024, 1, 15, 10, 30, 0, 1)));
  private static final TextForm OFFSET_DT_FORM   = new TextForm(DIGITS + "-+:T.Z",
      LOCAL_DATE_SHAPE + "T" + LOCAL_TIME_SHAPE + OFFSET_SHAPE,
      List.of(Instant.EPOCH, Instant.MIN, Instant.MAX, Instant.ofEpochSecond(1, 500), OffsetDateTime.MIN, OffsetDateTime.MAX,
          OffsetDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.ofHoursMinutesSeconds(1, 2, 3))));
  // the zone id: Europe/Rome, America/Port-au-Prince, Etc/GMT+5. The characters are the binding limit, the bracket of the
  // shape only says where the id goes
  private static final TextForm ZONED_DT_FORM    = new TextForm(DIGITS + "-+:T.Z[]/_" + LETTERS,
      LOCAL_DATE_SHAPE + "T" + LOCAL_TIME_SHAPE + OFFSET_SHAPE + "(\\[[^\\]]+\\])?",
      zonedSamples("Europe/Rome", "Etc/GMT+5", "America/Port-au-Prince", "America/Argentina/Buenos_Aires", "UTC"));
  private static final TextForm LOCAL_TIME_FORM  = new TextForm(DIGITS + ":.", LOCAL_TIME_SHAPE,
      List.of(LocalTime.MIDNIGHT, LocalTime.MAX, LocalTime.of(10, 30, 5)));
  private static final TextForm OFFSET_TIME_FORM = new TextForm(DIGITS + "-+:.Z", LOCAL_TIME_SHAPE + OFFSET_SHAPE,
      List.of(OffsetTime.MIN, OffsetTime.MAX, OffsetTime.of(10, 30, 0, 0, ZoneOffset.UTC)));
  // Mon Jan 15 10:30:00 GMT+01:00 2024; the zone is the default zone's short name, '_' and '/' in case it is a raw id
  private static final TextForm DATE_FORM        = new TextForm(DIGITS + " -+:_/" + LETTERS,
      "[A-Z][a-z]{2} [A-Z][a-z]{2} [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} .+",
      List.of(new Date(0), new Date(Long.MAX_VALUE / 1000), new Date(-62_135_769_600_000L), new Date(1_700_000_000_000L)));
  // java.util.GregorianCalendar[time=1705311000000,areFieldsSet=true,...,zone=sun.util.calendar.ZoneInfo[id="Europe/Rome",
  // ...]],firstDayOfWeek=2,...]: a dump of the calendar's fields in printable ASCII. No pattern was written for it, but it has
  // a shape, so a pattern meant for a date ('[0-9]{4}-.*') is refused on it rather than accepted (issue #9976)
  private static final TextForm CALENDAR_FORM    = new TextForm(PRINTABLE_ASCII, "[A-Za-z_$][A-Za-z0-9_$.]*\\[.*\\]",
      List.of(calendar(0), calendar(1_700_000_000_000L)));
  // P1Y2M3DT4H5M6.5S, PT-0.5S
  private static final TextForm DURATION_FORM    = new TextForm(DIGITS + "-.PYMWDTHS", "P[0-9YMWDTHS.-]*",
      List.of(new CypherDuration(0, 0, 0, 0), new CypherDuration(14, 10, 3_725, 500_000_000),
          new CypherDuration(-14, -10, -3_725, 0), new CypherDuration(0, 0, -1, 500_000_000)));
  // a LINK is matched as the RID of the record it points to (issue #9992), a temporary one negative: #12:0, #-1:-1
  private static final TextForm RID_FORM         = new TextForm(DIGITS + "#:-", "#-?[0-9]+:-?[0-9]+",
      List.of(new RID(12, 0), new RID(-1, -1), new RID(Integer.MAX_VALUE, Long.MAX_VALUE)));

  private TextForm(final String characters, final String shape, final List<Object> samples) {
    this(characters, Pattern.compile(shape), samples);
  }

  private static String printableAscii() {
    final StringBuilder printable = new StringBuilder(95);
    for (char c = ' '; c <= '~'; c++)
      printable.append(c);
    return printable.toString();
  }

  /**
   * A value per zone id the JVM knows, and one at UTC whatever it knows: a runtime image built with a trimmed time-zone
   * database must not fail the initialization of this class, and with it every REGEXP declaration.
   */
  private static List<Object> zonedSamples(final String... zoneIds) {
    final List<Object> samples = new ArrayList<>(zoneIds.length + 1);
    samples.add(ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC));
    for (final String zoneId : zoneIds)
      try {
        samples.add(ZonedDateTime.of(2024, 1, 15, 10, 30, 0, 0, ZoneId.of(zoneId)));
      } catch (final DateTimeException e) {
        // not in this runtime's time-zone database
      }
    return List.copyOf(samples);
  }

  private static Calendar calendar(final long time) {
    final Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    calendar.setTimeInMillis(time);
    return calendar;
  }

  /**
   * The texts a value held as {@code held} can be written as, or null when the class is not one a fixed set of characters
   * describes (a String is written as anything).
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
    if (held != null && Calendar.class.isAssignableFrom(held))
      return CALENDAR_FORM;
    if (held == CypherDuration.class)
      return DURATION_FORM;
    if (held == RID.class)
      return RID_FORM;
    // a class this method does not know - a String, a type added later - is never searched, so a pattern on it is never
    // refused: the safe default
    return null;
  }

  /** Every form, for the tests that pin them. */
  static List<TextForm> all() {
    return List.of(BYTE_FORM, SHORT_FORM, INTEGER_FORM, LONG_FORM, FLOATING_FORM, DECIMAL_FORM, LOCAL_DATE_FORM, LOCAL_DT_FORM,
        OFFSET_DT_FORM, ZONED_DT_FORM, LOCAL_TIME_FORM, OFFSET_TIME_FORM, DATE_FORM, CALENDAR_FORM, DURATION_FORM, RID_FORM);
  }

  /**
   * Whether some text of this form matches {@code pattern} (issues #9853, #9990). The answer is DECIDED whenever the pattern
   * is regular ({@link RegexAutomaton}): the pattern and the shape are compiled into automata over the characters, and their
   * product answers in both directions, in a time bound by the size of the two automata rather than by the number of texts,
   * so the width of a type no longer decides whether an impossible pattern is refused. A pattern no automaton expresses (a
   * back reference, a look-around) is tried against the sample values, and then against the texts themselves by a search that
   * gives up when its budget runs out ({@link RegexOverAlphabet}): the one case the answer can be {@link Verdict#UNDECIDED}.
   */
  Verdict admits(final Pattern pattern, final int maxLength, final int maxProbes, final long deadline) {
    // one deadline for the whole check, the automata and the fallbacks alike, given by the caller so a walk over many
    // properties shares one
    // compiled per check: an automaton memoizes its closures as it is walked, so it is not shared between threads
    final RegexAutomaton patternAutomaton = RegexAutomaton.compile(pattern.pattern(), characters);
    if (patternAutomaton != null) {
      final RegexAutomaton shapeAutomaton = RegexAutomaton.compile(shape.pattern(), characters);
      if (shapeAutomaton != null) {
        final Verdict verdict = RegexAutomaton.intersects(patternAutomaton, shapeAutomaton, deadline);
        if (verdict != Verdict.UNDECIDED)
          return verdict;
      }
    }

    try {
      for (final Object sample : samples)
        if (TimeBoundRegex.matchesUntil(pattern, sample.toString(), deadline))
          return Verdict.SATISFIABLE;
    } catch (final TimeoutException | StackOverflowError e) {
      return Verdict.UNDECIDED;
    }
    return RegexOverAlphabet.check(pattern, shape, characters, maxLength, maxProbes, deadline);
  }
}
