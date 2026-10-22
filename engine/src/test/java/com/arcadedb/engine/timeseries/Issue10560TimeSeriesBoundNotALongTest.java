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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10560: the range a time predicate is pushed down as is EXACTLY that predicate (the rule of #10528), and it was not for a
 * bound that is not a whole {@code long}. A fractional bound was truncated to a whole millisecond ({@code ts >= 2000.5} selected the
 * sample at 2000 and {@code DELETE ... WHERE ts = 2000.5} removed it), a numeric bound outside the range of a {@code long} selected
 * nothing whichever the predicate meant, and {@code 1000 <> ts} was ignored.
 * <p>
 * The oracle is the same three samples in a plain document type: every operator, written either way round, against every kind of
 * bound, answers the same rows for the SELECT and leaves the same rows after the DELETE.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10560TimeSeriesBoundNotALongTest extends TestHelper {
  private static final String[] OPERATORS = { "<", "<=", ">", ">=", "=", "<>" };

  /** Literals written in the statement: whole, fractional, out of the range of a long on either side, and on the edge of it. */
  private static final String[] LITERALS = {
      "2000", "2000.0", "2000.5", "1999.5", "2999.5", "999.5", "3000.5", "0.5", "-0.5", "1e19", "-1e19", "1.0E19", "9.3e18", "-9.3e18",
      "9223372036854775807", "-9223372036854775807", "9223372036854775807.5", "9223372036854775808.0", "-9223372036854775809.0" };

  /** Values bound as a parameter, the ones a literal cannot spell. */
  private static final Object[] PARAMETERS = {
      2000L, 2000.5d, 1999.5d, 2000.5f, Double.MAX_VALUE, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN,
      Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN, new BigDecimal("2000.5"), new BigDecimal("2000"),
      new BigDecimal("9223372036854775808"), new BigDecimal("-9223372036854775809"), new BigDecimal("9223372036854775807.5"),
      new BigInteger("9223372036854775808"), new BigInteger("-9223372036854775809"), new BigInteger("2000") };

  private int cases;

  @Test
  void aLiteralBoundSelectsTheRowsThePredicateSelectsInADocumentType() {
    final List<String> differing = new ArrayList<>();
    final String[] types = createSamples();

    for (final String operator : OPERATORS)
      for (final String literal : LITERALS) {
        compare(types, "ts " + operator + " " + literal, Map.of(), differing);
        compare(types, literal + " " + operator + " ts", Map.of(), differing);
      }

    assertThat(differing).as("predicates answered differently by the time series type").isEmpty();
  }

  @Test
  void aParameterBoundSelectsTheRowsThePredicateSelectsInADocumentType() {
    final List<String> differing = new ArrayList<>();
    final String[] types = createSamples();

    for (final String operator : OPERATORS)
      for (final Object parameter : PARAMETERS) {
        final String description = parameter.getClass().getSimpleName() + " " + parameter;
        compare(types, "ts " + operator + " :p", Map.of("p", parameter), differing, description);
        compare(types, ":p " + operator + " ts", Map.of("p", parameter), differing, description);
      }

    assertThat(differing).as("predicates answered differently by the time series type").isEmpty();
  }

  @Test
  void aBoundCombinedWithAnotherPredicateSelectsTheSameRows() {
    final List<String> differing = new ArrayList<>();
    final String[] types = createSamples();

    final String[] predicates = {
        "ts >= 1500.5 AND ts <= 2500.5", "ts > 1999.5 AND ts < 3000.5", "ts >= 2000.5 OR ts < 1000.5", "ts <= 1e19 AND ts >= 1500",
        "ts > -1e19 AND ts <> 2000", "ts BETWEEN 1500.5 AND 2500.5", "ts BETWEEN -1e19 AND 1e19", "ts BETWEEN 2000.5 AND 1e19",
        "ts <= 1e19 AND t = 'h'", "ts >= 2000.5 AND t = 'h'", "1000 <> ts AND t = 'h'", "1000 <> ts AND 3000 <> ts",
        "ts <= 1e19 OR ts > 5000", "NOT (ts >= 2000.5)" };
    for (final String predicate : predicates)
      compare(types, predicate, Map.of(), differing);

    assertThat(differing).as("predicates answered differently by the time series type").isEmpty();
  }

  @Test
  void aDeleteRemovesTheRowsThePredicateSelectsInADocumentType() {
    final List<String> differing = new ArrayList<>();

    for (final String operator : OPERATORS)
      for (final String literal : LITERALS) {
        compareDelete("ts " + operator + " " + literal, Map.of(), differing, "");
        compareDelete(literal + " " + operator + " ts", Map.of(), differing, "");
      }
    for (final String operator : OPERATORS)
      for (final Object parameter : PARAMETERS) {
        final String description = parameter.getClass().getSimpleName() + " " + parameter;
        compareDelete("ts " + operator + " :p", Map.of("p", parameter), differing, description);
        compareDelete(":p " + operator + " ts", Map.of("p", parameter), differing, description);
      }
    for (final String predicate : new String[] { "ts >= 1500.5 AND ts <= 2500.5", "ts BETWEEN 1500.5 AND 2500.5", "ts BETWEEN -1e19 AND 1e19",
        "1000 <> ts AND t = 'h'" })
      compareDelete(predicate, Map.of(), differing, "");

    assertThat(differing).as("DELETE statements that left other rows than the document type does").isEmpty();
  }

  @Test
  void theFractionalBoundsOfTheReport() {
    final String[] types = createSamples();

    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts >= 2000.5")).containsExactly(3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts = 2000.5")).isEmpty();
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts < 2000.5")).containsExactly(1000L, 2000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts <= 1e19")).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE ts > -1e19")).containsExactly(1000L, 2000L, 3000L);
    assertThat(tsOf("SELECT FROM " + types[0] + " WHERE 1000 <> ts")).containsExactly(2000L, 3000L);

    database.command("sql", "DELETE FROM " + types[0] + " WHERE ts = 2000.5");
    assertThat(tsOf("SELECT FROM " + types[0])).containsExactly(1000L, 2000L, 3000L);
    database.command("sql", "DELETE FROM " + types[0] + " WHERE ts >= 2000.5");
    assertThat(tsOf("SELECT FROM " + types[0])).containsExactly(1000L, 2000L);
    database.command("sql", "DELETE FROM " + types[0] + " WHERE ts < 1000.5");
    assertThat(tsOf("SELECT FROM " + types[0])).containsExactly(2000L);
  }

  /**
   * The three samples of the report, in a time series type and in a document type; returns the two type names. The types are created
   * once, and every later call empties them and writes the samples again.
   */
  private String[] createSamples() {
    final String ts = "D1";
    final String doc = "D2";
    if (cases++ == 0) {
      database.command("sql", "CREATE TIMESERIES TYPE " + ts + " TIMESTAMP ts TAGS (t STRING) FIELDS (i INTEGER)");
      database.command("sql", "CREATE DOCUMENT TYPE " + doc);
    } else {
      database.command("sql", "DELETE FROM " + ts);
      database.transaction(() -> database.command("sql", "DELETE FROM " + doc));
    }
    fill(ts, doc);
    return new String[] { ts, doc };
  }

  private void fill(final String tsType, final String docType) {
    for (final String type : new String[] { tsType, docType })
      // a time series insert opens no transaction of its own, a document insert needs one
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + type + " SET ts = 1000, t = 'h', i = 1");
        database.command("sql", "INSERT INTO " + type + " SET ts = 2000, t = 'h', i = 2");
        database.command("sql", "INSERT INTO " + type + " SET ts = 3000, t = 'h', i = 3");
      });
  }

  private void compare(final String[] types, final String where, final Map<String, Object> parameters, final List<String> differing) {
    compare(types, where, parameters, differing, "");
  }

  private void compare(final String[] types, final String where, final Map<String, Object> parameters, final List<String> differing,
      final String description) {
    final List<Long> expected = tsOf("SELECT FROM " + types[1] + " WHERE " + where, parameters);
    final List<Long> actual;
    try {
      actual = tsOf("SELECT FROM " + types[0] + " WHERE " + where, parameters);
    } catch (final RuntimeException e) {
      differing.add(where + " " + description + ": refused (" + e.getMessage() + ") where the document type answers " + expected);
      return;
    }
    if (!expected.equals(actual))
      differing.add(where + " " + description + ": " + actual + " where the document type answers " + expected);
  }

  /** Runs the same DELETE on a fresh pair of types and compares what each one keeps. */
  private void compareDelete(final String where, final Map<String, Object> parameters, final List<String> differing, final String description) {
    final String[] types = createSamples();
    final List<Long> expected;
    try {
      database.transaction(() -> database.command("sql", "DELETE FROM " + types[1] + " WHERE " + where, parameters));
      expected = tsOf("SELECT FROM " + types[1]);
    } catch (final RuntimeException e) {
      // the document type refuses the statement (a NaN comparison, for instance): there is no answer to hold the series to
      return;
    }
    try {
      database.command("sql", "DELETE FROM " + types[0] + " WHERE " + where, parameters);
    } catch (final RuntimeException e) {
      // `<>` against a whole millisecond is two ranges, which a DELETE of a series does not take: refused, and nothing removed
      if (where.contains("<>") && tsOf("SELECT FROM " + types[0]).size() == 3)
        return;
      differing.add(where + " " + description + ": refused (" + e.getMessage() + ") where the document type keeps " + expected);
      return;
    }
    final List<Long> actual = tsOf("SELECT FROM " + types[0]);
    if (!expected.equals(actual))
      differing.add(where + " " + description + ": keeps " + actual + " where the document type keeps " + expected);
  }

  private List<Long> tsOf(final String sql) {
    return tsOf(sql, Map.of());
  }

  private List<Long> tsOf(final String sql, final Map<String, Object> parameters) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet resultSet = database.query("sql", sql, parameters)) {
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        final Object ts = row.getProperty("ts");
        values.add(epochMillisOf(ts));
      }
    }
    values.sort(Long::compare);
    return values;
  }

  /** The timestamp column answers a datetime, the plain document type the number it was given. */
  private static long epochMillisOf(final Object ts) {
    if (ts instanceof Number number)
      return number.longValue();
    if (ts instanceof LocalDateTime dateTime)
      return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    return ((Date) ts).getTime();
  }
}
