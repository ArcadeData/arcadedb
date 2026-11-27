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
package com.arcadedb.query.sql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12936: a comparison answered other rows with an index on the property than by a scan. The scan read a
 * numeric string ordered against a DATETIME as an epoch count whose precision its digit count tells (issue #5956), while the
 * index lookup could not make a key of it and answered nothing; a DECIMAL compared with a boolean was ordered by the scan by the
 * name of the boolean's class (every row above {@code true}) and answered nothing by the index. Both paths now read the numeric
 * string as the scan did, and match no DECIMAL against a boolean, as no other numeric type does.
 */
class Issue12936IndexVersusScanCoercionTest extends TestHelper {
  private static final String[] PREDICATES = {
      // from the issue
      "dt > '10'", "dt < '10'", "dt >= '10'", "dt <= '10'", "dt = '5'", "dt > '2023'", "dec > true", "dec >= true", "dec < true",
      "dec <= true", "dec = true",
      // a numeric string of epoch seconds and of epoch milliseconds that names a stored value, as bounds and in BETWEEN
      "dt >= '1704067200'", "dt > '1704067200'", "dt < '1704067200'", "dt <= '1704067200000'", "dt > '1704067200000'",
      "dt BETWEEN '10' AND '1704067200'", "dt > '10' AND dt < '1704067200'",
      // controls that already agreed
      "dt > 10", "dt < 10", "dt = 5", "dt > '2023-06-01 12:00:00'", "dt <= '2024-01-01 00:00:00'", "dec > '1'", "dec < 2", "dec = 1.5",
      "dec > 'a'", "dec BETWEEN 1 AND 2" };

  /**
   * On the second fixture: a DATE key, a DATETIME past 2262 (the last instant a long count of nanoseconds holds), a DATETIME with
   * milliseconds, and an INTEGER beside the DECIMAL.
   */
  private static final String[] EDGE_PREDICATES = {
      // a DATE converts the numeric string to a day on both paths, so its key is not mapped and has to keep agreeing
      "d > '10'", "d < '10'", "d >= '1704067200'", "d > '1704067200'", "d <= '1704067199'", "d < '1704067200000'",
      "d >= '1704067200000000'", "d BETWEEN '10' AND '1704067200'", "d = '10'",
      // a digit string no long holds, and the empty string: no epoch count on either path
      "dt > '99999999999999999999'", "dt < '99999999999999999999'", "d >= '99999999999999999999'", "dt > ''", "dt <= ''",
      // 9999999999 seconds is in 2286, past 2262, the last instant the scan's count of nanoseconds holds (as the row of 2270)
      "dt < '9999999999'", "dt <= '9999999999'", "dt > '9999999999'", "dt >= '9999999999'", "d < '9999999999'",
      "dt < '9223372036'", "dt > '9223372036'",
      // counts of microseconds and nanoseconds carry more than the stored millisecond, and the key the index compares the bound with
      "dt >= '1704067200123999'", "dt > '1704067200123999'", "dt <= '1704067200122999'", "dt >= '1704067200123000001'",
      "dt > '1704067200122999999'", "dt < '1704067200123000001'", "dt <= '1704067200123000001'",
      // a composite index with the numeric string on its range slot
      "k = 1 AND dt > '10'", "k = 1 AND dt < '9999999999'", "k = 1 AND dt >= '1704067200123999'",
      "k = 1 AND dt BETWEEN '10' AND '1704067200123'",
      // a DECIMAL against a boolean answers what an INTEGER does, whatever the operator
      "dec <> true", "n <> true", "dec != false", "dec > false", "n > false" };

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      for (final String typeName : new String[] { "Plain2", "Indexed2", "IndexedComposite" }) {
        database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
        database.command("sql", "CREATE PROPERTY " + typeName + ".id INTEGER");
        database.command("sql", "CREATE PROPERTY " + typeName + ".k INTEGER");
        database.command("sql", "CREATE PROPERTY " + typeName + ".d DATE");
        database.command("sql", "CREATE PROPERTY " + typeName + ".dt DATETIME");
        database.command("sql", "CREATE PROPERTY " + typeName + ".n INTEGER");
        database.command("sql", "CREATE PROPERTY " + typeName + ".dec DECIMAL");
        if (typeName.equals("Indexed2"))
          for (final String property : new String[] { "d", "dt", "n", "dec" })
            database.command("sql", "CREATE INDEX ON " + typeName + " (" + property + ") NOTUNIQUE");
        else if (typeName.equals("IndexedComposite"))
          database.command("sql", "CREATE INDEX ON " + typeName + " (k, dt) NOTUNIQUE");
        database.newDocument(typeName).set("id", 1).set("k", 1).set("d", LocalDate.of(1970, 1, 5))
            .set("dt", LocalDateTime.of(2270, 1, 1, 0, 0)).set("n", 1).set("dec", new BigDecimal("1.5")).save();
        database.newDocument(typeName).set("id", 2).set("k", 1).set("d", LocalDate.of(2024, 1, 1))
            .set("dt", LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_000_000)).set("n", 2).set("dec", new BigDecimal("2.25")).save();
        database.newDocument(typeName).set("id", 3).set("k", 1).set("d", LocalDate.of(1969, 12, 31))
            .set("dt", LocalDateTime.of(1960, 1, 1, 0, 0)).set("n", 0).set("dec", BigDecimal.ZERO).save();
      }
    });
    database.transaction(() -> {
      for (final String typeName : new String[] { "Plain", "Indexed" }) {
        database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
        database.command("sql", "CREATE PROPERTY " + typeName + ".id INTEGER");
        database.command("sql", "CREATE PROPERTY " + typeName + ".dt DATETIME");
        database.command("sql", "CREATE PROPERTY " + typeName + ".dec DECIMAL");
        if (typeName.equals("Indexed")) {
          database.command("sql", "CREATE INDEX ON " + typeName + " (dt) NOTUNIQUE");
          database.command("sql", "CREATE INDEX ON " + typeName + " (dec) NOTUNIQUE");
        }
        database.newDocument(typeName).set("id", 1).set("dt", LocalDateTime.of(2024, 1, 1, 0, 0)).set("dec", new BigDecimal("1.5"))
            .save();
        database.newDocument(typeName).set("id", 2).set("dt", LocalDateTime.of(2023, 6, 1, 12, 0)).set("dec", new BigDecimal("2.25"))
            .save();
        database.newDocument(typeName).set("id", 3).set("dt", LocalDateTime.of(1970, 1, 1, 0, 0, 0, 5_000_000))
            .set("dec", BigDecimal.ZERO).save();
      }
    });
  }

  @Test
  void indexAnswersWhatTheScanAnswers() {
    final List<String> mismatches = new ArrayList<>();
    for (final String predicate : PREDICATES) {
      final String scan = rows("SELECT id FROM Plain WHERE " + predicate);
      final String indexed = rows("SELECT id FROM Indexed WHERE " + predicate);
      if (!scan.equals(indexed))
        mismatches.add(predicate + ": scan " + scan + ", index " + indexed);
    }
    assertThat(mismatches).as("predicates answered differently with an index").isEmpty();
  }

  @Test
  void indexIsUsed() {
    try (final ResultSet result = database.query("sql", "EXPLAIN SELECT id FROM Indexed WHERE dt > '10'")) {
      assertThat(result.next().<String>getProperty("executionPlanAsString")).contains("FETCH FROM INDEX");
    }
  }

  @Test
  void reportedCases() {
    for (final String typeName : new String[] { "Plain", "Indexed" }) {
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt > '10'")).isEqualTo("[1, 2]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt < '10'")).isEqualTo("[3]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt > '2023'")).isEqualTo("[1, 2]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dec > true")).isEqualTo("[]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dec >= true")).isEqualTo("[]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dec < true")).isEqualTo("[]");
      // 1704067200 is 2024-01-01 00:00:00 in epoch seconds, 1704067200000 the same moment in epoch milliseconds
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt >= '1704067200'")).isEqualTo("[1]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt > '1704067200000'")).isEqualTo("[]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt BETWEEN '10' AND '1704067200'")).isEqualTo("[1, 2]");
    }
  }

  @Test
  void edgeCasesAnswerWhatTheScanAnswers() {
    final List<String> mismatches = new ArrayList<>();
    for (final String predicate : EDGE_PREDICATES) {
      final String scan = rows("SELECT id FROM Plain2 WHERE " + predicate);
      for (final String indexedType : new String[] { "Indexed2", "IndexedComposite" }) {
        final String indexed = rows("SELECT id FROM " + indexedType + " WHERE " + predicate);
        if (!scan.equals(indexed))
          mismatches.add(indexedType + " " + predicate + ": scan " + scan + ", index " + indexed);
      }
    }
    assertThat(mismatches).as("predicates answered differently with an index").isEmpty();
  }

  @Test
  void edgeCasesUseTheIndex() {
    for (final String sql : new String[] { "SELECT id FROM Indexed2 WHERE d > '10'", "SELECT id FROM Indexed2 WHERE dt < '9999999999'",
        "SELECT id FROM IndexedComposite WHERE k = 1 AND dt > '10'" })
      try (final ResultSet result = database.query("sql", "EXPLAIN " + sql)) {
        assertThat(result.next().<String>getProperty("executionPlanAsString")).as(sql).contains("FETCH FROM INDEX");
      }
  }

  @Test
  void edgeCases() {
    for (final String typeName : new String[] { "Plain2", "Indexed2" }) {
      // the scan compares in nanoseconds: 1704067200123999 microseconds is after 2024-01-01 00:00:00.123, and so is the count of
      // nanoseconds one past it
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt >= '1704067200123999'")).isEqualTo("[1]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt <= '1704067200123999'")).isEqualTo("[2, 3]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt <= '1704067200123000001'")).isEqualTo("[2, 3]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt < '1704067200123000000'")).isEqualTo("[3]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt >= '1704067200123000000'")).isEqualTo("[1, 2]");
      // no epoch count: no row, not an error
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt > '99999999999999999999'")).isEqualTo("[]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dt < '99999999999999999999'")).isEqualTo("[]");
      // a DECIMAL equals no boolean, as an INTEGER does not, and is neither above nor below one
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dec <> true")).isEqualTo(rows("SELECT id FROM " + typeName + " WHERE n <> true"));
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dec <> true")).isEqualTo("[1, 2, 3]");
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dec > false")).isEqualTo(rows("SELECT id FROM " + typeName + " WHERE n > false"));
      assertThat(rows("SELECT id FROM " + typeName + " WHERE dec > false")).isEqualTo("[]");
    }
    assertThat(rows("SELECT id FROM IndexedComposite WHERE k = 1 AND dt BETWEEN '10' AND '1704067200123'")).isEqualTo("[2]");
  }

  /**
   * A ZONED_DATETIME key orders one instant in two zones as two values, by their offset: a bound built from a numeric string would
   * leave some zones of its instant on the other side. The scan compares no ZONED_DATETIME with such a string, so the index has no
   * bound to build and declines.
   */
  @Test
  void zonedDateTimeKeys() {
    database.transaction(() -> {
      for (final String typeName : new String[] { "PlainZoned", "IndexedZoned" }) {
        database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
        database.command("sql", "CREATE PROPERTY " + typeName + ".id INTEGER");
        database.command("sql", "CREATE PROPERTY " + typeName + ".zdt ZONED_DATETIME");
        if (typeName.startsWith("Indexed"))
          database.command("sql", "CREATE INDEX ON " + typeName + " (zdt) NOTUNIQUE");
        // 2024-01-01 00:00:00 UTC, in three zones, and one second later
        final ZonedDateTime utc = ZonedDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        database.newDocument(typeName).set("id", 1).set("zdt", utc).save();
        database.newDocument(typeName).set("id", 2).set("zdt", utc.withZoneSameInstant(ZoneOffset.ofHours(2))).save();
        database.newDocument(typeName).set("id", 3).set("zdt", utc.withZoneSameInstant(ZoneOffset.ofHours(-5))).save();
        database.newDocument(typeName).set("id", 4).set("zdt", utc.plusSeconds(1).withZoneSameInstant(ZoneId.of("Europe/Rome"))).save();
      }
    });
    final List<String> mismatches = new ArrayList<>();
    for (final String predicate : new String[] { "zdt >= '1704067200'", "zdt <= '1704067200'", "zdt > '1704067200'",
        "zdt < '1704067200'", "zdt BETWEEN '1704067200' AND '1704067200'", "zdt > '1704067200000'", "zdt >= '1704067201'",
        "zdt < '1704067201000000'", "zdt > '99999999999999999999'" }) {
      final String scan = rows("SELECT id FROM PlainZoned WHERE " + predicate);
      final String indexed = rows("SELECT id FROM IndexedZoned WHERE " + predicate);
      if (!scan.equals(indexed))
        mismatches.add(predicate + ": scan " + scan + ", index " + indexed);
    }
    assertThat(mismatches).as("predicates answered differently with an index").isEmpty();
    // the scan compares no ZONED_DATETIME with a numeric string, and the index declines as it does
    assertThat(rows("SELECT id FROM IndexedZoned WHERE zdt <= '1704067200'")).isEqualTo("[]");
    try (final ResultSet result = database.query("sql", "EXPLAIN SELECT id FROM IndexedZoned WHERE zdt >= '1704067200'")) {
      assertThat(result.next().<String>getProperty("executionPlanAsString")).contains("FETCH FROM INDEX");
    }
  }

  /** The bound takes the class the key reads back as, whichever implementation of DATETIME is configured. */
  @Test
  void otherDateTimeImplementations() {
    for (final Class<?> implementation : new Class<?>[] { Instant.class, ZonedDateTime.class }) {
      GlobalConfiguration.DATE_TIME_IMPLEMENTATION.setValue(implementation);
      ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(implementation);
      try {
        final String suffix = implementation.getSimpleName();
        database.transaction(() -> {
          for (final String typeName : new String[] { "Plain" + suffix, "Indexed" + suffix }) {
            database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
            database.command("sql", "CREATE PROPERTY " + typeName + ".id INTEGER");
            database.command("sql", "CREATE PROPERTY " + typeName + ".dt DATETIME");
            if (typeName.startsWith("Indexed"))
              database.command("sql", "CREATE INDEX ON " + typeName + " (dt) NOTUNIQUE");
            database.command("sql", "INSERT INTO " + typeName + " SET id = 1, dt = '2270-01-01 00:00:00'");
            database.command("sql", "INSERT INTO " + typeName + " SET id = 2, dt = '2024-01-01 00:00:00.123'");
            database.command("sql", "INSERT INTO " + typeName + " SET id = 3, dt = '1960-01-01 00:00:00'");
          }
        });
        final List<String> mismatches = new ArrayList<>();
        for (final String predicate : new String[] { "dt > '10'", "dt < '10'", "dt >= '1704067200123'", "dt > '1704067200123'",
            "dt <= '1704067200123999'", "dt < '9999999999'", "dt >= '1704067200123000001'", "dt > '99999999999999999999'" }) {
          final String scan = rows("SELECT id FROM Plain" + suffix + " WHERE " + predicate);
          final String indexed = rows("SELECT id FROM Indexed" + suffix + " WHERE " + predicate);
          if (!scan.equals(indexed))
            mismatches.add(suffix + " " + predicate + ": scan " + scan + ", index " + indexed);
        }
        assertThat(mismatches).as("predicates answered differently with an index").isEmpty();
        // an Instant reads the string as an epoch count on the scan, a ZonedDateTime is not compared with it
        assertThat(rows("SELECT id FROM Indexed" + suffix + " WHERE dt >= '1704067200123'"))
            .isEqualTo(implementation == Instant.class ? "[1, 2]" : "[]");
      } finally {
        GlobalConfiguration.DATE_TIME_IMPLEMENTATION.reset();
        ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(LocalDateTime.class);
      }
    }
  }

  /** The ids the query answers, sorted, or the class of the exception it raised. */
  private String rows(final String sql) {
    final TreeSet<Integer> ids = new TreeSet<>();
    try (final ResultSet result = database.query("sql", sql)) {
      while (result.hasNext())
        ids.add(result.next().<Integer>getProperty("id"));
    } catch (final RuntimeException e) {
      return "error " + e.getClass().getSimpleName();
    }
    return ids.toString();
  }
}
