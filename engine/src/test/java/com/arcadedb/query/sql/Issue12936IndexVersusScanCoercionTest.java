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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
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

  @Override
  protected void beginTest() {
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
