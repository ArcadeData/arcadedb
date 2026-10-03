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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8979: the SQL comparison operators answered false or true for a null operand instead of unknown, so a
 * NOT around them (or {@code <>} itself) turned a missing value into a match, disagreeing with {@code NOT IN}, openCypher and
 * the documented CASE behavior.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8979NullComparisonsThreeValuedTest extends TestHelper {

  @BeforeEach
  void load() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.k INTEGER");
    database.transaction(() -> {
      database.newVertex("T").set("name", "one", "k", 1).save();
      database.newVertex("T").set("name", "five", "k", 5).save();
      database.newVertex("T").set("name", "null", "k", null).save();
      database.newVertex("T").set("name", "missing").save();
    });
  }

  private List<String> names(final String where) {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE " + where + " ORDER BY name")) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("name"));
    }
    return got;
  }

  @Test
  void negatedComparisonsLeaveOutTheRecordsWithoutAValue() {
    for (final String where : new String[] { "k <> 5", "k != 5", "NOT (k = 5)", "k NOT IN [5]", "NOT (k IN [5])", "NOT (k > 2)",
        "NOT (k BETWEEN 4 AND 6)", "NOT (k >= 2)", "NOT (k > 1 AND k > 2)" })
      assertThat(names(where)).as(where).containsExactly("one");
  }

  @Test
  void comparisonWithNullMatchesNothing() {
    for (final String where : new String[] { "k = null", "k <> null", "k != null", "k < null", "k >= null", "k >= nothing",
        "k <= nothing", "k > nothing", "k < nothing", "k = nothing", "k <> nothing" })
      assertThat(names(where)).as(where).isEmpty();
  }

  @Test
  void explicitNullTestsStillWork() {
    assertThat(names("k IS NULL")).containsExactly("missing", "null");
    assertThat(names("k IS NOT NULL")).containsExactly("five", "one");
    assertThat(names("k <=> null")).containsExactly("missing", "null");
    assertThat(names("NOT (k IS NULL)")).containsExactly("five", "one");
    assertThat(names("NOT (k < 0)")).containsExactly("five", "one");
  }

  @Test
  void booleanLogicCarriesTheUnknown() {
    assertThat(names("k <> 5 OR k = 5")).containsExactly("five", "one");
    assertThat(names("k = 5 OR k IS NULL")).containsExactly("five", "missing", "null");
    assertThat(names("NOT (k = 5 AND name = 'x')")).containsExactly("five", "missing", "null", "one");
  }

  @Test
  void caseAnswersNoForAMissingValue() {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT name, CASE WHEN k <> 5 THEN 'yes' ELSE 'no' END AS ne FROM T ORDER BY name")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        got.add(r.getProperty("name") + ":" + r.getProperty("ne"));
      }
    }
    assertThat(got).containsExactly("five:no", "missing:no", "null:no", "one:yes");
  }

  @Test
  void indexedAndOrderedPathsAgreeWithTheScan() {
    database.command("sql", "CREATE INDEX ON T (k) NOTUNIQUE");
    assertThat(names("k <> 5")).containsExactly("one");
    assertThat(names("NOT (k > 2)")).containsExactly("one");
    try (final ResultSet rs = database.query("sql", "SELECT name FROM T WHERE k <> 5 ORDER BY k")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("one");
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
