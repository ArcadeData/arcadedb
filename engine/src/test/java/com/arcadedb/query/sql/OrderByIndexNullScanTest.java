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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8664: an ascending {@code ORDER BY} over an indexed property scanned the whole type looking for
 * null values before reading the index, even when the schema (NOTNULL) or the WHERE clause guarantees there are none.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OrderByIndexNullScanTest extends TestHelper {

  private String plan(final String query) {
    try (final ResultSet rs = database.command("sql", "EXPLAIN " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  private List<Integer> values(final String query) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty("x"));
      }
    }
    return out;
  }

  private void load(final String type, final boolean notNull, final boolean withNulls) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".x INTEGER" + (notNull ? " (notnull true, mandatory true)" : ""));
    database.command("sql", "CREATE INDEX ON " + type + " (x) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 20; i > 0; i--)
        database.newDocument(type).set("x", i).save();
      if (withNulls)
        database.newDocument(type).save();
    });
  }

  @Test
  void notNullPropertySkipsNullScan() {
    load("N", true, false);
    assertThat(plan("SELECT x FROM N ORDER BY x LIMIT 3")).doesNotContain("PARALLEL").doesNotContain("FETCH FROM TYPE");
    assertThat(values("SELECT x FROM N ORDER BY x LIMIT 3")).containsExactly(1, 2, 3);
    assertThat(values("SELECT x FROM N ORDER BY x DESC LIMIT 3")).containsExactly(20, 19, 18);
  }

  @Test
  void whereExcludingNullsSkipsNullScan() {
    load("D", false, true);
    assertThat(plan("SELECT x FROM D WHERE x IS NOT NULL ORDER BY x LIMIT 3")).doesNotContain("FETCH FROM TYPE");
    assertThat(values("SELECT x FROM D WHERE x IS NOT NULL ORDER BY x LIMIT 3")).containsExactly(1, 2, 3);
    assertThat(values("SELECT x FROM D WHERE x > 5 AND x IS NOT NULL ORDER BY x LIMIT 2")).containsExactly(6, 7);
  }

  @Test
  void nullableAscendingStillReturnsNullsFirst() {
    load("D", false, true);
    assertThat(plan("SELECT x FROM D ORDER BY x LIMIT 3")).contains("FETCH FROM TYPE");
    final List<Integer> asc = values("SELECT x FROM D ORDER BY x LIMIT 3");
    assertThat(asc).hasSize(3);
    assertThat(asc.get(0)).isNull();
    assertThat(values("SELECT x FROM D WHERE x IS NULL OR x < 3 ORDER BY x")).hasSize(3);
  }

  /** A comparison with a null operand is unknown (#8979), so {@code >=} and {@code <=} exclude null rows like {@code >}. */
  @Test
  void selfComparisonWithGreaterOrEqualExcludesTheNullRow() {
    load("D", false, true);
    assertThat(plan("SELECT x FROM D WHERE x >= x ORDER BY x LIMIT 3")).doesNotContain("FETCH FROM TYPE");
    assertThat(values("SELECT x FROM D WHERE x >= x ORDER BY x")).hasSize(20).doesNotContainNull();
  }

  /** Only one OR branch excludes nulls: the other can match a null row, so the null sub-plan must stay. */
  @Test
  void orBranchWithoutNullExclusionKeepsTheNullScan() {
    load("D", false, true);
    assertThat(plan("SELECT x FROM D WHERE x > 18 OR x < 3 ORDER BY x")).doesNotContain("FETCH FROM TYPE");
    assertThat(plan("SELECT x FROM D WHERE x > 18 OR x IS NULL ORDER BY x")).contains("FETCH FROM TYPE");
    assertThat(values("SELECT x FROM D WHERE x > 18 OR x IS NULL ORDER BY x")).hasSize(3);
  }

  /** Issue #8701: NOTNULL without MANDATORY accepts a record that never sets the property, which the index does not hold. */
  @Test
  void notNullWithoutMandatoryKeepsRecordsMissingTheProperty() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.x INTEGER (notnull true)");
    database.command("sql", "CREATE PROPERTY T.id STRING");
    database.command("sql", "CREATE INDEX ON T (x) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 4; i++)
        database.newDocument("T").set("x", i).set("id", "v" + i).save();
      database.newDocument("T").set("id", "missing").save();
    });

    assertThat(plan("SELECT FROM T ORDER BY x")).contains("FETCH FROM TYPE");
    for (final String q : new String[] { "SELECT FROM T ORDER BY x", "SELECT x, id FROM T ORDER BY x",
        "SELECT FROM T ORDER BY x LIMIT 10" }) {
      final List<String> ids = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", q)) {
        while (rs.hasNext())
          ids.add(rs.next().getProperty("id"));
      }
      assertThat(ids).as(q).containsExactly("missing", "v0", "v1", "v2", "v3");
    }
  }
}
