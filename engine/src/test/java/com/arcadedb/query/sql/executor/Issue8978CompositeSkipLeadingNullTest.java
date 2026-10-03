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
 * Regression guard for #8978: a composite index with the default NULL_STRATEGY SKIP holds a key whose first property is null
 * when a later one is not, so ORDER BY on the leading property returned those records twice and an upper-bound-only range
 * returned them as matches.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8978CompositeSkipLeadingNullTest extends TestHelper {

  @BeforeEach
  void load() {
    for (final String type : new String[] { "Plain", "Composite" }) {
      database.command("sql", "CREATE VERTEX TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".a INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".b INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
    }
    database.command("sql", "CREATE INDEX ON Composite (a, b) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "Plain", "Composite" }) {
        database.newVertex(type).set("name", "r1", "a", 1, "b", 1).save();
        database.newVertex(type).set("name", "r2", "a", 2, "b", null).save();
        database.newVertex(type).set("name", "r3", "a", null, "b", 3).save();
        database.newVertex(type).set("name", "r4", "a", null, "b", null).save();
        database.newVertex(type).set("name", "r5", "b", 5).save();
      }
    });
  }

  private List<String> names(final String sql) {
    final List<String> got = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        got.add(rs.next().getProperty("name"));
    }
    return got;
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }

  @Test
  void orderByLeadingPropertyReturnsEachRecordOnce() {
    final List<String> got = names("SELECT name FROM Composite ORDER BY a");
    assertThat(got).hasSize(5).containsExactlyInAnyOrder("r1", "r2", "r3", "r4", "r5");
    assertThat(got.subList(3, 5)).containsExactly("r1", "r2");

    final List<String> desc = names("SELECT name FROM Composite ORDER BY a DESC");
    assertThat(desc).hasSize(5).containsExactlyInAnyOrder("r1", "r2", "r3", "r4", "r5");
    assertThat(desc.subList(0, 2)).containsExactly("r2", "r1");
  }

  @Test
  void orderByAllIndexedPropertiesSortsTheNullRows() {
    assertThat(names("SELECT name FROM Composite ORDER BY a, b")).isEqualTo(names("SELECT name FROM Plain ORDER BY a, b"));
    assertThat(names("SELECT name FROM Composite ORDER BY a, b")).containsExactly("r4", "r3", "r5", "r1", "r2");
    assertThat(names("SELECT name FROM Composite ORDER BY a DESC, b DESC")).containsExactly("r2", "r1", "r5", "r3", "r4");
  }

  @Test
  void groupByLeadingPropertyCountsTheNullGroupOnce() {
    try (final ResultSet rs = database.query("sql", "SELECT a, count(*) AS n FROM Composite GROUP BY a ORDER BY a")) {
      final List<Long> counts = new ArrayList<>();
      while (rs.hasNext())
        counts.add(rs.next().<Number>getProperty("n").longValue());
      assertThat(counts).containsExactly(3L, 1L, 1L);
    }
  }

  @Test
  void upperBoundOnlyRangeSkipsLeadingNullKeys() {
    assertThat(names("SELECT name FROM Composite WHERE a < 5")).containsExactlyInAnyOrder("r1", "r2");
    assertThat(count("SELECT count(*) AS n FROM Composite WHERE a <= 2")).isEqualTo(2);
    assertThat(count("SELECT count(*) AS n FROM Composite WHERE a < 0")).isZero();
    assertThat(names("SELECT name FROM Composite WHERE a < 5 ORDER BY a DESC")).containsExactly("r2", "r1");
    assertThat(names("SELECT name FROM Composite WHERE a IS NULL")).containsExactlyInAnyOrder("r3", "r4", "r5");
  }

  @Test
  void everyComparisonOperatorAgreesWithTheScan() {
    for (final String where : new String[] { "a < 2", "a <= 2", "a > 0", "a >= 1", "a <> 1", "a != 2", "a = 1", "a BETWEEN 1 AND 2",
        "NOT (a > 1)", "a <= 2 AND b IS NULL" }) {
      assertThat(names("SELECT name FROM Composite WHERE " + where + " ORDER BY name")).as(where)
          .isEqualTo(names("SELECT name FROM Plain WHERE " + where + " ORDER BY name"));
      assertThat(names("SELECT name FROM Composite WHERE " + where + " ORDER BY a DESC")).as(where + " DESC")
          .containsExactlyInAnyOrderElementsOf(names("SELECT name FROM Plain WHERE " + where + " ORDER BY name"));
    }
  }
}
