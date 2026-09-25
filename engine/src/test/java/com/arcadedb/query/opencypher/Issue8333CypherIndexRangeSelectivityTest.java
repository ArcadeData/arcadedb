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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8333, the Cypher side: the optimizer anchors a pattern on an index range whenever a range predicate meets an
 * indexed property, with a fixed guess of how much of the label it holds. A range over most of the label was then
 * fetched one random page access per vertex. The range scan now reads the matching entries first and either loads the
 * vertices in physical order or gives way to a scan of the label, whose rows the pattern's WHERE filters the same way -
 * wherever the order the rows arrive in cannot show in the output: an aggregating RETURN, or an ORDER BY. A query
 * returning its rows as they come keeps the index order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8333CypherIndexRangeSelectivityTest extends TestHelper {
  private static final int PERSONS = 1_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.age INTEGER");
    database.command("sql", "CREATE INDEX ON Person (age) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < PERSONS; i++)
        // Inserted in a shuffled age order, so key order and physical order differ
        database.newVertex("Person").set("id", i).set("age", (int) ((i * 7919L) % PERSONS)).save();
    });
  }

  @Test
  void nonSelectiveRangeIsServedByALabelScan() {
    final String query = "MATCH (p:Person) WHERE p.age >= 20 RETURN p.id AS id ORDER BY id";
    assertThat(ids(query, Map.of())).isEqualTo(expected(20, PERSONS));
    assertThat(profile(query, Map.of())).contains("NodeIndexRangeScan").contains("served by label scan");
  }

  @Test
  void selectiveRangeIsServedInPhysicalOrder() {
    final String query = "MATCH (p:Person) WHERE p.age >= 100 AND p.age < 150 RETURN p.id AS id ORDER BY id";
    assertThat(ids(query, Map.of())).isEqualTo(expected(100, 150));
    assertThat(profile(query, Map.of())).contains("served in physical order");
  }

  @Test
  void upperBoundAloneStopsInsideTheIndex() {
    final String query = "MATCH (p:Person) WHERE p.age < $max RETURN p.id AS id ORDER BY id";
    assertThat(ids(query, Map.of("max", 30))).isEqualTo(expected(0, 30));
    assertThat(profile(query, Map.of("max", 30))).contains("served in physical order");
    // The same plan with a bound that covers most of the label
    assertThat(ids(query, Map.of("max", 900))).isEqualTo(expected(0, 900));
    assertThat(profile(query, Map.of("max", 900))).contains("served by label scan");
  }

  @Test
  void rowsReturnedAsTheyComeKeepTheIndexOrder() {
    for (final String query : new String[] { "MATCH (p:Person) WHERE p.age >= 20 RETURN p.age AS age",
        "MATCH (p:Person) WHERE p.age >= 20 RETURN p.age AS age LIMIT 5" }) {
      int previous = -1;
      try (final ResultSet rs = database.query("opencypher", query)) {
        while (rs.hasNext()) {
          final int age = rs.next().<Integer>getProperty("age");
          assertThat(age).as(query).isGreaterThanOrEqualTo(previous);
          previous = age;
        }
      }
      assertThat(profile(query, Map.of())).as(query).contains("NodeIndexRangeScan").doesNotContain("served")
          .doesNotContain("physical order");
    }
  }

  @Test
  void anAggregationOverAWideRange() {
    final String query = "MATCH (p:Person) WHERE p.age > 499 RETURN sum(p.age) AS s";
    assertThat(profile(query, Map.of())).contains("served by label scan");
    try (final ResultSet rs = database.query("opencypher", query)) {
      long expected = 0;
      for (int age = 500; age < PERSONS; age++)
        expected += age;
      assertThat(rs.next().<Number>getProperty("s").longValue()).isEqualTo(expected);
    }
  }

  @Test
  void orderSensitiveAggregationsKeepTheIndexOrder() {
    for (final String query : new String[] { "MATCH (p:Person) WHERE p.age >= 20 RETURN collect(p.id) AS ids",
        "MATCH (p:Person) WHERE p.age >= 20 RETURN p.age % 3 AS k, count(*) AS n LIMIT 1" })
      assertThat(profile(query, Map.of())).as(query).contains("NodeIndexRangeScan").doesNotContain("served");
  }

  private TreeSet<Integer> ids(final String query, final Map<String, Object> params) {
    final TreeSet<Integer> ids = new TreeSet<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        ids.add(row.<Integer>getProperty("id"));
      }
    }
    return ids;
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  /** The ids of the persons whose age is in [from, to). */
  private static TreeSet<Integer> expected(final int from, final int to) {
    final TreeSet<Integer> ids = new TreeSet<>();
    for (int i = 0; i < PERSONS; i++) {
      final int age = (int) ((i * 7919L) % PERSONS);
      if (age >= from && age < to)
        ids.add(i);
    }
    return ids;
  }
}
