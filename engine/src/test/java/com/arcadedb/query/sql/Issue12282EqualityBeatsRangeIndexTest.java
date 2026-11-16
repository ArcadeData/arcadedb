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
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An equality (or an IN list) on one indexed property next to a range on another indexed property fetches from the index of the
 * equality (issue #12282). Both descriptors cover one condition with a whole key and, without recorded statistics, every candidate
 * cost the same: the first index the type listed won, and an open range on it read most of the type to return a handful of records.
 */
class Issue12282EqualityBeatsRangeIndexTest extends TestHelper {
  private static final int          RECORDS = 2_000;
  // T1 creates the index of the equality first, T2 the index of the range first: the answer cannot come from the creation order
  private static final List<String> TYPES   = List.of("T1", "T2");

  @Override
  protected void beginTest() {
    for (final String type : TYPES) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 4");
      database.command("sql", "CREATE PROPERTY " + type + ".cat STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".score DOUBLE");
      final List<String> order = type.equals("T1") ? List.of("cat", "name", "score") : List.of("score", "name", "cat");
      for (final String property : order)
        database.command("sql",
            "CREATE INDEX ON " + type + " (" + property + ") " + (property.equals("name") ? "UNIQUE" : "NOTUNIQUE NULL_STRATEGY SKIP"));
      // A subtype inherits the indexes: the polymorphic query reads its records through them too
      database.command("sql", "CREATE DOCUMENT TYPE " + type + "Sub EXTENDS " + type);

      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++) {
          final String target = i % 10 == 9 ? type + "Sub" : type;
          final Object cat = i % 100 == 0 ? "rare" : i % 333 == 1 ? null : "c" + (i % 50);
          final Object score = i % 250 == 0 ? null : 2.0 + (i * 7919 % 1000) / 10.0;
          database.newDocument(target).set("cat", cat).set("name", "n" + i).set("score", score).save();
        }
      });
      // deleted records must stay out of the answer whatever index serves it
      database.transaction(() -> database.command("sql", "DELETE FROM " + type + " WHERE name = 'n300' OR name = 'n700'"));
    }
  }

  @Test
  void anEqualityBeatsAnOpenRange() {
    for (final String type : TYPES) {
      assertServedBy(type, "SELECT FROM " + type + " WHERE cat = 'rare' AND score > 1", Map.of(), "cat",
          d -> "rare".equals(d.getString("cat")) && greater(d, 1));
      assertServedBy(type, "SELECT FROM " + type + " WHERE score > 1 AND cat = 'rare'", Map.of(), "cat",
          d -> "rare".equals(d.getString("cat")) && greater(d, 1));
      assertServedBy(type, "SELECT FROM " + type + " WHERE cat = :cat AND score >= :min", Map.of("cat", "rare", "min", 50.0), "cat",
          d -> "rare".equals(d.getString("cat")) && d.getDouble("score") != null && d.getDouble("score") >= 50.0);
    }
  }

  @Test
  void anEqualityBeatsAClosedRange() {
    for (final String type : TYPES) {
      assertServedBy(type, "SELECT FROM " + type + " WHERE cat = 'rare' AND score BETWEEN 1 AND 2000", Map.of(), "cat",
          d -> "rare".equals(d.getString("cat")) && greater(d, 1));
      // two bounds on score: the second one is the descriptor's additional range condition
      assertServedBy(type, "SELECT FROM " + type + " WHERE score > 3 AND cat = 'rare' AND score < 60", Map.of(), "cat",
          d -> "rare".equals(d.getString("cat")) && greater(d, 3) && d.getDouble("score") < 60);
    }
  }

  @Test
  void anInListBeatsARange() {
    for (final String type : TYPES)
      assertServedBy(type, "SELECT FROM " + type + " WHERE cat IN ['rare', 'c7'] AND score < 30", Map.of(), "cat",
          d -> ("rare".equals(d.getString("cat")) || "c7".equals(d.getString("cat"))) && d.getDouble("score") != null
              && d.getDouble("score") < 30);
  }

  @Test
  void anEqualityOnAUniqueKeyBeatsARange() {
    for (final String type : TYPES)
      assertServedBy(type, "SELECT FROM " + type + " WHERE name = :name AND score > :min", Map.of("name", "n509", "min", 1), "name",
          d -> "n509".equals(d.getString("name")));
  }

  @Test
  void anEqualityOnANullKeyFindsNothing() {
    for (final String type : TYPES)
      assertServedBy(type, "SELECT FROM " + type + " WHERE cat = :cat AND score > 1", Collections.singletonMap("cat", null), "cat",
          d -> false);
  }

  @Test
  void twoRangesStillPlanAsBefore() {
    // no equality among the candidates: the choice is left to the rules that ran before (here the order of the indexes)
    for (final String type : TYPES)
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " WHERE cat > 'r' AND score > 100")) {
        assertThat(names(rs)).containsExactlyInAnyOrderElementsOf(
            expected(type, d -> d.getString("cat") != null && d.getString("cat").compareTo("r") > 0 && greater(d, 100)));
      }
  }

  @Test
  void uncommittedChangesAreSeen() {
    for (final String type : TYPES) {
      database.begin();
      try {
        database.newDocument(type).set("cat", "rare").set("name", "fresh").set("score", 99.0).save();
        database.command("sql", "UPDATE " + type + " SET cat = 'gone' WHERE name = 'n100'");
        assertServedBy(type, "SELECT FROM " + type + " WHERE cat = 'rare' AND score > 1", Map.of(), "cat",
            d -> "rare".equals(d.getString("cat")) && greater(d, 1));
        assertThat(namesOf("SELECT FROM " + type + " WHERE cat = 'rare' AND score > 1")).contains("fresh").doesNotContain("n100");
      } finally {
        database.rollback();
      }
    }
  }

  private void assertServedBy(final String type, final String sql, final Map<String, Object> params, final String property,
      final Predicate<Document> expected) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final List<String> names = names(rs);
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s fetches from the index of the equality (issue #12282):%n%s", sql, plan)
          .contains("FETCH FROM INDEX " + type + "[" + property + "]")
          .doesNotContain("FETCH FROM INDEX " + type + "[score]");
      assertThat(names).as(sql).containsExactlyInAnyOrderElementsOf(expected(type, expected));
    }
  }

  private List<String> namesOf(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return names(rs);
    }
  }

  private static List<String> names(final ResultSet rs) {
    return rs.stream().map(r -> r.<String>getProperty("name")).toList();
  }

  /** The answer of a scan of the type and its subtype, filtered here: what any plan must return. */
  private List<String> expected(final String type, final Predicate<Document> filter) {
    final List<String> names = new ArrayList<>();
    final Iterator<?> it = database.iterateType(type, true);
    while (it.hasNext()) {
      final Document document = ((Record) it.next()).asDocument();
      if (filter.test(document))
        names.add(document.getString("name"));
    }
    return names;
  }

  private static boolean greater(final Document document, final double bound) {
    final Double score = document.getDouble("score");
    return score != null && score > bound;
  }
}
