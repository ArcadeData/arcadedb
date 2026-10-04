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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9029: the other end of an index range was taken from the second range condition on the indexed property without
 * checking that its bound can be computed before the scan. A bound that reads another property of the record ({@code u <= id})
 * or an expression over one ({@code u <= w * 100}) was then evaluated by the index scan with no record, so the indexed type
 * answered differently from an identical unindexed one. Such a condition must stay in the filter.
 */
class Issue9029IndexRangePartnerBoundTest extends TestHelper {

  @Override
  public void beginTest() {
    for (final String type : new String[] { "I", "S" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".u INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".w INTEGER");
    }
    database.command("sql", "CREATE INDEX ON I (u) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "I", "S" }) {
        database.newDocument(type).set("id", 32, "u", 26, "w", 1).save();
        database.newDocument(type).set("id", 3, "u", 5, "w", 2).save();
        database.newDocument(type).set("id", 40, "u", 50, "w", 3).save();
      }
    });
  }

  @Test
  void upperBoundReadingAnotherPropertyStaysInTheFilter() {
    assertThat(sql("I", "u > 9 AND u <= id")).containsExactly(32);
    assertThat(sql("S", "u > 9 AND u <= id")).containsExactly(32);
    assertThat(sql("I", "u <= id AND u > 9")).containsExactly(32);
  }

  @Test
  void lowerBoundReadingAnotherPropertyStaysInTheFilter() {
    assertThat(sql("I", "u < 60 AND u > id")).containsExactly(3, 40);
    assertThat(sql("S", "u < 60 AND u > id")).containsExactly(3, 40);
    assertThat(sql("I", "u > id AND u < 60")).containsExactly(3, 40);
  }

  @Test
  void boundExpressionOverAnotherPropertyStaysInTheFilter() {
    assertThat(sql("I", "u > 9 AND u <= w * 100")).containsExactly(32, 40);
    assertThat(sql("S", "u > 9 AND u <= w * 100")).containsExactly(32, 40);
  }

  @Test
  void aLaterConstantBoundIsStillTakenAsTheOtherEnd() {
    // the record-dependent condition is skipped, the constant one after it closes the range, and the skipped one filters
    assertThat(sql("I", "u > 9 AND u <= id AND u < 45")).containsExactly(32);
    assertThat(sql("S", "u > 9 AND u <= id AND u < 45")).containsExactly(32);
    final String plan = plan("SELECT id FROM I WHERE u > 9 AND u <= id AND u < 45");
    assertThat(plan).contains("FETCH FROM INDEX I[u]").contains("u > 9 and u < 45");
  }

  @Test
  void parameterAndConstantBoundsStillCloseTheRange() {
    assertThat(sql("I", "u > 9 AND u <= 30")).containsExactly(32);
    assertThat(plan("SELECT id FROM I WHERE u > 9 AND u <= 30")).contains("u > 9 and u <= 30");
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM I WHERE u > ? AND u <= ? ORDER BY id", 9, 60)) {
      rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
    }
    assertThat(ids).containsExactly(32, 40);
  }

  @Test
  void planOfARecordDependentPartnerKeepsTheConditionAsAFilter() {
    final String plan = plan("SELECT id FROM I WHERE u > 9 AND u <= id");
    assertThat(plan).contains("FETCH FROM INDEX I[u]");
    assertThat(plan).doesNotContain("u > 9 and u <= id");
    // the condition is moved to the filter, not dropped
    assertThat(plan.substring(plan.indexOf("FILTER ITEMS WHERE"))).contains("u <= id");
  }

  @Test
  void recordDependentPartnerOnACaseInsensitiveIndexStaysInTheFilter() {
    // the field.toLowerCase() range of #8560 on a COLLATE ci index: its partner must be early calculated too
    for (final String type : new String[] { "CI", "CS" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".name STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".upTo STRING");
    }
    database.command("sql", "CREATE INDEX ON CI (name COLLATE ci) NOTUNIQUE");
    database.transaction(() -> {
      for (final String type : new String[] { "CI", "CS" }) {
        database.newDocument(type).set("name", "Anne", "upTo", "b").save();
        database.newDocument(type).set("name", "John", "upTo", "c").save();
        database.newDocument(type).set("name", "mary", "upTo", "z").save();
      }
    });

    for (final String where : new String[] { "name.toLowerCase() >= 'a' AND name.toLowerCase() <= upTo",
        "name.toLowerCase() <= upTo AND name.toLowerCase() >= 'a'",
        "name.toLowerCase() >= 'a' AND name.toLowerCase() <= upTo AND name.toLowerCase() < 'n'" }) {
      final List<String> indexed = names("SELECT name FROM CI WHERE " + where + " ORDER BY name");
      assertThat(indexed).as(where).isEqualTo(names("SELECT name FROM CS WHERE " + where + " ORDER BY name"));
      assertThat(indexed).as(where).containsExactly("Anne", "mary");
    }
    assertThat(plan("SELECT name FROM CI WHERE name.toLowerCase() >= 'a' AND name.toLowerCase() <= upTo"))
        .contains("FETCH FROM INDEX CI[name]");
  }

  private List<String> names(final String query) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      rs.forEachRemaining(r -> names.add(r.getProperty("name")));
    }
    return names;
  }

  @Test
  void cypherAgreesBetweenIndexedAndUnindexedTypes() {
    for (final String where : new String[] { "n.u > 9 AND n.u <= n.id", "n.u < 60 AND n.u > n.id", "n.u > 9 AND n.u <= n.w * 100" })
      assertThat(cypher("I", where)).as(where).isEqualTo(cypher("S", where));
  }

  private List<Integer> sql(final String type, final String where) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + type + " WHERE " + where + " ORDER BY id")) {
      rs.forEachRemaining(r -> ids.add(r.getProperty("id")));
    }
    return ids;
  }

  private List<Integer> cypher(final String type, final String where) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:" + type + ") WHERE " + where + " RETURN n.id AS id ORDER BY id",
        Map.of())) {
      rs.forEachRemaining(r -> ids.add(((Number) r.getProperty("id")).intValue()));
    }
    return ids;
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + query)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }
}
