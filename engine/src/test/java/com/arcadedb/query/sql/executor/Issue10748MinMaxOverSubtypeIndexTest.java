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
 * #10748: {@code SELECT max(p) FROM T} / {@code min(p)} is answered from the first entry of an ordered index, and for a subtype the
 * index found is routinely the one declared on an ancestor, which holds the entries of the ancestor and of every sibling subtype too:
 * {@code max(v) FROM Child} answered 901, a value held only by a record of {@code Other}. The step now reads the sub-indexes of the
 * target's own buckets only, and the planner hands it an index only when that index covers every one of them.
 * <p>
 * The oracle is the same statement with a WHERE clause that defeats the pushdown, over a three-type hierarchy where each type holds the
 * extreme values in turn.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10748MinMaxOverSubtypeIndexTest extends TestHelper {

  private static final List<String> TYPES = List.of("Parent", "Child", "Other", "GrandChild");

  private void createHierarchy(final String keyType, final String indexType) {
    database.command("sql", "CREATE DOCUMENT TYPE Parent").close();
    database.command("sql", "CREATE PROPERTY Parent.v " + keyType).close();
    database.command("sql", "CREATE INDEX ON Parent (v) " + indexType).close();
    database.command("sql", "CREATE DOCUMENT TYPE Child EXTENDS Parent").close();
    database.command("sql", "CREATE DOCUMENT TYPE Other EXTENDS Parent").close();
    database.command("sql", "CREATE DOCUMENT TYPE GrandChild EXTENDS Child").close();
  }

  private void insert(final String type, final Object... values) {
    database.transaction(() -> {
      for (final Object value : values)
        database.newDocument(type).set("v", value).save();
    });
  }

  private Object aggregate(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("m");
    }
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql)) {
      return rs.next().getProperty("executionPlanAsString");
    }
  }

  /** The hardwired answer agrees with the answer of the scan, for both functions and every type of the hierarchy. */
  private void assertEveryTypeAgreesWithTheScan() {
    for (final String type : TYPES)
      for (final String function : List.of("max", "min")) {
        final String hardwired = "SELECT " + function + "(v) AS m FROM " + type;
        assertThat(plan(hardwired)).as(hardwired).contains((function.equals("max") ? "MAX" : "MIN") + " FROM INDEX");
        assertThat(aggregate(hardwired)).as(hardwired).isEqualTo(aggregate(hardwired + " WHERE v IS NOT NULL OR v IS NULL"));
      }
  }

  @Test
  void theReportedHierarchy() {
    createHierarchy("INTEGER", "NOTUNIQUE");
    insert("Parent", 50, 60, 70);
    insert("Child", 5, 6, 7);
    insert("Other", 900, 901);

    assertThat(aggregate("SELECT max(v) AS m FROM Child")).isEqualTo(7);
    assertThat(aggregate("SELECT min(v) AS m FROM Child")).isEqualTo(5);
    assertThat(aggregate("SELECT max(v) AS m FROM Other")).isEqualTo(901);
    assertThat(aggregate("SELECT min(v) AS m FROM Other")).isEqualTo(900);
    assertThat(aggregate("SELECT max(v) AS m FROM Parent")).isEqualTo(901);
    assertThat(aggregate("SELECT min(v) AS m FROM Parent")).isEqualTo(5);
    // a subtype with no record answers null, as the scan does
    assertThat(aggregate("SELECT max(v) AS m FROM GrandChild")).isNull();
    assertEveryTypeAgreesWithTheScan();
  }

  @Test
  void eachTypeHoldsTheExtremesInTurn() {
    createHierarchy("INTEGER", "NOTUNIQUE");
    int base = 0;
    for (final String holder : TYPES) {
      for (final String type : TYPES)
        insert(type, base + (type.equals(holder) ? 1000 : 500), base + (type.equals(holder) ? -1000 : 0));
      assertEveryTypeAgreesWithTheScan();
      base += 10;
    }
  }

  @Test
  void aUniqueStringKey() {
    createHierarchy("STRING", "UNIQUE");
    insert("Parent", "mmm");
    insert("Child", "aaa", "bbb");
    insert("GrandChild", "ccc");
    insert("Other", "zzz", "000");
    assertThat(aggregate("SELECT max(v) AS m FROM Child")).isEqualTo("ccc");
    assertThat(aggregate("SELECT min(v) AS m FROM Child")).isEqualTo("aaa");
    assertThat(aggregate("SELECT max(v) AS m FROM GrandChild")).isEqualTo("ccc");
    assertEveryTypeAgreesWithTheScan();
  }

  @Test
  void deletedAndNullValuesAreNotTheExtreme() {
    createHierarchy("INTEGER", "NOTUNIQUE NULL_STRATEGY INDEX");
    insert("Child", 5, 6, 7);
    insert("Other", 900, 1);
    database.transaction(() -> {
      database.newDocument("Child").set("v", null).save();
      database.command("sql", "DELETE FROM Child WHERE v = 7").close();
    });
    assertThat(aggregate("SELECT max(v) AS m FROM Child")).isEqualTo(6);
    assertThat(aggregate("SELECT min(v) AS m FROM Child")).isEqualTo(5);
    assertEveryTypeAgreesWithTheScan();
  }

  /** The ordered fetch through the same ancestor index answers the subtype's records only, in order, as the scan does. */
  @Test
  void anOrderByOverTheAncestorIndexReadsTheSubtypeOnly() {
    createHierarchy("INTEGER", "NOTUNIQUE");
    insert("Parent", 50, 60, 70);
    insert("Child", 5, 6, 7);
    insert("Other", 900, 901);
    for (final String direction : List.of("ASC", "DESC")) {
      final List<Object> values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT v FROM Child ORDER BY v " + direction + " LIMIT 2", Map.of())) {
        rs.stream().forEach(r -> values.add(r.getProperty("v")));
      }
      assertThat(values).containsExactlyElementsOf(direction.equals("ASC") ? List.of(5, 6) : List.of(7, 6));
    }
  }
}
