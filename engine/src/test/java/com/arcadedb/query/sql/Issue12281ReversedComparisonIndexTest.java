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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A comparison on an indexed property written with the value on the left ({@code 5 = id}, {@code :lo < id}) or under NOT
 * ({@code NOT (id <> 5)}, {@code NOT (id >= 3)}) is answered by the index, as the same comparison written property first is
 * (issue #12281). A range whose value has another type than the property keeps the scan: the two spellings convert in opposite
 * directions and do not select the same records.
 */
class Issue12281ReversedComparisonIndexTest extends TestHelper {
  private static final int RECORDS = 2_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 4");
    database.command("sql", "CREATE PROPERTY T.id INTEGER");
    database.command("sql", "CREATE PROPERTY T.s STRING");
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE NULL_STRATEGY SKIP");
    database.command("sql", "CREATE INDEX ON T (s) NOTUNIQUE NULL_STRATEGY SKIP");
    database.command("sql", "CREATE DOCUMENT TYPE S EXTENDS T");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(i % 7 == 6 ? "S" : "T").set("id", i).set("s", "s" + (i % 50)).set("v", i).save();
      // records with no id: a comparison with null is unknown, under NOT too
      for (int i = 0; i < 5; i++)
        database.newDocument("T").set("s", "no-id").save();
    });
    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE id = 4 OR id = 1500"));

    database.command("sql", "CREATE VERTEX TYPE V BUCKETS 4");
    database.command("sql", "CREATE PROPERTY V.id INTEGER");
    database.command("sql", "CREATE INDEX ON V (id) UNIQUE NULL_STRATEGY SKIP");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newVertex("V").set("id", i).save();
      database.newVertex("V").save();
    });
  }

  @Test
  void aValueOnTheLeftIsAnIndexKey() {
    assertIndexed("SELECT id FROM T WHERE 5 = id", Map.of(), id(i -> i == 5));
    assertIndexed("SELECT id FROM T WHERE :k = id", Map.of("k", 5), id(i -> i == 5));
    assertIndexed("SELECT id FROM T WHERE :lo < id", Map.of("lo", 1990), id(i -> i > 1990));
    assertIndexed("SELECT id FROM T WHERE 3 >= id", Map.of(), id(i -> i <= 3));
    assertIndexed("SELECT id FROM T WHERE 10 > id AND id > 2", Map.of(), id(i -> i > 2 && i < 10));
    assertIndexed("SELECT id FROM T WHERE id > 2 AND 10 > id", Map.of(), id(i -> i > 2 && i < 10));
    assertIndexed("SELECT id FROM T WHERE 's7' = s", Map.of(), d -> "s7".equals(d.getString("s")));
  }

  @Test
  void aComparisonUnderNotIsAnIndexKey() {
    assertIndexed("SELECT id FROM T WHERE NOT (id <> 5)", Map.of(), id(i -> i == 5));
    assertIndexed("SELECT id FROM T WHERE NOT (id != 5)", Map.of(), id(i -> i == 5));
    assertIndexed("SELECT id FROM T WHERE NOT (id >= 3)", Map.of(), id(i -> i < 3));
    assertIndexed("SELECT id FROM T WHERE NOT (id < 1995)", Map.of(), id(i -> i >= 1995));
    assertIndexed("SELECT id FROM T WHERE NOT (3 <= id)", Map.of(), id(i -> i < 3));
  }

  @Test
  void aValueOfAnotherTypeKeepsTheScan() {
    // '10' > id compares text, id < '10' numbers: not the same records, so not the index
    assertScanned("SELECT id FROM T WHERE '10' > id", Map.of());
    assertScanned("SELECT id FROM T WHERE 10 > s", Map.of());
    assertScanned("SELECT id FROM T WHERE NOT (id >= 'abc')", Map.of());
  }

  @Test
  void aParameterDecidesThePlanForItsOwnBinding() {
    final String sql = "SELECT id FROM T WHERE :v > id";
    assertScanned(sql, Map.of("v", "10"));
    assertIndexed(sql, Map.of("v", 10), id(i -> i < 10));
    assertScanned(sql, Map.of("v", "10"));
  }

  @Test
  void openCypherSeeksOnANegatedInequality() {
    final String cypher = "MATCH (n:V) WHERE NOT (n.id <> $k) RETURN n.id AS id";
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher, Map.of("k", 5))) {
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("issue #12281:%n%s", plan).contains("NodeIndexSeek").doesNotContain("NodeByLabelScan");
    }
    for (final int k : new int[] { 5, 0, 1999, 2000 })
      try (final ResultSet rs = database.query("opencypher", cypher, Map.of("k", k))) {
        assertThat(rs.stream().map(r -> r.<Integer>getProperty("id")).toList()).as("k = %d", k)
            .containsExactlyElementsOf(k < RECORDS ? List.of(k) : List.of());
      }
  }

  @Test
  void aSqlMatchNodeFilterWithTheValueOnTheLeftIsAnIndexKey() {
    for (final String where : new String[] { "id = 5", "5 = id", "NOT (id <> 5)" })
      try (final ResultSet rs = database.query("sql", "MATCH {type: V, as: a, where: (" + where + ")} RETURN a.id AS id")) {
        final List<Integer> ids = rs.stream().map(r -> r.<Integer>getProperty("id")).toList();
        final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
        assertThat(plan).as("%s (issue #12281):%n%s", where, plan).contains("FETCH FROM INDEX V[id]");
        assertThat(ids).as(where).containsExactly(5);
      }
  }

  @Test
  void anUpdateFindsItsRecordThroughTheIndex() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE T SET v = -1 WHERE 5 = id")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
    });
    try (final ResultSet rs = database.query("sql", "SELECT v FROM T WHERE id = 5")) {
      assertThat(rs.next().<Integer>getProperty("v")).isEqualTo(-1);
    }
  }

  @Test
  void uncommittedChangesAreSeen() {
    database.begin();
    try {
      database.newDocument("T").set("id", 4).set("s", "back").save();
      database.command("sql", "UPDATE T SET id = 99999 WHERE id = 3");
      assertIndexed("SELECT id FROM T WHERE NOT (id >= 5)", Map.of(), id(i -> i < 5));
      assertIndexed("SELECT id FROM T WHERE 4 = id", Map.of(), id(i -> i == 4));
    } finally {
      database.rollback();
    }
  }

  private void assertIndexed(final String sql, final Map<String, Object> params, final Predicate<Document> expected) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      final List<Object> ids = rs.stream().map(r -> (Object) r.getProperty("id")).toList();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s %s is answered by an index (issue #12281):%n%s", sql, params, plan).contains("FETCH FROM INDEX T[")
          .doesNotContain("FETCH FROM TYPE");
      assertThat(ids).as("%s %s", sql, params).containsExactlyInAnyOrderElementsOf(expected(expected));
    }
  }

  private void assertScanned(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      rs.stream().count();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      assertThat(plan).as("%s %s keeps the scan:%n%s", sql, params, plan).contains("FETCH FROM TYPE").doesNotContain("FETCH FROM INDEX");
    }
  }

  private static Predicate<Document> id(final IntPredicate test) {
    return d -> d.getInteger("id") != null && test.test(d.getInteger("id"));
  }

  /** What a scan of T and its subtype keeps with {@code filter}. */
  private List<Object> expected(final Predicate<Document> filter) {
    final List<Object> ids = new ArrayList<>();
    final Iterator<?> it = database.iterateType("T", true);
    while (it.hasNext()) {
      final Document document = ((Record) it.next()).asDocument();
      if (filter.test(document))
        ids.add(document.getInteger("id"));
    }
    return ids;
  }
}
