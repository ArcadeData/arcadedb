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
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code p IS NOT NULL} on the only property of an index that skips null keys is the whole iteration of that index (issue #12285):
 * the index holds one entry for each record whose p is not null and none for the others. SQL and openCypher used to scan the type.
 * Every other index shape keeps the scan, because its iteration is not that set: one that stores null keys, a composite one (it
 * holds records whose first property is null), and one over the items of a list (an entry per item, none for an empty list).
 */
class Issue12285IsNotNullIndexTest extends TestHelper {
  private static final int RECORDS = 3_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE V BUCKETS 4");
    database.command("sql", "CREATE PROPERTY V.k INTEGER");
    database.command("sql", "CREATE PROPERTY V.sp INTEGER");
    database.command("sql", "CREATE PROPERTY V.ni STRING");
    database.command("sql", "CREATE PROPERTY V.c1 STRING");
    database.command("sql", "CREATE PROPERTY V.c2 STRING");
    database.command("sql", "CREATE PROPERTY V.tags LIST OF STRING");
    database.command("sql", "CREATE INDEX ON V (k) UNIQUE");
    database.command("sql", "CREATE INDEX ON V (sp) NOTUNIQUE NULL_STRATEGY SKIP");
    database.command("sql", "CREATE INDEX ON V (ni) NOTUNIQUE NULL_STRATEGY INDEX");
    database.command("sql", "CREATE INDEX ON V (c1, c2) NOTUNIQUE NULL_STRATEGY SKIP");
    database.command("sql", "CREATE INDEX ON V (tags BY ITEM) NOTUNIQUE");
    // the subtype inherits the indexes, and a polymorphic query reads its records through them
    database.command("sql", "CREATE VERTEX TYPE W EXTENDS V");

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final var v = database.newVertex(i % 7 == 6 ? "W" : "V").set("k", i);
        if (i % 50 == 0)
          v.set("sp", i % 3);
        if (i % 40 == 0)
          v.set("ni", "n" + (i % 5));
        if (i % 30 == 0)
          v.set("c1", "a" + (i % 4));
        if (i % 20 == 0)
          v.set("c2", "b" + (i % 4));
        if (i % 25 == 0)
          v.set("tags", i % 2 == 0 ? List.of("x", "y") : List.of());
        v.save();
      }
    });
    // deleted records must stay out of the answer whatever serves it
    database.transaction(() -> database.command("sql", "DELETE FROM V WHERE k = 100 OR k = 1200"));
  }

  @Test
  void sqlReadsTheIndexThatSkipsNulls() {
    final Predicate<Document> spSet = d -> d.get("sp") != null;
    assertSql("SELECT k FROM V WHERE sp IS NOT NULL", "FETCH FROM INDEX V[sp]", spSet);
    assertSql("SELECT k FROM V WHERE sp IS NOT NULL AND k < 1500", null, d -> spSet.test(d) && d.getInteger("k") < 1500);
    assertSql("SELECT k FROM V WHERE sp IS NOT NULL AND k % 3 = 0", "FETCH FROM INDEX V[sp]",
        d -> spSet.test(d) && d.getInteger("k") % 3 == 0);
    // the subtype alone, through the index it inherits
    assertSql("SELECT k FROM W WHERE sp IS NOT NULL", null, d -> spSet.test(d) && d.getType().getName().equals("W"));
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V WHERE sp IS NOT NULL")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(expected(spSet).size());
    }
  }

  @Test
  void sqlKeepsTheScanForEveryOtherIndexShape() {
    assertSql("SELECT k FROM V WHERE ni IS NOT NULL", "FETCH FROM TYPE V", d -> d.get("ni") != null);
    assertSql("SELECT k FROM V WHERE c1 IS NOT NULL", "FETCH FROM TYPE V", d -> d.get("c1") != null);
    assertSql("SELECT k FROM V WHERE c2 IS NOT NULL", "FETCH FROM TYPE V", d -> d.get("c2") != null);
    assertSql("SELECT k FROM V WHERE tags IS NOT NULL", "FETCH FROM TYPE V", d -> d.get("tags") != null);
  }

  @Test
  void anEqualityStillWinsOverTheWholeIndex() {
    assertSql("SELECT k FROM V WHERE sp IS NOT NULL AND k = 500", "FETCH FROM INDEX V[k]", d -> d.getInteger("k") == 500);
  }

  @Test
  void uncommittedChangesAreSeen() {
    database.begin();
    try {
      database.command("sql", "UPDATE V SET sp = 9 WHERE k = 7");
      database.command("sql", "UPDATE V SET sp = null WHERE k = 50");
      database.newVertex("V").set("k", 99_999).set("sp", 1).save();
      final List<Integer> sql = keys("sql", "SELECT k FROM V WHERE sp IS NOT NULL");
      assertThat(sql).contains(7, 99_999).doesNotContain(50).containsExactlyInAnyOrderElementsOf(keysOf(expected(d -> d.get("sp") != null)));
      final List<Integer> cypher = keys("opencypher", "MATCH (n:V) WHERE n.sp IS NOT NULL RETURN n.k AS k");
      assertThat(cypher).containsExactlyInAnyOrderElementsOf(sql);
    } finally {
      database.rollback();
    }
  }

  @Test
  void cypherReadsTheIndexThatSkipsNulls() {
    final Predicate<Document> spSet = d -> d.get("sp") != null;
    assertCypher("MATCH (n:V) WHERE n.sp IS NOT NULL RETURN n.k AS k", "NodeIndexRangeScan", spSet);
    assertCypher("MATCH (n:V) WHERE n.sp IS NOT NULL AND n.k % 3 = 0 RETURN n.k AS k", "NodeIndexRangeScan",
        d -> spSet.test(d) && d.getInteger("k") % 3 == 0);
    assertCypher("MATCH (n:W) WHERE n.sp IS NOT NULL RETURN n.k AS k", null, d -> spSet.test(d) && d.getType().getName().equals("W"));
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V) WHERE n.sp IS NOT NULL RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(expected(spSet).size());
    }
  }

  @Test
  void cypherKeepsTheScanForEveryOtherIndexShape() {
    assertCypher("MATCH (n:V) WHERE n.ni IS NOT NULL RETURN n.k AS k", "NodeByLabelScan", d -> d.get("ni") != null);
    assertCypher("MATCH (n:V) WHERE n.c1 IS NOT NULL RETURN n.k AS k", "NodeByLabelScan", d -> d.get("c1") != null);
    assertCypher("MATCH (n:V) WHERE n.c2 IS NOT NULL RETURN n.k AS k", "NodeByLabelScan", d -> d.get("c2") != null);
    assertCypher("MATCH (n:V) WHERE n.tags IS NOT NULL RETURN n.k AS k", "NodeByLabelScan", d -> d.get("tags") != null);
    // IS NULL is the complement of the index: still a scan
    assertCypher("MATCH (n:V) WHERE n.sp IS NULL RETURN n.k AS k", "NodeByLabelScan", d -> d.get("sp") == null);
  }

  private void assertSql(final String sql, final String firstStep, final Predicate<Document> filter) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final List<Integer> keys = rs.stream().map(r -> r.<Integer>getProperty("k")).toList();
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      if (firstStep != null)
        assertThat(plan).as("%s (issue #12285):%n%s", sql, plan).contains(firstStep);
      assertThat(keys).as(sql).containsExactlyInAnyOrderElementsOf(keysOf(expected(filter)));
    }
  }

  private void assertCypher(final String cypher, final String operator, final Predicate<Document> filter) {
    if (operator != null)
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + cypher)) {
        final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
        assertThat(plan).as("%s (issue #12285):%n%s", cypher, plan).contains(operator);
      }
    assertThat(keys("opencypher", cypher)).as(cypher).containsExactlyInAnyOrderElementsOf(keysOf(expected(filter)));
  }

  private List<Integer> keys(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      return rs.stream().map(r -> r.<Integer>getProperty("k")).toList();
    }
  }

  /** The records of V and its subtype a scan finds with {@code filter}: what any plan must return. */
  private List<Document> expected(final Predicate<Document> filter) {
    final List<Document> documents = new ArrayList<>();
    final Iterator<?> it = database.iterateType("V", true);
    while (it.hasNext()) {
      final Document document = ((Record) it.next()).asDocument();
      if (filter.test(document))
        documents.add(document);
    }
    return documents;
  }

  private static List<Integer> keysOf(final List<Document> documents) {
    return documents.stream().map(d -> d.getInteger("k")).toList();
  }
}
