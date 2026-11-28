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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code min()}, {@code max()} of indexed properties and {@code count(*)} asked together are each answered by their own index read or
 * count, and the row is assembled from them (issue #12284). Each one alone already was; together they scanned the type. Every answer
 * is compared, column names included, with the same statement on a twin type that holds the same records and no index.
 */
class Issue12284MinMaxTogetherTest extends TestHelper {
  private static final int RECORDS = 2_000;

  @Override
  protected void beginTest() {
    for (final String type : List.of("T", "U")) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type + " BUCKETS 4");
      database.command("sql", "CREATE PROPERTY " + type + ".k INTEGER");
      database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
      database.command("sql", "CREATE PROPERTY " + type + ".plain INTEGER");
      database.command("sql", "CREATE DOCUMENT TYPE " + type + "Sub EXTENDS " + type);
    }
    database.command("sql", "CREATE INDEX ON T (k) UNIQUE");
    database.command("sql", "CREATE INDEX ON T (s) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE TEmpty");
    database.command("sql", "CREATE PROPERTY TEmpty.k INTEGER");
    database.command("sql", "CREATE INDEX ON TEmpty (k) UNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE UEmpty");
    database.command("sql", "CREATE PROPERTY UEmpty.k INTEGER");

    for (final String label : List.of("V", "VU")) {
      database.command("sql", "CREATE VERTEX TYPE " + label + " BUCKETS 4");
      database.command("sql", "CREATE PROPERTY " + label + ".k INTEGER");
      database.command("sql", "CREATE PROPERTY " + label + ".s STRING");
    }
    database.command("sql", "CREATE INDEX ON V (k) UNIQUE");
    database.command("sql", "CREATE INDEX ON V (s) NOTUNIQUE");

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++) {
        final int k = (i * 7919) % 100_003;
        final String s = i % 9 == 0 ? null : "s" + (i % 300);
        for (final String type : List.of("T", "U"))
          database.newDocument(i % 6 == 5 ? type + "Sub" : type).set("k", k).set("s", s).set("plain", i).save();
        for (final String label : List.of("V", "VU"))
          database.newVertex(label).set("k", k).set("s", s).save();
      }
    });
    // the two ends of k deleted: the answer is the next live key
    database.transaction(() -> {
      for (final String type : List.of("T", "U", "V", "VU"))
        database.command("sql", "DELETE FROM " + type + " WHERE k = 0 OR k = (SELECT max(k) AS m FROM " + type + ").m[0]");
    });
  }

  @Test
  void minAndMaxTogetherAreReadFromTheIndex() {
    assertSql("SELECT min(k), max(k) FROM %s", "T", "MIN FROM INDEX T[k], MAX FROM INDEX T[k]");
    assertSql("SELECT min(k) AS lo, max(k) AS hi FROM %s", "T", "MIN FROM INDEX T[k], MAX FROM INDEX T[k]");
    assertSql("SELECT max(k), min(s), max(s) FROM %s", "T", "MAX FROM INDEX T[k], MIN FROM INDEX T[s], MAX FROM INDEX T[s]");
    assertSql("SELECT min(k), max(k) FROM %s LIMIT 1", "T", "MIN FROM INDEX T[k]");
  }

  @Test
  void aCountGoesWithThem() {
    assertSql("SELECT max(k), count(*) FROM %s", "T", "MAX FROM INDEX T[k], COUNT FROM TYPE T");
    assertSql("SELECT count(*) AS n, min(k) AS lo, max(k) AS hi FROM %s", "T", "COUNT FROM TYPE T, MIN FROM INDEX T[k]");
  }

  @Test
  void aSubtypeReadsOnlyItsOwnEntries() {
    assertSql("SELECT min(k), max(k), count(*) FROM %sSub", "T", "MIN FROM INDEX");
  }

  @Test
  void anEmptyTypeAnswersLikeTheScan() {
    final Map<String, Object> indexed = row("SELECT min(k), max(k), count(*) FROM TEmpty");
    assertThat(indexed).isEqualTo(row("SELECT min(k), max(k), count(*) FROM UEmpty"));
    assertThat(plan("SELECT min(k), max(k), count(*) FROM TEmpty")).contains("MIN FROM INDEX TEmpty[k]");
  }

  @Test
  void anythingElseInTheProjectionKeepsTheScan() {
    assertSql("SELECT min(k), max(plain) FROM %s", "T", "FETCH FROM TYPE T");
    assertSql("SELECT min(k), max(k) + 1 FROM %s", "T", "FETCH FROM TYPE T");
    assertSql("SELECT min(k), k FROM %s", "T", "FETCH FROM TYPE T");
    assertSql("SELECT min(k), sum(k) FROM %s", "T", "FETCH FROM TYPE T");
    assertSql("SELECT min(k), max(k) FROM %s WHERE plain > 10", "T", "T");
  }

  @Test
  void uncommittedChangesAreSeen() {
    database.begin();
    try {
      for (final String type : List.of("T", "U")) {
        database.newDocument(type).set("k", 1_000_000).save();
        database.newDocument(type).set("k", -5).save();
        database.command("sql", "DELETE FROM " + type + " WHERE k = 7919");
      }
      assertSql("SELECT min(k), max(k), count(*) FROM %s", "T", "MIN FROM INDEX T[k]");
      assertThat(row("SELECT min(k) AS lo, max(k) AS hi FROM T")).containsEntry("lo", -5).containsEntry("hi", 1_000_000);
    } finally {
      database.rollback();
    }
  }

  @Test
  void openCypherReadsEachEndFromItsIndex() {
    assertCypher("MATCH (n:%s) RETURN min(n.k), max(n.k)", "MIN FROM INDEX V[k], MAX FROM INDEX V[k]");
    assertCypher("MATCH (n:%s) RETURN min(n.k) AS lo, max(n.k) AS hi, max(n.s) AS top", "MAX FROM INDEX V[s]");
    // a count is not an end of an index: the scan answers, with the same row
    assertCypher("MATCH (n:%s) RETURN min(n.k), count(n)", null);
    assertCypher("MATCH (n:%s) WHERE n.k > 10 RETURN min(n.k), max(n.k)", null);
  }

  /** Runs {@code sql} on the indexed type and on its twin with no index: same row, and the indexed plan contains {@code step}. */
  private void assertSql(final String sql, final String type, final String step) {
    final String indexed = sql.formatted(type);
    final String twin = sql.formatted("U");
    assertThat(rows("sql", indexed)).as(indexed).isEqualTo(rows("sql", twin));
    assertThat(plan(indexed)).as("%s (issue #12284)", indexed).contains(step);
  }

  private void assertCypher(final String cypher, final String step) {
    final String indexed = cypher.formatted("V");
    assertThat(rows("opencypher", indexed)).as(indexed).isEqualTo(rows("opencypher", cypher.formatted("VU")));
    if (step != null)
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + indexed)) {
        final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
        assertThat(plan).as("%s (issue #12284):%n%s", indexed, plan).contains(step);
      }
  }

  private String plan(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      rs.stream().count();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private Map<String, Object> row(final String sql) {
    final List<Map<String, Object>> rows = rows("sql", sql);
    assertThat(rows).as(sql).hasSize(1);
    return rows.getFirst();
  }

  private List<Map<String, Object>> rows(final String language, final String query) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final Map<String, Object> row = new LinkedHashMap<>();
        for (final String name : r.getPropertyNames())
          row.put(name, r.getProperty(name));
        rows.add(row);
      }
    }
    return rows;
  }
}
