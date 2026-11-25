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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12136: an {@code EXISTS { }}, {@code COUNT { }}, {@code COLLECT { }} or {@code CALL { }} body is planned once per
 * execution of a statement that cannot write, and its step chain is run again for every outer row with the row as its seed.
 * The profile reports how many chains were built and how many outer rows reused one; a statement that can write keeps
 * planning the body for every row, so a write made for one row is seen by the body of the next.
 * <p>
 * Graph: {@code DOCS} documents {@code k00} .. with {@code key % 4} items each, item {@code j} running from 1.
 */
class CypherSubqueryPlanReuseIssue12136Test {
  private static final int DOCS = 20;

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-subquery-plan-reuse-12136").create();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.key INTEGER");
      database.command("sql", "CREATE VERTEX TYPE Item");
      database.command("sql", "CREATE EDGE TYPE OF");
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:Doc {key: i})", Map.of("n", DOCS));
      database.command("opencypher",
          "MATCH (d:Doc) UNWIND range(1, d.key % 4) AS j CREATE (:Item {j: j})-[:OF]->(d)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void existsBodyIsPlannedOnceForTheStatement() {
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (d:Doc) WHERE EXISTS { MATCH (d)<-[:OF]-(i:Item) WHERE i.j > 2 } RETURN count(d) AS n");
    // key % 4 == 3: 3, 7, 11, 15, 19
    assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(5);
    assertThat(profileOf(rs)).contains("Subquery step chains: 1 built, " + (DOCS - 1) + " reused");
  }

  @Test
  void notExistsBodyIsPlannedOnceForTheStatement() {
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (d:Doc) WHERE NOT EXISTS { MATCH (d)<-[:OF]-(i:Item) WHERE i.j >= 1 } RETURN count(d) AS n");
    // key % 4 == 0: 0, 4, 8, 12, 16
    assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(5);
    assertThat(profileOf(rs)).contains("Subquery step chains: 1 built, " + (DOCS - 1) + " reused");
  }

  @Test
  void countBodyIsPlannedOnceForTheStatement() {
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (d:Doc) RETURN d.key AS key, COUNT { MATCH (d)<-[:OF]-(i:Item) WITH i WHERE i.j > 1 RETURN i } AS n ORDER BY key");
    final List<Result> rows = drain(rs);
    assertThat(rows).hasSize(DOCS);
    for (final Result row : rows) {
      final int key = row.getProperty("key");
      assertThat(((Number) row.getProperty("n")).intValue()).as("doc %d", key).isEqualTo(Math.max(0, key % 4 - 1));
    }
    assertThat(profileOf(rs)).contains("Subquery step chains: 1 built, " + (DOCS - 1) + " reused");
  }

  @Test
  void collectBodyIsPlannedOnceForTheStatement() {
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (d:Doc) RETURN d.key AS key, COLLECT { MATCH (d)<-[:OF]-(i:Item) RETURN i.j AS j ORDER BY j } AS js ORDER BY key");
    final List<Result> rows = drain(rs);
    assertThat(rows).hasSize(DOCS);
    for (final Result row : rows) {
      final int key = row.getProperty("key");
      final List<Long> expected = new ArrayList<>();
      for (long j = 1; j <= key % 4; j++)
        expected.add(j);
      assertThat(row.<List<Object>>getProperty("js")).as("doc %d", key).containsExactlyElementsOf(expected);
    }
    assertThat(profileOf(rs)).contains("Subquery step chains: 1 built, " + (DOCS - 1) + " reused");
  }

  @Test
  void callBodyIsPlannedOnceForTheStatement() {
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (d:Doc) CALL { WITH d OPTIONAL MATCH (d)<-[:OF]-(i:Item) RETURN count(i) AS n } RETURN d.key AS key, n ORDER BY key");
    final List<Result> rows = drain(rs);
    assertThat(rows).hasSize(DOCS);
    for (final Result row : rows) {
      final int key = row.getProperty("key");
      assertThat(((Number) row.getProperty("n")).intValue()).as("doc %d", key).isEqualTo(key % 4);
    }
    assertThat(profileOf(rs)).contains("Subquery step chains: 1 built, " + (DOCS - 1) + " reused");
  }

  @Test
  void bodyWithALimitStoppedEarlyIsRunAgainFromTheStart() {
    // EXISTS reads one row of the body and closes it: the next outer row must not resume where the last one stopped
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH (d:Doc) WHERE EXISTS { MATCH (d)<-[:OF]-(i:Item) RETURN i ORDER BY i.j LIMIT 1 } RETURN count(d) AS n");
    assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(15);
    assertThat(profileOf(rs)).contains("Subquery step chains: 1 built, " + (DOCS - 1) + " reused");
  }

  @Test
  void nestedBodiesAreEachPlannedOnce() {
    final ResultSet rs = database.query("opencypher", """
        PROFILE MATCH (d:Doc)
        CALL { WITH d MATCH (d)<-[:OF]-(i:Item) WHERE EXISTS { MATCH (i)-[:OF]->(e:Doc) WHERE e.key = d.key } RETURN count(i) AS n }
        RETURN d.key AS key, n ORDER BY key""");
    final List<Result> rows = drain(rs);
    assertThat(rows).hasSize(DOCS);
    for (final Result row : rows) {
      final int key = row.getProperty("key");
      assertThat(((Number) row.getProperty("n")).intValue()).as("doc %d", key).isEqualTo(key % 4);
    }
    // One chain for the CALL body and one for the EXISTS body; the EXISTS body runs once per item (30 items)
    assertThat(profileOf(rs)).contains("Subquery step chains: 2 built, " + (DOCS - 1 + 30 - 1) + " reused");
  }

  @Test
  void aNullSeedAndAVertexSeedBothAnswer() {
    // The first outer row binds x to null; the count must still be the vertex's for the rows that bind one
    final ResultSet rs = database.query("opencypher", """
        MATCH (d:Doc) WITH d ORDER BY d.key UNWIND [null, d] AS x
        RETURN d.key AS key, x IS NULL AS isNull, COUNT { MATCH (x)<-[:OF]-(i:Item) WHERE i.j > 0 } AS n""");
    final List<Result> rows = drain(rs);
    assertThat(rows).hasSize(DOCS * 2);
    for (final Result row : rows) {
      final int key = row.getProperty("key");
      final int expected = Boolean.TRUE.equals(row.getProperty("isNull")) ? 0 : key % 4;
      assertThat(((Number) row.getProperty("n")).intValue()).as("doc %d, %s", key, row).isEqualTo(expected);
    }
  }

  @Test
  void aStatementThatWritesKeepsPlanningItsBodiesForEveryRow() {
    // A body run again on the chain of an earlier row keeps what its steps remember of the rows they read (a scan
    // snapshot, a memo): only a statement that cannot write shares one. The reads of this one are all made before its
    // first CREATE (the read/write barrier of #7171), so every row counts the 30 items there were
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", """
          PROFILE MATCH (d:Doc) WITH d ORDER BY d.key
          CALL { WITH d MATCH (x:Item) RETURN count(x) AS c }
          CREATE (:Item {j: 0})
          RETURN d.key AS key, c""");
      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(DOCS);
      for (final Result row : rows)
        assertThat(((Number) row.getProperty("c")).longValue()).as("doc %s", row.<Object>getProperty("key")).isEqualTo(30L);
      assertThat(profileOf(rs)).doesNotContain("Subquery step chains");
    });
  }

  private static List<Result> drain(final ResultSet rs) {
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }

  private static String profileOf(final ResultSet rs) {
    while (rs.hasNext())
      rs.next();
    return rs.getExecutionPlan().get().prettyPrint(0, 2);
  }
}
