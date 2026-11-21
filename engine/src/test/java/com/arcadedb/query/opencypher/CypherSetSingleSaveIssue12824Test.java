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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #12824: an openCypher {@code SET n.a = 2, n.b = 2, n.c = 2} saved the record once per
 * item, so a BEFORE/AFTER UPDATE trigger fired once per item and saw half-applied records. The items of one SET clause
 * are now applied together and every record the clause touches is saved once, as SQL {@code UPDATE} with several
 * assignments and Cypher {@code SET n += {...}} already did.
 * <p>
 * A labelled {@code MATCH} runs on the optimizer path and an unlabelled one on the legacy step chain: both build the same
 * SET step, and both are exercised.
 */
class CypherSetSingleSaveIssue12824Test extends TestHelper {

  @BeforeEach
  void setUpSchema() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE DOCUMENT TYPE Audit BUCKETS 1");
    database.command("sql",
        "CREATE TRIGGER bu BEFORE UPDATE ON TYPE T EXECUTE SQL 'INSERT INTO Audit SET phase = \"before\", k = $record.k, a = $record.a, b = $record.b, c = $record.c'");
    database.command("sql",
        "CREATE TRIGGER au AFTER UPDATE ON TYPE T EXECUTE SQL 'INSERT INTO Audit SET phase = \"after\", k = $record.k, a = $record.a, b = $record.b, c = $record.c'");
    insert("INSERT INTO T SET k = 1, a = 0, b = 0, c = 0");
  }

  @Test
  void multiItemSetFiresUpdateTriggersOnceWithFinalStateOnOptimizerPath() {
    database.command("opencypher", "MATCH (n:T) SET n.a = 2, n.b = 2, n.c = 2").close();

    assertThat(audit()).containsExactly("before k=1 a=2 b=2 c=2", "after k=1 a=2 b=2 c=2");
  }

  @Test
  void multiItemSetFiresUpdateTriggersOnceWithFinalStateOnLegacyPath() {
    database.command("opencypher", "MATCH (n) WHERE n.k = 1 SET n.a = 2, n.b = 2, n.c = 2").close();

    assertThat(audit()).containsExactly("before k=1 a=2 b=2 c=2", "after k=1 a=2 b=2 c=2");
  }

  @Test
  void multiItemSetInsideAnExplicitTransactionFiresOnce() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:T) SET n.a = 2, n.b = 2, n.c = 2").close());

    assertThat(audit()).containsExactly("before k=1 a=2 b=2 c=2", "after k=1 a=2 b=2 c=2");
  }

  @Test
  void mixedItemShapesFireOnce() {
    database.command("opencypher", "MATCH (n:T) SET n['a'] = 3, n.b = null, n += {c: 3}").close();

    assertThat(audit()).containsExactly("before k=1 a=3 b=null c=3", "after k=1 a=3 b=null c=3");
  }

  @Test
  void clauseWritingTwoRecordsSavesEachOnce() {
    insert("INSERT INTO T SET k = 2, a = 0, b = 0, c = 0");

    database.command("opencypher", "MATCH (x:T {k: 1}), (y:T {k: 2}) SET x.a = 1, y.a = 2, x.b = 3").close();

    assertThat(audit()).containsExactlyInAnyOrder("before k=1 a=1 b=3 c=0", "after k=1 a=1 b=3 c=0", "before k=2 a=2 b=0 c=0",
        "after k=2 a=2 b=0 c=0");
  }

  @Test
  void mergeOnMatchSetFiresOnce() {
    database.command("opencypher", "MERGE (n:T {k: 1}) ON MATCH SET n.a = 5, n.b = 5, n.c = 5").close();

    assertThat(audit()).containsExactly("before k=1 a=5 b=5 c=5", "after k=1 a=5 b=5 c=5");
  }

  @Test
  void mergeFollowedBySetClauseFiresOnce() {
    database.command("opencypher", "MERGE (n:T {k: 1}) SET n.a = 6, n.b = 6, n.c = 6").close();

    assertThat(audit()).containsExactly("before k=1 a=6 b=6 c=6", "after k=1 a=6 b=6 c=6");
  }

  @Test
  void foreachSetFiresOnce() {
    database.command("opencypher", "MATCH (n:T) FOREACH (i IN [7] | SET n.a = i, n.b = i, n.c = i)").close();

    assertThat(audit()).containsExactly("before k=1 a=7 b=7 c=7", "after k=1 a=7 b=7 c=7");
  }

  @Test
  void rowFanoutStillSavesOncePerRowAndAccumulates() {
    database.command("opencypher", "MATCH (n:T) UNWIND [1, 2] AS i SET n.a = n.a + i, n.b = i").close();

    assertThat(audit()).containsExactly("before k=1 a=1 b=1 c=0", "after k=1 a=1 b=1 c=0", "before k=1 a=3 b=2 c=0",
        "after k=1 a=3 b=2 c=0");
  }

  @Test
  void laterClausesReadTheFinalValues() {
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (n:T) SET n.a = 8, n.b = 9, n.c = 8 WITH n MATCH (m:T) WHERE m.a = 8 RETURN n.a AS a, n.b AS b, m.c AS c")) {
      final Result row = rs.next();
      assertThat(intOf(row, "a")).isEqualTo(8);
      assertThat(intOf(row, "b")).isEqualTo(9);
      assertThat(intOf(row, "c")).isEqualTo(8);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void labelWriteInTheSameClauseKeepsEveryItem() {
    try (final ResultSet rs = database.command("opencypher", "MATCH (n:T) SET n.a = 9, n:Extra, n.b = 9 RETURN n")) {
      final Vertex v = rs.next().<Vertex>getProperty("n");
      assertThat(v.getType().instanceOf("T")).isTrue();
      assertThat(v.getType().instanceOf("Extra")).isTrue();
      assertThat(v.getLong("a")).isEqualTo(9L);
      assertThat(v.getLong("b")).isEqualTo(9L);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Extra) RETURN n.a AS a, n.b AS b, n.c AS c")) {
      final Result row = rs.next();
      assertThat(intOf(row, "a")).isEqualTo(9);
      assertThat(intOf(row, "b")).isEqualTo(9);
      assertThat(intOf(row, "c")).isEqualTo(0);
    }
  }

  @Test
  void maxConstraintStillRefusesTheClause() {
    database.command("sql", "CREATE VERTEX TYPE M");
    database.command("sql", "CREATE PROPERTY M.a INTEGER (max 100)");
    insert("INSERT INTO M SET a = 1, b = 1");

    assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:M) SET n.b = 2, n.a = 1000").close())
        .hasRootCauseInstanceOf(ValidationException.class);

    try (final ResultSet rs = database.query("sql", "SELECT a, b FROM M")) {
      final Result row = rs.next();
      assertThat(intOf(row, "a")).isEqualTo(1);
      assertThat(intOf(row, "b")).isEqualTo(1);
    }
  }

  @Test
  void readonlyPropertyStillRefusesTheClause() {
    database.command("sql", "CREATE VERTEX TYPE R");
    database.command("sql", "CREATE PROPERTY R.code STRING (readonly true)");
    insert("INSERT INTO R SET code = 'A', x = 0");

    assertThatThrownBy(() -> database.command("opencypher", "MATCH (n:R) SET n.x = 1, n.code = 'B'").close())
        .hasRootCauseInstanceOf(ValidationException.class);

    try (final ResultSet rs = database.query("sql", "SELECT code, x FROM R")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("code")).isEqualTo("A");
      assertThat(intOf(row, "x")).isEqualTo(0);
    }
  }

  private static int intOf(final Result row, final String property) {
    return ((Number) row.getProperty(property)).intValue();
  }

  private void insert(final String sql) {
    database.transaction(() -> database.command("sql", sql).close());
  }

  private List<String> audit() {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM Audit ORDER BY @rid")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        rows.add(r.getProperty("phase") + " k=" + r.getProperty("k") + " a=" + r.getProperty("a") + " b=" + r.getProperty("b")
            + " c=" + r.getProperty("c"));
      }
    }
    return rows;
  }
}
