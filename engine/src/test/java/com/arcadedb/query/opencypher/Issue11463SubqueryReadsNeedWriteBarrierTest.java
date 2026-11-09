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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for GitHub issue #11463: a read that lives inside a {@code CALL} subquery body, or inside a
 * {@code COUNT {}} / {@code EXISTS {}} subquery or a pattern expression of a clause, was invisible to the read/write barrier
 * analysis. A later {@code CREATE} or {@code MERGE} of an entity those reads match therefore streamed behind them, and the
 * rows of the second pull batch (the first 100 rows are pulled before the write runs once) already saw what the write of the
 * first batch had created: the reporter's query answered 100 rows instead of 121, and an unrelated aggregating
 * {@code CALL} subquery in front of the MERGE "fixed" it by draining the rows.
 * <p>
 * openCypher requires a query's reads to be unaffected by that query's own writes; Neo4j plants an {@code Eager} in front of
 * the MERGE and answers 121 rows to both of the reporter's queries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11463SubqueryReadsNeedWriteBarrierTest {
  private static final String SETUP_EDGE = "CREATE (n:l2 {k10: 1912098346, k7: false})-[:rt1 {k1: 926826381, k10: 328995302}]->(:Z)";

  private static final String LEFT = """
      UNWIND range(1, 11) AS alias5
      UNWIND range(1, 11) AS alias6
      WITH alias5, alias6 ORDER BY alias5, alias6
      CALL (alias5, alias6) {
        MATCH () <-[:rt1 {k1: 926826381, k10: 328995302}]- (:l2 {k10: 1912098346, k7: false})
        WHERE COUNT {
          MATCH (:l2 {k7: false})
          RETURN alias5, alias6
        } = 1
        RETURN alias5, alias6
      }
      MERGE (:l11:l2:l10:l0 {k0: false, k7: false, id: 128})
      RETURN alias5, alias6, COUNT { MATCH (n) } AS graphNodeCount""";

  private static final String RIGHT = """
      UNWIND range(1, 11) AS alias5
      UNWIND range(1, 11) AS alias6
      WITH alias5, alias6 ORDER BY alias5, alias6
      CALL (alias5, alias6) {
        MATCH () <-[:rt1 {k1: 926826381, k10: 328995302}]- (:l2 {k10: 1912098346, k7: false})
        WHERE COUNT {
          MATCH (:l2 {k7: false})
          RETURN alias5, alias6
        } = 1
        RETURN alias5, alias6
      }
      CALL () {
        CALL db.index.vector.queryNodes('__ArcadeVectorBarrierProbe[embedding]', 1, [1.0, 0.0, 0.0]) YIELD node, score
        RETURN count(*) AS __discard
      }
      WITH alias5, alias6
      MERGE (:l11:l2:l10:l0 {k0: false, k7: false, id: 128})
      RETURN alias5, alias6, COUNT { MATCH (n) } AS graphNodeCount""";

  private static final String EAGER = "EAGER (read/write barrier)";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue11463");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void reportersLeftQueryReturnsEveryRow() {
    database.transaction(() -> database.command("opencypher", SETUP_EDGE).close());

    final List<Result> rows = run(LEFT);

    assertThat(rows).hasSize(121);
    for (final Result row : rows)
      assertThat(((Number) row.getProperty("graphNodeCount")).longValue()).isEqualTo(3L);
  }

  @Test
  void reportersLeftAndRightQueriesAgree() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:__ArcadeVectorBarrierProbe {embedding: [1.0, 0.0, 0.0]})").close();
      database.command("opencypher", """
          CREATE VECTOR INDEX __ArcadeVectorBarrierProbeEmbeddingIndex FOR (n:__ArcadeVectorBarrierProbe) ON n.embedding \
          OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}""").close();
      database.command("opencypher", SETUP_EDGE).close();
    });

    final List<Result> left = run(LEFT);
    database.transaction(() -> database.command("opencypher", "MATCH (n:l11) DETACH DELETE n").close());
    final List<Result> right = run(RIGHT);

    assertThat(left).hasSize(121);
    assertThat(right).hasSize(121);
    assertThat(pairs(left)).containsExactlyElementsOf(pairs(right));
  }

  /**
   * The reads of a CALL subquery body's MATCH are reads of the outer query: a later write they match needs the barrier. Each
   * MERGE creates a node of its own ({@code id: i}), so without the barrier the rows of the second pull batch count the nodes
   * the first one merged.
   */
  @Test
  void callSubqueryBodyMatchIsARead() {
    database.transaction(() -> database.command("opencypher", "CREATE (:A)").close());
    final String query = """
        UNWIND range(1, 150) AS i
        CALL (i) { MATCH (a:A) RETURN count(a) AS c }
        MERGE (:A {id: i})
        RETURN i, c""";

    assertThat(explain(query)).contains(EAGER);
    final List<Result> rows = run(query);

    assertThat(rows).hasSize(150);
    for (final Result row : rows)
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
    assertThat(countOf("A")).isEqualTo(151L);
  }

  /** The CREATE of a CALL body is not a read, its MATCH is: a write of what only the CREATE names keeps streaming. */
  @Test
  void callSubqueryBodyWritePatternIsNotARead() {
    assertThat(explain("UNWIND range(1, 150) AS i CALL (i) { CREATE (:A {id: i}) } MERGE (:A {id: i})")).doesNotContain(EAGER);
    assertThat(explain("UNWIND range(1, 150) AS i CALL (i) { MATCH (b:B) CREATE (:A {id: i}) } MERGE (:A {id: i})")).doesNotContain(EAGER);
    assertThat(explain("UNWIND range(1, 150) AS i CALL (i) { MATCH (b:A) CREATE (:B {id: i}) } MERGE (:A {id: i})")).contains(EAGER);
  }

  /** A CALL body's MATCH filtered by a COUNT subquery: both read the graph. */
  @Test
  void callSubqueryBodyWhereCountSubqueryIsARead() {
    database.transaction(() -> database.command("opencypher", "CREATE (:X), (:A)").close());
    final String query = """
        UNWIND range(1, 150) AS i
        CALL (i) { MATCH (x:X) WHERE COUNT { MATCH (:A) } = 1 RETURN i AS j }
        MERGE (:A {id: 2})
        RETURN j""";

    assertThat(explain(query)).contains(EAGER);
    assertThat(run(query)).hasSize(150);
  }

  /** A COUNT subquery in an outer WITH ... WHERE reads the graph once per row. */
  @Test
  void countSubqueryInWithWhereIsARead() {
    database.transaction(() -> database.command("opencypher", "CREATE (:B)").close());
    final String query = """
        UNWIND range(1, 150) AS i
        WITH i WHERE COUNT { MATCH (:B) } = 1
        MERGE (:B {id: i})
        RETURN i""";

    assertThat(explain(query)).contains(EAGER);
    assertThat(run(query)).hasSize(150);
    assertThat(countOf("B")).isEqualTo(151L);
  }

  /** A COUNT subquery projected by a WITH reads the graph once per row. */
  @Test
  void countSubqueryInWithProjectionIsARead() {
    database.transaction(() -> database.command("opencypher", "CREATE (:B)").close());
    final String query = """
        UNWIND range(1, 150) AS i
        WITH i, COUNT { MATCH (:B) } AS c
        MERGE (:B {id: i})
        RETURN c""";

    assertThat(explain(query)).contains(EAGER);
    final List<Result> rows = run(query);
    assertThat(rows).hasSize(150);
    for (final Result row : rows)
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
  }

  /** An EXISTS subquery in the WHERE of an outer MATCH reads the graph once per row. */
  @Test
  void existsSubqueryInMatchWhereIsARead() {
    database.transaction(() -> database.command("opencypher", "CREATE (:C)").close());
    final String query = """
        UNWIND range(1, 150) AS i
        MATCH (c:C) WHERE NOT EXISTS { MATCH (:D) }
        MERGE (:D {id: i})
        RETURN i""";

    assertThat(explain(query)).contains(EAGER);
    assertThat(run(query)).hasSize(150);
    assertThat(countOf("D")).isEqualTo(150L);
  }

  /** A pattern predicate in a WITH ... WHERE reads the graph once per row. */
  @Test
  void patternPredicateIsARead() {
    database.transaction(() -> database.command("opencypher", "CREATE (:F)").close());
    final String query = """
        UNWIND range(1, 150) AS i
        MATCH (f:F)
        WITH i, f WHERE NOT (f)-[:R]->(:E)
        MERGE (:E {id: i})<-[:R]-(:G)
        RETURN i""";

    assertThat(explain(query)).contains(EAGER);
    assertThat(run(query)).hasSize(150);
  }

  /** A pattern comprehension projected by a WITH reads the graph once per row. */
  @Test
  void patternComprehensionIsARead() {
    database.transaction(() -> database.command("opencypher", "CREATE (:F)-[:R]->(:E)").close());
    final String query = """
        UNWIND range(1, 150) AS i
        WITH i, size([(:F)-[:R]->(e:E) | e]) AS c
        MERGE (:F {id: i})-[:R]->(:E {id: i})
        RETURN c""";

    assertThat(explain(query)).contains(EAGER);
    final List<Result> rows = run(query);
    assertThat(rows).hasSize(150);
    for (final Result row : rows)
      assertThat(((Number) row.getProperty("c")).longValue()).isEqualTo(1L);
  }

  /** A read nested two levels deep (a COUNT inside an EXISTS inside a CALL inside a CALL) joins the footprint too. */
  @Test
  void deeplyNestedSubqueryReadIsARead() {
    database.transaction(() -> database.command("opencypher", "CREATE (:X), (:A), (:H)").close());
    final String query = """
        UNWIND range(1, 150) AS i
        CALL (i) {
          CALL (i) {
            MATCH (x:X) WHERE EXISTS { MATCH (a:A) WHERE COUNT { MATCH (:H) } = 1 }
            RETURN i AS j
          }
          RETURN j
        }
        MERGE (:H {id: i})
        RETURN j""";

    assertThat(explain(query)).contains(EAGER);
    assertThat(run(query)).hasSize(150);
    assertThat(countOf("H")).isEqualTo(151L);
  }

  /** The relationship a quantified path pattern repeats is a read of its type: a later write of that type needs the barrier. */
  @Test
  void quantifiedPathPatternInCallBodyIsARead() {
    database.transaction(() -> database.command("opencypher", "CREATE (:Q)-[:S]->(:Q)").close());
    final String query = """
        UNWIND range(1, 150) AS i
        CALL (i) { MATCH (:Q) ((:Q)-[:S]->(:Q)){1,2} (:Q) RETURN count(*) AS c }
        MERGE (:Q {id: i})-[:S]->(:Q {id: -i})
        RETURN c""";

    assertThat(explain(query)).contains(EAGER);
    final List<Result> rows = run(query);
    assertThat(rows).hasSize(150);
    final Object first = rows.getFirst().getProperty("c");
    for (final Result row : rows)
      assertThat((Object) row.getProperty("c")).isEqualTo(first);
  }

  /** A write the subquery's reads cannot match keeps streaming: no barrier is planned for it. */
  @Test
  void unrelatedWriteKeepsStreaming() {
    assertThat(explain("UNWIND range(1, 150) AS i CALL (i) { MATCH (a:A) RETURN count(a) AS c } MERGE (:Other {id: i}) RETURN i, c"))
        .doesNotContain(EAGER);
    assertThat(explain("UNWIND range(1, 150) AS i WITH i WHERE COUNT { MATCH (:B) } = 1 MERGE (:Other {id: i})")).doesNotContain(EAGER);
    // two upserting MERGE clauses read nothing the footprint holds: a MERGE's own lookup is not a read of it
    assertThat(explain("UNWIND range(1, 150) AS i MERGE (a:P {id: i}) MERGE (b:P {id: i + 1}) MERGE (a)-[:R]->(b)")).doesNotContain(EAGER);
  }

  private List<Result> run(final String query) {
    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet resultSet = database.command("opencypher", query)) {
        while (resultSet.hasNext())
          rows.add(resultSet.next());
      }
    });
    return rows;
  }

  private String explain(final String query) {
    try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private long countOf(final String label) {
    try (final ResultSet resultSet = database.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS c")) {
      return ((Number) resultSet.next().getProperty("c")).longValue();
    }
  }

  private static List<String> pairs(final List<Result> rows) {
    final List<String> pairs = new ArrayList<>(rows.size());
    for (final Result row : rows)
      pairs.add(row.getProperty("alias5") + ":" + row.getProperty("alias6") + ":" + row.getProperty("graphNodeCount"));
    return pairs;
  }
}
