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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for GitHub issue #7171: inserting empty {@code FOREACH} clauses into a query changed its
 * row count, from 480 rows to 474.
 * <p>
 * An empty FOREACH runs no body at all, so it cannot change anything - and it did not. What it changed was
 * the plan around it: a FOREACH followed by a clause that reads the graph is eager (issue #6922), so it
 * drained the MATCH before the write procedure behind it ran, and accidentally supplied a read/write barrier
 * the query was missing. 474 is the correct answer - the pattern matched 79 edges before the query wrote
 * anything, and 79 x 3 {@code alias0} values x 2 {@code n1} bindings is 474. The six extra rows the query
 * without the FOREACH clauses returned were the {@code rt2} edges {@code merge.relationship} had itself just
 * created, re-read by an enumeration that had not yet run when they appeared.
 * <p>
 * openCypher requires a query's reads to be unaffected by that query's own writes, so the fix is the barrier
 * itself rather than the FOREACH: see {@code CypherEagernessAnalyzer} for where it goes and, just as
 * importantly, where it must not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherEagerWriteBarrierIssue7171Test {
  /** The reporter's graph: 45 vertices carrying up to twelve labels each, and 78 {@code R} edges. */
  private static final String CREATE_NODES = "CREATE (:l6:l7:l0:l1:l2:l4:l8:l5:l11:l9:l3:l10 {id: 1}), (:l1:l3:l4:l8:l10:l0:l2:l7:l11:l6:l9 {id: 3}), (:l7:l5:l8:l1:l10:l6:l9:l11:l4:l3 {id: 5}), (:l4:l2:l8:l9:l0:l7:l3:l6:l11:l5:l10 {id: 7}), (:l3:l6:l7 {id: 8}), (:l3:l8:l2:l9:l10:l5:l6:l7 {id: 17}), (:l1:l0:l5:l10:l3:l2:l11:l6:l4 {id: 22}), (:l3:l8:l4:l10:l2:l7:l5:l11:l6:l9 {id: 23}), (:l10:l8:l4:l7:l6:l5 {id: 24}), (:l0:l7:l5:l6:l11:l10 {id: 26}), (:l0:l6:l7:l11:l10:l8 {id: 29}), (:l7:l8:l2:l0:l10:l11:l1 {id: 32}), (:l9:l6:l3:l8:l5:l2:l10:l0:l4:l7:l1:l11 {id: 34}), (:l11:l9:l8:l4 {id: 39}), (:l1:l10:l11:l0:l4:l6:l3:l9:l5:l7 {id: 41}), (:l1:l6:l5:l10:l8:l11:l9:l3:l0 {id: 46}), (:l5:l9:l6:l1:l2:l11:l0 {id: 47}), (:l11:l10:l2:l5:l0:l7:l6:l8:l3:l9 {id: 48, k0: ['EccO', 'Ng1RgDR', 'KLaHa4c']}), (:l11:l10:l2:l5:l0:l7:l6:l8:l3:l9 {id: 49, k0: ['EccO', 'Ng1RgDR', 'KLaHa4c']}), (:l3:l1 {id: 52}), (:l10:l11:l3:l7:l9 {id: 53}), (:l1:l6:l10 {id: 55}), (:l1:l8:l4:l3 {id: 58}), (:l5:l0:l2:l3:l6:l9:l1:l10:l11:l7 {id: 61}), (:l4:l8:l6:l7:l9:l2:l5:l11 {id: 63}), (:l8:l1:l5:l7:l10:l2:l6:l0:l11 {id: 65}), (:l10:l5:l8 {id: 68}), (:l2:l3 {id: 70}), (:l3:l8:l2:l1:l11:l7:l10 {id: 72}), (:l10:l6:l0:l3:l2:l7:l9:l11 {id: 75}), (:l7:l8:l5:l10:l6:l11:l0 {id: 76}), (:l11:l3:l4:l8:l6:l7 {id: 77}), (:l11:l6:l2:l5:l10:l1:l9:l3:l4 {id: 78}), (:l7:l6:l11:l5:l2:l8 {id: 80}), (:l10:l6:l2:l4:l5:l1:l3:l8:l9:l7:l0:l11 {id: 87}), (:l5:l7:l10:l9:l3 {id: 92}), (:l11:l9:l0:l6:l4:l3:l2:l1:l10:l8:l5 {id: 94}), (:l0:l1:l10:l3:l11:l5:l9:l8:l7:l2:l4:l6 {id: 96}), (:l0:l6:l3 {id: 97}), (:l2:l4:l9:l7:l3:l10:l6:l11:l5 {id: 105, k1: false, k3: 'vfn', k7: 1727121251}), (:l7:l0:l10:l11:l8:l6:l9:l3:l1:l5 {id: 106}), (:l8:l2:l7:l1:l3:l10:l11:l9:l0:l4:l6:l5 {id: 110}), (:l6:l8 {id: 114}), (:l11:l9:l8:l6:l10:l3:l7:l4:l2 {id: 117}), (:l3:l11:l7:l10:l8:l0:l1:l6:l2:l9 {id: 127})";
  private static final String CREATE_EDGES = "UNWIND [[22, 61], [96, 114], [127, 114], [46, 34], [17, 17], [46, 105], [29, 29], [96, 77], [55, 117], [114, 39], [127, 46], [117, 63], [22, 87], [8, 22], [5, 78], [34, 17], [22, 80], [80, 49], [97, 32], [117, 58], [75, 5], [7, 17], [26, 76], [63, 53], [29, 61], [47, 41], [24, 87], [127, 94], [80, 127], [105, 46], [65, 76], [96, 32], [94, 114], [87, 114], [77, 17], [17, 17], [24, 24], [75, 23], [114, 58], [114, 114], [80, 127], [17, 72], [78, 72], [1, 68], [5, 1], [5, 94], [78, 58], [5, 5], [17, 114], [105, 32], [24, 29], [117, 110], [41, 92], [106, 76], [77, 5], [17, 49], [3, 94], [7, 61], [17, 70], [78, 78], [46, 87], [80, 46], [49, 49], [46, 29], [63, 63], [26, 46], [75, 75], [26, 49], [97, 49], [117, 58], [8, 32], [46, 3], [17, 17], [17, 17], [127, 94], [46, 97], [17, 52], [61, 24], [114, 68]] AS edge MATCH (s {id: edge[0]}), (t {id: edge[1]}) CREATE (s)-[:R]->(t)";

  /** The reporter's query, verbatim. */
  private static final String WITHOUT_FOREACH = """
      FOR alias0 IN [-598838671,-1233757578,-1797380024]
      MATCH p0 = (:l3|l8) <-[r0]- (:l6),
        (n0 {k7: 1727121251, k1: false}),
        (n1:l9&l3&l8&l10&l5&l0&l6&l2&l11&l7
            {k0: ["EccO", "Ng1RgDR", "KLaHa4c"]})
      WHERE (toStringOrNull(n0.k3) > 'S')
      WITH * WHERE n1 IS NOT NULL AND n0 IS NOT NULL
      CALL merge.relationship(n1, 'rt2', {}, {generated: true}, n0) YIELD rel AS alias45
      RETURN {alias0: alias0, n0: n0, n1: n1, r0: r0, p0: p0, alias45: alias45} AS __layer_row""";

  /** The same query with an empty FOREACH before FOR, MATCH, WITH and CALL. */
  private static final String WITH_FOREACH = """
      FOREACH (elem37 IN [] | CREATE (:NoOp))
      FOR alias0 IN [-598838671,-1233757578,-1797380024]
      FOREACH (elem38 IN [] | CREATE (:NoOp))
      MATCH p0 = (:l3|l8) <-[r0]- (:l6),
        (n0 {k7: 1727121251, k1: false}),
        (n1:l9&l3&l8&l10&l5&l0&l6&l2&l11&l7
            {k0: ["EccO", "Ng1RgDR", "KLaHa4c"]})
      WHERE (toStringOrNull(n0.k3) > 'S')
      FOREACH (elem39 IN [] | CREATE (:NoOp))
      WITH * WHERE n1 IS NOT NULL AND n0 IS NOT NULL
      FOREACH (elem40 IN [] | CREATE (:NoOp))
      CALL merge.relationship(n1, 'rt2', {}, {generated: true}, n0) YIELD rel AS alias45
      RETURN {alias0: alias0, n0: n0, n1: n1, r0: r0, p0: p0, alias45: alias45} AS __layer_row""";

  /**
   * 79 edges match {@code (:l3|l8)<-[r0]-(:l6)} in the graph as loaded, and the MATCH cross-joins that with
   * three {@code alias0} values, one {@code n0} and two {@code n1} bindings.
   */
  private static final long ROWS_BEFORE_ANY_WRITE = 79 * 3 * 2;

  private Database database;

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void emptyForeachClausesDoNotChangeTheRowCount() {
    final long withoutForeach = rowCountOf("without-foreach", WITHOUT_FOREACH);
    final long withForeach = rowCountOf("with-foreach", WITH_FOREACH);

    assertThat(withForeach)
        .as("an empty FOREACH runs no body: it cannot change what the query returns")
        .isEqualTo(withoutForeach);
    assertThat(withoutForeach)
        .as("the rows are the ones the pattern matched before the query created anything")
        .isEqualTo(ROWS_BEFORE_ANY_WRITE);
  }

  /** No row may report an edge the query's own {@code merge.relationship} created. */
  @Test
  void theQueryNeverReadsBackTheEdgesItCreates() {
    open("no-readback");

    final List<String> types = new ArrayList<>();
    try (final ResultSet resultSet = database.command("opencypher", """
        FOR alias0 IN [-598838671,-1233757578,-1797380024]
        MATCH p0 = (:l3|l8) <-[r0]- (:l6),
          (n0 {k7: 1727121251, k1: false}),
          (n1:l9&l3&l8&l10&l5&l0&l6&l2&l11&l7 {k0: ["EccO", "Ng1RgDR", "KLaHa4c"]})
        CALL merge.relationship(n1, 'rt2', {}, {generated: true}, n0) YIELD rel AS alias45
        RETURN type(r0) AS relationshipType""")) {
      while (resultSet.hasNext())
        types.add(resultSet.next().getProperty("relationshipType"));
    }

    assertThat(types).hasSize((int) ROWS_BEFORE_ANY_WRITE).containsOnly("R");
    assertThat(count("MATCH ()-[r:rt2]->() RETURN count(*) AS c"))
        .as("one rt2 edge per (n1, n0) pair, and both n1 bindings resolve to the same n0")
        .isEqualTo(2);
  }

  /**
   * The same hazard reached through a plain MERGE rather than a procedure: {@code [r0]} is untyped, so it
   * matches the {@code rt2} edges the MERGE adds.
   */
  @Test
  void mergeAfterAnUntypedRelationshipPatternIsEager() {
    open("merge-eager");

    assertThat(rowCount("""
        FOR alias0 IN [-598838671,-1233757578,-1797380024]
        MATCH p0 = (:l3|l8) <-[r0]- (:l6),
          (n0 {k7: 1727121251, k1: false}),
          (n1:l9&l3&l8&l10&l5&l0&l6&l2&l11&l7 {k0: ["EccO", "Ng1RgDR", "KLaHa4c"]})
        MERGE (n1)-[:rt2]->(n0)
        RETURN r0"""))
        .isEqualTo(ROWS_BEFORE_ANY_WRITE);
  }

  /** A write whose type no pattern reads keeps streaming: the barrier is not a blanket eager mode. */
  @Test
  void aWriteThatNoPatternCanReadKeepsStreaming() {
    open("no-barrier");
    database.getSchema().createVertexType("Person");
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, 5) AS i CREATE (:Person {id: i})"));

    assertThat(explain("MATCH (a:Person)-[r:KNOWS]->(b:Person) CREATE (a)-[:SCORED]->(b) RETURN a"))
        .as("KNOWS is read, SCORED is written: nothing the CREATE adds can feed the MATCH")
        .doesNotContain("EAGER");
    assertThat(explain("UNWIND range(1, 10) AS i CREATE (:Person {id: i}) RETURN i"))
        .as("a bulk insert reads nothing at all")
        .doesNotContain("EAGER");
    assertThat(explain("MATCH (a:Person) CREATE (:Person {name: a.name}) RETURN a"))
        .as("the CREATE adds vertices of the very type the MATCH is scanning")
        .contains("EAGER");
  }

  /**
   * A FOREACH whose body actually writes goes through the analyzer's recursive body walk, not the
   * empty-body path the reported query exercises: {@code CREATE (:l6)} inside the body adds vertices the
   * MATCH's own {@code (:l6)} anchor scans, so the FOREACH gets a barrier of its own.
   */
  @Test
  void aForeachWhoseBodyWritesAConflictingPatternIsBarriered() {
    open("foreach-body");

    assertThat(explain("""
        MATCH (:l3|l8) <-[r0]- (:l6)
        FOREACH (x IN [1] | CREATE (:l6))
        RETURN r0"""))
        .as("the body creates vertices of a label the MATCH anchor scans")
        .contains("EAGER");
    assertThat(explain("""
        MATCH (:l3|l8) <-[r0]- (:l6)
        FOREACH (x IN [1] | CREATE (:Untouched))
        RETURN r0"""))
        .as("Untouched is a label no pattern reads")
        .doesNotContain("EAGER");
  }

  /** One barrier is enough for the writes behind it: it already drained every enumeration they could see. */
  @Test
  void consecutiveConflictingWritesShareOneBarrier() {
    open("one-barrier");

    final String plan = explain("""
        MATCH (n:l6)
        CREATE (:l6)
        CREATE (:l6)
        RETURN n""");
    assertThat(plan).contains("EAGER");
    assertThat(plan.split("EAGER", -1).length - 1).as("exactly one barrier, not one per write").isEqualTo(1);
  }

  private long rowCountOf(final String name, final String query) {
    open(name);
    return rowCount(query);
  }

  private long rowCount(final String query) {
    long rows = 0;
    try (final ResultSet resultSet = database.command("opencypher", query)) {
      while (resultSet.hasNext()) {
        final Result ignored = resultSet.next();
        ++rows;
      }
    }
    return rows;
  }

  private long count(final String query) {
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      return ((Number) resultSet.next().getProperty("c")).longValue();
    }
  }

  private String explain(final String query) {
    try (final ResultSet resultSet = database.command("opencypher", "EXPLAIN " + query)) {
      return resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private void open(final String name) {
    if (database != null)
      database.drop();
    database = new DatabaseFactory("./target/databases/cypher-eager-write-barrier-7171-" + name).create();
    database.transaction(() -> {
      database.command("opencypher", CREATE_NODES);
      database.command("opencypher", CREATE_EDGES);
    });
  }
}
