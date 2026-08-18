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
 * Regression test for issue #6352: the start node of a pattern comprehension that binds no outer variable picked its
 * scan root by taking the first label and nothing else, so {@code [(y:Author|Topic)-->() | y.k]} only ever saw the
 * {@code :Author} vertices, and an alternative naming a type nobody created emptied the whole comprehension.
 * <p>
 * Same failure mode as issue #6338, one code path over: the per-candidate filter honours the disjunction, but a vertex
 * carrying only the second alternative was never handed to it because it was never scanned. The MATCH spelling of each
 * query is asserted alongside the comprehension, since the two are the same question and must give the same answer -
 * which is also what Neo4j does.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherComprehensionStartLabelScanIssue6352Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-comprehension-start-label-6352");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Author {k:'a1'})");
      database.command("opencypher", "CREATE (:Topic {k:'t1'})");
      database.command("opencypher", "CREATE (:Sink {k:'s1'})");
      database.command("opencypher", "CREATE (:Author:Topic {k:'b1'})");
      database.command("opencypher", "MATCH (a:Author {k:'a1'}), (s:Sink) CREATE (a)-[:E]->(s)");
      database.command("opencypher", "MATCH (t:Topic {k:'t1'}), (s:Sink) CREATE (t)-[:E]->(s)");
      database.command("opencypher", "MATCH (b {k:'b1'}), (s:Sink) CREATE (b)-[:E]->(s)");
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
  void anUncorrelatedStartScansEveryAlternativeOfADisjunction() {
    // Only the :Author vertices used to be scanned, so 't1' was missing without any error or warning.
    assertThat(listKeys("RETURN [(y:Author|Topic)-->() | y.k] AS l")).containsExactlyInAnyOrder("a1", "t1", "b1");
    assertThat(keys("MATCH (y:Author|Topic)-->() RETURN y.k AS k")).containsExactlyInAnyOrder("a1", "t1", "b1");
  }

  @Test
  void anAlternativeTheSchemaDoesNotHaveDoesNotEmptyTheComprehension() {
    // The first alternative naming a type nobody created took an early return out of the whole comprehension.
    assertThat(listKeys("RETURN [(y:NoSuchLabel|Topic)-->() | y.k] AS l")).containsExactlyInAnyOrder("t1", "b1");
    assertThat(listKeys("RETURN [(y:Topic|NoSuchLabel)-->() | y.k] AS l")).containsExactlyInAnyOrder("t1", "b1");
    assertThat(keys("MATCH (y:NoSuchLabel|Topic)-->() RETURN y.k AS k")).containsExactlyInAnyOrder("t1", "b1");
  }

  @Test
  void aDisjunctionOfLabelsNoneOfWhichExistsMatchesNothing() {
    assertThat(listKeys("RETURN [(y:NoSuchLabel|NorThisOne)-->() | y.k] AS l")).isEmpty();
  }

  @Test
  void aConjunctionOnTheUncorrelatedStartStillRequiresEveryLabel() {
    assertThat(listKeys("RETURN [(y:Author:Topic)-->() | y.k] AS l")).containsExactly("b1");
    assertThat(keys("MATCH (y:Author:Topic)-->() RETURN y.k AS k")).containsExactly("b1");

    // A conjunction with an alternative the schema does not have can match nothing, unlike a disjunction.
    assertThat(listKeys("RETURN [(y:Author:NoSuchLabel)-->() | y.k] AS l")).isEmpty();
  }

  @Test
  void aSingleLabelStartStillScansItsSubtypesOnly() {
    // Polymorphic: the Author~Topic composite type is a subtype of both, so it answers to either single label.
    assertThat(listKeys("RETURN [(y:Author)-->() | y.k] AS l")).containsExactlyInAnyOrder("a1", "b1");
    assertThat(listKeys("RETURN [(y:Topic)-->() | y.k] AS l")).containsExactlyInAnyOrder("t1", "b1");
    assertThat(listKeys("RETURN [(y:NoSuchLabel)-->() | y.k] AS l")).isEmpty();
  }

  @Test
  void anUnlabelledStartStillScansEveryVertex() {
    assertThat(listKeys("RETURN [(y)-->() | y.k] AS l")).containsExactlyInAnyOrder("a1", "t1", "b1");
  }

  @Test
  void inlinePropertiesAndWhereStillNarrowADisjunctionStart() {
    assertThat(listKeys("RETURN [(y:Author|Topic {k:'t1'})-->() | y.k] AS l")).containsExactly("t1");
    assertThat(listKeys("RETURN [(y:Author|Topic WHERE y.k = 'a1')-->() | y.k] AS l")).containsExactly("a1");
  }

  @Test
  void aDisjunctionStartWorksInsideACorrelatedComprehensionToo() {
    // The comprehension is evaluated once per outer row: the start node is still uncorrelated, the target is not.
    assertThat(listKeys("MATCH (s:Sink) RETURN [(y:Author|Topic)-->(s) | y.k] AS l"))
        .containsExactlyInAnyOrder("a1", "t1", "b1");
  }

  @Test
  void aTypeExtendingACompositeCarriesItsLabelsEverywhere() {
    // The label constraint is answered by type inheritance, so a type extending the Author~Topic composite is an
    // Author and a Topic even though neither is listed as its own supertype. MERGE used to compare label lists
    // instead of asking the type, so it missed such a node and created a duplicate where MATCH found one.
    database.command("sql", "CREATE VERTEX TYPE Special EXTENDS `Author~Topic`");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Special SET k = 'sp1'");
      database.command("opencypher", "MATCH (n {k:'sp1'}), (s:Sink) CREATE (n)-[:E]->(s)");
    });

    assertThat(keys("MATCH (y:Author:Topic) RETURN y.k AS k")).containsExactlyInAnyOrder("b1", "sp1");
    assertThat(listKeys("RETURN [(y:Author:Topic)-->() | y.k] AS l")).containsExactlyInAnyOrder("b1", "sp1");
    assertThat(listKeys("RETURN [(y:Author|Topic)-->() | y.k] AS l"))
        .containsExactlyInAnyOrder("a1", "t1", "b1", "sp1");

    database.transaction(() -> database.command("opencypher", "MERGE (n:Author:Topic {k:'sp1'})"));
    assertThat(keys("MATCH (y {k:'sp1'}) RETURN y.k AS k")).containsExactly("sp1");
  }

  @Test
  void theOptimizerDisjunctionScanReturnsEveryAlternativeToo() {
    // The MATCH spellings above are answered by whichever path the planner picks; this pins the cost-based one,
    // whose NodeByLabelDisjunctionScan operator now shares the scan with everything else and lost its own copy
    // of the walk in the process.
    final String plan = explain("MATCH (y:Author|Topic) RETURN y.k AS k");
    assertThat(plan).contains("NodeByLabelDisjunctionScan(y:Author|Topic)");

    assertThat(keys("MATCH (y:Author|Topic) RETURN y.k AS k")).containsExactlyInAnyOrder("a1", "t1", "b1");
    assertThat(keys("MATCH (y:NoSuchLabel|Topic) RETURN y.k AS k")).containsExactlyInAnyOrder("t1", "b1");
    assertThat(keys("MATCH (y:NoSuchLabel|NorThisOne) RETURN y.k AS k")).isEmpty();
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  @SuppressWarnings("unchecked")
  private List<String> listKeys(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      return (List<String>) rs.next().getProperty("l");
    }
  }

  private List<String> keys(final String query) {
    final List<String> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        keys.add(row.getProperty("k"));
      }
    }
    return keys;
  }
}
