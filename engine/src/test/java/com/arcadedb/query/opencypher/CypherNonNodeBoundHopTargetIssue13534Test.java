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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #13534: a hop whose end variable is already bound to something that is not a node (a link read from
 * a property, a string, a number) cannot match, exactly as a bare {@code MATCH (m)} over such a binding does not
 * ({@code BoundNodeCheckStep}, issue #11761). The hop used to take the binding for an unbound variable, matched every neighbour
 * and rebound the variable to it, so the answer depended on how the query was split and {@code DETACH DELETE m} deleted the
 * neighbours.
 * <p>
 * Cy knows Bo and Dee and does not know Ada; Bo's {@code mentor} is a link to Ada.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherNonNodeBoundHopTargetIssue13534Test {
  private static final String BO_CY  = "MATCH (b:person {name: 'Bo'}), (c:person {name: 'Cy'}) ";
  private static final String LINK_M = BO_CY + "WITH c, b.mentor AS m ";

  private Database database;
  private RID      ada;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cypher13534").create();
    database.getSchema().createVertexType("person");
    database.getSchema().createEdgeType("knows");
    database.transaction(() -> {
      ada = database.newVertex("person").set("name", "Ada").save().getIdentity();
      final RID bo = database.newVertex("person").set("name", "Bo").set("mentor", ada).set("age", 42).save().getIdentity();
      database.newVertex("person").set("name", "Cy").save();
      database.newVertex("person").set("name", "Dee").set("friend", bo).save();
      database.command("opencypher",
          "MATCH (c:person {name: 'Cy'}), (x:person) WHERE x.name IN ['Bo', 'Dee'] CREATE (c)-[:knows]->(x)");
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void hopToALinkMatchesNothing() {
    assertThat(rows(LINK_M + "MATCH (c)-[:knows]->(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (c)-[r:knows]->(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (c)-[:knows]-(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (c)-[:knows]->(m:person) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (c:person)-[:knows]->(m:person) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (c:person)-[:knows*1..2]->(m:person) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (m)<-[:knows]-(c) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (c)-[:knows]->(y)<-[:knows]-(c2)-[:knows]->(m) RETURN m.name AS name")).isEmpty();
    // the control: m bound to the node Ada, whom Cy does not know
    assertThat(rows("MATCH (m:person {name: 'Ada'}), (c:person {name: 'Cy'}) MATCH (c)-[:knows]->(m) RETURN m.name AS name"))
        .isEmpty();
  }

  @Test
  void hopToAStringOrANumberMatchesNothing() {
    assertThat(rows(BO_CY + "WITH c, b.name AS m MATCH (c)-[:knows]->(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(BO_CY + "WITH c, b.name AS m MATCH (c)-[r:knows]->(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(BO_CY + "WITH c, b.age AS m MATCH (c)-[:knows]->(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(BO_CY + "WITH c, b.age AS m MATCH (c)-[:knows*1..2]->(m) RETURN m.name AS name")).isEmpty();
  }

  @Test
  void theAnswerDoesNotDependOnHowTheQueryIsSplit() {
    assertThat(rows("MATCH (b:person {name: 'Bo'}) WITH b.mentor AS m MATCH (c:person {name: 'Cy'})-[:knows]->(m) RETURN m.name AS name"))
        .isEmpty();
    assertThat(rows("MATCH (b:person {name: 'Bo'}) WITH b.mentor AS m MATCH (c:person {name: 'Cy'}) MATCH (c)-[:knows]->(m) RETURN m.name AS name"))
        .isEmpty();
    // an unbound source and a bound target: the planner walks the hop from the target
    assertThat(rows("MATCH (b:person {name: 'Bo'}) WITH b.mentor AS m MATCH (y)-[:knows]->(m) RETURN y.name AS name")).isEmpty();

    // a link to a vertex Cy does know is no node either: the hop agrees with a bare (m) over the same link, whichever end the
    // plan starts from
    final String linkToBo = "MATCH (d:person {name: 'Dee'}), (c:person {name: 'Cy'}) WITH c, d.friend AS m ";
    assertThat(rows(linkToBo + "MATCH (m) RETURN m AS m")).isEmpty();
    assertThat(rows(linkToBo + "MATCH (c)-[:knows]->(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(linkToBo + "MATCH (m)<-[:knows]-(c) RETURN m.name AS name")).isEmpty();
    assertThat(rows("MATCH (d:person {name: 'Dee'}) WITH d.friend AS m MATCH (c:person {name: 'Cy'})-[:knows]->(m) RETURN m.name AS name"))
        .isEmpty();
  }

  @Test
  void optionalHopToALinkKeepsTheRowAndTheBinding() {
    final List<Result> rows = rows(LINK_M + "OPTIONAL MATCH (c)-[:knows]->(m) RETURN c.name AS c, m AS m");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.getFirst().getProperty("c")).isEqualTo("Cy");
    assertThat((Object) rows.getFirst().getProperty("m")).isEqualTo(ada);

    final List<Result> withRel = rows(LINK_M + "OPTIONAL MATCH (c)-[r:knows]->(m) RETURN m AS m, r AS r");
    assertThat(withRel).hasSize(1);
    assertThat((Object) withRel.getFirst().getProperty("m")).isEqualTo(ada);
    assertThat((Object) withRel.getFirst().getProperty("r")).isNull();

    final List<Result> varLength = rows(LINK_M + "OPTIONAL MATCH (c)-[:knows*1..2]->(m) RETURN m AS m");
    assertThat(varLength).hasSize(1);
    assertThat((Object) varLength.getFirst().getProperty("m")).isEqualTo(ada);

    final List<Result> reversed = rows(LINK_M + "OPTIONAL MATCH (y)-[:knows]->(m) RETURN y AS y, m AS m");
    assertThat(reversed).hasSize(1);
    assertThat((Object) reversed.getFirst().getProperty("y")).isNull();
    assertThat((Object) reversed.getFirst().getProperty("m")).isEqualTo(ada);
  }

  @Test
  void variableLengthHopToALinkMatchesNothing() {
    assertThat(rows(LINK_M + "MATCH (c)-[:knows*1..2]->(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (c)-[:knows*]->(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (c)-[r:knows*1..2]->(m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH p = (c)-[:knows*1..2]->(m) RETURN m.name AS name")).isEmpty();
    // only the distinct end vertices are asked for: the expansion counts them itself, and still none for a link
    assertThat(((Number) single(LINK_M + "MATCH (c)-[:knows*1..2]->(m) RETURN count(DISTINCT m) AS v")).longValue()).isZero();
    assertThat(((Number) single(LINK_M + "MATCH (c)-[:knows*1..2]->(m) RETURN count(DISTINCT c) AS v")).longValue()).isZero();
  }

  @Test
  void quantifiedPathToALinkMatchesNothing() {
    assertThat(rows(LINK_M + "MATCH (c) ((p)-[:knows]->(q)){1,2} (m) RETURN m.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "MATCH (c) ((p)-[:knows]->(q))+ (m) RETURN m.name AS name")).isEmpty();
    final List<Result> rows = rows(LINK_M + "OPTIONAL MATCH (c) ((p)-[:knows]->(q)){1,2} (m) RETURN m AS m");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.getFirst().getProperty("m")).isEqualTo(ada);
  }

  @Test
  void comprehensionStartingAtALinkFindsNothing() {
    // a leading node bound to a link is not an uncorrelated start: it must not scan every vertex in its place
    assertThat(((Number) single(LINK_M + "RETURN size([(m)-[:knows]-() | 1]) AS v")).longValue()).isZero();
    assertThat(((Number) single(LINK_M + "RETURN size([(m)<-[:knows]-(x) | x]) AS v")).longValue()).isZero();
    assertThat((List<?>) single(LINK_M + "RETURN [(m)<-[:knows]-(x) | x.name] AS v")).isEmpty();
    assertThat(single(LINK_M + "RETURN EXISTS { (m)<-[:knows]-() } AS v")).isEqualTo(false);
    assertThat(((Number) single(LINK_M + "RETURN COUNT { (m)<-[:knows]-() } AS v")).longValue()).isZero();
  }

  @Test
  void patternPredicateOverALinkIsFalse() {
    assertThat(rows(LINK_M + "WHERE (c)-[:knows]->(m) RETURN c.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "WHERE (c)-[:knows]-(m) RETURN c.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "WHERE (c)-[:knows*1..2]->(m) RETURN c.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "WHERE (m)<-[:knows]-() RETURN c.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "WHERE (m)-->() RETURN c.name AS name")).isEmpty();
    assertThat(rows(LINK_M + "WHERE NOT (c)-[:knows]->(m) RETURN c.name AS name")).hasSize(1);
    assertThat(single(LINK_M + "RETURN exists((c)-[:knows]->(m)) AS v")).isEqualTo(false);
    // the node Ada is still told apart from Bo
    assertThat(rows("MATCH (m:person {name: 'Bo'}), (c:person {name: 'Cy'}) WHERE (c)-[:knows]->(m) RETURN c.name AS name"))
        .hasSize(1);
  }

  @Test
  void subqueriesAndComprehensionsOverALinkFindNothing() {
    assertThat(single(LINK_M + "RETURN EXISTS { (c)-[:knows]->(m) } AS v")).isEqualTo(false);
    assertThat(((Number) single(LINK_M + "RETURN COUNT { (c)-[:knows]->(m) } AS v")).longValue()).isZero();
    assertThat(((Number) single(LINK_M + "RETURN size([(c)-[:knows]->(m) | 1]) AS v")).longValue()).isZero();
    assertThat(single(LINK_M + "RETURN EXISTS { MATCH (c)-[:knows]->(m) } AS v")).isEqualTo(false);
    assertThat(single(LINK_M + "RETURN EXISTS { MATCH (c)-[:knows*1..2]->(m) } AS v")).isEqualTo(false);
    assertThat(((Number) single(LINK_M + "RETURN COUNT { MATCH (c)-[:knows]->(m) } AS v")).longValue()).isZero();
    assertThat(((Number) single(LINK_M + "RETURN COUNT { MATCH (c)-[:knows]-(m) } AS v")).longValue()).isZero();
    assertThat(((Number) single(LINK_M + "RETURN COUNT { MATCH (c)-[:knows*1..2]->(m) } AS v")).longValue()).isZero();
    assertThat((List<?>) single(LINK_M + "RETURN COLLECT { MATCH (c)-[:knows]->(m) RETURN m.name } AS v")).isEmpty();
    assertThat(rows(LINK_M + "WHERE EXISTS { (c)-[:knows]->(m) } RETURN c.name AS name")).isEmpty();
    // labelled bodies, the shape the cost-based plan takes
    assertThat(single(LINK_M + "RETURN EXISTS { MATCH (c:person)-[:knows]->(m:person) } AS v")).isEqualTo(false);
    assertThat(single(LINK_M + "RETURN EXISTS { MATCH (c:person)-[:knows*1..2]->(m:person) } AS v")).isEqualTo(false);
    assertThat(((Number) single(LINK_M + "RETURN COUNT { MATCH (c:person)-[:knows]->(m:person) } AS v")).longValue()).isZero();
    assertThat(((Number) single(LINK_M + "RETURN COUNT { MATCH (m:person)<-[:knows]-(c:person) } AS v")).longValue()).isZero();
  }

  @Test
  void subqueriesOverALiteralFindNothing() {
    final String literal = BO_CY + "WITH c, 42 AS m ";
    assertThat(single(literal + "RETURN EXISTS { (c)-[:knows]->(m) } AS v")).isEqualTo(false);
    assertThat(((Number) single(literal + "RETURN COUNT { (c)-[:knows]->(m) } AS v")).longValue()).isZero();
    assertThat(((Number) single(literal + "RETURN size([(c)-[:knows]->(m) | 1]) AS v")).longValue()).isZero();
  }

  @Test
  void detachDeleteThroughALinkDeletesNothing() {
    database.transaction(() -> database.command("opencypher", LINK_M + "MATCH (c)-[:knows]->(m) DETACH DELETE m").close());
    final List<Object> names = new ArrayList<>();
    for (final Result row : rows("MATCH (p:person) RETURN p.name AS name ORDER BY name"))
      names.add(row.getProperty("name"));
    assertThat(names).containsExactly("Ada", "Bo", "Cy", "Dee");
  }

  @Test
  void nodeBoundTargetStillConstrainsTheHop() {
    final String boundBo = "MATCH (m:person {name: 'Bo'}), (c:person {name: 'Cy'}) WITH c, m ";
    assertThat(rows(boundBo + "MATCH (c)-[:knows]->(m) RETURN m.name AS name")).hasSize(1);
    assertThat(rows(boundBo + "MATCH (c)-[r:knows]->(m) RETURN m.name AS name")).hasSize(1);
    assertThat(rows(boundBo + "MATCH (c)-[:knows*1..2]->(m) RETURN m.name AS name")).hasSize(1);
    assertThat(single(boundBo + "RETURN EXISTS { MATCH (c)-[:knows]->(m) } AS v")).isEqualTo(true);
    assertThat(((Number) single(boundBo + "RETURN COUNT { (c)-[:knows]->(m) } AS v")).longValue()).isEqualTo(1L);
    // an unbound target still matches every neighbour
    assertThat(rows("MATCH (c:person {name: 'Cy'}) MATCH (c)-[:knows]->(m) RETURN m.name AS name")).hasSize(2);
  }

  private List<Result> rows(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private Object single(final String query) {
    final List<Result> rows = rows(query);
    assertThat(rows).hasSize(1);
    return rows.getFirst().getProperty("v");
  }
}
