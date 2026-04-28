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
 * Regression test for GitHub issue #3999.
 * <p>
 * A previously bound relationship variable must remain bound after a later
 * {@code MATCH} uses a variable-length relationship pattern over the same
 * endpoints. Cypher's relationship uniqueness only applies within a single
 * {@code MATCH} clause, so the variable-length traversal is allowed to use
 * the same edge that the earlier {@code MATCH} bound to {@code r}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3999RelVarLostAfterVarLengthMatchTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-3999").create();
    database.getSchema().createVertexType("Person3999");
    database.getSchema().createEdgeType("KNOWS3999");
    database.transaction(() -> database.command("opencypher",
        """
        CREATE (a:Person3999 {name:'Alice'}),\
               (b:Person3999 {name:'Bob'}),\
               (a)-[:KNOWS3999]->(b)"""));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void countRelationshipBoundBeforeUnboundedVarLengthMatch() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (a:Person3999 {name:'Alice'})-[r:KNOWS3999]->(b:Person3999 {name:'Bob'}) \
        WITH a, b, r \
        MATCH path = (a)-[:KNOWS3999*]->(b) \
        RETURN count(r) AS rc""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
  }

  @Test
  void countRelationshipBoundBeforeBoundedVarLengthMatch() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (a:Person3999 {name:'Alice'})-[r:KNOWS3999]->(b:Person3999 {name:'Bob'}) \
        WITH a, b, r \
        MATCH path = (a)-[:KNOWS3999*1..1]->(b) \
        RETURN count(r) AS rc""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
  }

  @Test
  void collectTypeOfRelationshipBoundBeforeVarLengthMatch() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (a:Person3999 {name:'Alice'})-[r:KNOWS3999]->(b:Person3999 {name:'Bob'}) \
        WITH a, b, r \
        MATCH path = (a)-[:KNOWS3999*]->(b) \
        RETURN count(r) AS rc, collect(type(r)) AS rts""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
    final List<Object> rts = rows.get(0).getProperty("rts");
    assertThat(rts).containsExactly("KNOWS3999");
  }

  @Test
  void countRelationshipCreatedAndCarriedThroughVarLengthMatch() {
    // Drop the KNOWS edge created in setUp so only the CREATE'd edge exists
    database.transaction(() -> database.command("opencypher", "MATCH ()-[r:KNOWS3999]->() DELETE r"));

    final ResultSet[] resultRef = new ResultSet[1];
    database.transaction(() -> resultRef[0] = database.command("opencypher",
        """
        MATCH (a:Person3999), (b:Person3999) \
        WHERE a.name = 'Alice' AND b.name = 'Bob' \
        CREATE (a)-[r:KNOWS3999 {label:'second'}]->(b) \
        WITH a, b, r \
        MATCH path = (a)-[:KNOWS3999*]->(b) \
        WITH count(r) AS rc \
        RETURN rc"""));

    final List<Result> rows = collect(resultRef[0]);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
  }

  @Test
  void mergeRelationshipAndConsumePath() {
    // Drop the KNOWS edge created in setUp so MERGE creates fresh state
    database.transaction(() -> database.command("opencypher", "MATCH ()-[r:KNOWS3999]->() DELETE r"));

    final ResultSet[] resultRef = new ResultSet[1];
    database.transaction(() -> resultRef[0] = database.command("opencypher",
        """
        MATCH (p1:Person3999), (p2:Person3999) \
        WHERE p1.name = 'Alice' AND p2.name = 'Bob' \
        MERGE (p1)-[r:KNOWS3999 {since: 2020}]->(p2) \
        WITH p1, p2, r \
        MATCH path = (p1)-[:KNOWS3999*]->(p2) \
        RETURN p1.name AS startName, \
               p2.name AS endName, \
               count(r) AS relationshipCount"""));

    final List<Result> rows = collect(resultRef[0]);
    assertThat(rows).hasSize(1);
    assertThat((String) rows.get(0).getProperty("startName")).isEqualTo("Alice");
    assertThat((String) rows.get(0).getProperty("endName")).isEqualTo("Bob");
    assertThat(((Number) rows.get(0).getProperty("relationshipCount")).longValue()).isEqualTo(1L);
  }

  @Test
  void fixedLengthControlCaseAlreadyWorks() {
    final ResultSet result = database.query("opencypher",
        """
        MATCH (a:Person3999 {name:'Alice'})-[r:KNOWS3999]->(b:Person3999 {name:'Bob'}) \
        WITH a, b, r \
        MATCH path = (a)-[:KNOWS3999]->(b) \
        RETURN count(r) AS rc""");

    final List<Result> rows = collect(result);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("rc")).longValue()).isEqualTo(1L);
  }

  private static List<Result> collect(final ResultSet rs) {
    final List<Result> list = new ArrayList<>();
    while (rs.hasNext())
      list.add(rs.next());
    return list;
  }
}
