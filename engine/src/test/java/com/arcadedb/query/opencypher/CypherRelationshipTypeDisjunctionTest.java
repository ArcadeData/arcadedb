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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for relationship-type disjunction patterns in Cypher MATCH clauses.
 * Tests that -[:A|B]-> matches relationships of type A OR type B.
 *
 * Reproduces GitHub issue #4480 where MATCH (a)-[:R1|R2]->(b) returned 0 rows even when
 * a relationship of type R1 (or R2) existed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherRelationshipTypeDisjunctionTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-rel-type-disjunction");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() ->
        database.command("opencypher", "CREATE (a:T {id:1})-[:R1]->(b:T {id:2})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  private int count(final String query) {
    final ResultSet rs = database.query("opencypher", query);
    assertThat(rs.hasNext()).isTrue();
    return ((Number) rs.next().getProperty("c")).intValue();
  }

  @Test
  void singleTypeMatches() {
    assertThat(count("MATCH (a:T)-[:R1]->(b:T) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void typeDisjunctionFirstAlternativeMatches() {
    assertThat(count("MATCH (a:T)-[:R1|R2]->(b:T) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void typeDisjunctionSecondAlternativeMatches() {
    assertThat(count("MATCH (a:T)-[:R2|R1]->(b:T) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void typeDisjunctionNoMatch() {
    assertThat(count("MATCH (a:T)-[:R2|R3]->(b:T) RETURN count(*) AS c")).isEqualTo(0);
  }

  @Test
  void typeInListWorkaroundMatches() {
    assertThat(count("MATCH (a:T)-[r]->(b:T) WHERE type(r) IN ['R1','R2'] RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void typeDisjunctionWithVariableReturnsType() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a:T)-[r:R1|R2]->(b:T) RETURN type(r) AS t");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("t")).isEqualTo("R1");
  }

  @Test
  void typeDisjunctionUndirectedMatches() {
    assertThat(count("MATCH (a:T)-[:R1|R2]-(b:T) RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void typeDisjunctionReverseDirectionMatches() {
    assertThat(count("MATCH (b:T)<-[:R1|R2]-(a:T) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void typeDisjunctionBothTypesExistOnlyFirstHasData() {
    // Create R2 edge type so both alternatives are real types in the schema.
    database.transaction(() ->
        database.command("opencypher", "CREATE (c:T {id:3})-[:R2]->(d:T {id:4})"));
    // R1|R2 should now match both the R1 edge and the R2 edge.
    assertThat(count("MATCH (a:T)-[:R1|R2]->(b:T) RETURN count(*) AS c")).isEqualTo(2);
  }

  @Test
  void typeDisjunctionSecondTypeHasData() {
    database.transaction(() ->
        database.command("opencypher", "CREATE (c:T {id:3})-[:R2]->(d:T {id:4})"));
    // Match only via the second alternative.
    assertThat(count("MATCH (a:T {id:3})-[:R1|R2]->(b:T) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void typeDisjunctionVariableLengthMatches() {
    assertThat(count("MATCH (a:T)-[:R1|R2*1..2]->(b:T) RETURN count(*) AS c")).isEqualTo(1);
  }

  /**
   * Refined repro from issue #4480 (comment): the disjunction dropped the whole match only when
   * BOTH (1) one alternative was an undeclared edge type and (2) the target node carried an inline
   * property anchor. Removing either condition returned the correct count.
   */
  @Test
  void typeDisjunctionUndeclaredSecondTypeWithTargetPropertyAnchor() {
    database.transaction(() ->
        database.command("opencypher", "CREATE (a:Z {k:'a'})-[:RX]->(b:Z {k:'b'})"));
    // RY is never declared. The disjunction must still match the existing RX edge.
    assertThat(count("MATCH (a:Z)-[:RX|RY]->(b:Z) RETURN count(*) AS c")).isEqualTo(1);
    // Same query with an inline property anchor on the target node: must also match.
    assertThat(count("MATCH (a:Z)-[:RX|RY]->(b:Z {k:'b'}) RETURN count(*) AS c")).isEqualTo(1);
  }

  @Test
  void typeDisjunctionUndeclaredFirstTypeWithTargetPropertyAnchor() {
    database.transaction(() ->
        database.command("opencypher", "CREATE (a:Z {k:'a'})-[:RX]->(b:Z {k:'b'})"));
    // Order-independent: undeclared type listed first.
    assertThat(count("MATCH (a:Z)-[:RY|RX]->(b:Z {k:'b'}) RETURN count(*) AS c")).isEqualTo(1);
  }
}
