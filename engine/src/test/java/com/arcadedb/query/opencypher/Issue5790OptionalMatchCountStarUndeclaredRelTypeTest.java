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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces issue #5790: {@code count(*)} after an {@code OPTIONAL MATCH} over a relationship
 * type that is not declared in the schema returned HTTP 500 ({@code SchemaException: Type with
 * name '...' was not found}) instead of the correct aggregated count.
 * <p>
 * Root cause: the star-join count-push-down ({@code CypherExecutionPlan#tryDetectStarCountStar})
 * builds a {@code DegreeProductOp} arm for the OPTIONAL MATCH relationship without checking
 * whether the schema actually declares that edge type. {@code DegreeProductOp#executeOLTPDegreeMap}
 * then called {@code Database#iterateType(String, boolean)} unconditionally, which resolves the
 * type via {@code Schema#getType(String)} and throws for an undeclared name. Every other
 * aggregation form ({@code count(m)}, {@code sum(1)}, {@code collect(m)}) and the un-aggregated
 * query are answered by the ordinary materialization pipeline, which already tolerates an
 * undeclared relationship type via vertex-local edge-list filtering ({@code EdgeLinkedList#count},
 * fixed for issue #4199) - only the {@code count(*)} degree-product fast path skipped that guard.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/5790">Issue #5790</a>
 */
class Issue5790OptionalMatchCountStarUndeclaredRelTypeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testissue5790").create();
    database.getSchema().createVertexType("T");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void countStarAfterOptionalMatchOverUndeclaredRelTypeCountsAnchorRow() {
    database.transaction(() -> database.newVertex("T").set("id", 1).save());

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN count(*) AS c");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void countStarAfterOptionalMatchOverUndeclaredRelTypeOnEmptyGraphIsZero() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN count(*) AS c");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
    rs.close();
  }

  @Test
  void countStarAfterOptionalMatchOverUndeclaredRelTypeWithMultipleAnchors() {
    database.transaction(() -> {
      database.newVertex("T").set("id", 1).save();
      database.newVertex("T").set("id", 2).save();
      database.newVertex("T").set("id", 3).save();
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN count(*) AS c");

    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("c")).isEqualTo(3L);
    rs.close();
  }

  @Test
  void controlFormsAlreadyWorkedAndKeepWorking() {
    database.transaction(() -> database.newVertex("T").set("id", 1).save());

    try (ResultSet rs = database.query("opencypher",
        "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN count(m) AS c")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(0L);
    }

    try (ResultSet rs = database.query("opencypher",
        "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN sum(1) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1L);
    }

    try (ResultSet rs = database.query("opencypher",
        "MATCH (n:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(m:T) RETURN n.id AS id")) {
      assertThat(rs.next().<Number>getProperty("id").longValue()).isEqualTo(1L);
    }
  }

  @Test
  void countStarWithOneDeclaredAndOneUndeclaredOptionalArm() {
    // Star-join with two arms sharing the central variable n: a declared LINK arm and an
    // undeclared NO_SUCH arm. Only the undeclared arm's degree must be treated as always 0
    // (max(1, 0) = 1 multiplier), the declared arm must still be counted correctly.
    database.getSchema().createEdgeType("LINK");
    database.transaction(() -> {
      final MutableVertex n1 = database.newVertex("T").set("id", 1).save();
      final MutableVertex n2 = database.newVertex("T").set("id", 2).save();
      n1.newEdge("LINK", n2);
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (n:T) OPTIONAL MATCH (n)-[:LINK]->(x:T) OPTIONAL MATCH (n)-[:NO_SUCH]->(y:T) RETURN count(*) AS c");

    // n1 contributes 1 row (LINK degree 1 * NO_SUCH max(1,0)=1), n2 contributes 1 row
    // (LINK degree 0 -> optional null row * NO_SUCH max(1,0)=1) = 2 total.
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
    rs.close();
  }
}
