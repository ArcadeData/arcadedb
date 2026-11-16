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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12254: a {@code DETACH DELETE} of a node bound by a relationship pattern ran on the
 * streaming path, so the first row deleted every relationship of the anchor while the MATCH was still walking the
 * anchor's edge list. The walk then stopped early and the remaining rows were never produced:
 * {@code MATCH (a {k:0})-[r]->(b) DETACH DELETE a, b} over 500 relationships deleted 166 nodes and answered 200, and a
 * {@code SET} on {@code b} in the same statement reached a third of them. Neo4j reads the MATCH to completion first.
 */
class CypherDetachDeleteTraversedNodeIssue12254Test extends TestHelper {
  private static final int FAN_OUT = 500;

  private void createFan() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
    database.transaction(() -> {
      database.command("opencypher", "UNWIND range(0, " + FAN_OUT + ") AS i CREATE (:V {k: i})");
      database.command("opencypher", "MATCH (a:V {k: 0}), (b:V) WHERE b.k > 0 CREATE (a)-[:F]->(b)");
    });
    assertThat(countVertices()).isEqualTo(FAN_OUT + 1);
  }

  private long countVertices() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long countWithX() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V WHERE x = 1")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long rowsOf(final String cypher) {
    final long[] rows = { 0 };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", cypher)) {
        while (rs.hasNext()) {
          rs.next();
          rows[0]++;
        }
      }
    });
    return rows[0];
  }

  @Test
  void detachDeleteOfAnchorAndNeighboursDeletesEveryMatchedNode() {
    createFan();
    assertThat(rowsOf("MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE a, b RETURN b")).isEqualTo(FAN_OUT);
    assertThat(countVertices()).isZero();
  }

  @Test
  void reversedPatternDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (b)<-[r]-(a:V {k: 0}) DETACH DELETE a, b");
    assertThat(countVertices()).isZero();
  }

  @Test
  void countOfMatchedNeighboursIsNotCutShort() {
    createFan();
    final long[] count = { -1 };
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", "MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE a RETURN count(b) AS c")) {
        count[0] = rs.next().<Number>getProperty("c").longValue();
      }
    });
    assertThat(count[0]).isEqualTo(FAN_OUT);
    assertThat(countVertices()).isEqualTo(FAN_OUT);
  }

  @Test
  void setBeforeDetachDeleteReachesEveryNeighbour() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) SET b.x = 1 DETACH DELETE a");
    assertThat(countWithX()).isEqualTo(FAN_OUT);
    assertThat(countVertices()).isEqualTo(FAN_OUT);
  }

  @Test
  void setAfterDetachDeleteReachesEveryNeighbour() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE a SET b.x = 1");
    assertThat(countWithX()).isEqualTo(FAN_OUT);
  }

  @Test
  void detachDeleteAfterWithPassthroughDeletesEveryMatchedNode() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) WITH a, b DETACH DELETE a, b");
    assertThat(countVertices()).isZero();
  }

  @Test
  void secondDetachDeleteAfterWithDeletesEveryNeighbour() {
    createFan();
    rowsOf("MATCH (a:V {k: 0})-[r]->(b) DETACH DELETE a WITH b DETACH DELETE b");
    assertThat(countVertices()).isZero();
  }

  /** Neo4j deletes y and z: both rows of the chain x->y->z are read before either target is deleted. */
  @Test
  void chainDetachDeleteOfTargetsMatchesNeo4j() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:V {k: 'x'})-[:F]->(:V {k: 'y'})-[:F]->(:V {k: 'z'})"));
    rowsOf("MATCH (a:V)-[r]->(b:V) DETACH DELETE b");
    final List<String> left = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT k FROM V ORDER BY k")) {
      while (rs.hasNext())
        left.add(rs.next().getProperty("k"));
    }
    assertThat(left).containsExactly("x");
  }

  /** A node-only MATCH keeps the streaming path: nothing it reads is reached by the DETACH of another row. */
  @Test
  void nodeOnlyDetachDeleteStillDeletesEverything() {
    createFan();
    rowsOf("MATCH (n:V) DETACH DELETE n");
    assertThat(countVertices()).isZero();
  }
}
