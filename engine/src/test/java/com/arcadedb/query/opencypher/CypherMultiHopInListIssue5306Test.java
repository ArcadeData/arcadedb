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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5306.
 * <p>
 * A multi-hop Cypher pattern with an {@code IN}-list (or mid-pattern) filter on the anchor node
 * silently returned empty results when the edges were unidirectional or a Graph Analytical View
 * was present: the cost-based anchor selector ignored the {@code IN}-list predicate and started the
 * traversal from the far side of the pattern, then reverse-expanded over edges that do not store
 * incoming links. SQL MATCH was correct in all cases.
 * <p>
 * The fix teaches the Cypher optimizer to (1) treat an {@code IN}-list over an indexed property as
 * an index-seek anchor, and (2) require the anchor to reach every pattern node following edge
 * directions over unidirectional edges (reachability guard). A latent divide-by-zero in the GAV
 * fused-chain operator on empty input is also fixed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMultiHopInListIssue5306Test {
  private Database database;
  private static final String DB_PATH = "./target/databases/cypher-issue-5306";

  private static final int QUESTIONS = 200;
  private static final int USERS     = 20;
  private static final int IN_SIZE   = 50;

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  private void createSchema(final boolean bidirectional) {
    final Schema schema = database.getSchema();
    final VertexType q = schema.createVertexType("Question");
    q.createProperty("id", Integer.class);
    q.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    final VertexType a = schema.createVertexType("Answer");
    a.createProperty("id", Integer.class);
    a.createProperty("score", Integer.class);
    a.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    final VertexType u = schema.createVertexType("Userx");
    u.createProperty("id", Integer.class);
    u.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    schema.buildEdgeType().withName("HAS_ANSWER").withBidirectional(bidirectional).create();
    schema.buildEdgeType().withName("AUTHORED_BY").withBidirectional(bidirectional).create();
  }

  /**
   * Userx is deliberately the SMALLEST type (mirroring the reporter's dataset where Userx is the
   * least-numerous), so a purely cost-based anchor selector prefers scanning Userx and reverse-
   * traversing - which is exactly what breaks over unidirectional edges / a GAV.
   * <p>
   * Question q.id = i has answer a.id = 1000 + i with a.score = i % 10; the answer is authored by
   * user i % USERS.
   */
  private void buildData(final boolean bidirectional) {
    database.transaction(() -> {
      final MutableVertex[] users = new MutableVertex[USERS];
      for (int i = 0; i < users.length; i++)
        users[i] = database.newVertex("Userx").set("id", 2000 + i).save();

      for (int i = 0; i < QUESTIONS; i++) {
        final MutableVertex q = database.newVertex("Question").set("id", i).save();
        final MutableVertex a = database.newVertex("Answer").set("id", 1000 + i).set("score", i % 10).save();
        q.newEdge("HAS_ANSWER", a, bidirectional, new Object[0]).save();
        a.newEdge("AUTHORED_BY", users[i % users.length], bidirectional, new Object[0]).save();
      }
    });
  }

  private String inList() {
    final List<String> ids = new ArrayList<>();
    for (int i = 0; i < IN_SIZE; i++)
      ids.add(String.valueOf(i));
    return String.join(", ", ids);
  }

  private TreeSet<Object> collect(final String language, final String query, final String column) {
    final TreeSet<Object> out = new TreeSet<>();
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        out.add(r.getProperty(column));
      }
    }
    return out;
  }

  private void setUp(final boolean bidirectional, final boolean withGav) {
    database = new DatabaseFactory(DB_PATH).create();
    createSchema(bidirectional);
    buildData(bidirectional);
    if (withGav)
      database.command("sql",
          "CREATE GRAPH ANALYTICAL VIEW allGraph VERTEX TYPES (Question, Answer, Userx) EDGE TYPES (HAS_ANSWER, AUTHORED_BY) UPDATE MODE SYNCHRONOUS");
  }

  private void runInListScenario(final boolean bidirectional, final boolean withGav) {
    setUp(bidirectional, withGav);

    // Expected answer ids: 1000..1000+IN_SIZE-1 (each question in the IN-list has exactly one answer).
    final TreeSet<Object> expected = new TreeSet<>();
    for (int i = 0; i < IN_SIZE; i++)
      expected.add(1000 + i);

    final String cypher = "MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer)-[:AUTHORED_BY]->(u:Userx) "
        + "WHERE q.id IN [" + inList() + "] RETURN a.id AS aid";
    final String desc = "bidirectional=" + bidirectional + ", GAV=" + withGav;
    assertThat(collect("cypher", cypher, "aid")).as("Cypher IN-list for " + desc).isEqualTo(expected);
  }

  private void runMidFilterScenario(final boolean bidirectional, final boolean withGav) {
    setUp(bidirectional, withGav);

    // Mid-pattern filter: a.score >= 5. Among questions 0..IN_SIZE-1, score = id % 10 -> keep 5..9.
    final TreeSet<Object> expected = new TreeSet<>();
    for (int i = 0; i < IN_SIZE; i++)
      if (i % 10 >= 5)
        expected.add(1000 + i);

    final String cypher = "MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer)-[:AUTHORED_BY]->(u:Userx) "
        + "WHERE q.id IN [" + inList() + "] AND a.score >= 5 RETURN a.id AS aid";
    final String desc = "bidirectional=" + bidirectional + ", GAV=" + withGav;
    assertThat(collect("cypher", cypher, "aid")).as("Cypher mid-filter for " + desc).isEqualTo(expected);
  }

  // ---- IN-list 2-hop pattern ---------------------------------------------------------------

  @Test
  void inListBidirectionalNoGav() {
    runInListScenario(true, false);
  }

  @Test
  void inListBidirectionalWithGav() {
    runInListScenario(true, true);
  }

  @Test
  void inListUnidirectionalNoGav() {
    runInListScenario(false, false);
  }

  @Test
  void inListUnidirectionalWithGav() {
    runInListScenario(false, true);
  }

  // ---- IN-list + mid-pattern property filter -----------------------------------------------

  @Test
  void midFilterBidirectionalNoGav() {
    runMidFilterScenario(true, false);
  }

  @Test
  void midFilterUnidirectionalNoGav() {
    runMidFilterScenario(false, false);
  }

  @Test
  void midFilterUnidirectionalWithGav() {
    runMidFilterScenario(false, true);
  }

  // ---- SQL MATCH ground-truth cross-check (happy path) -------------------------------------

  @Test
  void sqlMatchGroundTruthBidirectional() {
    // Independent confirmation of the expected answer ids via SQL MATCH, which is correct on
    // bidirectional edges regardless of anchor choice.
    setUp(true, false);

    final TreeSet<Object> expected = new TreeSet<>();
    for (int i = 0; i < IN_SIZE; i++)
      expected.add(1000 + i);

    final String sql = "SELECT aid FROM ( MATCH {type:Question, as:q, where:(id IN [" + inList() + "])}"
        + "-HAS_ANSWER->{type:Answer, as:a}-AUTHORED_BY->{type:Userx, as:u} RETURN a.id AS aid )";
    assertThat(collect("sql", sql, "aid")).as("SQL MATCH IN-list ground truth").isEqualTo(expected);
  }

  // ---- parameterized IN $ids ---------------------------------------------------------------

  @Test
  void inListParameterUnidirectional() {
    setUp(false, false);

    final TreeSet<Object> expected = new TreeSet<>();
    for (int i = 0; i < IN_SIZE; i++)
      expected.add(1000 + i);

    final List<Object> ids = new ArrayList<>();
    for (int i = 0; i < IN_SIZE; i++)
      ids.add(i);

    final TreeSet<Object> actual = new TreeSet<>();
    try (final ResultSet rs = database.query("cypher",
        "MATCH (q:Question)-[:HAS_ANSWER]->(a:Answer)-[:AUTHORED_BY]->(u:Userx) "
            + "WHERE q.id IN $ids RETURN a.id AS aid", "ids", ids)) {
      while (rs.hasNext())
        actual.add(rs.next().getProperty("aid"));
    }
    assertThat(actual).as("Cypher IN $ids over unidirectional edges").isEqualTo(expected);
  }
}
