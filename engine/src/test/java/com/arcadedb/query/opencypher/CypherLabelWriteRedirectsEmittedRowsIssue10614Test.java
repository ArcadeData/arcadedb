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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10614: a label write rewrites the vertex and every incident edge under new RIDs and deletes the originals. A row
 * that left the write step before a LATER row's label write still held the deleted records - in a path, in a node - and
 * was answered with them: the Bolt serializer failed with "Record #4:0 not found", and HTTP answered RIDs of records that no
 * longer exist. Every row a statement answers must hold the live records, wherever it was buffered when the write ran.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelWriteRedirectsEmittedRowsIssue10614Test extends TestHelper {

  /** The reporter's query: row 1 (13) removes nothing, row 2 (27) removes l9 and so rewrites 27 and both relationships. */
  @Test
  void aPathOfAnEarlierRowFollowsALaterRowsLabelRemoval() {
    database.transaction(() -> database.command("opencypher", "CREATE (a {id: 13})-[:rt9]->(b:l9 {id: 27}), (b)-[:rt8]->(a)"));

    final List<Result> rows = rows("""
        MATCH p0 = (n0) <-[:rt7|rt8|rt9*1]-()
        WITH n0 AS n1, p0 AS p1, n0.k4 AS alias2
        WHERE n1.id IN [13, 27]
        REMOVE n1.k10, n1:l9:l7
        RETURN DISTINCT alias2, n1, p1""");

    assertThat(rows).hasSize(2);
    final List<Integer> ids = new ArrayList<>();
    for (final Result row : rows) {
      assertThat(row.<Object>getProperty("alias2")).isNull();
      final Vertex n1 = row.getProperty("n1");
      assertLive(n1);
      ids.add(n1.getInteger("id"));

      final TraversalPath p1 = row.getProperty("p1");
      assertLivePath(p1);
      assertThat(p1.getVertices().getFirst().getIdentity()).isEqualTo(n1.getIdentity());
      assertThat(p1.getVertices().getLast().getInteger("id")).isEqualTo(n1.getInteger("id") == 13 ? 27 : 13);
      assertThat(p1.getEdges().getFirst().getTypeName()).isEqualTo(n1.getInteger("id") == 13 ? "rt8" : "rt9");
    }
    assertThat(ids).containsExactlyInAnyOrder(13, 27);

    // Removing a label a node does not have is a no-op; the one that had it lost it, and the graph kept both relationships
    assertThat(count("MATCH (n:l9) RETURN count(n) AS c")).isZero();
    assertThat(count("MATCH (n) RETURN count(n) AS c")).isEqualTo(2L);
    assertThat(count("MATCH ()-[r]->() RETURN count(r) AS c")).isEqualTo(2L);
  }

  /**
   * What the clauses after the write read from an earlier row is the live graph too: the node a later row relabelled no
   * longer reports the label, and the relationships of the path still join its nodes. As Neo4j answers: a node is a
   * reference, so every row sees it as it is after the write.
   */
  @Test
  void theClausesAfterTheWriteReadTheLiveRecordsOfAnEarlierRow() {
    database.transaction(() -> database.command("opencypher", "CREATE (a {id: 13})-[:rt9]->(b:l9 {id: 27}), (b)-[:rt8]->(a)"));

    final List<Result> rows = rows("""
        MATCH p0 = (n0)<-[:rt8|rt9]-()
        WITH n0 AS n1, p0 AS p1 ORDER BY n0.id
        REMOVE n1:l9
        WITH n1, p1
        RETURN n1.id AS id,
               [x IN nodes(p1) | labels(x)] AS ls,
               [r IN relationships(p1) WHERE startNode(r) IN nodes(p1) AND endNode(r) IN nodes(p1)] AS joined""");

    assertThat(rows).hasSize(2);
    for (final Result row : rows) {
      final List<List<String>> labels = row.getProperty("ls");
      assertThat(labels).as("row %s", row.<Object>getProperty("id")).hasSize(2);
      for (final List<String> nodeLabels : labels)
        assertThat(nodeLabels).as("row %s", row.<Object>getProperty("id")).doesNotContain("l9");
      assertThat(row.<List<?>>getProperty("joined")).as("row %s", row.<Object>getProperty("id")).hasSize(1);
    }
  }

  /** The same through SET: row 1's path holds the node row 2 relabels. */
  @Test
  void aPathOfAnEarlierRowFollowsALaterRowsLabelSet() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:A {id: 1})-[:E]->(b:B {id: 2}), (b)-[:E]->(a)"));

    final List<Result> rows = rows("""
        MATCH p = (n)<-[:E]-()
        WITH n, p ORDER BY n.id
        FOREACH (x IN CASE WHEN n.id = 2 THEN [n] ELSE [] END | SET x:Tagged)
        RETURN n, p""");

    assertThat(rows).hasSize(2);
    for (final Result row : rows) {
      assertLive(row.getProperty("n"));
      assertLivePath(row.getProperty("p"));
    }

    final List<Result> direct = rows("""
        MATCH p = (n)<-[:E]-()
        WITH n, p ORDER BY n.id
        SET n:Again
        RETURN n, p""");
    assertThat(direct).hasSize(2);
    for (final Result row : direct) {
      final Vertex n = row.getProperty("n");
      assertLive(n);
      assertThat(n.getTypeName()).contains("Again");
      assertLivePath(row.getProperty("p"));
    }
  }

  /**
   * A row that left the write step in an earlier batch and waits in a barrier (ORDER BY) when a row of a later batch relabels
   * a node it holds: the statement answers it with the live records.
   */
  @Test
  void aRowBufferedDownstreamFollowsALabelWriteOfALaterBatch() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:L {id: 1})-[:E]->(b {id: 2})"));

    final List<Result> rows = rows("""
        UNWIND range(1, 250) AS i
        MATCH p = (a {id: 1})-[:E]->(b)
        WITH i, p, a, CASE WHEN i = 250 THEN a ELSE null END AS target
        REMOVE target:L
        RETURN i, a, p ORDER BY i DESC""");

    assertThat(rows).hasSize(250);
    final RID live = rows.getFirst().<Vertex>getProperty("a").getIdentity();
    for (final Result row : rows) {
      final Vertex a = row.getProperty("a");
      assertLive(a);
      assertThat(a.getIdentity()).as("row %s", row.<Object>getProperty("i")).isEqualTo(live);
      assertLivePath(row.getProperty("p"));
    }
    assertThat(count("MATCH (n:L) RETURN count(n) AS c")).isZero();

    // A single returned node is also the row's element, which is what the HTTP JSON answers with: it is the live one too
    database.transaction(() -> database.command("opencypher", "MATCH (n {id: 1}) SET n:L"));
    final List<Result> single = rows("""
        UNWIND range(1, 250) AS i
        MATCH (a {id: 1})
        WITH i, a, CASE WHEN i = 250 THEN a ELSE null END AS target
        REMOVE target:L
        WITH i, a ORDER BY i DESC
        RETURN a""");
    assertThat(single).hasSize(250);
    final RID liveSingle = single.getFirst().<Vertex>getProperty("a").getIdentity();
    for (final Result row : single) {
      assertThat(row.<Vertex>getProperty("a").getIdentity()).isEqualTo(liveSingle);
      assertThat(row.getElement().orElseThrow().getIdentity()).isEqualTo(liveSingle);
      assertThat(row.toJSON().getString("@rid")).isEqualTo(liveSingle.toString());
    }
  }

  /**
   * A RETURN DISTINCT projected the rows before a later batch's label write with the original node, and the rows after it
   * with the replacement, so it told the same node apart: once both are live they are one row, as Neo4j answers.
   */
  @Test
  void aDistinctAnswersOnceANodeReplacedBetweenItsRows() {
    database.transaction(() -> database.command("opencypher", "CREATE (:L {id: 1})"));

    final String query = """
        UNWIND range(1, 250) AS i
        MATCH (a {id: 1})
        WITH i, a, CASE WHEN i = 180 THEN a ELSE null END AS target
        REMOVE target:L
        RETURN DISTINCT a""";
    final List<Result> rows = rows(query);
    assertThat(rows).hasSize(1);
    final Vertex a = rows.getFirst().getProperty("a");
    assertLive(a);
    assertThat(rows.getFirst().getElement().orElseThrow().getIdentity()).isEqualTo(a.getIdentity());

    database.transaction(() -> database.command("opencypher", "MATCH (n) SET n:L"));
    final List<Result> profiled = rows("PROFILE " + query);
    assertThat(profiled).hasSize(1);
    assertLive(profiled.getFirst().getProperty("a"));
  }

  /** PROFILE drains the statement as well, and answers the same live rows. */
  @Test
  void aProfiledStatementAnswersTheLiveRecords() {
    database.transaction(() -> database.command("opencypher", "CREATE (a {id: 13})-[:rt9]->(b:l9 {id: 27}), (b)-[:rt8]->(a)"));

    final List<Result> rows = rows("""
        PROFILE MATCH p0 = (n0) <-[:rt8|rt9]-()
        WITH n0 AS n1, p0 AS p1 ORDER BY n1.id
        REMOVE n1:l9
        RETURN n1, p1""");

    assertThat(rows).hasSize(2);
    for (final Result row : rows) {
      assertLive(row.getProperty("n1"));
      assertLivePath(row.getProperty("p1"));
    }
  }

  private void assertLivePath(final TraversalPath path) {
    assertThat(path).isNotNull();
    final List<Vertex> vertices = path.getVertices();
    final List<Edge> edges = path.getEdges();
    assertThat(vertices).hasSize(edges.size() + 1);
    for (final Vertex vertex : vertices)
      assertLive(vertex);
    for (int i = 0; i < edges.size(); i++) {
      final Edge edge = edges.get(i);
      assertLive(edge);
      final RID from = vertices.get(i).getIdentity();
      final RID to = vertices.get(i + 1).getIdentity();
      // Each relationship of the path joins the two nodes it sits between, in either direction
      assertThat(List.of(edge.getOut(), edge.getIn())).containsExactlyInAnyOrder(from, to);
    }
  }

  private void assertLive(final Identifiable record) {
    assertThat(record).isNotNull();
    assertThat(database.existsRecord(record.getIdentity())).as("record %s exists", record.getIdentity()).isTrue();
  }

  private List<Result> rows(final String query) {
    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
    });
    return rows;
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
