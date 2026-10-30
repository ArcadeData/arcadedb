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
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11008: a relationship variable read only by a procedure {@code CALL} - in its arguments, or in its
 * {@code YIELD ... WHERE} - was bound to {@code null}. A relationship variable nothing reads is walked as an anonymous
 * hop, and the check deciding whether anything reads it ({@code CypherVariableUsage.isEdgeVariableReferenced})
 * enumerated clause types by hand and had no arm for {@code CALL}, nor for {@code LOAD CSV}, whose URL is an
 * expression too. Neo4j binds the relationship in every one of these spellings, and so does the same query with a
 * {@code WITH a, b, r} in front of the call, which is how the gap showed.
 * <p>
 * The {@code YIELD ... WHERE} spelling hid a second bug behind the first: the predicate was evaluated against the
 * yielded fields only, so it could not read any outer variable, bound or not. In Neo4j it sees the whole scope.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11008RelationshipReadOnlyByCallTest {
  private static final String DB_PATH = "./target/databases/issue-11008";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (:CallArgBugNode {marker: 'a'})-[:CALL_ARG_BUG_EDGE {weight: 7}]->(:CallArgBugNode {marker: 'b'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void relationshipPassedOnlyAsProcedureArgumentIsBound() {
    final Result row = single("""
        MATCH (a:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(b:CallArgBugNode {marker: 'b'})
        CALL do.when(true, 'RETURN $r AS r', null, {r: r}) YIELD value
        RETURN value.r AS captured, value.r IS NULL AS isNull""");

    assertThat(row.<Object>getProperty("captured")).isInstanceOf(Edge.class);
    assertThat(((Edge) row.getProperty("captured")).getTypeName()).isEqualTo("CALL_ARG_BUG_EDGE");
    assertThat(row.<Boolean>getProperty("isNull")).isFalse();
  }

  @Test
  void projectingTheRelationshipFirstStillAnswersTheSame() {
    final Result row = single("""
        MATCH (a:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(b:CallArgBugNode {marker: 'b'})
        WITH a, b, r
        CALL do.when(true, 'RETURN $r AS r', null, {r: r}) YIELD value
        RETURN value.r AS captured, value.r IS NULL AS isNull""");

    assertThat(row.<Object>getProperty("captured")).isInstanceOf(Edge.class);
    assertThat(row.<Boolean>getProperty("isNull")).isFalse();
  }

  @Test
  void relationshipPropertyReadOnlyByProcedureArgumentIsBound() {
    final Result row = single("""
        MATCH (:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(:CallArgBugNode {marker: 'b'})
        CALL do.when(true, 'RETURN $w AS w', null, {w: r.weight}) YIELD value
        RETURN value.w AS weight""");

    assertThat(row.<Number>getProperty("weight").intValue()).isEqualTo(7);
  }

  @Test
  void relationshipReadOnlyByYieldWhereIsBound() {
    final Result row = single("""
        MATCH (:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(:CallArgBugNode {marker: 'b'})
        CALL do.when(true, 'RETURN 1 AS one', null, {}) YIELD value WHERE r.weight = 7
        RETURN count(*) AS matched""");

    assertThat(row.<Number>getProperty("matched").longValue()).isEqualTo(1L);
  }

  /**
   * The second half of the YIELD WHERE case, which no binding fix reaches: the predicate was evaluated against the
   * yielded fields alone, so any outer variable - a projected value, not only a relationship - read as null there.
   */
  @Test
  void yieldWhereSeesTheOuterRow() {
    final Result row = single("""
        UNWIND [1, 7, 9] AS w
        CALL do.when(true, 'RETURN $x AS x', null, {x: w * 2}) YIELD value WHERE w = 7 AND value.x = 14
        RETURN collect(w) AS kept""");

    assertThat(row.<List<Object>>getProperty("kept")).hasSize(1);
    assertThat(((Number) row.<List<Object>>getProperty("kept").getFirst()).intValue()).isEqualTo(7);
  }

  @Test
  void relationshipReadOnlyByLoadCsvUrlIsBound() throws IOException {
    final File csv = new File(DB_PATH + "-csv/rows.csv");
    csv.getParentFile().mkdirs();
    try (final PrintWriter writer = new PrintWriter(csv, "UTF-8")) {
      writer.println("id");
      writer.println("1");
      writer.println("2");
    }
    try {
      database.transaction(() -> database.command("opencypher", "MATCH ()-[r:CALL_ARG_BUG_EDGE]->() SET r.url = $url",
          Map.of("url", "file://" + csv.getAbsolutePath())));

      final Result row = single("""
          MATCH (:CallArgBugNode {marker: 'a'})-[r:CALL_ARG_BUG_EDGE]->(:CallArgBugNode {marker: 'b'})
          LOAD CSV WITH HEADERS FROM r.url AS line
          RETURN count(line) AS loaded""");

      assertThat(row.<Number>getProperty("loaded").longValue()).isEqualTo(2L);
    } finally {
      csv.delete();
      csv.getParentFile().delete();
    }
  }

  private Result single(final String query) {
    // do.when may run a write statement, so it is classified as a write procedure and needs command().
    try (final ResultSet rs = database.command("opencypher", query)) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).hasSize(1);
      return rows.getFirst();
    }
  }
}
