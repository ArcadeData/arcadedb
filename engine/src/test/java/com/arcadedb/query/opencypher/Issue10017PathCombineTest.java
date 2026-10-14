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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #10017: {@code path.combine} accepted one argument (a list of path maps) while its documented
 * signature, the APOC one, is {@code path.combine(path1, path2)}; and none of the {@code path.*} functions understood
 * the path a {@code MATCH} binds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10017PathCombineTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> database.command("opencypher", "CREATE (a:N {id: 1})-[:R]->(b:N {id: 2})-[:R]->(c:N {id: 3})"));
  }

  @Test
  void combinesTheTwoPathsOfTheIssue() {
    try (final ResultSet rs = database.query("opencypher", """
        MATCH p1 = (a:N {id: 1})-[:R]->(b:N {id: 2}),
              p2 = (b)-[:R]->(c:N {id: 3})
        RETURN path.combine(p1, p2) AS combined""")) {
      final Result row = rs.next();
      assertThat(rs.hasNext()).isFalse();
      final TraversalPath combined = row.getProperty("combined");
      assertThat(combined.length()).isEqualTo(2);
      final List<Object> ids = new ArrayList<>();
      combined.getVertices().forEach(v -> ids.add(v.get("id")));
      assertThat(ids).containsExactly(1, 2, 3);
      assertThat(combined.getEdges()).allMatch(e -> e.getTypeName().equals("R"));
    }
  }

  @Test
  void theCombinedPathIsAPathToTheOtherFunctions() {
    assertThat(single("""
        MATCH p1 = (:N {id: 1})-[:R]->(b:N {id: 2}), p2 = (b)-[:R]->(:N {id: 3})
        WITH path.combine(p1, p2) AS p
        RETURN [n IN nodes(p) | n.id] AS ids, length(p) AS len, size(relationships(p)) AS rels""")).satisfies(row -> {
      assertThat(row.<List<Object>>getProperty("ids")).containsExactly(1, 2, 3);
      assertThat(row.<Number>getProperty("len").intValue()).isEqualTo(2);
      assertThat(row.<Number>getProperty("rels").intValue()).isEqualTo(2);
    });
    // the APOC name resolves to the same function
    assertThat(single("""
        MATCH p1 = (:N {id: 1})-[:R]->(b:N {id: 2}), p2 = (b)-[:R]->(:N {id: 3})
        RETURN length(apoc.path.combine(p1, p2)) AS len""").<Number>getProperty("len").intValue()).isEqualTo(2);
  }

  @Test
  void aNullPathLeavesTheOther() {
    assertThat(single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN length(path.combine(p, null)) AS len")
        .<Number>getProperty("len").intValue()).isEqualTo(1);
    assertThat(single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN length(path.combine(null, p)) AS len")
        .<Number>getProperty("len").intValue()).isEqualTo(1);
    assertThat(single("RETURN path.combine(null, null) AS p").<Object>getProperty("p")).isNull();
  }

  @Test
  void pathsThatDoNotConnectAreRefused() {
    assertThatThrownBy(() -> single("""
        MATCH p1 = (:N {id: 1})-[:R]->(b:N {id: 2}), p2 = (b)-[:R]->(:N {id: 3})
        RETURN path.combine(p2, p1) AS p""")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("do not connect");
  }

  @Test
  void whatIsNotAPathIsRefused() {
    assertThatThrownBy(() -> single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN path.combine(p, 42) AS p"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("expects paths");
    assertThatThrownBy(() -> single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN path.combine([p, 'x']) AS p"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("expects paths");
  }

  @Test
  void theListFormJoinsEveryPathInOrder() {
    assertThat(single("""
        MATCH p1 = (:N {id: 1})-[:R]->(b:N {id: 2}), p2 = (b)-[:R]->(:N {id: 3})
        RETURN [n IN nodes(path.combine([p1, p2])) | n.id] AS ids""").<List<Object>>getProperty("ids")).containsExactly(1, 2, 3);
  }

  @Test
  void createDerivesTheNodesFromTheRelationships() {
    assertThat(single("""
        MATCH (a:N {id: 1})-[r1:R]->(:N {id: 2})-[r2:R]->(c:N {id: 3})
        WITH path.create(a, [r1, r2]) AS p
        RETURN [n IN nodes(p) | n.id] AS ids, length(p) AS len""")).satisfies(row -> {
      assertThat(row.<List<Object>>getProperty("ids")).containsExactly(1, 2, 3);
      assertThat(row.<Number>getProperty("len").intValue()).isEqualTo(2);
    });
    // walked against the stored direction, from the far end
    assertThat(single("""
        MATCH (a:N {id: 1})-[r1:R]->(:N {id: 2})-[r2:R]->(c:N {id: 3})
        RETURN [n IN nodes(path.create(c, [r2, r1])) | n.id] AS ids""").<List<Object>>getProperty("ids")).containsExactly(3, 2, 1);
    assertThatThrownBy(() -> single("""
        MATCH (a:N {id: 1})-[r1:R]->(:N {id: 2})-[r2:R]->(c:N {id: 3})
        RETURN path.create(a, [r2]) AS p""")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("does not continue the path");
  }

  @Test
  void sliceAndElementsReadAMatchedPath() {
    assertThat(single("""
        MATCH p = (:N {id: 1})-[:R]->(:N {id: 2})-[:R]->(:N {id: 3})
        RETURN [n IN nodes(path.slice(p, 1)) | n.id] AS ids, size(path.elements(p)) AS elements""")).satisfies(row -> {
      assertThat(row.<List<Object>>getProperty("ids")).containsExactly(2, 3);
      assertThat(row.<Number>getProperty("elements").intValue()).isEqualTo(5);
    });
  }

  @Test
  void combinesAProcedurePathWithAMatchedOne() {
    assertThat(single("""
        MATCH (a:N {id: 1}), (b:N {id: 2})
        CALL algo.dijkstra(a, b, 'R', 'weight') YIELD path
        MATCH p2 = (b)-[:R]->(:N {id: 3})
        RETURN [n IN nodes(path.combine(path, p2)) | n.id] AS ids""").<List<Object>>getProperty("ids")).containsExactly(1, 2, 3);
  }

  private Result single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next();
    }
  }
}
