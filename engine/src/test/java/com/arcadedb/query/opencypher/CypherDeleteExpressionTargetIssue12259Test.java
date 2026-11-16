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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for GitHub issue #12259: an openCypher {@code DELETE} target that is not a plain
 * {@code name(.key|[index])*} chain was resolved from its source text, which found no variable for {@code [a][0]} or
 * {@code [a, b]} and silently deleted nothing. Such a target is now evaluated from its parsed expression.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDeleteExpressionTargetIssue12259Test extends TestHelper {
  private static final String MATCH = "MATCH (a:V)-[r:F]->(b:V) ";

  @BeforeEach
  void createGraph() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
    database.transaction(() -> database.command("opencypher", "CREATE (:V {k:0})-[:F]->(:V {k:1}), (:V {k:2})-[:F]->(:V {k:3})").close());
  }

  @Test
  void indexedListLiteralDeletesTheElement() {
    run(MATCH + "DETACH DELETE [a][0]");
    assertThat(nodes()).isEqualTo(2L);
    assertThat(keysLeft()).isEqualTo("[1, 3]");
  }

  @Test
  void indexedListLiteralOfAFunctionDeletesTheElement() {
    run(MATCH + "DETACH DELETE [startNode(r)][0]");
    assertThat(nodes()).isEqualTo(2L);
    assertThat(keysLeft()).isEqualTo("[1, 3]");
  }

  /** A list target deletes its elements, the same as the list-variable form {@code WITH [a, b] AS l DETACH DELETE l}. */
  @Test
  void listLiteralDeletesEveryElementLikeAListVariable() {
    run(MATCH + "DETACH DELETE [a, b]");
    assertThat(nodes()).isEqualTo(0L);

    createGraphAgain();
    run(MATCH + "DETACH DELETE [startNode(r), endNode(r)]");
    assertThat(nodes()).isEqualTo(0L);

    createGraphAgain();
    run(MATCH + "WITH [a, b] AS l DETACH DELETE l");
    assertThat(nodes()).isEqualTo(0L);
  }

  @Test
  void listLiteralOfRelationshipsDeletesThem() {
    run(MATCH + "DELETE [r]");
    assertThat(relationships()).isEqualTo(0L);
    assertThat(nodes()).isEqualTo(4L);
  }

  @Test
  void parenthesizedVariableIsDeleted() {
    run(MATCH + "DETACH DELETE (a)");
    assertThat(nodes()).isEqualTo(2L);
    assertThat(keysLeft()).isEqualTo("[1, 3]");
  }

  /** The text resolver read the index as a literal integer: a variable index threw a NumberFormatException. */
  @Test
  void listIndexedByAVariableDeletesTheElement() {
    run(MATCH + "WITH a, b, [a, b] AS l, 1 AS i DETACH DELETE l[i]");
    assertThat(nodes()).isEqualTo(2L);
    assertThat(keysLeft()).isEqualTo("[0, 2]");
  }

  @Test
  void caseAndCoalesceTargetsAreEvaluated() {
    run(MATCH + "DETACH DELETE CASE WHEN a.k = 0 THEN a ELSE b END");
    assertThat(keysLeft()).isEqualTo("[1, 2]");

    createGraphAgain();
    run(MATCH + "DETACH DELETE coalesce(null, b)");
    assertThat(keysLeft()).isEqualTo("[0, 2]");
  }

  /** What the text resolver already read keeps working: a variable, a map key, a literal and a parameter index. */
  @Test
  void chainedTargetsStillWork() {
    run(MATCH + "WITH a, {n: b} AS m DETACH DELETE a, m.n");
    assertThat(nodes()).isEqualTo(0L);

    createGraphAgain();
    database.transaction(() -> database.command("opencypher", MATCH + "WITH [a, b] AS l DETACH DELETE l[$i]", "i", 0).close());
    assertThat(keysLeft()).isEqualTo("[1, 3]");
  }

  /** A target that cannot be a node, relationship or path is still refused before anything runs. */
  @Test
  void scalarTargetsAreStillRefused() {
    assertThatThrownBy(() -> run(MATCH + "DELETE 1 + 1")).isInstanceOf(CommandParsingException.class);
    assertThatThrownBy(() -> run(MATCH + "DELETE a:V")).isInstanceOf(CommandParsingException.class);
    assertThat(nodes()).isEqualTo(4L);
  }

  private void createGraphAgain() {
    database.transaction(() -> {
      database.command("opencypher", "MATCH (n) DETACH DELETE n").close();
      database.command("opencypher", "CREATE (:V {k:0})-[:F]->(:V {k:1}), (:V {k:2})-[:F]->(:V {k:3})").close();
    });
  }

  private void run(final String cypher) {
    database.transaction(() -> database.command("opencypher", cypher).close());
  }

  private long nodes() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V) RETURN count(n) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long relationships() {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r:F]->() RETURN count(r) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private String keysLeft() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:V) WITH n ORDER BY n.k RETURN collect(n.k) AS k")) {
      return rs.next().getProperty("k").toString();
    }
  }
}
