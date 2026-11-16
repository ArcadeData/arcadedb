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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A DELETE target that starts with a list literal is evaluated, not read from its text as a variable name (issue #12260).
 */
class CypherDeleteListLiteralTargetIssue12260Test extends TestHelper {

  @BeforeEach
  void createGraph() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("F");
    database.transaction(() -> database.command("opencypher",
        "CREATE (:V {k: 0})-[:F]->(:V {k: 1}), (:V {k: 2})-[:F]->(:V {k: 3})").close());
  }

  @Test
  void indexedListLiteralDeletesTheElement() {
    run("MATCH (a:V)-[r:F]->(b:V) DETACH DELETE [a][0]");
    assertThat(count("V")).isEqualTo(2);
  }

  @Test
  void indexedListOfEndpointsDeletesTheElement() {
    run("MATCH ()-[r:F]->() DETACH DELETE [startNode(r)][0]");
    assertThat(count("V")).isEqualTo(2);
  }

  @Test
  void listLiteralDeletesLikeAListVariable() {
    run("MATCH (a:V)-[r:F]->(b:V) DETACH DELETE [a, b]");
    assertThat(count("V")).isZero();
  }

  @Test
  void listLiteralOfRelationshipsDeletesThem() {
    run("MATCH (a:V)-[r:F]->(b:V) DELETE [r]");
    assertThat(count("F")).isZero();
    assertThat(count("V")).isEqualTo(4);
  }

  private void run(final String cypher) {
    database.transaction(() -> database.command("opencypher", cypher).close());
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + type)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
