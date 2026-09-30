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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * APOC declares the config of {@code path.subgraphNodes}, {@code path.subgraphAll} and {@code path.spanningTree} with
 * a default, so Cypher migrated from Neo4j calls them with the start node only (issue #8163).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8163PathOptionalConfigTest extends TestHelper {

  @Test
  void pathProceduresAcceptACallWithoutConfig() {
    database.getSchema().createVertexType("N");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX N SET id = 1");
      database.command("sql", "CREATE VERTEX N SET id = 2");
      database.command("sql", "CREATE EDGE R FROM (SELECT FROM N WHERE id = 1) TO (SELECT FROM N WHERE id = 2)");
    });

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:N {id: 1}) CALL path.subgraphNodes(n) YIELD node RETURN count(node) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:N {id: 1}) CALL path.subgraphAll(n) YIELD nodes, relationships RETURN size(nodes) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(2L);
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:N {id: 1}) CALL path.spanningTree(n) YIELD path RETURN count(path) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isGreaterThanOrEqualTo(1L);
    }
  }
}
