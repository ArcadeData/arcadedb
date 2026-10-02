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
package com.arcadedb.function.sql.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #8938: the trailing options map of {@code shortestPath()} (position 4) validated its keys
 * against the full option set, so {@code edgeTypeNames} passed validation there and was then silently dropped, and the
 * walk ran unrestricted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8938ShortestPathTrailingOptionsTest extends TestHelper {

  private void buildGraph() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("Road");
    database.getSchema().createEdgeType("Rail");
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX V SET name = 'A'");
      database.command("sql", "CREATE VERTEX V SET name = 'B'");
      database.command("sql", "CREATE VERTEX V SET name = 'C'");
      database.command("sql", "CREATE EDGE Road FROM (SELECT FROM V WHERE name = 'A') TO (SELECT FROM V WHERE name = 'C')");
      database.command("sql", "CREATE EDGE Road FROM (SELECT FROM V WHERE name = 'C') TO (SELECT FROM V WHERE name = 'B')");
      database.command("sql", "CREATE EDGE Rail FROM (SELECT FROM V WHERE name = 'A') TO (SELECT FROM V WHERE name = 'B')");
    });
  }

  private int pathLength(final String args) {
    try (final ResultSet rs = database.query("sql",
        "SELECT shortestPath((SELECT FROM V WHERE name = 'A'), (SELECT FROM V WHERE name = 'B')" + args + ") AS p")) {
      return rs.next().<List<?>>getProperty("p").size();
    }
  }

  @Test
  void positionalAndConsolidatedFormsHonourTheEdgeTypes() {
    buildGraph();
    assertThat(pathLength(", 'OUT', ['Road']")).isEqualTo(3);
    assertThat(pathLength(", {direction:'OUT', edgeTypeNames:['Road']}")).isEqualTo(3);
  }

  @Test
  void aPathKnobInTheTrailingMapIsRefusedByName() {
    buildGraph();
    assertThatThrownBy(() -> pathLength(", 'OUT', null, {edgeTypeNames:['Road']}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("edgeTypeNames");
    assertThatThrownBy(() -> pathLength(", 'OUT', null, {direction:'IN'}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("direction");
  }

  @Test
  void theTrailingMapStillTakesMaxDepthAndEdge() {
    buildGraph();
    assertThat(pathLength(", 'OUT', ['Road'], {'maxDepth':5, 'edge':false}")).isEqualTo(3);
  }
}
