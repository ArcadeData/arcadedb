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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CreateEdgeStatementExecutionTest extends TestHelper {
  public CreateEdgeStatementExecutionTest() {
    autoStartTx = true;
  }

  @Test
  public void okEdgesContentJsonArray() {
    final String vertexClassName = "testVertexContentArray";
    database.getSchema().createVertexType(vertexClassName, 1);
    final String edgeClassName = "testEdgeContentArray";
    database.getSchema().createEdgeType(edgeClassName, 1);

    MutableVertex v1 = database.newVertex(vertexClassName).save();
    MutableVertex v2 = database.newVertex(vertexClassName).save();

    String array = "[";
    for (int i = 0; i < 1; i++) {
      if (i > 0)
        array += ",";
      array += "{'x':" + i + "}";
    }
    array += "]";

    ResultSet result = database.command("sql", "create edge " + edgeClassName + " from ? to ? CONTENT " + array, v1, v2);

    int count = 0;
    while (result.hasNext()) {
      final Result r = result.next();
      assertThat(r.isEdge()).isTrue();

      Edge edge = r.getEdge().get();

      assertThat(edge.getInteger("x")).isEqualTo(count);

      ++count;
    }
    result.close();

    assertThat(count).isEqualTo(1);
  }

  @Test
  public void errorEdgesContentJsonArray() {
    final String vertexClassName = "testVertexContentArray";
    database.getSchema().createVertexType(vertexClassName, 1);
    final String edgeClassName = "testEdgeContentArray";
    database.getSchema().createEdgeType(edgeClassName, 1);

    MutableVertex v1 = database.newVertex(vertexClassName).save();
    MutableVertex v2 = database.newVertex(vertexClassName).save();

    String array = "[";
    for (int i = 0; i < 10; i++) {
      if (i > 0)
        array += ",";
      array += "{'x':" + i + "}";
    }
    array += "]";

    try {
      ResultSet result = database.command("sql", "create edge " + edgeClassName + " from ? to ? CONTENT " + array, v1, v2);
      fail("");
    } catch (CommandSQLParsingException e) {
      // EXPECTED
    }
  }

  @Test
  @DisplayName("createEdgeIfNotExists - test Issue #1763")
  void createEdgeIfNotExists() {
    database.transaction(() -> {
      database.command("sqlscript", """
          CREATE VERTEX TYPE vex;
          CREATE EDGE TYPE edg;
          CREATE PROPERTY edg.label STRING;
          CREATE VERTEX vex;
          CREATE VERTEX vex;
          CREATE VERTEX vex;
          """);
    });

    // CREATE EDGES FROM #1:0 TO [#1:1,#1:2]
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", """
          CREATE EDGE edg FROM #1:0 TO [#1:1,#1:2] IF NOT EXISTS
          """);
      assertThat(rs.stream().count()).isEqualTo(2);
    });

    // CREATE AGAIN (should not create any edge)
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", """
          CREATE EDGE edg FROM #1:0 TO [#1:1,#1:2] IF NOT EXISTS
          """);
      assertThat(rs.stream().count()).isEqualTo(2);
    });
    // CHECK THAT TOTAL  EDGES ARE STILL 2
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", """
          select from edg
          """);
      assertThat(rs.stream().count()).isEqualTo(2);
    });
    // CREATE AGAIN (should create 1 edge)
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", """
          CREATE EDGE edg FROM #1:0 TO [#1:1,#1:2,#1:0] IF NOT EXISTS
          """);
      assertThat(rs.stream().count()).isEqualTo(3);
    });

    // CHECK THAT TOTAL  EDGES ARE STILL 3
    database.transaction(() -> {
      final ResultSet rs = database.query("SQL", """
          select from edg
          """);
      assertThat(rs.stream().count()).isEqualTo(3);
    });


  }
}
