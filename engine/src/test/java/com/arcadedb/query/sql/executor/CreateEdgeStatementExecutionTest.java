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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CreateEdgeStatementExecutionTest extends TestHelper {
  public CreateEdgeStatementExecutionTest() {
    autoStartTx = true;
  }

  @Test
  void okEdgesContentJsonArray() {
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

    ResultSet result = database.command("sql", "create edge " + edgeClassName + " from ? to ? CONTENT " + array, v1,
        v2);

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
  void errorEdgesContentJsonArray() {
    final String vertexClassName = "testVertexContentArray";
    database.getSchema().createVertexType(vertexClassName, 1);
    final String edgeClassName = "testEdgeContentArray";
    database.getSchema().buildEdgeType().withName(edgeClassName).withTotalBuckets(1).create();

    MutableVertex v1 = database.newVertex(vertexClassName).save();
    MutableVertex v2 = database.newVertex(vertexClassName).save();

    final StringBuffer array = new StringBuffer("[");
    for (int i = 0; i < 10; i++) {
      if (i > 0)
        array.append(",");
      array.append("{'x':" + i + "}");
    }
    array.append("]");

    assertThatThrownBy(() ->
        database.command("sql", "create edge " + edgeClassName + " from ? to ? CONTENT " + array, v1, v2))
        .isInstanceOf(CommandSQLParsingException.class);
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

  @Test
  @DisplayName("createEdgeWithMandatoryDefaultProperty - test Issue #2164")
  void createEdgeWithMandatoryDefaultProperty() {
    database.getSchema().createVertexType("testVertex");
    database.getSchema().createEdgeType("transmit");
    database.command("sql", """
        CREATE PROPERTY transmit.created_timestamp LONG (MANDATORY true, NOTNULL true, DEFAULT \
        SYSDATE().asLong())""");

    database.transaction(() -> {
      // Create two vertices using the API (like the passing test)
      MutableVertex v1 = database.newVertex("testVertex").save();
      MutableVertex v2 = database.newVertex("testVertex").save();

      // Create edge with CONTENT but without the mandatory property
      // Should apply DEFAULT value and succeed
      final ResultSet rs = database.command("sql",
          "CREATE EDGE transmit FROM ? TO ? CONTENT [{'test':'test'}]", v1, v2);
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.isEdge()).isTrue();
      assertThat((String) result.getProperty("test")).isEqualTo("test");
      assertThat((Object) result.getProperty("created_timestamp")).isNotNull();
    });
  }

  @Test
  @DisplayName("createEdgeWithDefaultPropertyOnly - test Issue #2164")
  void createEdgeWithDefaultPropertyOnly() {
    database.getSchema().createVertexType("testVertex2");
    database.getSchema().createEdgeType("transmit2");
    database.command("sql", "CREATE PROPERTY transmit2.created_timestamp LONG (DEFAULT SYSDATE().asLong())");

    database.transaction(() -> {
      // Create two vertices using the API
      MutableVertex v1 = database.newVertex("testVertex2").save();
      MutableVertex v2 = database.newVertex("testVertex2").save();

      // Create edge with CONTENT but without the property
      // Should apply DEFAULT value
      final ResultSet rs = database.command("sql",
          "CREATE EDGE transmit2 FROM ? TO ? CONTENT [{'test':'test'}]", v1, v2);
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.isEdge()).isTrue();
      assertThat((String) result.getProperty("test")).isEqualTo("test");
      assertThat((Object) result.getProperty("created_timestamp")).isNotNull();
    });
  }

  @Test
  @DisplayName("createVertexWithMandatoryDefaultProperty - verify vertices work correctly")
  void createVertexWithMandatoryDefaultProperty() {
    database.transaction(() -> {
      // Create vertex type with mandatory property that has a default value
      database.command("sql", "CREATE VERTEX TYPE message IF NOT EXISTS");
      database.command("sql", """
          CREATE PROPERTY message.created_timestamp LONG (MANDATORY true, NOTNULL true, DEFAULT \
          SYSDATE().asLong())""");
    });

    // Create vertex with CONTENT but without the mandatory property
    // Should apply DEFAULT value and succeed
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", """
          CREATE VERTEX message CONTENT {"test":"test"}
          """);
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.isVertex()).isTrue();
      assertThat((String) result.getProperty("test")).isEqualTo("test");
      assertThat((Object) result.getProperty("created_timestamp")).isNotNull();
    });
  }

  @Test
  @DisplayName("createEdgeEmptyArrayDestination - test Issue #3518")
  void createEdgeEmptyArrayDestination() {
    database.getSchema().createVertexType("V3518", 1);
    database.getSchema().createEdgeType("E3518", 1);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO V3518");

      // Empty array as TO destination should create no edges
      final ResultSet rs = database.command("sql",
          "CREATE EDGE E3518 FROM (SELECT FROM V3518 LIMIT 1) TO []");
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });

    database.transaction(() -> {
      // Empty array as FROM source should also create no edges
      final ResultSet rs = database.command("sql",
          "CREATE EDGE E3518 FROM [] TO (SELECT FROM V3518 LIMIT 1)");
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });

    database.transaction(() -> {
      // Using LET with empty array should also work (already works per issue)
      database.command("sqlscript",
          "LET $x = []; CREATE EDGE E3518 FROM (SELECT FROM V3518 LIMIT 1) TO $x;");
    });

    // Verify no edges were created
    final ResultSet check = database.query("sql", "SELECT FROM E3518");
    assertThat(check.hasNext()).isFalse();
    check.close();
  }

  @Test
  @DisplayName("createEdgeWithDefaultNoContent - test defaults without CONTENT")
  void createEdgeWithDefaultNoContent() {
    database.transaction(() -> {
      // Create vertex type
      database.command("sql", "CREATE VERTEX TYPE testVertex4 IF NOT EXISTS");

      // Create edge type with default value
      database.command("sql", "CREATE EDGE TYPE transmit4 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY transmit4.created_timestamp LONG (DEFAULT SYSDATE().asLong())");

      // Create two vertices
      ResultSet v1Rs = database.command("sql", "CREATE VERTEX testVertex4");
      Result v1 = v1Rs.next();
      String v1Id = v1.getElement().get().getIdentity().toString();

      ResultSet v2Rs = database.command("sql", "CREATE VERTEX testVertex4");
      Result v2 = v2Rs.next();
      String v2Id = v2.getElement().get().getIdentity().toString();

      // Create edge without CONTENT - should still apply DEFAULT
      final ResultSet rs = database.command("sql",
          "CREATE EDGE transmit4 FROM " + v1Id + " TO " + v2Id);
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.isEdge()).isTrue();
      assertThat((Object) result.getProperty("created_timestamp")).isNotNull();
    });
  }

  @Test
  @DisplayName("createEdgeWithVariableModifiers - batch $var[0].rid pattern")
  void createEdgeWithVariableModifiers() {
    database.getSchema().createVertexType("VarModV", 1);
    database.getSchema().createEdgeType("VarModE", 1);

    database.transaction(() -> {
      // Create a source vertex
      final MutableVertex v1 = database.newVertex("VarModV").save();

      // Use batch script with LET variable + array index + property accessor
      final ResultSet rs = database.command("sqlscript", """
          LET $t0 = INSERT INTO VarModV CONTENT {} RETURN @rid AS rid;
          CREATE EDGE VarModE FROM %s TO $t0[0].rid;
          """.formatted(v1.getIdentity()));

      // Verify edge was created
      final ResultSet edges = database.query("sql", "SELECT FROM VarModE");
      assertThat(edges.hasNext()).isTrue();
      final Result edge = edges.next();
      assertThat(edge.isEdge()).isTrue();
      assertThat(edges.hasNext()).isFalse();
      edges.close();
    });
  }

  @Test
  @DisplayName("createEdgeWithVariableArrayIndex - batch $var[0] pattern")
  void createEdgeWithVariableArrayIndex() {
    database.getSchema().createVertexType("VarIdxV", 1);
    database.getSchema().createEdgeType("VarIdxE", 1);

    database.transaction(() -> {
      // Create a source vertex
      final MutableVertex v1 = database.newVertex("VarIdxV").save();

      // Use batch script with LET variable + array index (Result contains @rid)
      final ResultSet rs = database.command("sqlscript", """
          LET $t0 = INSERT INTO VarIdxV CONTENT {};
          CREATE EDGE VarIdxE FROM %s TO $t0[0];
          """.formatted(v1.getIdentity()));

      // Verify edge was created
      final ResultSet edges = database.query("sql", "SELECT FROM VarIdxE");
      assertThat(edges.hasNext()).isTrue();
      final Result edge = edges.next();
      assertThat(edge.isEdge()).isTrue();
      assertThat(edges.hasNext()).isFalse();
      edges.close();
    });
  }
}
