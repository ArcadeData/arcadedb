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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test case for issue #1941: dijkstra function does not accept documented arguments
 * https://github.com/ArcadeData/arcadedb/issues/1941
 */
class SQLFunctionDijkstraIssue1941Test {

  @Test
  void dijkstraWithVertexVariables() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDijkstraIssue1941Test", (db) -> {
      // Setup schema
      db.transaction(() -> {
        db.getSchema().createVertexType("V");
        db.getSchema().createEdgeType("E");
        db.getSchema().getType("E").createProperty("distance", Long.class);
      });

      // Execute the script as in the issue report
      String script = """
        LET $src = (INSERT INTO V);
        LET $dst = (INSERT INTO V);
        CREATE EDGE E FROM $src TO $dst SET distance = 1;
        CREATE EDGE E FROM $src TO $dst SET distance = 10;

        LET $src = (SELECT FROM V LIMIT 1);
        LET $dst = (SELECT FROM V LIMIT 1 SKIP 1);
        SELECT dijkstra($src, $dst, 'distance') as path FROM V LIMIT 1;
        """;

      ResultSet result = db.command("sqlscript", script);

      // Get to the last result (the SELECT query result)
      Object path = null;
      while (result.hasNext()) {
        var r = result.next();
        if (r.getPropertyNames().contains("path")) {
          path = r.getProperty("path");
        }
      }

      assertThat(path).as("Path should not be null").isNotNull();
    });
  }

  @Test
  void dijkstraWithRIDs() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDijkstraIssue1941TestRID", (db) -> {
      // Setup schema
      db.transaction(() -> {
        db.getSchema().createVertexType("V");
        db.getSchema().createEdgeType("E");
        db.getSchema().getType("E").createProperty("distance", Long.class);
      });

      // Test with RID extraction as in the issue report
      String script = """
        LET $src = (INSERT INTO V);
        LET $dst = (INSERT INTO V);
        CREATE EDGE E FROM $src TO $dst SET distance = 1;
        CREATE EDGE E FROM $src TO $dst SET distance = 10;

        LET $src = (SELECT FROM V LIMIT 1);
        LET $dst = (SELECT FROM V LIMIT 1 SKIP 1);
        SELECT dijkstra($src.@rid, $dst.@rid, 'distance') as path FROM V LIMIT 1;
        """;

      ResultSet result = db.command("sqlscript", script);

      // Get to the last result (the SELECT query result)
      Object path = null;
      while (result.hasNext()) {
        var r = result.next();
        if (r.getPropertyNames().contains("path")) {
          path = r.getProperty("path");
        }
      }

      assertThat(path).as("Path should not be null").isNotNull();
    });
  }

  @Test
  void astarWithVertexVariables() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDijkstraIssue1941TestAstar", (db) -> {
      // Setup schema
      db.transaction(() -> {
        db.getSchema().createVertexType("V");
        db.getSchema().createEdgeType("E");
        db.getSchema().getType("E").createProperty("distance", Long.class);
      });

      // Test astar with vertex variables - should also work since dijkstra calls astar
      String script = """
        LET $src = (INSERT INTO V);
        LET $dst = (INSERT INTO V);
        CREATE EDGE E FROM $src TO $dst SET distance = 1;
        CREATE EDGE E FROM $src TO $dst SET distance = 10;

        LET $src = (SELECT FROM V LIMIT 1);
        LET $dst = (SELECT FROM V LIMIT 1 SKIP 1);
        SELECT astar($src, $dst, 'distance') as path FROM V LIMIT 1;
        """;

      ResultSet result = db.command("sqlscript", script);

      // Get to the last result (the SELECT query result)
      Object path = null;
      while (result.hasNext()) {
        var r = result.next();
        if (r.getPropertyNames().contains("path")) {
          path = r.getProperty("path");
        }
      }

      assertThat(path).as("Path should not be null").isNotNull();
    });
  }

  @Test
  void dijkstraReturnsListOfRIDs() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionDijkstraReturnsRIDs", (db) -> {
      // Setup schema
      db.transaction(() -> {
        db.getSchema().createVertexType("V");
        db.getSchema().createEdgeType("E");
        db.getSchema().getType("E").createProperty("distance", Long.class);
      });

      String script = """
        LET $src = (INSERT INTO V);
        LET $dst = (INSERT INTO V);
        CREATE EDGE E FROM $src TO $dst SET distance = 1;

        LET $src = (SELECT FROM V LIMIT 1);
        LET $dst = (SELECT FROM V LIMIT 1 SKIP 1);
        SELECT dijkstra($src, $dst, 'distance') as path FROM V LIMIT 1;
        """;

      ResultSet result = db.command("sqlscript", script);

      // Get to the last result (the SELECT query result)
      Object path = null;
      while (result.hasNext()) {
        var r = result.next();
        if (r.getPropertyNames().contains("path")) {
          path = r.getProperty("path");
        }
      }

      assertThat(path).as("Path should not be null").isNotNull();
      assertThat(path).as("Path should be a List").isInstanceOf(List.class);

      @SuppressWarnings("unchecked")
      List<Object> pathList = (List<Object>) path;
      assertThat(pathList).hasSize(2);

      // Verify all elements are RIDs, not Vertices
      for (Object element : pathList) {
        assertThat(element).as("Path element should be a RID").isInstanceOf(RID.class);
      }
    });
  }

  @Test
  void astarReturnsListOfRIDs() throws Exception {
    TestHelper.executeInNewDatabase("SQLFunctionAstarReturnsRIDs", (db) -> {
      // Setup schema
      db.transaction(() -> {
        db.getSchema().createVertexType("V");
        db.getSchema().createEdgeType("E");
        db.getSchema().getType("E").createProperty("distance", Long.class);
      });

      String script = """
        LET $src = (INSERT INTO V);
        LET $dst = (INSERT INTO V);
        CREATE EDGE E FROM $src TO $dst SET distance = 1;

        LET $src = (SELECT FROM V LIMIT 1);
        LET $dst = (SELECT FROM V LIMIT 1 SKIP 1);
        SELECT astar($src, $dst, 'distance') as path FROM V LIMIT 1;
        """;

      ResultSet result = db.command("sqlscript", script);

      // Get to the last result (the SELECT query result)
      Object path = null;
      while (result.hasNext()) {
        var r = result.next();
        if (r.getPropertyNames().contains("path")) {
          path = r.getProperty("path");
        }
      }

      assertThat(path).as("Path should not be null").isNotNull();
      assertThat(path).as("Path should be a List").isInstanceOf(List.class);

      @SuppressWarnings("unchecked")
      List<Object> pathList = (List<Object>) path;
      assertThat(pathList).hasSize(2);

      // Verify all elements are RIDs, not Vertices
      for (Object element : pathList) {
        assertThat(element).as("Path element should be a RID").isInstanceOf(RID.class);
      }
    });
  }
}
