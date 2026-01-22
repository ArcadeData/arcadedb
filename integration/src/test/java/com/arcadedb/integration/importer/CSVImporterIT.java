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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CSVImporterIT {

  @Test
  void importDocuments() {
    final String databasePath = "target/databases/test-import-documents";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", """
          IMPORT DATABASE file://src/test/resources/importer-vertices.csv
          WITH maxProperties=1000, maxPropertySize=8192
          """);
      assertThat(db.countType("Document", true)).isEqualTo(6);
    } finally {
      db.drop();
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void importDocumentsWithBigTextColumns() {
    final String databasePath = "target/databases/test-import-documents";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();

    //Max property size is 1024: should fail
    assertThatThrownBy(() -> {
      db.command("sql", """
          IMPORT DATABASE file://src/test/resources/importer-big-size.csv
          WITH maxProperties=1000, maxPropertySize=1024
          """);
    }).isInstanceOf(Exception.class);

    //Max property size is 8192: should succeed
    try {
      db.command("sql", """
          IMPORT DATABASE file://src/test/resources/importer-big-size.csv
          WITH maxProperties=1000, maxPropertySize=8192
          """);
      assertThat(db.countType("Document", true)).isEqualTo(3);
    } finally {
      db.drop();
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void importGraph() {
    final String databasePath = "target/databases/test-import-graph";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    Importer importer = new Importer(("-vertices src/test/resources/importer-vertices.csv -database " + databasePath
        + " -typeIdProperty Id -typeIdType Long -typeIdPropertyIsUnique true -forceDatabaseCreate true").split(" "));
    importer.load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Node", true)).isEqualTo(6);
    }

    importer = new Importer(("-edges src/test/resources/importer-edges.csv -database " + databasePath
        + " -typeIdProperty Id -typeIdType Long -edgeFromField From -edgeToField To").split(" "));
    importer.load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Node", true)).isEqualTo(6);
      assertThat(db.lookupByKey("Node", "Id", 0).next().getRecord().asVertex().get("First Name")).isEqualTo("Jay");
    }

    databaseFactory.open().drop();

    TestHelper.checkActiveDatabases();
  }

  /**
   * Regression test for GitHub issue #1552: Edge import should use typeIdType for from/to fields.
   * By default typeIdType is String, but edge from/to fields were always parsed as Long.
   * This test verifies that String IDs work correctly for both vertices and edges.
   */
  @Test
  void testRegression_Issue1552_StringIdType() {
    final String databasePath = "target/databases/test-import-graph-stringid";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    // Import with default typeIdType (String) - this should work now
    Importer importer = new Importer(new String[] {
        "-vertices", "src/test/resources/importer-vertices-stringid.csv",
        "-edges", "src/test/resources/importer-edges-stringid.csv",
        "-database", databasePath,
        "-typeIdProperty", "Id",
        // typeIdType defaults to "String" - this is the key part of the test
        "-typeIdUnique", "true",
        "-forceDatabaseCreate", "true",
        "-edgeFromField", "From",
        "-edgeToField", "To"
    });
    importer.load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Node", true)).isEqualTo(6);
      assertThat(db.countType("Relationship", true)).as("All 3 edges should be imported with String IDs").isEqualTo(3);

      // Verify specific edges exist using String IDs
      var vertexA = db.lookupByKey("Node", "Id", "A").next().getRecord().asVertex();
      assertThat(vertexA.get("First Name")).isEqualTo("Jay");
      assertThat(vertexA.countEdges(com.arcadedb.graph.Vertex.DIRECTION.OUT, "Relationship"))
          .as("Vertex A should have 2 outgoing edges").isEqualTo(2);

      var vertexB = db.lookupByKey("Node", "Id", "B").next().getRecord().asVertex();
      assertThat(vertexB.countEdges(com.arcadedb.graph.Vertex.DIRECTION.OUT, "Relationship"))
          .as("Vertex B should have 1 outgoing edge (B->E)").isEqualTo(1);
      assertThat(vertexB.countEdges(com.arcadedb.graph.Vertex.DIRECTION.IN, "Relationship"))
          .as("Vertex B should have 1 incoming edge (A->B)").isEqualTo(1);

      var vertexE = db.lookupByKey("Node", "Id", "E").next().getRecord().asVertex();
      assertThat(vertexE.countEdges(com.arcadedb.graph.Vertex.DIRECTION.IN, "Relationship"))
          .as("Vertex E should have 1 incoming edge (B->E)").isEqualTo(1);
    }

    databaseFactory.open().drop();

    TestHelper.checkActiveDatabases();
  }

  /**
   * Test for GitHub issue #1198: IMPORT DATABASE misses edge
   * This test verifies that ALL edges are imported, including the last one.
   * The bug was that the last edge batch was never flushed when importing ended.
   */
  @Test
  void importGraphVerifyAllEdgesImported() {
    final String databasePath = "target/databases/test-import-graph-edges";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    // Import both vertices and edges in a single Importer instance
    // This tests the scenario where vertices and edges are loaded in the same import operation
    Importer importer = new Importer(new String[] {
        "-vertices", "src/test/resources/importer-vertices.csv",
        "-edges", "src/test/resources/importer-edges.csv",
        "-database", databasePath,
        "-typeIdProperty", "Id",
        "-typeIdType", "Long",
        "-typeIdUnique", "true",
        "-forceDatabaseCreate", "true",
        "-edgeFromField", "From",
        "-edgeToField", "To"
    });
    importer.load();

    try (final Database db = databaseFactory.open()) {
      assertThat(db.countType("Node", true)).isEqualTo(6);

      // Verify ALL 3 edges are imported (this is the key assertion for issue #1198)
      // edges.csv has: 0->1, 0->2, 1->4
      assertThat(db.countType("Relationship", true)).as("All 3 edges should be imported").isEqualTo(3);

      // Verify specific edges exist
      var vertex0 = db.lookupByKey("Node", "Id", 0).next().getRecord().asVertex();
      assertThat(vertex0.get("First Name")).isEqualTo("Jay");
      assertThat(vertex0.countEdges(com.arcadedb.graph.Vertex.DIRECTION.OUT, "Relationship"))
          .as("Vertex 0 should have 2 outgoing edges").isEqualTo(2);

      var vertex1 = db.lookupByKey("Node", "Id", 1).next().getRecord().asVertex();
      assertThat(vertex1.countEdges(com.arcadedb.graph.Vertex.DIRECTION.OUT, "Relationship"))
          .as("Vertex 1 should have 1 outgoing edge (the last edge 1->4)").isEqualTo(1);
      assertThat(vertex1.countEdges(com.arcadedb.graph.Vertex.DIRECTION.IN, "Relationship"))
          .as("Vertex 1 should have 1 incoming edge (0->1)").isEqualTo(1);

      var vertex4 = db.lookupByKey("Node", "Id", 4).next().getRecord().asVertex();
      assertThat(vertex4.countEdges(com.arcadedb.graph.Vertex.DIRECTION.IN, "Relationship"))
          .as("Vertex 4 should have 1 incoming edge (1->4)").isEqualTo(1);
    }

    databaseFactory.open().drop();

    TestHelper.checkActiveDatabases();
  }

}
