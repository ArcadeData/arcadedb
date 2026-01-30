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
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

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
  void regressionIssue1552StringIdType() {
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
      assertThat(vertexA.countEdges(Vertex.DIRECTION.OUT, "Relationship"))
          .as("Vertex A should have 2 outgoing edges").isEqualTo(2);

      var vertexB = db.lookupByKey("Node", "Id", "B").next().getRecord().asVertex();
      assertThat(vertexB.countEdges(Vertex.DIRECTION.OUT, "Relationship"))
          .as("Vertex B should have 1 outgoing edge (B->E)").isEqualTo(1);
      assertThat(vertexB.countEdges(Vertex.DIRECTION.IN, "Relationship"))
          .as("Vertex B should have 1 incoming edge (A->B)").isEqualTo(1);

      var vertexE = db.lookupByKey("Node", "Id", "E").next().getRecord().asVertex();
      assertThat(vertexE.countEdges(Vertex.DIRECTION.IN, "Relationship"))
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
      assertThat(vertex0.countEdges(Vertex.DIRECTION.OUT, "Relationship"))
          .as("Vertex 0 should have 2 outgoing edges").isEqualTo(2);

      var vertex1 = db.lookupByKey("Node", "Id", 1).next().getRecord().asVertex();
      assertThat(vertex1.countEdges(Vertex.DIRECTION.OUT, "Relationship"))
          .as("Vertex 1 should have 1 outgoing edge (the last edge 1->4)").isEqualTo(1);
      assertThat(vertex1.countEdges(Vertex.DIRECTION.IN, "Relationship"))
          .as("Vertex 1 should have 1 incoming edge (0->1)").isEqualTo(1);

      var vertex4 = db.lookupByKey("Node", "Id", 4).next().getRecord().asVertex();
      assertThat(vertex4.countEdges(Vertex.DIRECTION.IN, "Relationship"))
          .as("Vertex 4 should have 1 incoming edge (1->4)").isEqualTo(1);
    }

    databaseFactory.open().drop();

    TestHelper.checkActiveDatabases();
  }

  /**
   * Regression test for GitHub issue #2267: CSV importer bug with separate vertex type imports
   * When vertices of different types are imported in separate IMPORT commands, and then edges
   * are imported in another separate IMPORT command, the edges get skipped because the
   * GraphImporter's in-memory vertex index is empty (it was closed after the vertex imports).
   *
   * This test reproduces the exact scenario from the issue:
   * 1. Import Tenant vertices (IDs 0-9)
   * 2. Import Supervisor vertices (IDs 10-19) in a separate command
   * 3. Import edges connecting them in yet another separate command
   *
   * Expected: All edges should be created by querying the database for vertices
   * Actual (before fix): All edges are skipped because vertices are not in the in-memory index
   */
  @Test
  void regressionIssue2267SeparateVertexTypeImports() throws Exception {
    final String databasePath = "target/databases/test-import-separate-types";

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      // Create schema
      db.command("sql", "CREATE VERTEX TYPE Tenant");
      db.command("sql", "CREATE VERTEX TYPE Supervisor");
      db.command("sql", "CREATE EDGE TYPE Belongs");
      db.command("sql", "CREATE PROPERTY Tenant.id LONG");
      db.command("sql", "CREATE PROPERTY Tenant.name STRING");
      db.command("sql", "CREATE PROPERTY Supervisor.id LONG");
      db.command("sql", "CREATE PROPERTY Supervisor.name STRING");
      db.command("sql", "CREATE INDEX ON Tenant (id) UNIQUE");
      db.command("sql", "CREATE INDEX ON Supervisor (id) UNIQUE");

      // Create test CSV files
      final File tempDir = new File("target/test-csv-issue2267");
      tempDir.mkdirs();

      // Create tenants.csv
      final File tenantsFile = new File(tempDir, "tenants.csv");
      Files.writeString(tenantsFile.toPath(), """
          @class,id,name
          Tenant,0,Tenant-1
          Tenant,1,Tenant-2
          Tenant,2,Tenant-3
          Tenant,3,Tenant-4
          Tenant,4,Tenant-5
          Tenant,5,Tenant-6
          """);

      // Create supervisors.csv
      final File supervisorsFile = new File(tempDir, "supervisors.csv");
      Files.writeString(supervisorsFile.toPath(), """
          @class,id,name
          Supervisor,10,Supervisor-1
          Supervisor,11,Supervisor-2
          Supervisor,12,Supervisor-3
          Supervisor,13,Supervisor-4
          Supervisor,14,Supervisor-5
          Supervisor,15,Supervisor-6
          """);

      // Create edges.csv (Tenant -> Supervisor)
      final File edgesFile = new File(tempDir, "edges.csv");
      Files.writeString(edgesFile.toPath(), """
          Tenant,Supervisor
          0,10
          1,11
          2,12
          3,13
          4,14
          5,15
          """);

      // Create empty.csv (required for the import command syntax)
      final File emptyFile = new File(tempDir, "empty.csv");
      Files.writeString(emptyFile.toPath(), "");

      // Import Tenants in first command
      db.command("sql", String.format("""
          IMPORT DATABASE file://%s WITH vertices="file://%s", vertexType=Tenant, verticesFileType=csv, typeIdProperty=id, typeIdType=Long, typeIdPropertyIsUnique=true
          """, emptyFile.getAbsolutePath(), tenantsFile.getAbsolutePath()));

      assertThat(db.countType("Tenant", true)).isEqualTo(6);

      // Import Supervisors in second command (separate GraphImporter instance)
      db.command("sql", String.format("""
          IMPORT DATABASE file://%s WITH vertices="file://%s", vertexType=Supervisor, verticesFileType=csv, typeIdProperty=id, typeIdType=Long, typeIdPropertyIsUnique=true
          """, emptyFile.getAbsolutePath(), supervisorsFile.getAbsolutePath()));

      assertThat(db.countType("Supervisor", true)).isEqualTo(6);

      // Import edges in third command (YET ANOTHER separate GraphImporter instance with empty vertex index)
      // THIS IS WHERE THE BUG OCCURS - edges are skipped because the vertices are not in the in-memory index
      db.command("sql", String.format("""
          IMPORT DATABASE file://%s WITH edges="file://%s", edgesFileType=csv, edgeType=Belongs, edgeFromField=Tenant, edgeToField=Supervisor, typeIdProperty=id, typeIdType=Long
          """, emptyFile.getAbsolutePath(), edgesFile.getAbsolutePath()));

      // Verify edges were created (this will fail before the fix)
      assertThat(db.countType("Belongs", true))
          .as("All 6 edges should be imported even though vertices are not in the in-memory index")
          .isEqualTo(6);

      // Verify specific edge connections
      var tenant0 = db.lookupByKey("Tenant", "id", 0L).next().getRecord().asVertex();
      assertThat(tenant0.countEdges(Vertex.DIRECTION.OUT, "Belongs"))
          .as("Tenant 0 should have 1 outgoing Belongs edge to Supervisor 10")
          .isEqualTo(1);

      var supervisor10 = db.lookupByKey("Supervisor", "id", 10L).next().getRecord().asVertex();
      assertThat(supervisor10.countEdges(Vertex.DIRECTION.IN, "Belongs"))
          .as("Supervisor 10 should have 1 incoming Belongs edge from Tenant 0")
          .isEqualTo(1);

    } finally {
      db.drop();
    }
    TestHelper.checkActiveDatabases();
  }

}
