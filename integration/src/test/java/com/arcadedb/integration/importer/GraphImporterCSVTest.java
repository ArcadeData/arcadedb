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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.importer.graph.CsvRowSource;
import com.arcadedb.integration.importer.graph.GraphImporter;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the generic {@link GraphImporter} with {@link CsvRowSource} using the existing
 * CSV test data (importer-vertices.csv + importer-edges.csv).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphImporterCSVTest {

  private static final String DB_PATH = "target/databases/graph-importer-csv-test";
  private Database database;

  @BeforeEach
  void setup() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void cleanup() {
    if (database != null)
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void importVerticesAndEdgesFromCSV() throws Exception {
    final String resourceDir = new File("src/test/resources").getAbsolutePath();

    database.transaction(() -> {
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("Knows");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Node", new CsvRowSource(resourceDir + "/importer-vertices.csv"), v -> {
          v.id("Id");
          v.intProperty("Id", "Id");
          v.property("firstName", "First Name");
          v.property("lastName", "Last Name");
        })
        .edgeSource("Knows", new CsvRowSource(resourceDir + "/importer-edges.csv"), e -> {
          e.from("From", "Node");
          e.to("To", "Node");
          e.intProperty("since", "Since");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(6);
      assertThat(importer.getEdgeCount()).isEqualTo(3);
    }

    // Verify vertices
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT count(*) as c FROM Node")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(6);
      }
    });

    // Verify edges and properties
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Node WHERE Id = 0")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex jay = rs.next().getVertex().get();
        assertThat(jay.getString("firstName")).isEqualTo("Jay");
        assertThat(jay.getString("lastName")).isEqualTo("Miner");

        // Jay (0) should have 2 outgoing Knows edges: to 1 (John) and 2 (Steve)
        int outCount = 0;
        for (final Edge e : jay.getEdges(Vertex.DIRECTION.OUT, "Knows"))
          outCount++;
        assertThat(outCount).isEqualTo(2);
      }
    });

    // Verify bidirectional: John (1) should have IN edge from Jay (0) + OUT edge to Tesla (4)
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Node WHERE Id = 1")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex john = rs.next().getVertex().get();

        int inCount = 0;
        for (final Edge ignored : john.getEdges(Vertex.DIRECTION.IN, "Knows"))
          inCount++;
        assertThat(inCount).isEqualTo(1);

        int outCount = 0;
        for (final Edge e : john.getEdges(Vertex.DIRECTION.OUT, "Knows")) {
          outCount++;
          // Verify edge property
          assertThat(e.getInteger("since")).isEqualTo(1970);
        }
        assertThat(outCount).isEqualTo(1);
      }
    });
  }

  @Test
  void importForeignKeyEdgesFromCSV() throws Exception {
    final String resourceDir = new File("src/test/resources").getAbsolutePath();

    database.transaction(() -> {
      database.getSchema().createVertexType("Department");
      database.getSchema().createVertexType("Employee");
      database.getSchema().createEdgeType("WORKS_IN");
    });

    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex("Department", new CsvRowSource(resourceDir + "/importer-departments.csv"), v -> {
          v.id("Id");
          v.intProperty("deptId", "Id");
          v.property("name", "Name");
        })
        .vertex("Employee", new CsvRowSource(resourceDir + "/importer-employees.csv"), v -> {
          v.id("Id");
          v.intProperty("empId", "Id");
          v.property("name", "Name");
          v.edgeOut("DeptId", "WORKS_IN", "Department");
        })
        .build()) {

      importer.run();

      assertThat(importer.getVertexCount()).isEqualTo(8);
      assertThat(importer.getEdgeCount()).isEqualTo(5);
    }

    // Verify edge records exist and are countable
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT count(*) as c FROM WORKS_IN")) {
        assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(5);
      }
    });

    // Verify Alice (DeptId=1) works in Engineering
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Employee WHERE name = 'Alice'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex alice = rs.next().getVertex().get();
        int outCount = 0;
        for (final Edge e : alice.getEdges(Vertex.DIRECTION.OUT, "WORKS_IN")) {
          assertThat(e.getInVertex().asVertex().getString("name")).isEqualTo("Engineering");
          outCount++;
        }
        assertThat(outCount).isEqualTo(1);
      }
    });

    // Verify Engineering dept has 2 incoming WORKS_IN edges (Alice, Bob)
    database.transaction(() -> {
      try (ResultSet rs = database.query("sql", "SELECT FROM Department WHERE name = 'Engineering'")) {
        assertThat(rs.hasNext()).isTrue();
        final Vertex eng = rs.next().getVertex().get();
        int inCount = 0;
        for (final Edge ignored : eng.getEdges(Vertex.DIRECTION.IN, "WORKS_IN"))
          inCount++;
        assertThat(inCount).isEqualTo(2);
      }
    });
  }
}
