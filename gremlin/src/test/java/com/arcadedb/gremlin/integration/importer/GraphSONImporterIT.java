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
package com.arcadedb.gremlin.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.test.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.arcadedb.gremlin.ArcadeGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.*;
import java.util.*;
import java.util.stream.*;
import java.util.zip.*;

import static org.assertj.core.api.Assertions.assertThat;

class GraphSONImporterIT {
  private final static String DATABASE_PATH     = "target/databases/performance";
  private final static String FILE              = "arcadedb-export.graphson.tgz";
  private final static String UNCOMPRESSED_FILE = "target/arcadedb-export.graphson";

  private final static File databaseDirectory = new File(DATABASE_PATH);

  @Test
  void importCompressedOK() {
    final URL inputFile = GraphSONImporterIT.class.getClassLoader().getResource(FILE);

    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {

      final Importer importer = new Importer(database, inputFile.getFile());
      importer.load();

      assertThat(databaseDirectory.exists()).isTrue();

      assertThat(database.getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet())).isEqualTo(new HashSet<>(Arrays.asList("Friend", "Person")));

      for (final DocumentType type : database.getSchema().getTypes()) {
        assertThat(database.countType(type.getName(), true) > 0).isTrue();
      }
    }
  }

  @Test
  void importNotCompressedOK() throws Exception {
    final URL inputFile = GraphSONImporterIT.class.getClassLoader().getResource(FILE);

    try (final GZIPInputStream gis = new GZIPInputStream(new FileInputStream(inputFile.getFile()));
        final FileOutputStream fos = new FileOutputStream(UNCOMPRESSED_FILE)) {
      final byte[] buffer = new byte[1024 * 8];
      int len;
      while ((len = gis.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
      }
    }

    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {

      final Importer importer = new Importer(database, UNCOMPRESSED_FILE);
      importer.load();

      assertThat(databaseDirectory.exists()).isTrue();

      assertThat(database.getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet())).isEqualTo(new HashSet<>(Arrays.asList("Friend", "Person")));

      for (final DocumentType type : database.getSchema().getTypes()) {
        assertThat(database.countType(type.getName(), true) > 0).isTrue();
      }
    }
  }

  @Test
  void importFromSQL() {
    final URL inputFile = GraphSONImporterIT.class.getClassLoader().getResource(FILE);

    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {

      database.command("sql", "import database file://" + inputFile.getFile() + " WITH commitEvery = 1000");

      assertThat(databaseDirectory.exists()).isTrue();

      assertThat(database.getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet())).isEqualTo(new HashSet<>(Arrays.asList("Friend", "Person")));

      for (final DocumentType type : database.getSchema().getTypes()) {
        assertThat(database.countType(type.getName(), true) > 0).isTrue();
      }
    }
    assertThat(DatabaseFactory.getActiveDatabaseInstance(DATABASE_PATH)).isNull();
  }

  /**
   * Test importing GraphSON with string-based IDs (like URIs).
   * This tests the fix for GitHub issue #1714.
   *
   * The issue is that when importing GraphSON files where vertex IDs are strings
   * (not ArcadeDB RIDs), the original IDs should be preserved as a property
   * and edges should correctly connect vertices.
   */
  @Test
  void importGraphSONWithStringIds() throws Exception {
    final URL inputFile = GraphSONImporterIT.class.getClassLoader().getResource("graphson-with-string-ids.graphson");
    assertThat(inputFile).isNotNull();

    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {

      final Importer importer = new Importer(database, inputFile.getFile());
      importer.load();

      assertThat(databaseDirectory.exists()).isTrue();

      // Verify types were created
      assertThat(database.getSchema().existsType("Class")).isTrue();
      assertThat(database.getSchema().existsType("Person")).isTrue();

      // Test that we can query vertices by their original IDs using Gremlin
      try (final ArcadeGraph graph = ArcadeGraph.open(database)) {
        final GraphTraversalSource g = graph.traversal();

        // Verify vertices were created
        assertThat(database.countType("Class", true)).isEqualTo(2);
        assertThat(database.countType("Person", true)).isEqualTo(2);

        // Find vertex by original ID stored as property
        List<Vertex> datatype = g.V().has("@id", "http://www.w3.org/2000/01/rdf-schema#Datatype").toList();
        assertThat(datatype).hasSize(1);
        assertThat(datatype.getFirst().property("name").value()).isEqualTo("Datatype");

        List<Vertex> resource = g.V().has("@id", "http://www.w3.org/2000/01/rdf-schema#Resource").toList();
        assertThat(resource).hasSize(1);
        assertThat(resource.getFirst().property("name").value()).isEqualTo("Resource");

        // Verify edges were correctly connected
        List<Vertex> subclassTargets = g.V().has("@id", "http://www.w3.org/2000/01/rdf-schema#Datatype")
            .out("subClassOf").toList();
        assertThat(subclassTargets).hasSize(1);
        assertThat(subclassTargets.getFirst().property("@id").value())
            .isEqualTo("http://www.w3.org/2000/01/rdf-schema#Resource");

        // Verify Person vertices and their edges
        List<Vertex> john = g.V().has("@id", "http://example.org/person/John").toList();
        assertThat(john).hasSize(1);
        assertThat(john.getFirst().property("name").value()).isEqualTo("John");

        List<Vertex> janeFromJohn = g.V().has("@id", "http://example.org/person/John").out("knows").toList();
        assertThat(janeFromJohn).hasSize(1);
        assertThat(janeFromJohn.getFirst().property("@id").value()).isEqualTo("http://example.org/person/Jane");
      }
    }
  }

  /**
   * Test that files can be deleted after import.
   * This tests the fix for GitHub issue #1627.
   *
   * The issue is that file handles are not properly closed after import,
   * preventing the file from being deleted (especially on Windows).
   */
  @Test
  void importFileCanBeDeletedAfterImport() throws Exception {
    // Create a temporary GraphSON file to import
    final File tempFile = File.createTempFile("test-import-", ".graphson");
    try (final FileOutputStream fos = new FileOutputStream(tempFile)) {
      // Write a simple GraphSON file with RID-format IDs
      final String graphson = """
          {"id":{"@type":"g:Int64","@value":0},"label":"TestVertex","properties":{"name":[{"id":{"@type":"g:Int64","@value":1},"value":"Test"}]}}
          """;
      fos.write(graphson.getBytes());
    }

    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      final Importer importer = new Importer(database, tempFile.getAbsolutePath());
      importer.load();

      // Verify import worked
      assertThat(database.getSchema().existsType("TestVertex")).isTrue();
      assertThat(database.countType("TestVertex", true)).isEqualTo(1);
    }

    // The file should be deletable after import completes
    // On Windows, this will fail if file handles are not properly closed
    final boolean deleted = tempFile.delete();
    assertThat(deleted)
        .as("File should be deletable after import - file handles must be properly closed (issue #1627)")
        .isTrue();
  }

  @BeforeEach
  @AfterEach
  void clean() {
    TestServerHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(databaseDirectory);
    if (new File(UNCOMPRESSED_FILE).exists())
      new File(UNCOMPRESSED_FILE).delete();
  }
}
