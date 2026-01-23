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
package com.arcadedb.integration.exporter;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.OrientDBImporterIT;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

public class SQLLocalExporterTest {

  public static final String DATABASE_PATH = "databases/importedFromOrientDB";

  @BeforeEach
  @AfterEach
  void beforeTests() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void importAndExportDatabase() {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    try (final Database database = new DatabaseFactory("databases/importedFromOrientDB").create()) {
      database.getConfiguration()
          .setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE,
              ((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()) * 10);

      database.command("sql", "import database file://" + inputFile.getFile());

      assertThat(database.countType("Person", false)).isEqualTo(500);
      assertThat(database.countType("Friend", false)).isEqualTo(10000);

      final ResultSet result = database.command("sql", "export database file://export.jsonl.tgz with `overwrite` = true");

      final Result stats = result.next();
      assertThat((long) stats.getProperty("vertices")).isEqualTo(500L);
      assertThat((long) stats.getProperty("edges")).isEqualTo(10000L);
      assertThat(stats.<Object>getProperty("documents")).isNull();

      final File exportFile = new File("./exports/export.jsonl.tgz");
      assertThat(exportFile.exists()).isTrue();
      assertThat(exportFile.length() > 50_000).isTrue();
    }

  }

  @Test
  void importAndExportPartialDatabase() {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    try (final Database database = new DatabaseFactory("databases/importedFromOrientDB").create()) {
      database.getConfiguration()
          .setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE,
              ((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()) * 10);

      database.command("sql", "import database file://" + inputFile.getFile());

      assertThat(database.countType("Person", false)).isEqualTo(500);
      assertThat(database.countType("Friend", false)).isEqualTo(10000);

      final ResultSet result = database.command("sql",
          "export database file://export.jsonl.tgz with `overwrite` = true, includeTypes = Person");

      final Result stats = result.next();
      assertThat(stats.<Long>getProperty("vertices")).isEqualTo(500L);
      assertThat(stats.<Object>getProperty("edges")).isNull();
      assertThat(stats.<Object>getProperty("documents")).isNull();

      final File exportFile = new File("./exports/export.jsonl.tgz");
      assertThat(exportFile.exists()).isTrue();
      assertThat(exportFile.length()).isGreaterThan(40_000);
    }

  }

  @Test
  void testExportAndReimportSmallDatabase() {
    // Test case for issue #1839 - exported database cannot be imported
    final String dbPath = "databases/testExportImport";
    FileUtils.deleteRecursively(new File(dbPath));

    try (final Database database = new DatabaseFactory(dbPath).create()) {
      // Create a simple schema with vertices and edges like in the bug report
      database.getSchema().createVertexType("TestVertex");
      database.getSchema().createEdgeType("TestEdge");

      // Create some test data: 110 vertices and 135 edges (similar to bug report)
      database.transaction(() -> {
        final var vertices = new java.util.ArrayList<com.arcadedb.graph.Vertex>();
        for (int i = 0; i < 110; i++) {
          final var v = database.newVertex("TestVertex");
          v.set("id", i);
          v.set("name", "Vertex" + i);
          v.save();
          vertices.add(v);
        }

        // Create edges
        for (int i = 0; i < 135; i++) {
          final var from = vertices.get(i % 110);
          final var to = vertices.get((i + 1) % 110);
          from.newEdge("TestEdge", to).save();
        }
      });

      assertThat(database.countType("TestVertex", false)).isEqualTo(110);
      assertThat(database.countType("TestEdge", false)).isEqualTo(135);

      // Export the database
      final ResultSet exportResult = database.command("sql", "export database file://test-export.jsonl.tgz with `overwrite` = true");
      final Result exportStats = exportResult.next();

      assertThat((long) exportStats.getProperty("vertices")).isEqualTo(110L);
      assertThat((long) exportStats.getProperty("edges")).isEqualTo(135L);

      final File exportFile = new File("./exports/test-export.jsonl.tgz");
      assertThat(exportFile.exists()).isTrue();
      assertThat(exportFile.length()).isGreaterThan(0);
    }

    // Now try to import it into a new database
    final String dbPath2 = "databases/testExportImport2";
    FileUtils.deleteRecursively(new File(dbPath2));

    try (final Database database2 = new DatabaseFactory(dbPath2).create()) {
      // Import the exported database
      final ResultSet importResult = database2.command("sql", "import database file://exports/test-export.jsonl.tgz");
      final Result importStats = importResult.next();

      // Verify the import worked
      assertThat((String) importStats.getProperty("result")).isEqualTo("OK");
      assertThat(database2.countType("TestVertex", false)).isEqualTo(110L);
      assertThat(database2.countType("TestEdge", false)).isEqualTo(135L);
    } finally {
      FileUtils.deleteRecursively(new File(dbPath));
      FileUtils.deleteRecursively(new File(dbPath2));
      new File("./exports/test-export.jsonl.tgz").delete();
    }
  }
}
