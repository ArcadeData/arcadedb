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
package com.arcadedb.gremlin.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.integration.importer.OrientDBImporter;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class GraphMLExporterIT {
  private final static String DATABASE_PATH = "target/databases/performance";
  private final static String FILE          = "target/arcadedb-export.graphml.tgz";

  private final static File databaseDirectory         = new File(DATABASE_PATH);
  private final static File importedDatabaseDirectory = new File(DATABASE_PATH + "_imported");
  private final static File file                      = new File(FILE);

  @Test
  public void testExportOK() throws IOException {
    final var inputFile = GraphMLExporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    final var importer = new OrientDBImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    Assertions.assertFalse(importer.isError());
    Assertions.assertTrue(databaseDirectory.exists());

    new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format graphml").split(" ")).exportDatabase();

    Assertions.assertTrue(file.exists());
    Assertions.assertTrue(file.length() > 0);

    try (ArcadeGraph graph = ArcadeGraph.open(importedDatabaseDirectory.getAbsolutePath())) {
      try (GZIPInputStream is = new GZIPInputStream(new FileInputStream(file))) {
        graph.io(IoCore.graphml()).reader().create().readGraph(is, graph);
      }

      Assertions.assertTrue(importedDatabaseDirectory.exists());

      try (Database originalDatabase = new DatabaseFactory(DATABASE_PATH).open(PaginatedFile.MODE.READ_ONLY)) {
        Assertions.assertEquals(//
            originalDatabase.getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet()),//
            graph.getDatabase().getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet()));

        for (DocumentType type : originalDatabase.getSchema().getTypes()) {
          Assertions.assertEquals(//
              originalDatabase.countType(type.getName(), true),//
              graph.getDatabase().countType(type.getName(), true));
        }
      }
    }
  }

  @BeforeEach
  @AfterEach
  public void clean() {
    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
    FileUtils.deleteRecursively(databaseDirectory);
    FileUtils.deleteRecursively(importedDatabaseDirectory);
    if (file.exists())
      file.delete();
  }
}
