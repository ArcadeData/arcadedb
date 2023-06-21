/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.gremlin.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.integration.exporter.Exporter;
import com.arcadedb.integration.importer.OrientDBImporter;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.stream.*;
import java.util.zip.*;

public class GraphSONExporterIT {
  private final static String DATABASE_PATH = "target/databases/performance";
  private final static String FILE          = "target/arcadedb-export.graphson.tgz";

  private final static File databaseDirectory         = new File(DATABASE_PATH);
  private final static File importedDatabaseDirectory = new File(DATABASE_PATH + "_imported");
  private final static File file                      = new File(FILE);

  @Test
  public void testExportOK() throws Exception {
    final var inputFile = GraphSONExporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    final var importer = new OrientDBImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    Assertions.assertFalse(importer.isError());
    Assertions.assertTrue(databaseDirectory.exists());

    new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format graphson").split(" ")).exportDatabase();

    Assertions.assertTrue(file.exists());
    Assertions.assertTrue(file.length() > 0);

    try (final ArcadeGraph graph = ArcadeGraph.open(importedDatabaseDirectory.getAbsolutePath())) {
      try (final GZIPInputStream is = new GZIPInputStream(new FileInputStream(file))) {
        graph.io(IoCore.graphson()).reader().create().readGraph(is, graph);
      }

      Assertions.assertTrue(importedDatabaseDirectory.exists());

      try (final Database originalDatabase = new DatabaseFactory(DATABASE_PATH).open(ComponentFile.MODE.READ_ONLY)) {
        Assertions.assertEquals(//
            originalDatabase.getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet()),//
            graph.getDatabase().getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet()));

        for (final DocumentType type : originalDatabase.getSchema().getTypes()) {
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
    TestServerHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(databaseDirectory);
    FileUtils.deleteRecursively(importedDatabaseDirectory);
    if (file.exists())
      file.delete();
  }
}
