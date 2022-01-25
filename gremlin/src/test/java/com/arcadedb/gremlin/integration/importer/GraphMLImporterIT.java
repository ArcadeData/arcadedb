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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class GraphMLImporterIT {
  private final static String DATABASE_PATH     = "target/databases/performance";
  private final static String FILE              = "arcadedb-export.graphml.tgz";
  private final static String UNCOMPRESSED_FILE = "target/arcadedb-export.graphml";

  private final static File databaseDirectory = new File(DATABASE_PATH);

  @Test
  public void testImportCompressedOK() {
    final URL inputFile = GraphMLImporterIT.class.getClassLoader().getResource(FILE);

    try (Database database = new DatabaseFactory(DATABASE_PATH).create()) {

      final Importer importer = new Importer(database, inputFile.getFile());
      importer.load();

      Assertions.assertTrue(databaseDirectory.exists());

      Assertions.assertEquals(//
          new HashSet<>(Arrays.asList("Friend", "Person")),//
          database.getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet()));

      for (DocumentType type : database.getSchema().getTypes()) {
        Assertions.assertTrue(database.countType(type.getName(), true) > 0);
      }
    }
  }

  @Test
  public void testImportNotCompressedOK() throws IOException {
    final URL inputFile = GraphMLImporterIT.class.getClassLoader().getResource(FILE);

    try (final GZIPInputStream gis = new GZIPInputStream(new FileInputStream(inputFile.getFile()));
        final FileOutputStream fos = new FileOutputStream(UNCOMPRESSED_FILE)) {
      final byte[] buffer = new byte[1024 * 8];
      int len;
      while ((len = gis.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
      }
    }

    try (Database database = new DatabaseFactory(DATABASE_PATH).create()) {

      final Importer importer = new Importer(database, UNCOMPRESSED_FILE);
      importer.load();

      Assertions.assertTrue(databaseDirectory.exists());

      Assertions.assertEquals(//
          new HashSet<>(Arrays.asList("Friend", "Person")),//
          database.getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet()));

      for (DocumentType type : database.getSchema().getTypes()) {
        Assertions.assertTrue(database.countType(type.getName(), true) > 0);
      }
    }
  }

  @Test
  public void testImportFromSQL() {
    final URL inputFile = GraphMLImporterIT.class.getClassLoader().getResource(FILE);

    try (Database database = new DatabaseFactory(DATABASE_PATH).create()) {

      database.command("sql", "import database file://" + inputFile.getFile());

      Assertions.assertTrue(databaseDirectory.exists());

      Assertions.assertEquals(//
          new HashSet<>(Arrays.asList("Friend", "Person")),//
          database.getSchema().getTypes().stream().map(DocumentType::getName).collect(Collectors.toSet()));

      for (DocumentType type : database.getSchema().getTypes()) {
        Assertions.assertTrue(database.countType(type.getName(), true) > 0);
      }
    }
    Assertions.assertNull(DatabaseFactory.getActiveDatabaseInstance(DATABASE_PATH));
  }

  @BeforeEach
  @AfterEach
  public void clean() {
    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
    FileUtils.deleteRecursively(databaseDirectory);
    if (new File(UNCOMPRESSED_FILE).exists())
      new File(UNCOMPRESSED_FILE).delete();
  }
}
