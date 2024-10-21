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
package com.arcadedb.gremlin.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.stream.*;
import java.util.zip.*;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphSONImporterIT {
  private final static String DATABASE_PATH     = "target/databases/performance";
  private final static String FILE              = "arcadedb-export.graphson.tgz";
  private final static String UNCOMPRESSED_FILE = "target/arcadedb-export.graphson";

  private final static File databaseDirectory = new File(DATABASE_PATH);

  @Test
  public void testImportCompressedOK() {
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
  public void testImportNotCompressedOK() throws IOException {
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
  public void testImportFromSQL() {
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

  @BeforeEach
  @AfterEach
  public void clean() {
    TestServerHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(databaseDirectory);
    if (new File(UNCOMPRESSED_FILE).exists())
      new File(UNCOMPRESSED_FILE).delete();
  }
}
