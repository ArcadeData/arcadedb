/*
 * Copyright 2021 Arcade Data Ltd
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

package com.arcadedb.integration;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class Neo4jImporterIT {
  private final static String DATABASE_PATH = "target/databases/neo4j";

  @Test
  public void testImportOK() throws IOException {
    final File databaseDirectory = new File(DATABASE_PATH);

    try {
      final URL inputFile = Neo4jImporterIT.class.getClassLoader().getResource("neo4j-export-mini.jsonl");

      final Neo4jImporter importer = new Neo4jImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
      importer.run();

      Assertions.assertFalse(importer.isError());

      Assertions.assertTrue(databaseDirectory.exists());

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (Database database = factory.open()) {
          DocumentType personType = database.getSchema().getType("User");
          Assertions.assertNotNull(personType);
          Assertions.assertEquals(3, database.countType("User", true));

          DocumentType friendType = database.getSchema().getType("KNOWS");
          Assertions.assertNotNull(friendType);
          Assertions.assertEquals(1, database.countType("KNOWS", true));
        }
      }
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
    }
  }

  @Test
  public void testImportNoFile() throws IOException {
    final URL inputFile = Neo4jImporterIT.class.getClassLoader().getResource("neo4j-export-mini.jsonl");
    final Neo4jImporter importer = new Neo4jImporter(("-i " + inputFile.getFile() + "2 -d " + DATABASE_PATH + " -o").split(" "));
    try {
      importer.run();
      Assertions.fail("Expected File Not Found Exception");
    } catch (IllegalArgumentException e) {
    }
    Assertions.assertTrue(importer.isError());
  }
}
