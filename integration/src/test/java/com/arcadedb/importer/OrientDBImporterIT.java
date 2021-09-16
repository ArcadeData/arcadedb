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

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class OrientDBImporterIT {
  private final static String DATABASE_PATH = "target/databases/performance";
  private final static String SECURITY_PATH = "target/databases/performance/security.json";

  @Test
  public void testImportOK() throws IOException {
    final File databaseDirectory = new File(DATABASE_PATH);

    try {
      final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

      final OrientDBImporter importer = new OrientDBImporter(
          ("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -s " + SECURITY_PATH + " -o").split(" "));
      importer.run();

      Assertions.assertFalse(importer.isError());

      Assertions.assertTrue(databaseDirectory.exists());

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (Database database = factory.open()) {
          DocumentType personType = database.getSchema().getType("Person");
          Assertions.assertNotNull(personType);
          Assertions.assertEquals(Type.INTEGER, personType.getProperty("id").getType());
          Assertions.assertEquals(500, database.countType("Person", true));
          Assertions.assertEquals(Schema.INDEX_TYPE.LSM_TREE, database.getSchema().getIndexByName("Person[id]").getType());

          DocumentType friendType = database.getSchema().getType("Friend");
          Assertions.assertNotNull(friendType);
          Assertions.assertEquals(Type.INTEGER, friendType.getProperty("id").getType());
          Assertions.assertEquals(10_000, database.countType("Friend", true));
          Assertions.assertEquals(Schema.INDEX_TYPE.LSM_TREE, database.getSchema().getIndexByName("Friend[id]").getType());

          final File securityFile = new File(SECURITY_PATH);
          Assertions.assertTrue(securityFile.exists());

          final JSONObject security = new JSONObject(FileUtils.readFileAsString(securityFile, "UTF8"));
          Assertions.assertNotNull(security.getJSONObject("users"));
        }
      }
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
    }
  }

  @Test
  public void testImportNoFile() throws IOException {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");
    final OrientDBImporter importer = new OrientDBImporter(("-i " + inputFile.getFile() + "2 -d " + DATABASE_PATH + " -s security.json -o").split(" "));
    try {
      importer.run();
      Assertions.fail("Expected File Not Found Exception");
    } catch (IllegalArgumentException e) {
    }
    Assertions.assertTrue(importer.isError());
  }
}
