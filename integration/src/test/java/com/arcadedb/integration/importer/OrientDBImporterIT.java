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
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;

public class OrientDBImporterIT {
  private final static String DATABASE_PATH = "target/databases/performance";

  @Test
  public void testImportOK() throws Exception {
    final File databaseDirectory = new File(DATABASE_PATH);

    try {
      final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

      final OrientDBImporter importer = new OrientDBImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -s -o").split(" "));
      importer.run().close();

      Assertions.assertFalse(importer.isError());
      Assertions.assertTrue(databaseDirectory.exists());

      try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
        try (final Database database = factory.open()) {
          final DocumentType personType = database.getSchema().getType("Person");
          Assertions.assertNotNull(personType);
          Assertions.assertEquals(Type.INTEGER, personType.getProperty("id").getType());
          Assertions.assertEquals(500, database.countType("Person", true));
          Assertions.assertEquals(Schema.INDEX_TYPE.LSM_TREE, database.getSchema().getIndexByName("Person[id]").getType());

          final DocumentType friendType = database.getSchema().getType("Friend");
          Assertions.assertNotNull(friendType);
          Assertions.assertEquals(Type.INTEGER, friendType.getProperty("id").getType());
          Assertions.assertEquals(10_000, database.countType("Friend", true));
          Assertions.assertEquals(Schema.INDEX_TYPE.LSM_TREE, database.getSchema().getIndexByName("Friend[id]").getType());

          final File securityFile = new File("./server-users.jsonl");
          Assertions.assertTrue(securityFile.exists());

          final String fileContent = FileUtils.readFileAsString(securityFile);
          final JSONObject security = new JSONObject(fileContent.substring(0, fileContent.indexOf("\n")));
          Assertions.assertEquals("admin", security.getString("name"));
        }
      }
    } finally {
      TestHelper.checkActiveDatabases();
      FileUtils.deleteRecursively(databaseDirectory);
    }
  }

  @Test
  public void testImportNoFile() throws Exception {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");
    final OrientDBImporter importer = new OrientDBImporter(("-i " + inputFile.getFile() + "2 -d " + DATABASE_PATH + " -s -o").split(" "));
    try {
      importer.run();
      Assertions.fail("Expected File Not Found Exception");
    } catch (final IllegalArgumentException e) {
    }
    Assertions.assertTrue(importer.isError());
  }
}
