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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.importer.OrientDBImporter;
import com.arcadedb.integration.importer.OrientDBImporterIT;
import com.arcadedb.utility.FileUtils;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URL;
import java.util.zip.GZIPInputStream;

public class JsonlExporterIT {
  private final static String DATABASE_PATH = "target/databases/performance";
  private final static String FILE          = "target/arcadedb-export.jsonl.tgz";

  @Test
  public void testExportOK() throws IOException {
    final File databaseDirectory = new File(DATABASE_PATH);

    File file = new File(FILE);

    try {
      final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

      final OrientDBImporter importer = new OrientDBImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
      importer.run().close();

      Assertions.assertFalse(importer.isError());
      Assertions.assertTrue(databaseDirectory.exists());

      new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

      Assertions.assertTrue(file.exists());
      Assertions.assertTrue(file.length() > 0);

      int lines = 0;
      try (BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {
        while (in.ready()) {
          final String line = in.readLine();
          new JSONObject(line);
          ++lines;
        }
      }

      Assertions.assertTrue(lines > 10);

    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
      file.delete();
    }
  }

  @Test
  public void testFormatError() {
    try {
      emptyDatabase().close();
      new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format unknown").split(" ")).exportDatabase();
      Assertions.fail();
    } catch (ExportException e) {
      // EXPECTED
    }
  }

  @Test
  public void testFileCannotBeOverwrittenError() throws IOException {
    try {
      emptyDatabase().close();
      new File(FILE).createNewFile();
      new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -format jsonl").split(" ")).exportDatabase();
      Assertions.fail();
    } catch (ExportException e) {
      // EXPECTED
    }
  }

  private Database emptyDatabase() {
    return new DatabaseFactory(DATABASE_PATH).create();
  }

  @BeforeEach
  @AfterEach
  public void beforeTests() {
    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }
}
