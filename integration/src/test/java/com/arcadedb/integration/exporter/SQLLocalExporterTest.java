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
import com.arcadedb.engine.Bucket;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.OrientDBImporterIT;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;

public class SQLLocalExporterTest {
  @Test
  public void importAndExportDatabase() {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    FileUtils.deleteRecursively(new File("databases/importedFromOrientDB"));

    try (final Database database = new DatabaseFactory("databases/importedFromOrientDB").create()) {
      database.getConfiguration().setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, Bucket.DEF_PAGE_SIZE * 10);

      database.command("sql", "import database file://" + inputFile.getFile());

      Assertions.assertEquals(500, database.countType("Person", false));
      Assertions.assertEquals(10000, database.countType("Friend", false));

      database.command("sql", "export database file://export.jsonl.tgz");

      final File exportFile = new File("./exports/export.jsonl.tgz");
      Assertions.assertTrue(exportFile.exists());
      Assertions.assertTrue(exportFile.length() > 50_000);
      exportFile.delete();
    }

    TestHelper.checkActiveDatabases();

    FileUtils.deleteRecursively(new File("databases/importedFromOrientDB"));
  }
}
