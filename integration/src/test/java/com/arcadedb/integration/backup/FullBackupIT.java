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

package com.arcadedb.integration.backup;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.integration.importer.OrientDBImporter;
import com.arcadedb.integration.importer.OrientDBImporterIT;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.*;

public class FullBackupIT {
  private final static String DATABASE_PATH = "target/databases/performance";
  private final static String FILE          = "target/arcadedb-backup.zip";

  @Test
  public void testFullBackupCommandLineOK() throws IOException {
    final File databaseDirectory = new File(DATABASE_PATH);
    final File restoredDirectory = new File(DATABASE_PATH + "_restored");
    final File file = new File(FILE);

    try {
      final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

      final OrientDBImporter importer = new OrientDBImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
      final Database importedDatabase = importer.run();
      importedDatabase.close();

      Assertions.assertFalse(importer.isError());
      Assertions.assertTrue(databaseDirectory.exists());

      new Backup(("-f " + FILE + " -d " + DATABASE_PATH + " -o").split(" ")).backupDatabase();

      Assertions.assertTrue(file.exists());
      Assertions.assertTrue(file.length() > 0);

      new Restore(("-f " + FILE + " -d " + restoredDirectory + " -o").split(" ")).restoreDatabase();

      try (Database originalDatabase = new DatabaseFactory(DATABASE_PATH).open(PaginatedFile.MODE.READ_ONLY)) {
        try (Database restoredDatabase = new DatabaseFactory(restoredDirectory.getAbsolutePath()).open(PaginatedFile.MODE.READ_ONLY)) {
          new DatabaseComparator().compare(originalDatabase, restoredDatabase);
        }
      }
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
      FileUtils.deleteRecursively(restoredDirectory);
      file.delete();
    }
  }

  @Test
  public void testFullBackupAPIOK() throws IOException {
    final File databaseDirectory = new File(DATABASE_PATH);
    FileUtils.deleteRecursively(databaseDirectory);

    final File restoredDirectory = new File(DATABASE_PATH + "_restored");
    FileUtils.deleteRecursively(restoredDirectory);

    final File file = new File(FILE);
    file.delete();

    try {
      final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

      final OrientDBImporter importer = new OrientDBImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
      final Database importedDatabase = importer.run();

      Assertions.assertFalse(importer.isError());
      Assertions.assertTrue(databaseDirectory.exists());

      new Backup(importedDatabase, FILE).backupDatabase();

      Assertions.assertTrue(file.exists());
      Assertions.assertTrue(file.length() > 0);

      new Restore(FILE, restoredDirectory.getAbsolutePath()).restoreDatabase();

      try (Database restoredDatabase = new DatabaseFactory(restoredDirectory.getAbsolutePath()).open(PaginatedFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(importedDatabase, restoredDatabase);
      }
    } finally {
      FileUtils.deleteRecursively(databaseDirectory);
      FileUtils.deleteRecursively(restoredDirectory);
      file.delete();
    }
  }
}
