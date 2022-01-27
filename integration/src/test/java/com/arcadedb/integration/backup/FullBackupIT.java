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
package com.arcadedb.integration.backup;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.integration.importer.OrientDBImporter;
import com.arcadedb.integration.importer.OrientDBImporterIT;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

public class FullBackupIT {
  private final static String DATABASE_PATH     = "target/databases/performance";
  private final static String FILE              = "target/arcadedb-backup.zip";
  private final        File   databaseDirectory = new File(DATABASE_PATH);
  private final        File   restoredDirectory = new File(DATABASE_PATH + "_restored");
  private final        File   file              = new File(FILE);

  @Test
  public void testFullBackupCommandLineOK() throws IOException {
    final Database importedDatabase = importDatabase();
    importedDatabase.close();

    new Backup(("-f " + FILE + " -d " + DATABASE_PATH + " -o").split(" ")).backupDatabase();

    Assertions.assertTrue(file.exists());
    Assertions.assertTrue(file.length() > 0);

    new Restore(("-f " + FILE + " -d " + restoredDirectory + " -o").split(" ")).restoreDatabase();

    try (Database originalDatabase = new DatabaseFactory(DATABASE_PATH).open(PaginatedFile.MODE.READ_ONLY)) {
      try (Database restoredDatabase = new DatabaseFactory(restoredDirectory.getAbsolutePath()).open(PaginatedFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(originalDatabase, restoredDatabase);
      }
    }
    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
  }

  @Test
  public void testFullBackupAPIOK() throws IOException {
    try (final Database importedDatabase = importDatabase()) {

      new Backup(importedDatabase, FILE).backupDatabase();

      Assertions.assertTrue(file.exists());
      Assertions.assertTrue(file.length() > 0);

      new Restore(FILE, restoredDirectory.getAbsolutePath()).restoreDatabase();

      try (Database restoredDatabase = new DatabaseFactory(restoredDirectory.getAbsolutePath()).open(PaginatedFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(importedDatabase, restoredDatabase);
      }
    }

    Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());
  }

  /**
   * This test allocates 8 parallel threads which insert 500 transactions each with 500 vertices per transaction. Vertices are indexed on thread+id properties.
   * A not unique index has been selected to speed up the insertion avoiding concurrency on buckets/indexes.
   * During the parallel insertion, 8 full backups are scheduled with 1 second pause between each other. When the insertion is completed (2M vertices in total),
   * each backup file is restored and tested the number of vertices is mod (%) 500, so no inconsistent backup has been taken (each transaction is 500 vertices).
   */
  @Test
  public void testFullBackupConcurrency() throws IOException, InterruptedException {
    final int CONCURRENT_THREADS = 8;

    for (int i = 0; i < CONCURRENT_THREADS; i++) {
      new File(FILE + "_" + i).delete();
      FileUtils.deleteRecursively(new File(DATABASE_PATH + "_restored_" + i));
    }

    final Thread[] threads = new Thread[CONCURRENT_THREADS];

    final Database importedDatabase = importDatabase();
    try {

      final VertexType type = importedDatabase.getSchema().createVertexType("BackupTest", CONCURRENT_THREADS);

      importedDatabase.transaction(() -> {
        type.createProperty("thread", Type.INTEGER);
        type.createProperty("id", Type.INTEGER);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "thread", "id");
        type.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy() {
          @Override
          public int getBucketIdByRecord(Document record, boolean async) {
            return record.getInteger("thread");
          }
        });
      });

      for (int i = 0; i < CONCURRENT_THREADS; i++) {
        final int threadId = i;
        final Bucket threadBucket = type.getBuckets(false).get(i);

        threads[i] = new Thread("Inserter-" + i) {
          public void run() {
            final AtomicInteger totalPerThread = new AtomicInteger();
            for (int j = 0; j < 500; j++) {
              importedDatabase.begin();
              for (int k = 0; k < 500; k++) {
                MutableVertex v = importedDatabase.newVertex("BackupTest").set("thread", threadId).set("id", totalPerThread.getAndIncrement()).save();
                Assertions.assertEquals(threadBucket.getId(), v.getIdentity().getBucketId());

                if (k + 1 % 100 == 0) {
                  importedDatabase.commit();
                  importedDatabase.begin();
                }
              }
              importedDatabase.commit();
            }

          }
        };
      }

      // START THREADS 300MS DISTANCE FROM EACH OTHER
      for (int i = 0; i < CONCURRENT_THREADS; i++) {
        threads[i].start();
        Thread.sleep(300);
      }

      // EXECUTE 10 BACKUPS EVERY SECOND
      for (int i = 0; i < CONCURRENT_THREADS; i++) {
        Assertions.assertFalse(importedDatabase.isTransactionActive());
        final long totalVertices = importedDatabase.countType("BackupTest", true);
        new Backup(importedDatabase, FILE + "_" + i).setVerboseLevel(1).backupDatabase();
        Thread.sleep(1000);
      }

      for (int i = 0; i < CONCURRENT_THREADS; i++)
        threads[i].join();

      for (int i = 0; i < CONCURRENT_THREADS; i++) {
        final File file = new File(FILE + "_" + i);
        Assertions.assertTrue(file.exists());
        Assertions.assertTrue(file.length() > 0);

        final String databasePath = DATABASE_PATH + "_restored_" + i;

        new Restore(FILE + "_" + i, databasePath).setVerboseLevel(1).restoreDatabase();

        try (Database restoredDatabase = new DatabaseFactory(databasePath).open(PaginatedFile.MODE.READ_ONLY)) {
          // VERIFY ONLY WHOLE TRANSACTION ARE WRITTEN
          Assertions.assertTrue(restoredDatabase.countType("BackupTest", true) % 500 == 0);
        }
      }

    } finally {
      importedDatabase.close();

      Assertions.assertTrue(DatabaseFactory.getActiveDatabaseInstances().isEmpty(), "Found active databases: " + DatabaseFactory.getActiveDatabaseInstances());

      for (int i = 0; i < CONCURRENT_THREADS; i++) {
        new File(FILE + "_" + i).delete();
        FileUtils.deleteRecursively(new File(DATABASE_PATH + "_restored_" + i));
      }
    }
  }

  @Test
  public void testFormatError() {
    try {
      emptyDatabase().close();
      new Backup(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format unknown").split(" ")).backupDatabase();
      Assertions.fail();
    } catch (BackupException e) {
      // EXPECTED
    }
  }

  @Test
  public void testFileCannotBeOverwrittenError() throws IOException {
    try {
      emptyDatabase().close();
      new File(FILE).createNewFile();
      new Backup(("-f " + FILE + " -d " + DATABASE_PATH).split(" ")).backupDatabase();
      Assertions.fail();
    } catch (BackupException e) {
      // EXPECTED
    }
  }

  private Database importDatabase() throws IOException {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    final OrientDBImporter importer = new OrientDBImporter(("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
    final Database importedDatabase = importer.run();

    Assertions.assertFalse(importer.isError());
    Assertions.assertTrue(new File(DATABASE_PATH).exists());
    return importedDatabase;
  }

  private Database emptyDatabase() {
    return new DatabaseFactory(DATABASE_PATH).create();
  }

  @BeforeEach
  @AfterEach
  public void beforeTests() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(DATABASE_PATH + "_restored"));
    if (file.exists())
      file.delete();
  }
}
