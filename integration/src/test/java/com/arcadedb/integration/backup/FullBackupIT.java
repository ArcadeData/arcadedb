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
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.ConsoleLogger;
import com.arcadedb.integration.importer.OrientDBImporter;
import com.arcadedb.integration.importer.OrientDBImporterIT;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class FullBackupIT {
  private final static String DATABASE_PATH     = "target/databases/performance";
  private final static String FILE              = "target/arcadedb-backup.zip";
  private final        File   restoredDirectory = new File(DATABASE_PATH + "_restored");
  private final        File   file              = new File(FILE);

  @Test
  public void testFullBackupCommandLineOK() throws Exception {
    final Database importedDatabase = importDatabase();
    importedDatabase.close();

    new Backup(("-f " + FILE + " -d " + DATABASE_PATH + " -o").split(" ")).backupDatabase();

    assertThat(file.exists()).isTrue();
    assertThat(file.length() > 0).isTrue();

    new Restore(("-f " + FILE + " -d " + restoredDirectory + " -o").split(" ")).restoreDatabase();

    try (final Database originalDatabase = new DatabaseFactory(DATABASE_PATH).open(ComponentFile.MODE.READ_ONLY)) {
      try (final Database restoredDatabase = new DatabaseFactory(restoredDirectory.getAbsolutePath()).open(
          ComponentFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(originalDatabase, restoredDatabase);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  public void testFullBackupAPIOK() throws Exception {
    try (final Database importedDatabase = importDatabase()) {

      new Backup(importedDatabase, FILE).backupDatabase();

      assertThat(file.exists()).isTrue();
      assertThat(file.length() > 0).isTrue();

      new Restore(FILE, restoredDirectory.getAbsolutePath()).restoreDatabase();

      try (final Database restoredDatabase = new DatabaseFactory(restoredDirectory.getAbsolutePath()).open(
          ComponentFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(importedDatabase, restoredDatabase);
      }
    }

    TestHelper.checkActiveDatabases();
  }

  /**
   * This test allocates "number of processors-1" parallel threads which insert 500 transactions each with 500 vertices per transaction. Vertices are indexed on thread+id properties.
   * A not unique index has been selected to speed up the insertion avoiding concurrency on buckets/indexes.
   * During the parallel insertion, 8 full backups are scheduled with 1 second pause between each other. When the insertion is completed (2M vertices in total),
   * each backup file is restored and tested the number of vertices is mod (%) 500, so no inconsistent backup has been taken (each transaction is 500 vertices).
   */
  @Test
  public void testFullBackupConcurrency() throws Exception {
    final int concurrentThreads = Math.min(Runtime.getRuntime().availableProcessors() - 1, 4);

    final ConsoleLogger logger = new ConsoleLogger(1);
    logger.logLine(1, "Starting test with %d threads", concurrentThreads);
    for (int i = 0; i < concurrentThreads; i++) {
      new File(FILE + "_" + i).delete();
      FileUtils.deleteRecursively(new File(DATABASE_PATH + "_restored_" + i));
    }

    final Thread[] threads = new Thread[concurrentThreads];

    try (Database importedDatabase = importDatabase()) {

      final VertexType type = importedDatabase.getSchema().buildVertexType().withName("BackupTest")
          .withTotalBuckets(concurrentThreads).create();

      importedDatabase.transaction(() -> {
        type.createProperty("thread", Type.INTEGER);
        type.createProperty("id", Type.INTEGER);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "thread", "id");
        type.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy() {
          @Override
          public int getBucketIdByRecord(final Document record, final boolean async) {
            return record.getInteger("thread");
          }
        });
      });

      for (int i = 0; i < concurrentThreads; i++) {
        final int threadId = i;
        final Bucket threadBucket = type.getBuckets(false).get(i);

        threads[i] = new Thread("Inserter-" + i) {
          public void run() {

            logger.logLine(1, "Inserter-%d started", threadId);
            final AtomicInteger totalPerThread = new AtomicInteger();
            for (int j = 0; j < 500; j++) {
              importedDatabase.begin();
              for (int k = 0; k < 500; k++) {
                final MutableVertex v = importedDatabase.newVertex("BackupTest")
                    .set("thread", threadId)
                    .set("id", totalPerThread.getAndIncrement())
                    .save();
                assertThat(v.getIdentity().getBucketId()).isEqualTo(threadBucket.getFileId());
              }
              importedDatabase.commit();
            }
            logger.logLine(1, "Inserter-%d completed - total-> %d", threadId, totalPerThread.get());
          }

        };
      }

      // START THREADS 300MS DISTANCE FROM EACH OTHER
      for (int i = 0; i < concurrentThreads; i++) {
        threads[i].start();
        Thread.sleep(1000);

        Backup backup = new Backup(importedDatabase, FILE + "_" + i).setVerboseLevel(1);
        backup.backupDatabase();
      }

      for (int i = 0; i < concurrentThreads; i++)
        threads[i].join();

      logger.logLine(1, "All threads completed - checking...");
      for (int i = 0; i < concurrentThreads; i++) {
        final File file = new File(FILE + "_" + i);
        logger.logLine(1, "Checking backup file %s", file.getAbsolutePath());
        assertThat(file.exists()).isTrue();
        assertThat(file.length() > 0).isTrue();

        final String databasePath = DATABASE_PATH + "_restored_" + i;

        new Restore(FILE + "_" + i, databasePath).setVerboseLevel(1).restoreDatabase();
        logger.logLine(1, "Checking restored database %s", databasePath);
        try (final Database restoredDatabase = new DatabaseFactory(databasePath).open(ComponentFile.MODE.READ_ONLY)) {
          // VERIFY ONLY WHOLE TRANSACTION ARE WRITTEN
          assertThat(restoredDatabase.countType("BackupTest", true) % 500).isEqualTo(0);
        }
      }

    } finally {
      TestHelper.checkActiveDatabases();

      for (int i = 0; i < concurrentThreads; i++) {
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
      fail("");
    } catch (final BackupException e) {
      // EXPECTED
    }
  }

  @Test
  public void testFileCannotBeOverwrittenError() throws IOException {
    try {
      emptyDatabase().close();
      new File(FILE).createNewFile();
      new Backup(("-f " + FILE + " -d " + DATABASE_PATH).split(" ")).backupDatabase();
      fail("");
    } catch (final BackupException e) {
      // EXPECTED
    }
  }

  private Database importDatabase() throws Exception {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
    final Database importedDatabase = importer.run();

    assertThat(importer.isError()).isFalse();
    assertThat(new File(DATABASE_PATH).exists()).isTrue();
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
