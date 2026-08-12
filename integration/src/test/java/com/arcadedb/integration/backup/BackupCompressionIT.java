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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end coverage of the configurable and parallel backup compression introduced by issue #6072. The contract
 * being defended is that none of the new knobs changes what a backup means: whatever the compression level, the thread
 * count or the throttle, the archive restores to a database identical to the source, and it restores through the
 * unchanged restore path.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BackupCompressionIT {
  private static final String DATABASE_PATH  = "target/databases/backup-compression";
  private static final String RESTORED_PATH  = "target/databases/backup-compression-restored";
  private static final String BACKUP_FILE    = "target/backup-compression.zip";
  private static final int    RECORDS        = 20_000;

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * The full matrix that matters: the legacy single-threaded writer (threads 0), the parallel writer with one worker
   * and with several, at the extreme deflate levels as well as the new default.
   */
  @ParameterizedTest
  @CsvSource({ "0,9", "0,1", "1,1", "4,0", "4,1", "4,6", "4,9", "8,1" })
  void backupAndRestoreRoundTrip(final int threads, final int level) throws Exception {
    try (final Database database = createDatabase()) {
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).setCompressionThreads(threads).setCompressionLevel(level)
          .backupDatabase();

      assertThat(new File(BACKUP_FILE)).exists();
      assertArchiveIsReadableByStandardTools();

      new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

      try (final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(database, restored);
        assertThat(restored.countType("Doc", true)).isEqualTo(RECORDS);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  @ParameterizedTest
  @CsvSource({ "0,9", "4,1" })
  void encryptedBackupAndRestoreRoundTrip(final int threads, final int level) throws Exception {
    final String key = "AnotherTestWithHard2GuessPassword";

    try (final Database database = createDatabase()) {
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).setEncryptionKey(key).setCompressionThreads(threads)
          .setCompressionLevel(level).backupDatabase();

      new Restore(("-f " + BACKUP_FILE + " -d " + RESTORED_PATH + " -o -encryptionKey " + key).split(" "))
          .setVerboseLevel(0).restoreDatabase();

      try (final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
        new DatabaseComparator().compare(database, restored);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * An archive produced by the parallel writer has to restore through the very same code path as one produced by the
   * legacy writer - that is the "no archive format change" requirement, and comparing the restored databases is the
   * only way to prove it rather than assume it.
   */
  @Test
  void parallelAndLegacyArchivesRestoreIdentically() throws Exception {
    final String legacyFile = BACKUP_FILE + ".legacy";
    final String legacyRestore = RESTORED_PATH + "_legacy";
    try {
      try (final Database database = createDatabase()) {
        new Backup(database, legacyFile).setVerboseLevel(0).setCompressionThreads(0).setCompressionLevel(9)
            .backupDatabase();
        new Backup(database, BACKUP_FILE).setVerboseLevel(0).setCompressionThreads(8).setCompressionLevel(1)
            .backupDatabase();
      }

      new Restore(legacyFile, legacyRestore).setVerboseLevel(0).restoreDatabase();
      new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

      try (final Database legacy = new DatabaseFactory(legacyRestore).open(ComponentFile.MODE.READ_ONLY)) {
        try (final Database parallel = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
          new DatabaseComparator().compare(legacy, parallel);
        }
      }
    } finally {
      new File(legacyFile).delete();
      FileUtils.deleteRecursively(new File(legacyRestore));
      TestHelper.checkActiveDatabases();
    }
  }

  @Test
  void sqlSettingsAreApplied() throws Exception {
    final Object oldDirectory = GlobalConfiguration.SERVER_BACKUP_DIRECTORY.getValue();
    final String sqlBackupRoot = "target/backup-compression-sql";
    FileUtils.deleteRecursively(new File(sqlBackupRoot));
    try {
      GlobalConfiguration.SERVER_BACKUP_DIRECTORY.setValue(sqlBackupRoot);

      try (final Database database = createDatabase()) {
        database.command("sql", "backup database file://sql-backup.zip"
            + " with compressionLevel = 9, compressionThreads = 2, maxMBPerSecond = 0");

        final String archive = sqlBackupRoot + File.separator + database.getName() + File.separator + "sql-backup.zip";
        assertThat(new File(archive)).exists();

        new Restore(archive, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

        try (final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
          new DatabaseComparator().compare(database, restored);
        }
      }
    } finally {
      GlobalConfiguration.SERVER_BACKUP_DIRECTORY.setValue(oldDirectory);
      FileUtils.deleteRecursively(new File(sqlBackupRoot));
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * Regression: every {@code WITH ...} setting of {@code BACKUP DATABASE} was dropped, because the statement matched
   * the setting name against {@code Expression.toString()}, which renders a string quoted. The visible consequence was
   * that {@code WITH encryptionKey = '...'} produced an archive in clear - it restored without the key.
   */
  @Test
  void sqlEncryptionKeyIsActuallyApplied() throws Exception {
    final Object oldDirectory = GlobalConfiguration.SERVER_BACKUP_DIRECTORY.getValue();
    final String sqlBackupRoot = "target/backup-compression-encrypted-sql";
    FileUtils.deleteRecursively(new File(sqlBackupRoot));
    try {
      GlobalConfiguration.SERVER_BACKUP_DIRECTORY.setValue(sqlBackupRoot);

      final String archive;
      try (final Database database = createDatabase()) {
        database.command("sql", "backup database file://encrypted.zip with encryptionKey = 'SuperSecretKey'");
        archive = sqlBackupRoot + File.separator + database.getName() + File.separator + "encrypted.zip";
        assertThat(new File(archive)).exists();

        assertThatThrownBy(() -> new Restore(archive, RESTORED_PATH).setVerboseLevel(0).restoreDatabase())
            .as("an encrypted archive must not restore without the key").isInstanceOf(Exception.class);

        FileUtils.deleteRecursively(new File(RESTORED_PATH));
        new Restore(("-f " + archive + " -d " + RESTORED_PATH + " -o -encryptionKey SuperSecretKey").split(" "))
            .setVerboseLevel(0).restoreDatabase();

        try (final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
          new DatabaseComparator().compare(database, restored);
        }
      }
    } finally {
      GlobalConfiguration.SERVER_BACKUP_DIRECTORY.setValue(oldDirectory);
      FileUtils.deleteRecursively(new File(sqlBackupRoot));
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * Regression: a backup that failed halfway used to report success. {@code PageManager.suspendFlushAndExecute} runs
   * its callback through {@code CodeUtils.executeIgnoringExceptions}, so the archive was finalized with a valid
   * central directory over a truncated set of entries - a backup that looks valid and is not.
   * <p>
   * The failure is injected on the SCHEMA configuration file, which both backup paths read straight off the
   * filesystem. Since #6075 the page files are read through the snapshot's already-open channels, so revoking their
   * permission mid-run no longer breaks a backup at all - the coverage that matters here is the archive lifecycle,
   * and it is exercised on the snapshot path and the suspend-and-freeze fallback alike.
   */
  @ParameterizedTest
  @CsvSource({ "0,true", "4,true", "0,false", "4,false" })
  void aFailedBackupThrowsAndLeavesNoArchive(final int threads, final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);
    try (final Database database = createDatabase()) {
      final File unreadable = ((LocalSchema) database.getSchema()).getConfigurationFile();

      // POSIX ONLY: WHERE THE PERMISSION CANNOT BE TAKEN AWAY (WINDOWS, RUNNING AS ROOT) THERE IS NO WAY TO MAKE A
      // FILE FAIL TO OPEN, SO THE SCENARIO IS NOT REPRODUCIBLE AND THE TEST SKIPS RATHER THAN PASSING VACUOUSLY
      assumeTrue(unreadable.setReadable(false) && !unreadable.canRead(),
          "cannot make a database file unreadable on this platform");
      try {
        assertThatThrownBy(() -> new Backup(database, BACKUP_FILE).setVerboseLevel(0).setCompressionThreads(threads)
            .backupDatabase()).isInstanceOf(BackupException.class);

        assertThat(new File(BACKUP_FILE)).as("a partial archive must not survive a failed backup").doesNotExist();
      } finally {
        unreadable.setReadable(true);
      }
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * A setting name that is not recognised must be refused, not ignored. Silently dropping it is the same failure mode
   * as the {@code Expression.toString()} bug: {@code WITH encryptionkey = '...'} - one wrong character - would look
   * like a request for an encrypted archive and produce a cleartext one.
   */
  @Test
  void sqlRejectsAnUnknownSetting() {
    try (final Database database = createDatabase()) {
      assertThatThrownBy(
          () -> database.command("sql", "backup database file://typo.zip with encryptionkey = 'SuperSecretKey'"))
          .hasMessageContaining("encryptionkey");
      assertThat(new File("typo.zip")).doesNotExist();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The out-of-range case takes a different route from the non-numeric one: it parses cleanly in the statement and is
   * only refused further in, inside {@code Backup.setCompressionLevel}, which reaches the caller wrapped in an
   * {@code InvocationTargetException} from the reflective boundary. Worth its own test precisely because the two look
   * like the same check and are not.
   */
  @Test
  void sqlRejectsAnOutOfRangeSetting() {
    try (final Database database = createDatabase()) {
      assertThatThrownBy(() -> database.command("sql", "backup database file://range.zip with compressionLevel = 20"))
          .hasMessageContaining("compressionLevel");
      assertThatThrownBy(() -> database.command("sql", "backup database file://range.zip with compressionThreads = -2"))
          .hasMessageContaining("compressionThreads");
      assertThat(new File("range.zip")).doesNotExist();
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void sqlRejectsANonNumericSetting() {
    try (final Database database = createDatabase()) {
      assertThatThrownBy(() -> database.command("sql", "backup database file://bad.zip with compressionLevel = 'high'"))
          .hasMessageContaining("compressionLevel");
    }
    TestHelper.checkActiveDatabases();
  }

  @Test
  void globalConfigurationDefaultsAreHonoured() throws Exception {
    final Object oldLevel = GlobalConfiguration.BACKUP_COMPRESSION_LEVEL.getValue();
    final Object oldThreads = GlobalConfiguration.BACKUP_COMPRESSION_THREADS.getValue();
    try {
      GlobalConfiguration.BACKUP_COMPRESSION_LEVEL.setValue(9);
      GlobalConfiguration.BACKUP_COMPRESSION_THREADS.setValue(3);

      try (final Database database = createDatabase()) {
        new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();

        new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

        try (final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
          new DatabaseComparator().compare(database, restored);
        }
      }
    } finally {
      GlobalConfiguration.BACKUP_COMPRESSION_LEVEL.setValue(oldLevel);
      GlobalConfiguration.BACKUP_COMPRESSION_THREADS.setValue(oldThreads);
      TestHelper.checkActiveDatabases();
    }
  }

  /**
   * A backup taken while transactions are committing must restore to a consistent database. The parallel writer moves
   * the compression off the reader thread but the read itself still happens inside the flush suspension, so this is
   * the regression that would catch a chunking bug that only shows up under a moving source.
   */
  @Test
  void backupUnderConcurrentWriteLoadRestoresConsistently() throws Exception {
    final AtomicBoolean stop = new AtomicBoolean(false);

    try (final Database database = createDatabase()) {
      final Thread writer = new Thread(() -> {
        int id = RECORDS;
        while (!stop.get()) {
          final int base = id;
          database.transaction(() -> {
            for (int i = 0; i < 100; i++)
              database.newVertex("Doc").set("id", base + i).set("payload", payload(base + i)).save();
          });
          id += 100;
        }
      }, "BackupCompressionIT-writer");
      writer.start();

      try {
        Thread.sleep(500);
        new Backup(database, BACKUP_FILE).setVerboseLevel(0).setCompressionThreads(8).setCompressionLevel(1)
            .backupDatabase();
      } finally {
        stop.set(true);
        writer.join();
      }

      new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();

      try (final Database restored = new DatabaseFactory(RESTORED_PATH).open(ComponentFile.MODE.READ_ONLY)) {
        // EVERY COMMITTED BATCH IS 100 RECORDS, SO A TORN BACKUP WOULD SHOW UP AS A COUNT THAT IS NOT A WHOLE NUMBER
        // OF BATCHES ON TOP OF THE INITIAL LOAD
        final long count = restored.countType("Doc", true);
        assertThat(count).isGreaterThanOrEqualTo(RECORDS);
        assertThat((count - RECORDS) % 100).isEqualTo(0);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static void assertArchiveIsReadableByStandardTools() throws Exception {
    try (final ZipFile zip = new ZipFile(new File(BACKUP_FILE))) {
      int entries = 0;
      final Enumeration<? extends ZipEntry> enumeration = zip.entries();
      while (enumeration.hasMoreElements()) {
        final ZipEntry entry = enumeration.nextElement();
        final long read = zip.getInputStream(entry).readAllBytes().length;
        assertThat(read).as(entry.getName()).isEqualTo(entry.getSize());
        ++entries;
      }
      assertThat(entries).isGreaterThan(2);
    }
  }

  private static Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("payload", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    });

    database.begin();
    for (int i = 0; i < RECORDS; i++) {
      database.newVertex("Doc").set("id", i).set("payload", payload(i)).save();
      if (i % 1000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();
    return database;
  }

  private static String payload(final int id) {
    return "record-" + id + "-" + "x".repeat(200);
  }
}
