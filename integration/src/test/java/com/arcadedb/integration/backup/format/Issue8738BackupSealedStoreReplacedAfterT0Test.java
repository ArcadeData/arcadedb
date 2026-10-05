/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.integration.backup.format;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.backup.Backup;
import com.arcadedb.integration.backup.BackupException;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8738, on the full backup: the backup lists the TimeSeries sealed stores with its page window (#7705) and reads
 * them later by PATH, the same shape as the HA verify and snapshot ship. A store whose path names a different file by
 * the time it is read - a type dropped and recreated under the same name, or a file replaced by hand - must fail the
 * backup instead of being archived beside a schema and page image it does not belong to.
 */
class Issue8738BackupSealedStoreReplacedAfterT0Test {
  private static final String DATABASE_PATH = "target/databases/backup-sealed-replaced-8738";
  private static final String BACKUP_FILE   = "target/backup-sealed-replaced-8738.zip";
  private static final String TYPE          = "Reading";
  private static final String TS_DDL        =
      "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1";
  private static final int    SAMPLES       = 5_000;
  private static final long   BASE_TS       = 1_700_000_000_000L;
  /** Budget for something expected to happen. Generous: a wider bound cannot turn a passing run red. */
  private static final long   WAIT_MS       = 30_000L;

  @BeforeEach
  @AfterEach
  void clean() {
    FullBackupFormat.beforeSealedStoreReadForTesting = null;
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    new File(BACKUP_FILE).delete();
  }

  /**
   * The identity, not the size or the content, is what is compared: a byte-identical copy with the same size and
   * last-modified time renamed over the listed path is a different file, and fails the backup on both paths.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aSealedStoreReplacedAfterTheListingFailsTheBackup(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    try (final Database database = createDatabase()) {
      final AtomicBoolean replaced = new AtomicBoolean();
      FullBackupFormat.beforeSealedStoreReadForTesting = file -> {
        try {
          replaceWithIdenticalCopy(file);
          replaced.set(true);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      };

      assertThatThrownBy(() -> new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase())
          .isInstanceOf(BackupException.class)
          .hasStackTraceContaining(TYPE + "_shard_0.ts.sealed")
          .hasStackTraceContaining("changed after being listed");

      assertThat(replaced.get()).as("the fixture must actually have replaced the store").isTrue();
      assertThat(new File(BACKUP_FILE)).as("a failed backup must not leave a partial archive behind").doesNotExist();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The reported shape: the type is dropped and recreated under the same name after the window path's t0 (the
   * fallback holds the read lock over the whole backup, so no DDL can land there). The recreated store must not be
   * archived as the t0 one.
   */
  @Test
  void aTypeDroppedAndRecreatedAfterT0FailsTheBackup() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    try (final Database database = createDatabase()) {
      final AtomicReference<Throwable> ddlFailure = new AtomicReference<>();
      final AtomicBoolean ddlCompleted = new AtomicBoolean();
      final AtomicBoolean recreatedUnderTheListedName = new AtomicBoolean();
      FullBackupFormat.beforeSealedStoreReadForTesting = file -> {
        if (ddlCompleted.get())
          return;
        // ON ANOTHER THREAD, SO A REGRESSION THAT HOLDS THE READ LOCK HERE FAILS THIS TEST INSTEAD OF HANGING IT
        final Thread ddl = new Thread(() -> {
          try {
            database.command("sql", "DROP TYPE " + TYPE);
            database.command("sql", TS_DDL);
            recreatedUnderTheListedName.set(file.exists());
          } catch (final Throwable t) {
            ddlFailure.compareAndSet(null, t);
          }
        }, "issue8738-backup-drop-recreate");
        ddl.setDaemon(true);
        ddl.start();
        try {
          ddl.join(WAIT_MS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        ddlCompleted.set(!ddl.isAlive());
      };

      assertThatThrownBy(() -> new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase())
          .isInstanceOf(BackupException.class)
          .hasStackTraceContaining(TYPE + "_shard_0.ts.sealed")
          .hasStackTraceContaining("changed after being listed");

      assertThat(ddlFailure.get()).isNull();
      assertThat(ddlCompleted.get()).as("the drop and recreate must have run inside the backup").isTrue();
      assertThat(recreatedUnderTheListedName.get())
          .as("the recreated type must own a store under the listed name, or this proves nothing").isTrue();
      assertThat(new File(BACKUP_FILE)).doesNotExist();
    }
    TestHelper.checkActiveDatabases();
  }

  /** The counterweight: with nothing replaced, the identity check passes and the backup completes on both paths. */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void anUndisturbedBackupStillCompletes(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    try (final Database database = createDatabase()) {
      final AtomicBoolean sawSealedStore = new AtomicBoolean();
      FullBackupFormat.beforeSealedStoreReadForTesting = file -> sawSealedStore.set(true);

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();

      assertThat(sawSealedStore.get()).as("the backup must have archived a sealed store, or this proves nothing").isTrue();
      assertThat(new File(BACKUP_FILE)).exists();
    }
    TestHelper.checkActiveDatabases();
  }

  // --- helpers ---

  private static void replaceWithIdenticalCopy(final File file) throws IOException {
    final FileTime lastModified = Files.getLastModifiedTime(file.toPath());
    final File copy = new File(file.getParentFile(), file.getName() + ".copy-8738");
    Files.copy(file.toPath(), copy.toPath(), StandardCopyOption.REPLACE_EXISTING);
    Files.setLastModifiedTime(copy.toPath(), lastModified);
    Files.move(copy.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
  }

  private Database createDatabase() throws Exception {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    try {
      database.command("sql", TS_DDL);
      final long[] timestamps = new long[SAMPLES];
      final Object[] hosts = new Object[SAMPLES];
      final Object[] values = new Object[SAMPLES];
      for (int i = 0; i < SAMPLES; i++) {
        timestamps[i] = BASE_TS + i * 1_000L;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
      }
      final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType(TYPE)).getEngine();
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      engine.compactAll();
      return database;
    } catch (final Throwable t) {
      database.close();
      throw t;
    }
  }
}
