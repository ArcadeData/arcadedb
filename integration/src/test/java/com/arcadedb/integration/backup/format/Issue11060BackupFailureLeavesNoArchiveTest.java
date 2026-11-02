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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #11060: the full backup deleted its partial archive in two catch arms, {@code PageSnapshotException} and
 * {@code Exception}, so an {@code Error} - an {@code OutOfMemoryError} on a large archive is the realistic one - left the
 * claimed, partially written file on disk, and a delete the filesystem refused was silent. Found on the way, and worse: on
 * the frozen-files path the backup runs inside {@code PageManager.suspendFlushAndExecute}, whose
 * {@code executeIgnoringExceptions} swallows an {@code Error} too, and the callback recorded only an {@code Exception} as the
 * failure, so the backup went on to write the central directory of a truncated archive and reported success.
 * <p>
 * The failure is injected through the sealed-store seam of #8738, which both paths run through, mid-archive.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11060BackupFailureLeavesNoArchiveTest {
  private static final String DATABASE_PATH = "target/databases/issue11060-backup-failure";
  private static final String BACKUP_DIR    = "target/issue11060-backups";
  private static final String BACKUP_FILE   = BACKUP_DIR + "/backup-11060.zip";
  private static final String TYPE          = "Reading11060";
  private static final int    SAMPLES       = 2_000;
  private static final long   BASE_TS       = 1_700_000_000_000L;

  @BeforeEach
  @AfterEach
  void clean() {
    FullBackupFormat.beforeSealedStoreReadForTesting = null;
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    new File(BACKUP_DIR).setWritable(true);
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(BACKUP_DIR));
  }

  /** The reported shape: an {@code Error} mid-archive leaves no archive behind, on both paths, and reaches the caller as such. */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void anErrorMidArchiveFailsTheBackupAndLeavesNoArchive(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    try (final Database database = createDatabase()) {
      final AtomicBoolean thrown = new AtomicBoolean();
      FullBackupFormat.beforeSealedStoreReadForTesting = file -> {
        thrown.set(true);
        throw new OutOfMemoryError("simulated by issue #11060");
      };

      assertThatThrownBy(() -> new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase())
          .as("the frozen-files path must not swallow the Error and report a truncated archive as a backup")
          .isInstanceOf(OutOfMemoryError.class)
          .hasMessageContaining("simulated by issue #11060");

      assertThat(thrown.get()).as("the fixture must have reached the sealed store, or this proves nothing").isTrue();
      assertThat(new File(BACKUP_FILE)).as("a failed backup must not leave a partial archive behind").doesNotExist();

      // THE DATABASE IS STILL BACKED UP CLEANLY AFTERWARDS: THE FAILED ATTEMPT RELEASED THE FLUSH SUSPENSION AND THE PATH
      FullBackupFormat.beforeSealedStoreReadForTesting = null;
      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
      assertThat(new File(BACKUP_FILE)).exists();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * A partial archive the filesystem refuses to delete is reported, on the failure the caller sees, instead of being left
   * behind silently. The directory is made read-only from inside the backup, after the archive was opened, so the write
   * goes on and only the delete is refused.
   */
  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void aRefusedDeleteOfThePartialArchiveIsReported(final boolean snapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);
    final File backupDir = new File(BACKUP_DIR);

    try (final Database database = createDatabase()) {
      FullBackupFormat.beforeSealedStoreReadForTesting = file -> {
        backupDir.setWritable(false);
        throw new IllegalStateException("failure injected by issue #11060");
      };

      try {
        final Throwable failure = catchFailure(() -> new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase());
        // A SUPERUSER, OR A FILESYSTEM WITHOUT POSIX PERMISSIONS, DELETES FROM A READ-ONLY DIRECTORY ANYWAY
        assumeTrue(new File(BACKUP_FILE).exists(), "the filesystem did not refuse the delete");

        assertThat(failure).isInstanceOf(BackupException.class).hasStackTraceContaining("failure injected by issue #11060");
        assertThat(Arrays.stream(allSuppressed(failure)).map(Throwable::getMessage))
            .as("the archive left behind must be reported on the failure")
            .anyMatch(m -> m != null && m.contains("could not be deleted") && m.contains("backup-11060.zip"));
      } finally {
        backupDir.setWritable(true);
      }
    }
    TestHelper.checkActiveDatabases();
  }

  private static Throwable catchFailure(final Runnable runnable) {
    try {
      runnable.run();
    } catch (final Throwable t) {
      return t;
    }
    throw new AssertionError("the backup was expected to fail");
  }

  /** The suppressed exceptions of the failure and of its causes: the wrapper the caller sees may carry them one level down. */
  private static Throwable[] allSuppressed(final Throwable failure) {
    Throwable[] all = new Throwable[0];
    for (Throwable t = failure; t != null; t = t.getCause()) {
      final Throwable[] suppressed = t.getSuppressed();
      final Throwable[] merged = Arrays.copyOf(all, all.length + suppressed.length);
      System.arraycopy(suppressed, 0, merged, all.length, suppressed.length);
      all = merged;
    }
    return all;
  }

  private Database createDatabase() throws Exception {
    Files.createDirectories(new File(BACKUP_DIR).toPath());
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    try {
      database.command("sql", "CREATE TIMESERIES TYPE " + TYPE
          + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
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
