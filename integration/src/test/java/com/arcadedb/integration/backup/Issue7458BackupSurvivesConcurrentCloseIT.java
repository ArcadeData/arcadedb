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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.PageManager;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7458, end to end: a database close requested while a snapshot backup is streaming waits for the backup,
 * the backup completes and restores, and the close completes right after. The engine-level property (the close
 * waits for the window and refuses new ones) is covered by {@code Issue7458CloseWaitsForSnapshotWindowTest}; this
 * checks the outcome the operator sees - a good archive and a closed database, in that order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7458BackupSurvivesConcurrentCloseIT {
  private static final String DATABASE_PATH     = "target/databases/backup-close-7458";
  private static final String RESTORED_PATH     = "target/databases/backup-close-7458-restored";
  private static final String BACKUP_FILE       = "target/backup-close-7458.zip";
  private static final String TYPE              = "Doc";
  private static final int    RECORDS           = 20_000;
  /** Throttled so the backup is still streaming when the close lands. */
  private static final int    MAX_MB_PER_SECOND = 4;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  @Test
  void closeRequestedMidBackupWaitsForTheBackupToComplete() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    boolean closed = false;
    try {
      database.getSchema().createDocumentType(TYPE);
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE).set("id", i).set("payload", "x".repeat(500)).save();
      });
      final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
      pageManager.waitAllPagesOfDatabaseAreFlushed(database);

      final AtomicReference<Throwable> backupFailure = new AtomicReference<>();
      final Thread backup = new Thread(() -> {
        try {
          new Backup(database, BACKUP_FILE).setVerboseLevel(0).setMaxMBPerSecond(MAX_MB_PER_SECOND).backupDatabase();
        } catch (final Throwable e) {
          backupFailure.set(e);
        }
      }, "backup");
      backup.start();

      // WAIT FOR THE BACKUP TO BE INSIDE ITS SNAPSHOT WINDOW, THEN ASK FOR THE CLOSE
      final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
      while (!pageManager.isSnapshotWindowOpen(database) && backup.isAlive() && System.currentTimeMillis() < deadline)
        Thread.sleep(5);
      assertThat(pageManager.isSnapshotWindowOpen(database)).as("the backup must be streaming through a window").isTrue();

      final long closeRequestedAt = System.currentTimeMillis();
      database.close();
      closed = true;
      final long closeCompletedAt = System.currentTimeMillis();

      // THE CLOSE RETURNED, SO THE BACKUP HAD TO BE DONE WITH THE FILES: ITS THREAD ENDS RIGHT AFTER
      backup.join(30_000);
      assertThat(backup.isAlive()).isFalse();
      assertThat(backupFailure.get()).as("the backup must complete despite the close requested while it ran").isNull();
      assertThat(database.isOpen()).isFalse();
      assertThat(closeCompletedAt - closeRequestedAt).as("the close must have waited for the window, not raced it")
          .isPositive();

      new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();
      try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
        assertThat(restored.command("sql", "check database").nextIfAvailable().<Long>getProperty("totalErrors")).isZero();
        assertThat(restored.countType(TYPE, false)).isEqualTo(RECORDS);
      }
    } finally {
      if (!closed && database.isOpen())
        database.close();
    }
    TestHelper.checkActiveDatabases();
  }
}
