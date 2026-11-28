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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A full backup walks the files of the database. A slot of the file table that was reserved for a component and never got
 * its file (the component could not open it) is not a file, so the backup skips it on both of its paths: the page snapshot
 * window, and the fallback that archives the registered files one by one when the window is disabled or unusable.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BackupWithReservedFileSlotTest {
  private static final String DATABASE_PATH = "target/databases/backup-reserved-file-slot";
  private static final String RESTORED_PATH = "target/databases/backup-reserved-file-slot-restored";
  private static final String BACKUP_FILE   = "target/backup-reserved-file-slot.zip";
  private static final String TYPE          = "SlotDoc";
  private static final int    RECORDS       = 50;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    FileUtils.deleteRecursively(new File(RESTORED_PATH));
    new File(BACKUP_FILE).delete();
  }

  @ParameterizedTest(name = "pageSnapshot={0}")
  @ValueSource(booleans = { true, false })
  void aBackupAfterAFailedBucketCreationCompletes(final boolean pageSnapshot) {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(pageSnapshot);
    try (final Database database = createDatabase()) {
      assertThatThrownBy(() -> database.getSchema().createBucket("b".repeat(400))).isInstanceOf(SchemaException.class);

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    assertRestoredDatabaseHoldsTheRecords();
  }

  @ParameterizedTest(name = "pageSnapshot={0}")
  @ValueSource(booleans = { true, false })
  void aBackupWithAReservedFileSlotCompletes(final boolean pageSnapshot) {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(pageSnapshot);
    try (final Database database = createDatabase()) {
      // THE STATE A COMPONENT THAT FAILED TO OPEN ITS FILE LEAVES: AN ID TAKEN, NO FILE OPENED ON IT
      ((DatabaseInternal) database).getFileManager().newFileId();

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    assertRestoredDatabaseHoldsTheRecords();
  }

  private static void assertRestoredDatabaseHoldsTheRecords() {
    assertThat(new File(BACKUP_FILE)).exists();
    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.countType(TYPE, true)).isEqualTo(RECORDS);
    }
    TestHelper.checkActiveDatabases();
  }

  private static Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).save();
    });
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    return database;
  }
}
