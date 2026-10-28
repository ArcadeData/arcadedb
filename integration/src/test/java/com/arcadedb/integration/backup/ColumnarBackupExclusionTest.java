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
package com.arcadedb.integration.backup;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.columnar.ColumnarStats;
import com.arcadedb.index.columnar.ColumnarStore;
import com.arcadedb.index.columnar.ColumnarTestData;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.backup.format.FullBackupFormat;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The columnar projection is node-local derived state (plan 3, Task 22): a full backup never archives it, on either of its
 * paths, and a restored database rebuilds it at its first open with the reason {@code missing}.
 */
class ColumnarBackupExclusionTest {
  private static final String DATABASE_PATH = "target/databases/columnar-backup-exclusion";
  private static final String RESTORED_PATH = "target/databases/columnar-backup-exclusion-restored";
  private static final String BACKUP_FILE   = "target/columnar-backup-exclusion.zip";
  private static final int    RECORDS       = 600;

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
  void backupArchiveHasNoProjectionEntry(final boolean pageSnapshot) throws Exception {
    backUpAProjectedDatabase(pageSnapshot);

    final List<String> entries = entryNames();
    assertThat(entries).as("the control: the bucket of the projected type is archived").anyMatch(n -> n.startsWith(ColumnarTestData.TYPE));
    assertThat(entries).noneMatch(ColumnarStore::isProjectionEntry);
  }

  @ParameterizedTest(name = "pageSnapshot={0}")
  @ValueSource(booleans = { true, false })
  void restoredDatabaseRebuildsAsMissing(final boolean pageSnapshot) throws Exception {
    backUpAProjectedDatabase(pageSnapshot);

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();
    assertThat(new File(RESTORED_PATH, ColumnarStore.ROOT_DIR)).as("the restore extracts no projection").doesNotExist();

    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.countType(ColumnarTestData.TYPE, true)).isEqualTo(RECORDS);
      FacetOracle.assertEquivalent(restored, ColumnarTestData.TYPE, "brand", null);
      FacetOracle.assertEquivalent(restored, ColumnarTestData.TYPE, "fileSize", null);
      final ColumnarStats stats = ((LocalDatabase) restored).getColumnarIndexManager().getStats();
      assertThat(stats.rebuilds(ColumnarTestData.TYPE, "missing")).isEqualTo(1);
      assertThat(stats.rebuildsTotal()).isEqualTo(1);
    }
    TestHelper.checkActiveDatabases();
  }

  /** The predicate both backup paths apply: a projection entry is never an archived page file. */
  @Test
  void aProjectionEntryIsNeverAnArchivedPageFile() {
    assertThat(FullBackupFormat.isArchivedPageFile("columnar/1/manifest.json")).isFalse();
    assertThat(FullBackupFormat.isArchivedPageFile(ColumnarStore.ROOT_DIR)).isFalse();
    assertThat(FullBackupFormat.isArchivedPageFile("columnar_0.1.65536.v0.bucket")).as("a type named columnar").isTrue();
  }

  private static void backUpAProjectedDatabase(final boolean pageSnapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(pageSnapshot);
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      final ColumnarTestData data = new ColumnarTestData(31L);
      data.createSchema(database);
      data.insertAssets(database, RECORDS);
      FacetOracle.awaitCaughtUp(database);
      ((LocalDatabase) database).getColumnarIndexManager().flushNow();
      assertThat(new File(DATABASE_PATH, ColumnarStore.ROOT_DIR)).as("the fixture: a flushed projection is on disk").isNotEmptyDirectory();

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }
  }

  private static List<String> entryNames() throws Exception {
    final List<String> names = new ArrayList<>();
    try (final ZipFile zip = new ZipFile(BACKUP_FILE)) {
      final Enumeration<? extends ZipEntry> it = zip.entries();
      while (it.hasMoreElements())
        names.add(it.nextElement().getName());
    }
    return names;
  }
}
