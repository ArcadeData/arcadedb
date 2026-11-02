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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #11151: after a simulated power loss (Jepsen LazyFS) a recovered node served records whose property names resolved to
 * nothing or to another name. The schema dictionary page had several versions pending in the WAL (one per new name, each its
 * own transaction), and the power loss persisted the page's first sector - the one holding the version - while the sectors
 * holding the names appended by the earlier versions stayed stale. Recovery skipped every WAL entry older than the version on
 * disk, so only the last name's range was repaired and the dictionary kept holes where ids that records already carry should
 * have resolved.
 */
class Issue11151TornMultiVersionPageRecoveryTest {

  private static final String DB_PATH = "target/databases/Issue11151TornMultiVersionPageRecoveryTest";
  private static final String[] NAMES = { "acctId", "balance", "val", "key" };

  @BeforeEach
  void fileSyncOn() {
    GlobalConfiguration.FILE_SYNC.setValue(true);
  }

  @AfterEach
  void cleanup() {
    GlobalConfiguration.FILE_SYNC.reset();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    try {
      if (factory.exists())
        factory.open().drop();
    } catch (final Exception e) {
      // A test may deliberately leave the database unreopenable: remove it on disk below
    }
    factory.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void dictionaryTornAcrossSeveralPendingVersionsIsFullyRepairedByRecovery() throws Exception {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    final int[] ids = new int[NAMES.length];
    final String dictionaryFilePath;
    final int pageSize;
    final long finalVersion;
    final int finalContentSize;

    {
      final DatabaseInternal db = (DatabaseInternal) factory.create();
      db.getSchema().createDocumentType("Doc");
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

      final Dictionary dictionary = db.getSchema().getDictionary();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(dictionary.getFileId());
      dictionaryFilePath = file.getFilePath();
      pageSize = file.getPageSize();

      // KEEP THE NEW NAMES OFF THE DISK: EACH ONE IS ITS OWN COMMIT, HENCE ITS OWN VERSION OF DICTIONARY PAGE 0 IN THE WAL
      assertThat(PageManager.INSTANCE.getFlushThread().setSuspended(db, true))
          .as("flush suspension must engage or the pages could reach disk, invalidating the test").isTrue();

      for (int i = 0; i < NAMES.length; ++i)
        ids[i] = dictionary.getIdByName(NAMES[i], true);

      final ImmutablePage committed = db.getPageManager()
          .getImmutablePage(new PageId(db, dictionary.getFileId(), 0), pageSize, false, false);
      finalVersion = committed.getVersion();
      finalContentSize = committed.getContentSize();

      ((LocalDatabase) db).kill();
      db.close();
    }

    // FORGE THE TORN WRITE: SECTOR 0 (VERSION, CONTENT SIZE) OF THE LAST VERSION REACHED THE DISK, THE NAMES BEHIND IT DID NOT
    try (final RandomAccessFile raf = new RandomAccessFile(dictionaryFilePath, "rw")) {
      raf.seek(0);
      final int diskVersion = raf.readInt();
      assertThat(diskVersion).as("the new names must not have reached the disk").isLessThan((int) finalVersion - 1);
      raf.seek(0);
      raf.writeInt((int) finalVersion);
      raf.writeInt(finalContentSize);
    }

    final Database reopened = factory.open();
    try {
      final Dictionary dictionary = ((DatabaseInternal) reopened).getSchema().getDictionary();
      for (int i = 0; i < NAMES.length; ++i) {
        assertThat(dictionary.getIdByName(NAMES[i], false))
            .as("'%s' must keep the id records already carry (#11151)", NAMES[i]).isEqualTo(ids[i]);
        assertThat(dictionary.getNameById(ids[i])).isEqualTo(NAMES[i]);
      }
    } finally {
      reopened.close();
      factory.close();
    }
  }
}
