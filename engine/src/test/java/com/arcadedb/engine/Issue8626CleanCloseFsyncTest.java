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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8626: a clean close used to fsync every data file of the database, whether or not the session wrote to it -
 * about half a millisecond per bucket or index file on an NVMe drive, so a read-only open + close of a database with
 * 100 indexed types paid 100 ms of fsyncs that had nothing to persist. The close now forces only the files written
 * (or created, or renamed) since their last successful fsync, which is exactly the set the WAL it is about to delete
 * still protects (#4332, #4934).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8626CleanCloseFsyncTest extends TestHelper {
  private static final int TYPES            = 10;
  private static final int RECORDS_PER_TYPE = 100;

  @Override
  protected void beginTest() {
    for (int t = 0; t < TYPES; t++) {
      final DocumentType type = database.getSchema().createDocumentType("T" + t);
      type.createProperty("k", Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "T" + t, "k");
    }
    database.transaction(() -> {
      for (int t = 0; t < TYPES; t++)
        for (int i = 0; i < RECORDS_PER_TYPE; i++)
          database.newDocument("T" + t).set("k", (long) i).save();
    });
  }

  @Test
  void cleanCloseOfAReadOnlySessionSyncsNoFile() {
    // The close of the write session above is what makes every file durable; from here on nothing is written.
    reopenDatabase();

    for (int t = 0; t < TYPES; t++) {
      assertThat(database.countType("T" + t, false)).isEqualTo(RECORDS_PER_TYPE);
      try (final ResultSet rs = database.query("sql", "select from T" + t + " where k = ?", 42L)) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
    }

    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    assertThat(paginatedFiles(fileManager)).hasSizeGreaterThanOrEqualTo(2 * TYPES);
    assertThat(unsyncedFiles(fileManager)).as("reading a file owes the disk nothing").isEmpty();

    final long syncedBefore = fileManager.getStats().syncedFiles;
    database.close();

    assertThat(fileManager.getStats().syncedFiles - syncedBefore)
        .as("a clean close with nothing written since the open must not fsync any data file").isZero();
    assertThat(new File(getDatabasePath()).listFiles((d, n) -> n.endsWith(".wal"))).isEmpty();

    reopenDatabase();
    assertThat(database.countType("T0", false)).isEqualTo(RECORDS_PER_TYPE);
  }

  @Test
  void cleanCloseSyncsExactlyTheFilesTheSessionWrote() {
    reopenDatabase();

    final RID[] written = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("T0").set("k", 1_000L);
      doc.save();
      written[0] = doc.getIdentity();
    });

    final DatabaseInternal db = (DatabaseInternal) database;
    final FileManager fileManager = db.getFileManager();
    PageManager.INSTANCE.waitAllPagesOfDatabaseAreFlushed(db);

    final List<PaginatedComponentFile> unsynced = unsyncedFiles(fileManager);
    final Set<Integer> unsyncedIds = new HashSet<>();
    for (final PaginatedComponentFile f : unsynced)
      unsyncedIds.add(f.getFileId());

    assertThat(unsyncedIds).as("the bucket the record landed in must owe an fsync").contains(written[0].getBucketId());
    assertThat(unsynced.size()).as("only the files the transaction touched").isLessThan(paginatedFiles(fileManager).size());

    final long syncedBefore = fileManager.getStats().syncedFiles;
    database.close();

    assertThat(fileManager.getStats().syncedFiles - syncedBefore).isEqualTo(unsynced.size());
    assertThat(new File(getDatabasePath()).listFiles((d, n) -> n.endsWith(".wal"))).isEmpty();

    reopenDatabase();
    assertThat(database.countType("T0", false)).isEqualTo(RECORDS_PER_TYPE + 1);
    try (final ResultSet rs = database.query("sql", "select from T0 where k = ?", 1_000L)) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
  }

  @Test
  void recoverySyncsEveryFileBeforeDroppingTheReplayedWal() {
    reopenDatabase();

    database.transaction(() -> {
      for (int i = 0; i < RECORDS_PER_TYPE; i++)
        database.newDocument("T1").set("k", (long) (RECORDS_PER_TYPE + i)).save();
    });

    // A crash: the WAL of the transaction above is all the next open has to go on.
    ((DatabaseInternal) database).kill();
    database.close();

    database = factory.open();

    // After an unclean shutdown nothing is known about what reached the disk, so recovery treats every file as
    // unsynced and forces all of them before it deletes the WAL it replayed: a power loss right after the open must
    // not find the replayed pages only in the OS page cache with their WAL already gone.
    final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
    assertThat(unsyncedFiles(fileManager)).as("recovery must leave no file owing an fsync").isEmpty();
    assertThat(fileManager.getStats().syncedFiles).isGreaterThanOrEqualTo(paginatedFiles(fileManager).size());

    assertThat(database.countType("T1", false)).isEqualTo(2L * RECORDS_PER_TYPE);
  }

  private static List<PaginatedComponentFile> paginatedFiles(final FileManager fileManager) {
    final List<PaginatedComponentFile> result = new ArrayList<>();
    for (final ComponentFile f : fileManager.getFiles())
      if (f instanceof PaginatedComponentFile pcf && pcf.getFileId() > -1 && pcf.isOpen())
        result.add(pcf);
    return result;
  }

  private static List<PaginatedComponentFile> unsyncedFiles(final FileManager fileManager) {
    final List<PaginatedComponentFile> result = new ArrayList<>();
    for (final PaginatedComponentFile f : paginatedFiles(fileManager))
      if (f.isModifiedSinceLastSync())
        result.add(f);
    return result;
  }
}
