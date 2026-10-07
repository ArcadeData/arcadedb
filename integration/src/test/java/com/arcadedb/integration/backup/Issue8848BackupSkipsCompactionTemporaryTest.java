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
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.restore.Restore;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
 * Issue #8848: a full backup taken while an index was being compacted archived the half-built compaction temporary
 * ({@code *.temp_<ext>}), on both of its paths - {@code backupFromSnapshot} copies every file of the page snapshot
 * window and {@code backupFromFrozenFiles} every file of {@code FileManager.getFiles()}, and the temporary is a
 * registered component in both. The archive's {@code schema.json} still names the pre-compaction files, so the entry
 * is dead weight that a restore extracts into the database directory, where nothing registers it.
 * <p>
 * Both paths are driven, because they enumerate their files separately.
 */
class Issue8848BackupSkipsCompactionTemporaryTest {
  private static final String DATABASE_PATH  = "target/databases/issue8848-backup-skips-temp";
  private static final String RESTORED_PATH  = "target/databases/issue8848-backup-skips-temp-restored";
  private static final String BACKUP_FILE    = "target/issue8848-backup-skips-temp.zip";
  private static final String TYPE           = "Issue8848Doc";
  private static final String INDEX_PROPERTY = "id";
  private static final int    RECORDS        = 2_000;

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
  void aCompactionTemporaryIsNotArchived(final boolean pageSnapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(pageSnapshot);
    final String tempName;
    try (final Database database = createDatabaseWithIndex()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      tempName = startCompactionTemporary(db);

      // THE FIXTURE: the temporary really is something the path being tested would enumerate, with bytes in it
      if (pageSnapshot) {
        try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
          assertThat(snapshot.getFiles()).as("the window carries the temporary").anyMatch(f -> f.fileName().equals(tempName) && f.size() > 0);
        }
      } else {
        assertThat(db.getFileManager().getFiles()).as("the file manager lists the temporary")
            .anyMatch(f -> f != null && f.getFileName().equals(tempName));
        assertThat(new File(db.getDatabasePath(), tempName).length()).as("the temporary is on disk with bytes").isPositive();
      }

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    final List<String> entries = entryNames();
    assertThat(entries).as("a compaction temporary is half-built output the archived schema does not reference")
        .doesNotContain(tempName);
    assertThat(entries).as("the control: the published file of the same index is still archived")
        .anyMatch(name -> name.startsWith(TYPE) && name.endsWith(".umtidx"));
    assertThat(entries).noneMatch(PaginatedComponent::isTemporaryFileName);

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();
    assertThat(new File(RESTORED_PATH, tempName)).as("the restore must not produce the orphan in the first place")
        .doesNotExist();

    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.countType(TYPE, true)).isEqualTo(RECORDS);
      final TypeIndex index = (TypeIndex) restored.getSchema().getIndexByName(TYPE + "[" + INDEX_PROPERTY + "]");
      for (final int key : new int[] { 0, RECORDS / 2, RECORDS - 1 }) {
        final IndexCursor cursor = index.get(new Object[] { key });
        assertThat(cursor.hasNext()).as("key %d must still be found through the restored index", key).isTrue();
      }
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The filter is on the EXTENSION ({@link PaginatedComponent#isTemporaryFileName(String)}), so the files of a type
   * whose NAME starts with the temporary prefix are real data and must still be archived on both paths.
   */
  @ParameterizedTest(name = "pageSnapshot={0}")
  @ValueSource(booleans = { true, false })
  void aTypeNamedLikeTheTemporaryPrefixIsStillArchived(final boolean pageSnapshot) throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(pageSnapshot);
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      database.getSchema().createDocumentType("temp_readings").createProperty("v", Integer.class)
          .createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("temp_readings").set("v", i).save();
      });

      new Backup(database, BACKUP_FILE).setVerboseLevel(0).backupDatabase();
    } finally {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      TestHelper.checkActiveDatabases();
    }

    final List<String> entries = entryNames();
    assertThat(entries).as("the bucket of a type called temp_readings").anyMatch(n -> n.startsWith("temp_readings_") && n.endsWith(".bucket"));
    assertThat(entries).as("the index of a type called temp_readings").anyMatch(n -> n.startsWith("temp_readings_") && n.endsWith("umtidx"));

    new Restore(BACKUP_FILE, RESTORED_PATH).setVerboseLevel(0).restoreDatabase();
    try (final Database restored = new DatabaseFactory(RESTORED_PATH).open()) {
      assertThat(restored.countType("temp_readings", true)).isEqualTo(10);
    }
    TestHelper.checkActiveDatabases();
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private static List<String> entryNames() throws Exception {
    final List<String> names = new ArrayList<>();
    try (final ZipFile zip = new ZipFile(BACKUP_FILE)) {
      final Enumeration<? extends ZipEntry> it = zip.entries();
      while (it.hasMoreElements())
        names.add(it.nextElement().getName());
    }
    return names;
  }

  /**
   * Creates the compaction output the way the LSM-tree compactor does - {@code createNewForCompaction()} - and writes
   * one page into it, then never calls {@code removeTempSuffix()}: the state an in-flight compaction leaves the
   * database in.
   */
  private static String startCompactionTemporary(final DatabaseInternal db) throws Exception {
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName(TYPE + "[" + INDEX_PROPERTY + "]");
    final LSMTreeIndex index = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    final LSMTreeIndexCompacted temporary = index.getMutableIndex().createNewForCompaction();

    final MutablePage page = new MutablePage(new PageId(db, temporary.getFileId(), 0), temporary.getPageSize());
    db.getPageManager().writePages(List.of(db.getPageManager().updatePageVersion(page, true)), false);
    temporary.updatePageCount(1);
    db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

    final String tempName = temporary.getComponentFile().getFileName();
    assertThat(PaginatedComponent.isTemporaryFileName(tempName)).as("'%s' must be a compaction temporary", tempName).isTrue();
    return tempName;
  }

  private static Database createDatabaseWithIndex() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createDocumentType(TYPE).createProperty(INDEX_PROPERTY, Integer.class)
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set(INDEX_PROPERTY, i).set("payload", "x".repeat(300)).save();
    });
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    return database;
  }
}
