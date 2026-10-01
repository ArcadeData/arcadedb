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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8019: the full-snapshot ZIP a leader ships to a resyncing follower must not carry an index-compaction
 * temporary ({@code temp_*}).
 * <p>
 * The temporary is a REGISTERED component file, so it is in {@code FileManager.getFiles()} and in every page
 * snapshot window opened while a compaction runs. Shipped, it lands on the follower as a file nothing there ever
 * registers ({@code temp_umtidx} is not in {@code LocalDatabase.SUPPORTED_FILE_EXT}), renames or deletes: a
 * permanent orphan. It is also bytes nobody needs - the window's own {@code schema.json} still names the index files
 * the temporary has not replaced yet, because {@code removeTempSuffix()} runs before the schema is switched over.
 * <p>
 * Both branches of the ship are driven - the point-in-time window and the frozen-files fallback - because they
 * enumerate different sources, and the size announced to the follower's up-front space check (#7037) is pinned to
 * the entries actually written, so an estimate still counting the temporary is caught too.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8019">issue #8019</a>
 */
class Issue8019SnapshotShipSkipsCompactionTemporaryTest {

  private static final String DATABASE_PATH  = "target/databases/snapshot-ship-skips-temp-8019";
  private static final String TYPE           = "Doc";
  private static final String INDEX_PROPERTY = "id";
  private static final int    RECORDS        = 2_000;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void theWindowPathDoesNotShipACompactionTemporary() throws Exception {
    try (final Database database = createDatabaseWithIndex()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final String tempName = startCompactionTemporary(db);

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        assertThat(snapshot.getFiles()).as("the fixture: the window really carries the temporary, with pages in it")
            .anyMatch(f -> f.fileName().equals(tempName) && f.size() > 0);

        assertShipSkipsTheTemporary(db, snapshot, tempName);
      }
    }
  }

  @Test
  void theFrozenFilesPathDoesNotShipACompactionTemporary() throws Exception {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);

    try (final Database database = createDatabaseWithIndex()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final String tempName = startCompactionTemporary(db);

      assertThat(new File(db.getDatabasePath(), tempName).length())
          .as("the fixture: the temporary is on disk with bytes in it, so the fallback would size and ship it").isPositive();

      assertShipSkipsTheTemporary(db, null, tempName);
    }
  }

  /**
   * The predicate the ship relies on is the one the checksum endpoints already use (#7955), so a mutable index file
   * of a type whose NAME starts with {@code temp_} must still be shipped: the test is on the extension.
   */
  @Test
  void aTypeNamedLikeTheTemporaryPrefixIsStillShipped() throws Exception {
    try (final Database database = new DatabaseFactory(DATABASE_PATH).create()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      database.getSchema().createDocumentType("temp_readings");

      for (final boolean pageSnapshot : new boolean[] { true, false }) {
        GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(pageSnapshot);
        try (final PageSnapshot snapshot = pageSnapshot ? db.getPageManager().openSnapshot(db) : null) {
          assertThat(archivePageFiles(db, snapshot).keySet())
              .as("pageSnapshotEnabled=%s: a bucket of a type called temp_readings is real data", pageSnapshot)
              .anyMatch(name -> name.startsWith("temp_readings"));
        }
      }
    }
  }

  // ------------------------------------------------------------------------------------------------- HELPERS

  private static void assertShipSkipsTheTemporary(final DatabaseInternal db, final PageSnapshot snapshot,
      final String tempName) throws Exception {
    final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
    final Map<String, byte[]> entries = archive(db, snapshot, manifest);

    assertThat(entries.keySet())
        .as("a compaction temporary would land on the follower as a file nothing there registers or deletes")
        .doesNotContain(tempName);
    assertThat(entries.keySet())
        .as("the control: the published file of the same index must still be shipped")
        .anyMatch(name -> name.endsWith(".umtidx"));

    long written = 0L;
    for (final SnapshotManager.ManifestEntry entry : manifest)
      written += entry.size();
    assertThat(SnapshotHttpHandler.estimateUncompressedBytes(db, snapshot, List.of()))
        .as("the size announced to the follower's space check (#7037) must describe the archive actually sent")
        .isEqualTo(written + Long.BYTES);
  }

  /** The configuration and page-file steps of the ship, in the order the handler runs them, into an in-memory ZIP. */
  private static Map<String, byte[]> archive(final DatabaseInternal db, final PageSnapshot snapshot,
      final List<SnapshotManager.ManifestEntry> manifest) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ZipOutputStream zipOut = new ZipOutputStream(bytes)) {
      SnapshotHttpHandler.addConfigurationToZip(zipOut, db, snapshot, manifest);
      SnapshotHttpHandler.addPageFilesToZip(zipOut, db, snapshot, manifest);
      zipOut.finish();
    }
    final Map<String, byte[]> entries = readEntries(bytes.toByteArray());
    assertThat(manifest).extracting(SnapshotManager.ManifestEntry::name)
        .as("the manifest the follower verifies must describe exactly the entries written (#4831)")
        .containsExactlyInAnyOrderElementsOf(entries.keySet());
    return entries;
  }

  private static Map<String, byte[]> archivePageFiles(final DatabaseInternal db, final PageSnapshot snapshot)
      throws Exception {
    final List<SnapshotManager.ManifestEntry> manifest = new ArrayList<>();
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ZipOutputStream zipOut = new ZipOutputStream(bytes)) {
      SnapshotHttpHandler.addPageFilesToZip(zipOut, db, snapshot, manifest);
      zipOut.finish();
    }
    return readEntries(bytes.toByteArray());
  }

  private static Map<String, byte[]> readEntries(final byte[] zip) throws Exception {
    final Map<String, byte[]> entries = new HashMap<>();
    try (final ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(zip))) {
      for (ZipEntry entry = zipIn.getNextEntry(); entry != null; entry = zipIn.getNextEntry())
        entries.put(entry.getName(), zipIn.readAllBytes());
    }
    return entries;
  }

  /**
   * Creates the compaction output the way {@code LSMTreeIndexCompactor} does - {@code createNewForCompaction()} -
   * and writes one page into it, so it has bytes both on disk and in a window: an empty temporary would make a size
   * estimate that still counted it indistinguishable from one that did not. {@code removeTempSuffix()} is never
   * called, which is the state an in-flight compaction leaves the database in.
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
