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
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8018 on the live endpoints, which is where the two halves of the issue have to be answered rather than in
 * the scan in isolation.
 * <p>
 * <b>#7955</b> - an index-compaction temporary is the one scratch file in a database directory that is a fully
 * REGISTERED component file: {@code PaginatedComponent}'s constructor calls {@code FileManager.getOrCreateFile}, so
 * a {@code temp_*} file is in {@code getFiles()}, in every page snapshot window opened while the compaction runs,
 * and in a directory listing. Only the node that happens to be compacting has one, so every endpoint that exists to
 * compare two nodes has to leave it out. That is two endpoints, not one: {@code /checksums} reaches it through the
 * directory listing and {@code /cluster/verify} through the registry, and the fixture below builds the temporary the
 * way compaction itself does - {@code LSMTreeIndexMutable.createNewForCompaction()} is the method
 * {@code LSMTreeIndexCompactor} line 83 calls - so neither test is asserting about a file of its own invention.
 * <p>
 * <b>#7956</b> - a file that disappears between {@code listFiles} and the {@code FileInputStream} used to take the
 * whole {@code /checksums} answer down as a 500, which reported the node as ERROR to the cluster comparison at
 * exactly the moment an operator was using it to decide whether a follower had diverged. The answer now survives and
 * names what it could not cover.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8018">issue #8018</a>
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7955">issue #7955</a>
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7956">issue #7956</a>
 */
class Issue8018ChecksumTransientFilesTest {

  private static final String DATABASE_PATH = "target/databases/checksums-transient-files";
  private static final String TYPE          = "Doc";
  private static final String INDEX_PROPERTY = "id";
  private static final int    RECORDS       = 2_000;

  private final SnapshotHttpHandler      checksumsHandler = new SnapshotHttpHandler(null);
  private final PostVerifyDatabaseHandler verifyHandler   = new PostVerifyDatabaseHandler(null, null);

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @AfterEach
  void closeHandlers() {
    checksumsHandler.close();
    verifyHandler.close();
  }

  /**
   * #7955 on {@code GET /api/v1/ha/snapshot/{db}/checksums}, driven through BOTH of its branches: the point-in-time
   * window and the suspend-and-freeze fallback. They fail differently without the fix - with a window the temporary
   * lands in the map with a consistent but node-local checksum, without one it is CRC'd live while compaction is
   * writing it - and the same skip settles both, which is only visible if both are run.
   * <p>
   * The already-published index file is the control: it has the same stem and the same directory, and it is exactly
   * what the temporary becomes once {@code removeTempSuffix} renames it, so a skip that reached it would be caught
   * here.
   */
  @Test
  void neitherChecksumsBranchReportsAnIndexCompactionTemporary() throws Exception {
    try (final Database database = createDatabaseWithIndex()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final String tempName = startCompactionTemporary(db);

      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
      final JSONObject viaSnapshot = checksumsHandler.computeChecksums(db);
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);
      final JSONObject viaSuspension = checksumsHandler.computeChecksums(db);

      for (final JSONObject answer : new JSONObject[] { viaSnapshot, viaSuspension }) {
        assertThat(answer.keySet())
            .as("a compaction temporary exists only on the node that is compacting: reporting it is a false divergence")
            .doesNotContain(tempName);
        assertThat(answer.keySet())
            .as("the control: the published index file of the same index must still be compared")
            .anyMatch(name -> name.endsWith(".umtidx") || name.endsWith(".uctidx"));
      }
    }
  }

  /**
   * #7955 on {@code POST /api/v1/cluster/verify/{database}}, the twin endpoint. It reads the REGISTERED files rather
   * than the directory, so none of the scratch families the directory scan skips can reach it - and the compaction
   * temporary is the single exception, because it is registered. Both of its branches are driven for the same
   * reason as above: one enumerates the window, the other {@code FileManager.getFiles()}.
   */
  @Test
  void neitherVerifyBranchReportsAnIndexCompactionTemporary() throws Exception {
    try (final Database database = createDatabaseWithIndex()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final String tempName = startCompactionTemporary(db);

      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
      final JSONObject viaSnapshot = new JSONObject();
      verifyHandler.computeLocalChecksums(db, viaSnapshot, new JSONArray());

      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);
      final JSONObject viaSuspension = new JSONObject();
      verifyHandler.computeLocalChecksums(db, viaSuspension, new JSONArray());

      for (final JSONObject answer : new JSONObject[] { viaSnapshot, viaSuspension }) {
        assertThat(answer.keySet()).as("the fixture must have produced a comparable answer at all").isNotEmpty();
        assertThat(answer.keySet())
            .as("the verify reads the registry, and the compaction temporary is the one scratch file IN the registry")
            .doesNotContain(tempName);
      }
    }
  }

  /**
   * The fixture's own proof, kept apart from the assertions it underwrites. Without it a skip that happened to be
   * unreachable - a temporary that was never created, never registered, never in the directory - would make both
   * tests above pass while proving nothing at all.
   */
  @Test
  void theFixtureReallyProducesARegisteredTemporaryInTheDatabaseDirectory() throws Exception {
    try (final Database database = createDatabaseWithIndex()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final String tempName = startCompactionTemporary(db);

      assertThat(PaginatedComponent.isTemporaryFileName(tempName))
          .as("'%s' must be what compaction actually names its output", tempName).isTrue();
      assertThat(new File(db.getDatabasePath(), tempName)).as("it is on disk, so the directory scan lists it").exists();
      assertThat(db.getFileManager().getFiles().stream().filter(f -> f != null).map(f -> f.getFileName()))
          .as("and it is registered, so the verify handler and every page snapshot window carry it")
          .contains(tempName);
    }
  }

  /**
   * #7956 through the real handler. The listing-then-open gap is between two syscalls and cannot be provoked from
   * outside, so the directory handed to the handler lists itself with the JDK's own call and then deletes one of the
   * files it is about to return - which is exactly what TimeSeries retention does in that window, holding no lock
   * that would stop it.
   * <p>
   * Both branches are driven: the fallback one is the branch #7956 named as the exposure, and the window one shares
   * the same scan.
   */
  @Test
  void aFileThatVanishesDuringTheScanIsNamedInTheAnswerRatherThanFailingIt() throws Exception {
    try (final Database database = createDatabaseWithIndex()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      for (final boolean pageSnapshot : new boolean[] { true, false }) {
        GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(pageSnapshot);
        final String doomed = "weather_shard_" + pageSnapshot + ".ts.sealed";
        Files.writeString(new File(db.getDatabasePath(), doomed).toPath(), "dropped by retention mid-scan");
        // The control: a file the scan has no business dropping. Without it this test would pass against a scan
        // that gave up on everything after the first failure.
        final String survivor = "operator-notes-" + pageSnapshot + ".txt";
        Files.writeString(new File(db.getDatabasePath(), survivor).toPath(), "not scratch, and not a page file");

        final JSONObject answer = checksumsHandler.computeChecksums(db, vanishingDirectory(db.getDatabasePath(), doomed));

        assertThat(answer.has(SnapshotHttpHandler.UNREADABLE_FILES_KEY))
            .as("pageSnapshotEnabled=%s: the answer must say which file it does not cover", pageSnapshot).isTrue();
        assertThat(toList(answer.getJSONArray(SnapshotHttpHandler.UNREADABLE_FILES_KEY))).containsExactly(doomed);
        assertThat(answer.keySet())
            .as("pageSnapshotEnabled=%s: and the rest of the answer must survive", pageSnapshot)
            .contains(survivor);
      }
    }
  }

  /**
   * The reserved key is absent, not empty, when nothing went wrong - so the ordinary answer keeps exactly the flat
   * shape it has always had and no existing client sees a new key. The two assertions belong together: the second
   * one is what says the first is about a complete answer rather than about a broken one.
   */
  @Test
  void aCompleteAnswerCarriesNoReservedKeyAtAll() throws Exception {
    try (final Database database = createDatabaseWithIndex()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      final JSONObject answer = checksumsHandler.computeChecksums(db);

      assertThat(answer.keySet()).isNotEmpty().doesNotContain(SnapshotHttpHandler.UNREADABLE_FILES_KEY);
      for (final String name : answer.keySet())
        assertThat(name).as("every key of a complete answer is a file name").doesNotContain("/");
    }
  }

  /**
   * A directory that behaves exactly like the real one except that it drops {@code doomed} in the gap between the
   * listing and the read. {@code File} is subclassed rather than mocked: the listing IS the JDK's, the deletion IS
   * the filesystem's, and only the interleaving is arranged.
   */
  private static File vanishingDirectory(final String path, final String doomed) {
    return new File(path) {
      @Override
      public File[] listFiles(final FileFilter filter) {
        final File[] listed = super.listFiles(filter);
        assertThat(new File(this, doomed).delete())
            .as("the fixture must really enter the gap, or the test proves nothing").isTrue();
        return listed;
      }
    };
  }

  /**
   * Creates the compaction output the way compaction does, and returns its file name. This is the constructor
   * {@code LSMTreeIndexCompactor} calls at line 83 before it starts streaming pages into it; not calling
   * {@code removeTempSuffix()} afterwards is what leaves the database in the state an in-flight compaction has it
   * in.
   */
  private static String startCompactionTemporary(final DatabaseInternal db) throws Exception {
    // A property index is a TypeIndex over one LSMTreeIndex per bucket; compaction runs on those, not on the wrapper.
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName(TYPE + "[" + INDEX_PROPERTY + "]");
    final LSMTreeIndex index = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    final LSMTreeIndexCompacted temporary = index.getMutableIndex().createNewForCompaction();
    return temporary.getComponentFile().getFileName();
  }

  private static List<String> toList(final JSONArray array) {
    final List<String> names = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      names.add(array.getString(i));
    return names;
  }

  private Database createDatabaseWithIndex() {
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
