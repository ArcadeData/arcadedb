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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6106: a persisted JVector graph used to be accepted on reload purely because its node count matched the
 * number of currently live vectors. A count says nothing about WHICH records the graph was built over, and since the
 * renumbering compaction of issue #5870 every generation of an index carries ids densely in {@code [0, N)} - so two
 * different generations produce id lists of exactly the same shape and length. Pairing one generation's graph with
 * another's ordinal map is silent: ordinal {@code i} addresses a record the node was never built from, so searches
 * answer with wrong-but-plausible neighbours instead of failing.
 * <p>
 * The reproduction places exactly that pair on disk: a graph of 400 nodes built over one set of records, next to an
 * index whose 400 live vectors carry the same dense ids {@code [0, 400)} but belong to different records. The two
 * are indistinguishable by any count, which is what makes this the input the old check could not reject. Building
 * the pair through a crash is not possible today - see the class comment of {@link LSMVectorIndexGraphManifest} and
 * the note below - so it is assembled at the file level, which is the same on-disk state.
 * <p>
 * <b>Reachability.</b> Two accidents currently keep the engine off this input: a composition change without a
 * compaction leaves tombstones, and any tombstone already forces a rebuild (issue #3135); and a compaction renames
 * the index component, after which {@code discoverAndLoadGraphFile()} no longer recognises the graph file at all, so
 * the graph is rebuilt rather than reused. Neither is a check of what the graph contains, and the second one costs a
 * full rebuild on the first open after every compaction. The manifest is what makes the correspondence explicit
 * rather than accidental.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
@Tag("vector")
class Issue6106StaleVectorGraphTest extends TestHelper {
  private static final int DIMENSIONS = 48;
  private static final int LIVE       = 400;
  /** Records inserted and deleted in the donor database before its index exists, to shift its RIDs. */
  private static final int RID_SHIFT  = 60;
  /**
   * Well under the live count on purpose. The beam is what makes the topology matter at all: widen it towards the
   * node count and the search visits everything, finds the right answer whatever the edges say, and the test stops
   * testing anything.
   */
  private static final int EF_SEARCH  = 32;

  @Test
  void aGraphBuiltOverOtherRecordsWithTheSameDenseIdsIsNotReused() throws Exception {
    createSchema(database);
    insertDocs(database, 0, LIVE);
    buildAndPersistGraph();

    final String databasePath = database.getDatabasePath();
    database.close();

    // A graph of exactly LIVE nodes whose ordinals carry the same dense ids [0, LIVE) but resolve to other records.
    // Its manifest travels with it, the way the pair would survive a crash together.
    installGraphOfAnotherGeneration(databasePath, LIVE, true);

    database = factory.open();

    final List<String> live = liveDocIds();
    assertThat(live).hasSize(LIVE);

    final int selfRetrieved = countDocsThatAreTheirOwnNearestNeighbour(live);

    assertThat(vectorIndex().getStats().get("graphRebuildCount"))
        .as("the graph on disk describes other records: it must be rebuilt, not paired with this ordinal map")
        .isEqualTo(1L);
    assertThat(selfRetrieved)
        .as("after the rebuild every live record must be its own nearest neighbour: %d of %d found", selfRetrieved,
            LIVE)
        .isGreaterThanOrEqualTo(LIVE * 99 / 100);
  }

  /**
   * A graph persisted before the manifest existed carries none, and is still judged by node count alone. That
   * comparison used to reject only a graph SMALLER than the live set (issue #3722), and reuse a larger one - whose
   * ordinals line up no better: everything past the end of the rebuilt map is dropped as out of bounds and
   * everything before it can address the wrong record.
   */
  @Test
  void aLegacyGraphWithMoreNodesThanLiveVectorsIsNotReused() throws Exception {
    createSchema(database);
    insertDocs(database, 0, LIVE);
    buildAndPersistGraph();

    final String databasePath = database.getDatabasePath();
    database.close();

    installGraphOfAnotherGeneration(databasePath, LIVE + 40, false);

    database = factory.open();

    final int selfRetrieved = countDocsThatAreTheirOwnNearestNeighbour(liveDocIds());

    assertThat(vectorIndex().getStats().get("graphRebuildCount"))
        .as("a graph with %d nodes cannot describe %d live vectors, whichever side is bigger", LIVE + 40, LIVE)
        .isEqualTo(1L);
    assertThat(selfRetrieved).as("and after the rebuild the answers are right: %d of %d found", selfRetrieved, LIVE)
        .isGreaterThanOrEqualTo(LIVE * 99 / 100);
  }

  /**
   * The other half of the contract: a graph that really does describe the live set must still be reused as it is, or
   * every restart would pay for a full rebuild it does not need.
   */
  @Test
  void anUntouchedGraphIsStillReusedAcrossARestart() {
    createSchema(database);
    insertDocs(database, 0, LIVE);
    buildAndPersistGraph();

    reopenDatabase();

    final List<String> live = liveDocIds();
    final int selfRetrieved = countDocsThatAreTheirOwnNearestNeighbour(live);

    assertThat(vectorIndex().getStats().get("graphRebuildCount"))
        .as("nothing changed between the persist and the reopen: the persisted graph must be used as it is")
        .isEqualTo(0L);
    assertThat(selfRetrieved).as("and it must answer correctly: %d of %d found", selfRetrieved, LIVE)
        .isGreaterThanOrEqualTo(LIVE * 95 / 100);
  }

  /**
   * A graph persist that fails leaves pages nobody can describe: the write drops the manifest before touching a
   * page, and the rollback may or may not have put the previous generation back. Leaving no manifest at all would
   * read as "persisted by an older version" on the next open and fall back to the node count - so a failed persist
   * writes a manifest that refuses the pages instead. Without that, an index that had a verified graph is silently
   * demoted to the weaker comparison by a failure that never damaged anything.
   */
  @Test
  void aFailedPersistLeavesAManifestThatRefusesTheGraphRatherThanNoneAtAll() throws Exception {
    createSchema(database);
    insertDocs(database, 0, LIVE);
    buildAndPersistGraph();

    final String databasePath = database.getDatabasePath();
    database.close();

    // What LSMVectorIndexGraphFile.writeGraph() and LSMVectorIndex.build() leave behind when they fail: the graph
    // pages are whatever the rollback made of them - here, still perfectly valid - and the manifest refuses them.
    new LSMVectorIndexGraphManifest(graphFileIn(databasePath).getPath()).markUnusable("simulated persist failure");

    database = factory.open();

    final int selfRetrieved = countDocsThatAreTheirOwnNearestNeighbour(liveDocIds());

    assertThat(vectorIndex().getStats().get("graphRebuildCount"))
        .as("a graph a failed persist could not vouch for must be rebuilt, not judged by its node count")
        .isEqualTo(1L);
    assertThat(vectorIndex().getStats().get("unverifiedGraphReuses"))
        .as("and it must not be counted as an unverified reuse either: it was not reused").isEqualTo(0L);
    assertThat(selfRetrieved).as("the rebuilt graph answers correctly: %d of %d found", selfRetrieved, LIVE)
        .isGreaterThanOrEqualTo(LIVE * 99 / 100);
  }

  /**
   * The one case that still rides on the node count is a graph with no manifest at all - written by a version older
   * than this mechanism, or restored from a backup, which does not carry the sidecar. It is reused, because the
   * alternative is a full rebuild of every existing index on the first open after an upgrade, and the reuse is
   * counted so an operator can ask the stats rather than grep the log for the WARNING.
   */
  @Test
  void aGraphWithNoManifestIsReusedButCounted() {
    createSchema(database);
    insertDocs(database, 0, LIVE);
    buildAndPersistGraph();

    final String databasePath = database.getDatabasePath();
    database.close();

    assertThat(new File(manifestOf(databasePath).toString()).delete())
        .as("remove the sidecar, leaving the graph the way a pre-manifest version wrote it").isTrue();

    database = factory.open();

    final int selfRetrieved = countDocsThatAreTheirOwnNearestNeighbour(liveDocIds());

    assertThat(vectorIndex().getStats().get("graphRebuildCount"))
        .as("its node count matches, so it is reused rather than rebuilt").isEqualTo(0L);
    assertThat(vectorIndex().getStats().get("unverifiedGraphReuses"))
        .as("but the index has to say it is running on the weaker comparison").isEqualTo(1L);
    assertThat(selfRetrieved).as("%d of %d found", selfRetrieved, LIVE).isGreaterThanOrEqualTo(LIVE * 99 / 100);
  }

  /**
   * The sidecar sits next to the graph file and its name ends in the graph file's own extension plus one more, so
   * the only thing keeping the {@code FileManager} from opening it as a page-backed component is that the scan
   * matches on the LAST extension. Everything downstream of that rule - the backup enumeration, the HA
   * {@code /checksums} walk - follows. Its temporaries have to be as invisible, or a crash between the write and
   * the atomic move would leave something a scan might try to open.
   */
  @Test
  void theSidecarIsNeverMistakenForAComponentFile() {
    createSchema(database);
    insertDocs(database, 0, 8);
    buildAndPersistGraph();

    final String graph = graphFileIn(database.getDatabasePath()).getName();
    assertThat(LocalDatabase.isComponentFileName(graph)).as("the graph file itself IS one").isTrue();
    assertThat(LocalDatabase.isComponentFileName(graph + "." + LSMVectorIndexGraphManifest.FILE_EXT))
        .as("its manifest is not, or the FileManager would open a JSON file as pages").isFalse();
    assertThat(LocalDatabase.isComponentFileName(graph + "." + LSMVectorIndexGraphManifest.FILE_EXT + ".1a2b.tmp"))
        .as("and neither is a temporary left behind by an interrupted manifest write").isFalse();

    assertThat(((DatabaseInternal) database).getFileManager().getFiles().stream()
        .anyMatch(f -> f != null && LSMVectorIndexGraphManifest.FILE_EXT.equals(f.getFileExtension())))
        .as("nothing registered the sidecar as a component, so backup and file shipping never see it").isFalse();
  }

  /**
   * The fingerprint has to cover the RIDs and not only the vector ids: dense {@code [0, N)} is precisely the id list
   * that every generation of a compacted index produces, so ids alone cannot tell two of them apart.
   */
  @Test
  void theFingerprintSeparatesTwoGenerationsCarryingTheSameDenseIds() {
    final int[] denseIds = new int[LIVE];
    for (int i = 0; i < LIVE; i++)
      denseIds[i] = i;

    final long first = LSMVectorIndexGraphManifest.fingerprintOf(denseIds,
        id -> new com.arcadedb.database.RID(3, id));
    final long second = LSMVectorIndexGraphManifest.fingerprintOf(denseIds,
        id -> new com.arcadedb.database.RID(3, id + RID_SHIFT));

    assertThat(second).as("same ids, other records: the fingerprint must not match").isNotEqualTo(first);
    assertThat(LSMVectorIndexGraphManifest.fingerprintOf(denseIds, id -> new com.arcadedb.database.RID(3, id)))
        .as("and it must be stable for the same correspondence").isEqualTo(first);
  }

  // ------------------------------------------------------------------------------------------------- helpers

  /**
   * Builds, in a throwaway database, a graph over records that are not the ones the test database holds, and drops
   * it onto the test database's graph file. With {@code nodes == LIVE} the two are indistinguishable by any count:
   * same number of nodes, and the same dense vector ids {@code [0, LIVE)}, over different records. That is exactly
   * the pair the old check could not reject.
   *
   * @param databasePath the test database, closed
   * @param nodes        how many vectors the donor graph should cover
   * @param withManifest whether the donor's manifest travels with its graph, as the two would survive a crash
   *                     together; false leaves the graph unaccompanied, which is what a graph persisted by a
   *                     version older than the manifest looks like
   */
  private void installGraphOfAnotherGeneration(final String databasePath, final int nodes,
      final boolean withManifest) throws IOException {
    final String donorPath = getDatabasePath() + "-donor";
    FileUtils.deleteRecursively(new File(donorPath));

    final DatabaseFactory donorFactory = new DatabaseFactory(donorPath);
    final Database donor = donorFactory.create();
    try {
      // The bucket positions are consumed BEFORE the vector index exists, so these records never reach it: the
      // donor index ends up with dense ids [0, nodes) exactly like the test database, over higher RIDs.
      donor.transaction(() -> {
        donor.command("sql", "CREATE DOCUMENT TYPE Doc");
        donor.command("sql", "CREATE PROPERTY Doc.id STRING");
        donor.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      });
      donor.transaction(() -> {
        for (int i = 0; i < RID_SHIFT; i++)
          donor.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "filler" + i, embedding(100_000 + i));
        donor.command("sql", "DELETE FROM Doc");
      });

      donor.transaction(() -> {
        final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) donor.getSchema()
            .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
        builder.withDimensions(DIMENSIONS).withEfSearch(EF_SEARCH).create();
      });

      insertDocs(donor, LIVE, LIVE + nodes);
      vectorIndexOf(donor).buildVectorGraphNow();

      final LSMVectorIndex index = vectorIndexOf(donor);
      assertThat(index.getVectorIndex().size()).as("the donor graph must cover %d vectors", nodes).isEqualTo(nodes);
      assertThat(index.getVectorIndex().getAllVectorIds().max().orElse(-1))
          .as("and carry dense ids, so no count can tell the two graphs apart").isEqualTo(nodes - 1);

      final String path = donor.getDatabasePath();
      donor.close();

      Files.copy(graphFileIn(path).toPath(), graphFileIn(databasePath).toPath(),
          StandardCopyOption.REPLACE_EXISTING);

      Files.deleteIfExists(manifestOf(databasePath));
      if (withManifest) {
        assertThat(manifestOf(path)).as("the donor must have recorded what its own graph covers").exists();
        Files.copy(manifestOf(path), manifestOf(databasePath), StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      if (donor.isOpen())
        donor.close();
      FileUtils.deleteRecursively(new File(donorPath));
    }
  }

  private void createSchema(final Database db) {
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE Doc");
      db.command("sql", "CREATE PROPERTY Doc.id STRING");
      db.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");

      final TypeLSMVectorIndexBuilder builder = (TypeLSMVectorIndexBuilder) db.getSchema()
          .buildTypeIndex("Doc", new String[] { "embedding" }).withLSMVectorType();
      builder.withDimensions(DIMENSIONS).withEfSearch(EF_SEARCH).create();
    });
  }

  private void insertDocs(final Database db, final int fromInclusive, final int toExclusive) {
    db.transaction(() -> {
      for (int i = fromInclusive; i < toExclusive; i++)
        db.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i));
    });
  }

  private void buildAndPersistGraph() {
    vectorIndex().buildVectorGraphNow();
  }

  private List<String> liveDocIds() {
    final List<String> ids = new ArrayList<>();
    final ResultSet rs = database.query("sql", "SELECT id FROM Doc");
    while (rs.hasNext())
      ids.add(rs.next().getProperty("id"));
    return ids;
  }

  /**
   * A record's own embedding is at distance zero from itself, so a graph that describes the live set returns it
   * first, every time. A graph whose ordinals address other records leaves the beam walking edges that mean nothing
   * for these vectors, and the count collapses towards the beam width over the node count.
   */
  private int countDocsThatAreTheirOwnNearestNeighbour(final List<String> docIds) {
    final int[] found = { 0 };
    database.transaction(() -> {
      for (final String id : docIds) {
        final int doc = Integer.parseInt(id.substring("doc".length()));
        final ResultSet rs = database.query("sql",
            "SELECT `vector.neighbors`('Doc[embedding]', ?, 1) AS neighbors FROM schema:types LIMIT 1",
            (Object) embedding(doc));
        assertThat(rs.hasNext()).isTrue();
        final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
        if (neighbors != null && !neighbors.isEmpty() && id.equals(neighbors.get(0).get("id")))
          found[0]++;
      }
    });
    return found[0];
  }

  /**
   * Pseudo-random and unrelated between records on purpose: embeddings built from a smooth function of the record
   * number describe the same manifold whatever the ordinal offset, so a foreign graph would keep answering
   * correctly for the wrong reason.
   */
  private static float[] embedding(final int doc) {
    final Random random = new Random(0x6106L * 31 + doc);
    final float[] v = new float[DIMENSIONS];
    for (int j = 0; j < DIMENSIONS; j++)
      v[j] = (float) random.nextGaussian();
    return v;
  }

  private LSMVectorIndex vectorIndex() {
    return vectorIndexOf(database);
  }

  private static LSMVectorIndex vectorIndexOf(final Database db) {
    return (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
  }

  private static File graphFileIn(final String databasePath) {
    final File[] files = new File(databasePath)
        .listFiles((dir, name) -> name.endsWith("." + LSMVectorIndexGraphFile.FILE_EXT));
    assertThat(files).as("the index must have persisted its graph").isNotNull().hasSize(1);
    return files[0];
  }

  private static Path manifestOf(final String databasePath) {
    return Path.of(graphFileIn(databasePath).getPath() + "." + LSMVectorIndexGraphManifest.FILE_EXT);
  }
}
