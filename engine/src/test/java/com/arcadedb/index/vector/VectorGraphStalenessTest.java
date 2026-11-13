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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.StallAwareStopwatch;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Writes, compaction and stale graphs. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorGraphStalenessTest {
  /**
   * Regression for #6105: write after compaction.
   * <p>
   * Issue #6105: a write issued in the same session right after {@code COMPACT INDEX} on an {@code LSM_VECTOR} index
   * was silently not indexed.
   * <p>
   * An {@link LSMVectorIndex} is named after the component file it holds, and a compaction swaps that file in - so
   * the compaction reassigns {@code indexName} to the new component's name. {@code LocalSchema.indexMap} is keyed by
   * the name the index was registered under, and nothing re-keyed it, so after a compaction the index was reachable
   * in the schema only under a name it no longer answers to. Every index operation of a transaction is queued on
   * {@link com.arcadedb.database.TransactionIndexContext} under {@code index.getName()} and
   * {@code TransactionIndexContext.commit()} opens by dropping the lanes of indexes that no longer exist in the
   * schema (the TYPE DROP case) - which matched the freshly compacted vector index exactly, so its queued entries
   * were discarded without a word. The record itself was written, and a reopen (which rebuilds {@code indexMap} from
   * the persisted names) made writes land again, which is what made this look like a phantom.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class WriteAfterCompaction extends TestHelper {

    private static final int DIMENSIONS = 16;
    private static final int VERTICES   = 100;

    /**
     * The name the index answers to must be the name the schema knows it by, at every moment of its life - a
     * compaction included. This is the structural invariant the two behavioural tests below depend on.
     */
    @Test
    void aCompactedIndexIsStillReachableUnderTheNameItAnswersTo() {
      createSchema();
      insertVertices();

      final LSMVectorIndex index = vectorIndex();
      final String nameBefore = index.getName();
      assertThat(database.getSchema().existsIndex(nameBefore)).as("registered before the compaction").isTrue();

      database.command("sql", "COMPACT INDEX `Doc[embedding]`");

      final String nameAfter = index.getName();
      assertThat(nameAfter).as("a compaction renames the index after the file it swapped in").isNotEqualTo(nameBefore);
      assertThat(database.getSchema().existsIndex(nameAfter))
          .as("the schema must know the compacted index under its current name").isTrue();
      assertThat(database.getSchema().getIndexByName(nameAfter))
          .as("and must resolve that name to this very index instance").isSameAs(index);
      assertThat(database.getSchema().existsIndex(nameBefore))
          .as("the pre-compaction name must not linger in the schema").isFalse();
    }

    /**
     * An INSERT in the same session right after a compaction must reach the vector index: the document is created
     * either way, so a silent miss shows up only as a vector count that never moved and a vector search that cannot
     * find the new document.
     */
    @Test
    void anInsertRightAfterACompactionIsIndexed() {
      createSchema();
      insertVertices();

      database.command("sql", "COMPACT INDEX `Doc[embedding]`");
      assertThat(countLive()).as("compaction must not lose or gain live vectors").isEqualTo(VERTICES);

      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + VERTICES,
          embedding(VERTICES, 0)));

      assertThat(countLive()).as("the insert right after the compaction must reach the vector index")
          .isEqualTo(VERTICES + 1);

      final RID rid = ridOf("doc" + VERTICES);
      assertThat(vectorIndex().getVectorIndex().getVectorIdsForRid(rid))
          .as("exactly one vector id for the newly inserted document").hasSize(1);

      assertSearchWorks(VERTICES, 0);
    }

    /**
     * Same for an UPDATE: the re-embedded document must be found by its NEW vector, not only by the one the
     * compaction persisted. A dropped index operation leaves the live count unchanged AND the old vector in place,
     * so the count alone is not enough - the search below is what proves the new embedding actually landed.
     */
    @Test
    void anUpdateRightAfterACompactionIsIndexed() {
      createSchema();
      insertVertices();

      database.command("sql", "COMPACT INDEX `Doc[embedding]`");

      final RID rid = ridOf("doc0");
      final int[] idsBefore = vectorIndex().getVectorIndex().getVectorIdsForRid(rid);
      assertThat(idsBefore).as("doc0 holds one vector before the update").hasSize(1);

      // Move doc0 far away from where it was, into a region no other document occupies.
      final float[] relocated = embedding(VERTICES + 7, 0);
      database.transaction(() -> database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", relocated, "doc0"));

      assertThat(countLive()).as("an update replaces a vector, it does not add one").isEqualTo(VERTICES);

      final int[] idsAfter = vectorIndex().getVectorIndex().getVectorIdsForRid(rid);
      assertThat(idsAfter).as("doc0 still holds exactly one vector after the update").hasSize(1);
      assertThat(idsAfter[0]).as("the update must have minted a new vector id for doc0").isNotEqualTo(idsBefore[0]);

      assertThat(nearestId(relocated)).as("doc0 must be found at its new embedding").isEqualTo("doc0");
    }

    /**
     * The same loss, reached the way production reaches it: a compaction is scheduled automatically after a commit
     * ({@code LSMVectorIndex.onAfterCommit}) and runs on the async executor, so it can rename the index between an
     * entry being queued on an open transaction and that transaction committing. Nothing in the writer's own session
     * says a compaction happened - which is why the index reference the transaction captured, not the name it was
     * queued under, has to be what resolves the entry at commit time.
     */
    @Test
    void aWriteQueuedBeforeACompactionOnAnotherThreadIsStillIndexed() throws Exception {
      createSchema();
      insertVertices();

      final String nameWhenQueued = vectorIndex().getName();

      database.begin();
      database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + VERTICES, embedding(VERTICES, 0));

      // The compaction refuses to run inside a transaction, so it has to come from another thread - exactly where the
      // async executor runs it.
      final Throwable[] failure = new Throwable[1];
      final Thread compactor = new Thread(() -> {
        try {
          database.command("sql", "COMPACT INDEX `Doc[embedding]`");
        } catch (final Throwable t) {
          failure[0] = t;
        }
      }, "issue6105-compactor");
      compactor.start();
      compactor.join(120_000);

      assertThat(compactor.isAlive()).as("the compaction must have finished").isFalse();
      assertThat(failure[0]).as("the compaction must not have failed").isNull();
      assertThat(vectorIndex().getName()).as("the compaction must have renamed the index").isNotEqualTo(nameWhenQueued);

      database.commit();

      assertThat(countLive()).as("a write queued before the compaction must still reach the vector index")
          .isEqualTo(VERTICES + 1);
      assertSearchWorks(VERTICES, 0);
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private void createSchema() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.id STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

        // Automatic compaction and background rebuilds are disabled: only the explicit COMPACT INDEX below must
        // rewrite the data file, and the assertions must observe the write path, not a rebuild that papered over it.
        database.command("sql", """
            CREATE INDEX ON Doc (embedding) LSM_VECTOR
            METADATA {
              "dimensions": %d,
              "similarity": "COSINE",
              "quantization": "INT8",
              "mutationsBeforeRebuild": 100000000,
              "inactivityRebuildTimeoutMs": 0
            }""".formatted(DIMENSIONS));
      });
    }

    private void insertVertices() {
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i, 0));
      });
    }

    /** Well-separated vectors: the nearest neighbor of vertex i must be vertex i itself. */
    private static float[] embedding(final int vertex, final int cycle) {
      final float[] v = new float[DIMENSIONS];
      for (int j = 0; j < DIMENSIONS; j++)
        v[j] = (float) Math.sin((vertex + 1) * 0.37 * (j + 1) + cycle * 0.0001);
      return v;
    }

    private LSMVectorIndex vectorIndex() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    }

    private long countLive() {
      return vectorIndex().countEntries();
    }

    private RID ridOf(final String id) {
      final RID[] holder = new RID[1];
      database.transaction(() -> holder[0] = database.query("sql", "SELECT FROM Doc WHERE id = ?", id).next()
          .getIdentity().get());
      return holder[0];
    }

    private String nearestId(final float[] target) {
      final String[] holder = new String[1];
      database.transaction(() -> {
        final ResultSet rs = database.query("sql",
            "SELECT `vector.neighbors`('Doc[embedding]', ?, 3) AS neighbors FROM Doc LIMIT 1", (Object) target);
        assertThat(rs.hasNext()).isTrue();
        final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
        assertThat(neighbors).as("neighbors must not be empty").isNotEmpty();
        holder[0] = (String) neighbors.getFirst().get("id");
      });
      return holder[0];
    }

    private void assertSearchWorks(final int vertex, final int cycle) {
      assertThat(nearestId(embedding(vertex, cycle))).as("closest vector to doc%d", vertex).isEqualTo("doc" + vertex);
    }
  }

  /**
   * Regression for #6106: stale vector graph.
   * <p>
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
  @Nested
  @Tag("vector")
  class StaleVectorGraph extends TestHelper {
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
     * than this mechanism, or restored from a backup taken before #9888 (a backup now archives the graph empty rather
     * than without its sidecar, so the restored index builds its own). It is reused, because the
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
          if (neighbors != null && !neighbors.isEmpty() && id.equals(neighbors.getFirst().get("id")))
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

  /**
   * Regression for #5870: vector index id renumbering.
   * <p>
   * Issue #5870: {@code LSMVectorIndex.rewriteDataFileWithLiveEntries} renumbers the live set densely from 0 during
   * a compaction instead of preserving each vector's pre-compaction id, so the id space is dense by construction
   * after every compaction rather than merely bounding the partially drained band trailing the live region (the
   * residual weakness issue #5588 left open).
   * <p>
   * Both files a compaction replaces - the previous data file and the legacy compacted component - are dropped
   * afterwards, and {@code publishLocationIndex} always rebuilds {@link VectorLocationIndex} fresh rather than
   * refilling it in place, so no on-disk tombstone entry and no in-memory {@code DeletedIds} bit for a discarded or
   * reissued id survives a compaction: that is what makes reissuing ids safe rather than merely convenient.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("slow")
  class VectorIndexIdRenumbering extends TestHelper {

    private static final int DIMENSIONS = 16;
    private static final int VERTICES   = 300;

    /**
     * Acceptance criterion 1: {@code getMaxVectorId() == size() - 1} after a compaction. Acceptance criterion 2: no
     * partially drained band survives a compaction - {@code chunkCount() == ceil(live / CHUNK_SIZE)}.
     * <p>
     * The "before" state is built the way a real re-embedding workload builds it (issue #5516/#5588): one vertex
     * ({@code doc0}) is updated hundreds of times, so its live id climbs far past the dense block the other
     * {@value #VERTICES}{@code - 1} vertices still occupy. Every chunk behind it drains and releases wholesale (the
     * existing #5516 mechanism already handles that), but the chunk holding its current id is a lone straggler - the
     * one thing only a renumbering compaction removes.
     */
    @Test
    void compactionMakesTheLiveIdSpaceDenseWithNoTrailingStraggler() {
      createSchema();
      insertVertices();

      final LSMVectorIndex index = vectorIndex();
      final int denseChunksForLiveSet =
          (VERTICES + VectorLocationIndex.CHUNK_SIZE - 1) / VectorLocationIndex.CHUNK_SIZE;

      // Re-embed doc0 enough times that its live id crosses at least one chunk boundary past the dense block the
      // other vertices still occupy (ids 1..VERTICES-1, i.e. chunks 0..denseChunksForLiveSet-1). One transaction per
      // update, like a real re-embedding workload issues it: each commit tombstones doc0's current id and mints
      // exactly one new one.
      final int updateCycles = VERTICES + 2 * VectorLocationIndex.CHUNK_SIZE;
      for (int cycle = 0; cycle < updateCycles; cycle++) {
        final int c = cycle;
        database.transaction(() -> database.command("sql", "UPDATE Doc SET embedding = ? WHERE id = ?", embedding(0, c + 1), "doc0"));
      }

      assertThat(index.getVectorIndex().size()).as("re-embedding must not change the live count").isEqualTo(VERTICES);
      assertThat(index.getVectorIndex().chunkCount())
          .as("before compaction: the dense block plus one straggler chunk trailing far behind it")
          .isGreaterThan(denseChunksForLiveSet);

      database.command("sql", "COMPACT INDEX `Doc[embedding]`");

      assertThat(index.getVectorIndex().size()).as("compaction must not lose or gain live vectors").isEqualTo(VERTICES);
      assertThat(index.getVectorIndex().getMaxVectorId())
          .as("the live set occupies exactly [0, size()) after a renumbering compaction")
          .isEqualTo(index.getVectorIndex().size() - 1);
      assertThat(index.getVectorIndex().chunkCount())
          .as("no partially drained band survives a renumbering compaction")
          .isEqualTo(denseChunksForLiveSet);

      assertSearchWorks(0, updateCycles);
      assertSearchWorks(VERTICES / 2, 0);

      reopenDatabase();

      final LSMVectorIndex reopened = vectorIndex();
      assertThat(reopened.getVectorIndex().size()).as("density survives a reopen").isEqualTo(VERTICES);
      assertThat(reopened.getVectorIndex().getMaxVectorId()).isEqualTo(reopened.getVectorIndex().size() - 1);
      assertSearchWorks(0, updateCycles);
      assertSearchWorks(VERTICES / 2, 0);
    }

    /**
     * Acceptance criterion 4: a tombstone written before a renumbering cannot suppress a vector that inherits its id
     * afterwards.
     * <p>
     * Which document ends up holding vector id 0 after the initial insert is an implementation detail (a single
     * transaction's queued index operations are not guaranteed to replay in statement order), so this test does not
     * assume "doc0" got id 0. It reads the id assignment back from the location index instead, and deletes exactly
     * the documents holding the lowest half of the ids - guaranteeing id 0 itself is tombstoned before the
     * compaction. Renumbering then reissues id 0 to whichever survivor held the smallest surviving old id: without
     * the fix that survivor keeps its old (higher) id and id 0 stays absent; with the fix it inherits id 0, the exact
     * integer a tombstone referred to moments earlier.
     */
    @Test
    void aTombstoneWrittenBeforeRenumberingCannotSuppressAVectorThatInheritsItsId() {
      createSchema();
      insertVertices();

      final int deleted = VERTICES / 2;
      final int[] vertexOfLowestIds = resolveVertexIndicesOfLowestIds(deleted);
      database.transaction(() -> {
        for (final int vertex : vertexOfLowestIds)
          database.command("sql", "DELETE FROM Doc WHERE id = ?", "doc" + vertex);
      });
      assertThat(countLive()).isEqualTo(VERTICES - deleted);

      database.command("sql", "COMPACT INDEX `Doc[embedding]`");
      assertThat(countLive()).as("compaction must not resurrect the deleted half").isEqualTo(VERTICES - deleted);

      final LSMVectorIndex index = vectorIndex();
      final VectorLocationIndex locations = index.getVectorIndex();

      // Id 0 is reissued to a survivor by a renumbering compaction, and it must NOT read back as deleted even though
      // a tombstone for id 0 - a different vector entirely - was written moments before the compaction.
      assertThat(locations.isLive(0)).as("id 0 must be live: it was reissued to a surviving vector").isTrue();
      assertThat(locations.isDeleted(0)).as("no stale tombstone for the discarded owner of id 0 may survive").isFalse();

      final RID survivorRid = locations.getRid(0);
      assertThat(survivorRid).as("id 0 must resolve to a document").isNotNull();
      final int[] survivorVertexHolder = new int[1];
      database.transaction(() -> {
        final Document survivor = (Document) database.lookupByRID(survivorRid, true);
        final String survivorId = survivor.getString("id");
        survivorVertexHolder[0] = Integer.parseInt(survivorId.substring("doc".length()));
      });
      final int survivorVertex = survivorVertexHolder[0];
      boolean wasDeleted = false;
      for (final int vertex : vertexOfLowestIds)
        wasDeleted |= vertex == survivorVertex;
      assertThat(wasDeleted).as("id 0's owner must be one of the survivors, not one of the deleted vertices").isFalse();

      // And the survivor is not silently buried: it is still found as its own nearest neighbor.
      assertSearchWorks(survivorVertex, 0);

      reopenDatabase();

      assertThat(countLive()).as("the reissued id must not resurrect a deleted vector after a reopen")
          .isEqualTo(VERTICES - deleted);
      assertSearchWorks(survivorVertex, 0);
    }

    /**
     * Exercises the dense sequence from the insert side rather than only observing it through
     * {@code getMaxVectorId()}: a compaction renumbers the live set to exactly {@code [0, VERTICES)}, and the next
     * insert must mint id {@code VERTICES} - not collide with the vector that now legitimately holds
     * {@code VERTICES - 1} (an off-by-one in the reset would do exactly that), and not merely resume from whatever
     * the pre-compaction high-water mark used to be (which would still be correct but would defeat the point of
     * renumbering: the id space would grow unboundedly again on the very first write after every compaction).
     * <p>
     * The insert runs after a {@link #reopenDatabase()}, not in the same session the compaction ran in. That was
     * originally forced on this test: a write issued immediately after {@code COMPACT INDEX} in the same session did
     * not reach the vector index at all, independently of this fix, which was filed and fixed separately as issue
     * #6105 (the schema's index registry was left keyed by the name the compaction retired) and is pinned by
     * {@code VectorGraphStalenessTest.WriteAfterCompaction}. The reopen is kept because it is what this test is actually about:
     * it forces {@code loadVectorsFromPages} to recompute {@code nextId} from the (now dense) persisted file, which
     * is the code path a real "compact, then restart, then keep writing" operational sequence takes.
     */
    @Test
    void insertingAfterARenumberingCompactionDoesNotCollideWithAnExistingId() {
      createSchema();
      insertVertices();

      database.command("sql", "COMPACT INDEX `Doc[embedding]`");
      assertThat(vectorIndex().getVectorIndex().getMaxVectorId()).as("the live set is dense after compaction")
          .isEqualTo(VERTICES - 1);

      reopenDatabase();

      final RID ridHoldingTheHighestId = vectorIndex().getVectorIndex().getRid(VERTICES - 1);
      assertThat(ridHoldingTheHighestId).isNotNull();

      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + VERTICES,
          embedding(VERTICES, 0)));

      final RID[] newRidHolder = new RID[1];
      database.transaction(() -> newRidHolder[0] = database.query("sql", "SELECT FROM Doc WHERE id = ?", "doc" + VERTICES)
          .next().getIdentity().get());
      final int[] newIds = vectorIndex().getVectorIndex().getVectorIdsForRid(newRidHolder[0]);
      assertThat(newIds).as("exactly one id for the new vector").hasSize(1);
      assertThat(newIds[0]).as("the next insert after a renumbering compaction must continue the dense sequence")
          .isEqualTo(VERTICES);

      assertThat(vectorIndex().getVectorIndex().getRid(VERTICES - 1))
          .as("the new insert must not have overwritten the vector that already held id VERTICES-1")
          .isEqualTo(ridHoldingTheHighestId);
      assertThat(countLive()).as("both the pre-existing and the new vector must be live").isEqualTo(VERTICES + 1);

      assertSearchWorks(VERTICES, 0);
      assertSearchWorks(VERTICES / 2, 0);
    }

    // ------------------------------------------------------------------------------------------------- helpers

    private void createSchema() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.id STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON Doc (id) UNIQUE");

        // Automatic compaction of vector indexes is disabled (LSMVectorIndex.onAfterCommit): only the explicit
        // COMPACT INDEX below must trigger the renumbering rewrite this test is about.
        database.command("sql", """
            CREATE INDEX ON Doc (embedding) LSM_VECTOR
            METADATA {
              "dimensions": %d,
              "similarity": "COSINE",
              "quantization": "INT8",
              "mutationsBeforeRebuild": 100000000,
              "inactivityRebuildTimeoutMs": 0
            }""".formatted(DIMENSIONS));
      });
    }

    private void insertVertices() {
      database.transaction(() -> {
        for (int i = 0; i < VERTICES; i++)
          database.command("sql", "INSERT INTO Doc SET id = ?, embedding = ?", "doc" + i, embedding(i, 0));
      });
    }

    /** Well-separated vectors: the nearest neighbor of vertex i must be vertex i itself. */
    private static float[] embedding(final int vertex, final int cycle) {
      final float[] v = new float[DIMENSIONS];
      for (int j = 0; j < DIMENSIONS; j++)
        v[j] = (float) Math.sin((vertex + 1) * 0.37 * (j + 1) + cycle * 0.0001);
      return v;
    }

    private LSMVectorIndex vectorIndex() {
      return (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets()[0];
    }

    private long countLive() {
      return vectorIndex().countEntries();
    }

    /**
     * The vertex index (the {@code N} in {@code "docN"}) of the {@code count} documents currently holding the
     * lowest-numbered vector ids, resolved through the location index rather than assumed from insertion order.
     */
    private int[] resolveVertexIndicesOfLowestIds(final int count) {
      final VectorLocationIndex locations = vectorIndex().getVectorIndex();
      final int[] lowestIds = locations.getAllVectorIds().limit(count).toArray(); // already ascending
      final int[] vertices = new int[lowestIds.length];
      database.transaction(() -> {
        for (int i = 0; i < lowestIds.length; i++) {
          final Document doc = (Document) database.lookupByRID(locations.getRid(lowestIds[i]), true);
          vertices[i] = Integer.parseInt(doc.getString("id").substring("doc".length()));
        }
      });
      return vertices;
    }

    private void assertSearchWorks(final int vertex, final int cycle) {
      database.transaction(() -> {
        final ResultSet rs = database.query("sql",
            "SELECT `vector.neighbors`('Doc[embedding]', ?, 3) AS neighbors FROM Doc LIMIT 1",
            (Object) embedding(vertex, cycle));
        assertThat(rs.hasNext()).isTrue();
        final List<Map<String, Object>> neighbors = rs.next().getProperty("neighbors");
        assertThat(neighbors).as("neighbors of doc%d", vertex).isNotEmpty();
        assertThat(neighbors.getFirst().get("id")).as("closest vector to doc%d", vertex).isEqualTo("doc" + vertex);
      });
    }
  }

  /**
   * Regression for #6655: reuse of a stale persisted graph prefix.
   * <p>
   * Regression test for issue #6655: a follow-up to #6067's close-time deferral. {@code ensureGraphAvailable()}'s
   * stale-persisted-graph fallback (issue #3722 - a persisted graph with fewer nodes than there are live vectors,
   * because vectors were added after the graph was last built and persisted) called {@code buildGraphFromScratch()}
   * unconditionally and SYNCHRONOUSLY, with no size-based deferral of its own - unlike
   * {@link LSMVectorIndex#rebuildGraphBeforeSearch()}, which already prefers an async rebuild above
   * {@code ASYNC_REBUILD_MIN_GRAPH_SIZE} (1000) vectors for the identical staleness reached mid-session.
   * <p>
   * Net effect before the fix: for a large index, the FIRST search after a reopen that finds a stale persisted graph
   * pays the full rebuild cost inline, on the calling search thread - the same wall-clock cost #6067 moved off
   * {@code close()}, just relocated to whichever query happens to run first. The fix reuses the persisted graph
   * immediately as a verified PREFIX of the live set (manifest-verified, no deletions) instead, queues the vectors
   * past it into the delta buffer so nothing is silently missing from the answer, and kicks the same async rebuild
   * {@code rebuildGraphBeforeSearch()} would use mid-session.
   * <p>
   * Two things are pinned, because either alone would pass against a half-fix: the first search must return promptly
   * (not merely "eventually", which an async-but-still-blocking variant could also satisfy) AND it must already
   * answer correctly for a vector written after the persisted build - proving the fix does not trade away the very
   * data the synchronous rebuild used to guarantee was found.
   * <p>
   * Same per-vector build parameters and vector count as {@code VectorRebuildLifecycleTest.DeferredCloseRebuild} (128 dimensions,
   * {@code maxConnections=64}, {@code beamWidth=400}, 20,000 vectors), for the same reason: a full rebuild has to
   * reliably take multiple real seconds, or the timing tripwire below cannot fail on the pre-fix code by accident.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class StalePrefixReuse {
    private static final String DB_ROOT         = "target/test-databases/StalePrefixReuse";
    private static final int    DIMENSIONS      = 128;
    private static final int    NUM_VECTORS     = 20_000;
    private static final int    GAP_VECTORS     = 200;
    private static final int    MAX_CONNECTIONS = 64;
    private static final int    BEAM_WIDTH      = 400;

    // How long to let the async rebuild of 20,200 x 128 vectors converge. Sized for the worst case rather
    // than for a developer machine, which is what the @Tag("vector") note in CLAUDE.md asks for: the rebuild
    // has to wait out whatever else in this JVM holds the sole LSMVectorIndex.REBUILD_SEMAPHORE permit before
    // it can even start, and then runs on however many cores the runner has. At 60s a green local run
    // (~10s for this whole class) still went red on a 4-vCPU CI runner with the rebuild reporting itself
    // still in flight (run 33161892670). This is a convergence wait, not a latency bound - it asserts that
    // the rebuild happens, not that it is fast - so a generous value cannot turn a passing run red, and the
    // assertion below distinguishes "still working" from "never ran" for whoever reads the next timeout.
    private static final Duration ASYNC_REBUILD_TIMEOUT = Duration.ofMinutes(4);

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @Test
    void reopenWithMoreVectorsThanThePersistedGraphReusesItAsAPrefixInsteadOfBlockingOnAFullRebuild() throws Exception {
      // Session 1: build and persist a graph over exactly NUM_VECTORS, then close cleanly - the persisted graph and
      // its manifest end up describing precisely the first NUM_VECTORS records.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db, 0, NUM_VECTORS);
          final LSMVectorIndex index = vectorIndex(db);
          index.buildVectorGraphNow();
          assertThat(index.getStats().get("graphState")).as("precondition: graph must be built and IMMUTABLE first")
              .isEqualTo(1L); // GraphState.IMMUTABLE
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // Session 2: write GAP_VECTORS more records (committed, so WAL-durable), then crash instead of closing
      // cleanly. On main, close()/flush() would rebuild the graph synchronously and erase the very staleness this
      // test needs; kill() skips that, leaving the persisted graph and manifest exactly as session 1 left them -
      // still describing only the first NUM_VECTORS records - while the live vector count is now NUM_VECTORS +
      // GAP_VECTORS.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          populate(db, NUM_VECTORS, NUM_VECTORS + GAP_VECTORS);
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphState"))
              .as("precondition: MUTABLE, otherwise this session never reaches a state kill() can usefully crash")
              .isEqualTo(2L); // GraphState.MUTABLE

          // Crash: the on-disk state (files, WAL) is left exactly as an abrupt kill leaves it - the persisted graph
          // and manifest untouched, the extra records only in the WAL. close() right after, on this same already-
          // killed instance, only clears the process-wide active-instance bookkeeping DatabaseFactory.open() checks
          // (issue #6655's test setup, same pattern as UnflushedDictionaryRecoveryTest) - it does not get a chance
          // to flush or rebuild anything the crash already bypassed.
          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // Session 3: reopen. WAL recovery restores all NUM_VECTORS + GAP_VECTORS records; the persisted graph and
      // manifest still describe only the first NUM_VECTORS. This is the reproduction: a stale persisted graph, more
      // live vectors than it covers, no deletions, a manifest-verifiable prefix.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          // The reuse hands its gap to an async rebuild only when that gap crosses the ordinary rebuild threshold
          // (issue #7183): rebuilding every node of a graph for a gap below it is exactly what the threshold exists
          // to refuse, and doing it anyway meant a session that opened, searched once and exited paid a full rebuild
          // for one vector. GAP_VECTORS is 1% of this fixture, so the ratio-scaled threshold - 20% of the graph, 4,000
          // here - would not be crossed. Disabling the scaling leaves the absolute floor of 100, which 200 genuinely
          // exceeds, so the fold-in this test asserts is triggered BY the policy rather than in spite of it.
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

          try (final ResultSet rs = db.query("sql", "SELECT count(*) as cnt FROM Doc")) {
            assertThat(rs.next().<Long>getProperty("cnt"))
                .as("precondition: WAL recovery must have restored every record from the crashed session")
                .isEqualTo((long) (NUM_VECTORS + GAP_VECTORS));
          }

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("precondition: nothing must have rebuilt yet in this fresh session").isZero();
          assertThat(index.getStats().get("totalVectors"))
              .as("precondition: above the async-rebuild threshold, otherwise this is the pre-existing small-graph "
                  + "synchronous path, unaffected by this fix")
              .isGreaterThanOrEqualTo(1000L);

          // The query is one of the gap records' own embedding: a record is always its own nearest neighbour, so
          // finding it in the answer proves the gap vectors are searchable from the very first query - not only
          // once the deferred rebuild eventually runs.
          final int gapDocId = NUM_VECTORS; // first record written after the persisted build
          final RID gapDocRid = ridOf(db, gapDocId);

          // The async rebuild the reuse dispatches resets the very gauges asserted below - deltaVectorsCount,
          // mutationsSinceRebuild, graphState, graphRebuildCount - so the permits are saturated for the whole
          // observation window. Without that, whether these assertions see the state the reuse LEFT or the state the
          // rebuild ALREADY CONSUMED is decided by how quickly this JVM hands out the sole vector-rebuild permit; the
          // block used to pass only because a preceding test's rebuild happened to be starving it. Both acquire sites
          // are tryAcquire on background threads and no search path takes a permit, so this cannot stall the queries.
          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
            final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(gapDocId), 5, 64);
            // Measured on this fixture: a prefix reuse returns in well under a second; a full synchronous rebuild of
            // 20,200 vectors at these build parameters takes several real seconds (see VectorRebuildLifecycleTest.DeferredCloseRebuild
            // for the same measurement on an equivalent fixture). The bound is a tripwire between the two, wide on
            // both sides.
            stopwatch.assertGaveUpWithin(2_000,
                "a search that reuses a stale prefix graph from one that ran a full synchronous rebuild to completion");

            assertThat(results.stream().map(Pair::getFirst))
                .as("the gap record must be found by its own embedding on the very first query, proving the queued "
                    + "delta entries - not only the reused graph - are already searchable")
                .contains(gapDocRid);

            assertThat(index.getStats().get("graphState"))
                .as("the reuse must leave the index MUTABLE (graph + queued delta), not a synchronously-rebuilt "
                    + "IMMUTABLE graph")
                .isEqualTo(2L); // GraphState.MUTABLE
            assertThat(index.getStats().get("stalePrefixGraphReuses"))
                .as("the reuse path introduced by this fix must be the one that actually ran").isEqualTo(1L);
            assertThat(index.getStats().get("graphRebuildCount"))
                .as("no synchronous rebuild may have run on the calling thread for this search to have returned "
                    + "already").isZero();
          } finally {
            // Released before the convergence wait below: that wait is precisely for the rebuild these permits park.
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          }


          // The async rebuild kicked off by the reuse eventually folds the gap into the graph proper.
          Awaitility.await("the async rebuild kicked off by the stale-prefix reuse completes")
              .atMost(ASYNC_REBUILD_TIMEOUT)
              .pollInterval(Duration.ofMillis(200))
              .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount"))
                  .as("graphRebuildCount, with asyncRebuildInProgress=%s: a 1 there means the rebuild is still "
                          + "running or still parked on the JVM-wide REBUILD_SEMAPHORE and this wait is simply "
                          + "too short; a 0 means it was never started or was skipped, which is a real defect",
                      index.getStats().get("asyncRebuildInProgress"))
                  .isEqualTo(1L));

          assertThat(index.getStats().get("graphState"))
              .as("once the deferred rebuild completes the graph goes back to IMMUTABLE").isEqualTo(1L);

          final List<Pair<RID, Float>> afterRebuild = index.findNeighborsFromVector(embedding(gapDocId), 5, 64);
          assertThat(afterRebuild.stream().map(Pair::getFirst))
              .as("the gap record must still be found once it is folded into the graph proper, not only while it "
                  + "sat in the delta buffer")
              .contains(gapDocRid);
        } finally {
          db.drop();
        }
      }
    }

    private static void populate(final Database db, final int fromInclusive, final int toExclusive) {
      db.transaction(() -> {
        if (db.getSchema().existsType("Doc"))
          return;
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\", \"maxConnections\": " + MAX_CONNECTIONS
            + ", \"beamWidth\": " + BEAM_WIDTH + " }");
      });

      db.begin();
      for (int i = fromInclusive; i < toExclusive; i++) {
        db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
        if ((i - fromInclusive) % 1000 == 999) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }

    /** Deterministic per-id embedding, so any record's own vector can be reconstructed independently at query time. */
    private static float[] embedding(final int id) {
      final Random random = new Random(0x6655L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static RID ridOf(final Database db, final int id) {
      try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
        assertThat(rs.hasNext()).as("the gap record must exist").isTrue();
        return rs.next().getProperty("@rid");
      }
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #6772: prefix reuse after a write that precedes the first search.
   * <p>
   * Regression test for issue #6772: the ingest-then-query arm of issue #6655's stale-prefix reuse.
   * <p>
   * {@code put()}/{@code putBatch()}/{@code remove()} promote {@code graphState} from {@code LOADING} straight to
   * {@code MUTABLE} on the first write of a session. {@code ensureGraphAvailable()} used to return at its first
   * statement on anything but {@code LOADING}, so a session that wrote before it searched never loaded the persisted
   * graph at all: {@code graphIndex} stayed null, #6655's prefix test was unreachable - not because any of its guards
   * failed, but because the method exited before reaching them - and {@code rebuildGraphBeforeSearch()} then read that
   * null as "small graph" and rebuilt the WHOLE index synchronously on the search thread. Which is the ordinary
   * ingest-then-query shape, and the one #6655's own test does not cover: its searches follow writes made by a
   * PREVIOUS session, never one in the searching session.
   * <p>
   * Three things are pinned, because no one of them alone would fail on the pre-fix code for the right reason:
   * <ul>
   *   <li>the reuse counter and the graph's node count, which say the persisted prefix was reused rather than the
   *       whole live set rebuilt - the two state facts the issue reports as stable across runs while wall clock is
   *       not;</li>
   *   <li>the delta buffer holding each pending vector exactly once, which is what a naive "make the gate reachable"
   *       fix gets wrong: this session's own writes are already in the buffer AND inside the gap past the persisted
   *       prefix, so queuing the gap wholesale would duplicate every one of them;</li>
   *   <li>the answer itself - both a vector written by the crashed session and one written by THIS session, before
   *       the search, must come back from the very first query.</li>
   * </ul>
   * <p>
   * Same fixture as {@code StalePrefixReuse} (128 dimensions, {@code maxConnections=64},
   * {@code beamWidth=400}, 20,000 vectors), for the same reason: a full rebuild has to reliably take multiple real
   * seconds, or the timing tripwire cannot fail on the pre-fix code by accident.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class WriteBeforeSearchPrefixReuse {
    private static final String DB_ROOT         = "target/test-databases/WriteBeforeSearchPrefixReuse";
    private static final int    DIMENSIONS      = 128;
    private static final int    NUM_VECTORS     = 20_000;
    private static final int    GAP_VECTORS     = 200;
    private static final int    MAX_CONNECTIONS = 64;
    private static final int    BEAM_WIDTH      = 400;

    // How long to let the async rebuild of 20,201 x 128 vectors converge. Sized for the worst case rather
    // than for a developer machine, which is what the @Tag("vector") note in CLAUDE.md asks for: the rebuild
    // has to wait out whatever else in this JVM holds the sole LSMVectorIndex.REBUILD_SEMAPHORE permit before
    // it can even start, and then runs on however many cores the runner has. At 60s a green local run
    // (~20s for this whole class) still went red on a 4-vCPU CI runner with the rebuild reporting itself
    // still in flight (run 33161892670). This is a convergence wait, not a latency bound - it asserts that
    // the rebuild happens, not that it is fast - so a generous value cannot turn a passing run red, and the
    // assertions below distinguish "still working" from "never ran" for whoever reads the next timeout.
    private static final Duration ASYNC_REBUILD_TIMEOUT = Duration.ofMinutes(4);

    /** The single record the searching session writes before it searches - the whole point of this test. */
    private static final int IN_SESSION_DOC_ID = NUM_VECTORS + GAP_VECTORS;

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @Test
    void aSessionThatInsertsBeforeItSearchesStillReusesThePersistedGraphAsAPrefix() throws Exception {
      buildStalePersistedGraphFixture();

      // Session 3: reopen, INSERT ONE VECTOR, and only then search. That single insert is the difference between
      // this test and StalePrefixReuse, and on the pre-fix code it is enough to lose the reuse
      // entirely: the write promotes graphState to MUTABLE, ensureGraphAvailable() returns at its first statement
      // for the rest of the session, and the first search rebuilds all 20,201 vectors synchronously.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          // The reuse hands its gap to an async rebuild only when that gap crosses the ordinary rebuild threshold
          // (issue #7183): rebuilding every node of a graph for a gap below it is exactly what the threshold exists
          // to refuse, and doing it anyway meant a session that opened, searched once and exited paid a full rebuild
          // for one vector. The gap here is 1% of the fixture, so the ratio-scaled threshold - 20% of the graph, 4,000
          // here - would not be crossed. Disabling the scaling leaves the absolute floor of 100, which the 201 pending
          // vectors genuinely exceed, so the fold-in this test asserts is triggered BY the policy, not in spite of it.
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

          // count(`@rid`) rather than count(*): the latter answers from the maintained counter, which is written by
          // the very session kill() interrupted, so it can agree with the expected number while the records are
          // absent. This precondition is about what WAL recovery actually restored, so it has to count records.
          try (final ResultSet rs = db.query("sql", "SELECT count(`@rid`) as cnt FROM Doc")) {
            assertThat(rs.next().<Long>getProperty("cnt"))
                .as("precondition: WAL recovery must have restored every record from the crashed session")
                .isEqualTo((long) (NUM_VECTORS + GAP_VECTORS));
          }

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("precondition: nothing must have rebuilt yet in this fresh session").isZero();
          assertThat(index.getStats().get("graphState"))
              .as("precondition: the graph is still unloaded - no search has run in this session yet")
              .isEqualTo(0L); // GraphState.LOADING

          populate(db, IN_SESSION_DOC_ID, IN_SESSION_DOC_ID + 1);

          assertThat(index.getStats().get("graphState"))
              .as("precondition: the in-session write must have promoted the state past LOADING - that promotion "
                  + "is exactly what used to make the persisted graph unreachable")
              .isEqualTo(2L); // GraphState.MUTABLE
          assertThat(index.getStats().get("deltaVectorsCount"))
              .as("precondition: only this session's own write is buffered so far").isEqualTo(1L);
          assertThat(index.getStats().get("totalVectors"))
              .as("precondition: above the async-rebuild threshold, otherwise this is the pre-existing small-graph "
                  + "synchronous path, unaffected by this fix")
              .isGreaterThanOrEqualTo(1000L);

          final RID crashedSessionDocRid = ridOf(db, NUM_VECTORS); // first record written after the persisted build
          final RID inSessionDocRid = ridOf(db, IN_SESSION_DOC_ID);

          // The async rebuild the reuse dispatches resets the very gauges asserted below - deltaVectorsCount,
          // mutationsSinceRebuild, graphState, graphRebuildCount - so the permits are saturated for the whole
          // observation window. Without that, whether these assertions see the state the reuse LEFT or the state the
          // rebuild ALREADY CONSUMED is decided by how quickly this JVM hands out the sole vector-rebuild permit; the
          // block used to pass only because a preceding test's rebuild happened to be starving it. Both acquire sites
          // are tryAcquire on background threads and no search path takes a permit, so this cannot stall the queries.
          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
            final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(IN_SESSION_DOC_ID), 5, 64);
            // The stopwatch keeps running across the cheap getStats() assertions below and is asserted on after
            // them - see the comment there.
            // The state facts are asserted before the wall clock, deliberately: the issue reports the elapsed time
            // moving between runs (7.5x to 15.9x) while these two counters do not, so they are what should name the
            // regression when this test goes red, with the timing tripwire below as the corroborating - not the
            // leading - signal.
            assertThat(index.getStats().get("stalePrefixGraphReuses"))
                .as("the reuse path must run for a session that wrote before it searched, exactly as it does for one "
                    + "that did not (issue #6772)")
                .isEqualTo(1L);
            assertThat(index.getStats().get("graphRebuildCount"))
                .as("no synchronous rebuild may have run on the calling thread for this search to have returned "
                    + "already").isZero();
            assertThat(index.getStats().get("graphNodeCount"))
                .as("the graph in memory must be the persisted PREFIX, not a graph rebuilt over the whole live set")
                .isEqualTo((long) NUM_VECTORS);

            // The duplicate check. This session's own write is already in the delta buffer AND inside the gap past
            // the persisted prefix, so a reuse that queued the gap wholesale would buffer it twice - and count it
            // twice in mutationsSinceRebuild.
            assertThat(index.getStats().get("deltaVectorsCount"))
                .as("every vector missing from the persisted prefix must be buffered exactly once: the %d written by "
                    + "the crashed session plus the 1 written by this one, with no duplicate of this session's own "
                    + "write", GAP_VECTORS)
                .isEqualTo((long) (GAP_VECTORS + 1));
            assertThat(index.getStats().get("mutationsSinceRebuild"))
                .as("and counted exactly once each, for the same reason")
                .isEqualTo((long) (GAP_VECTORS + 1));

            // Measured on this fixture: a prefix reuse returns in well under a second; a full synchronous rebuild of
            // 20,201 vectors at these build parameters takes several real seconds. The bound is a tripwire between
            // the two, wide on both sides.
            stopwatch.assertGaveUpWithin(2_000,
                "a search that reuses a stale prefix graph from one that ran a full synchronous rebuild to completion");

            // Both kinds of pending vector must already be findable by their own embedding - a record is always its
            // own nearest neighbour - proving the reuse did not trade correctness for the latency win.
            assertThat(results.stream().map(Pair::getFirst))
                .as("the vector this session wrote before searching must be found by the very first query")
                .contains(inSessionDocRid);
            assertThat(index.findNeighborsFromVector(embedding(NUM_VECTORS), 5, 64).stream().map(Pair::getFirst))
                .as("so must a vector written by the crashed session, which only the queued gap makes searchable")
                .contains(crashedSessionDocRid);
          } finally {
            // Released before the convergence wait below: that wait is precisely for the rebuild these permits park.
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          }


          // The async rebuild kicked off by the reuse eventually folds everything into the graph proper.
          Awaitility.await("the async rebuild kicked off by the stale-prefix reuse completes")
              .atMost(ASYNC_REBUILD_TIMEOUT)
              .pollInterval(Duration.ofMillis(200))
              .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount"))
                  .as("graphRebuildCount, with asyncRebuildInProgress=%s: a 1 there means the rebuild is still "
                          + "running or still parked on the JVM-wide REBUILD_SEMAPHORE and this wait is simply "
                          + "too short; a 0 means it was never started or was skipped, which is a real defect",
                      index.getStats().get("asyncRebuildInProgress"))
                  .isEqualTo(1L));

          assertThat(index.getStats().get("graphNodeCount"))
              .as("once the deferred rebuild completes the graph covers every live vector")
              .isEqualTo((long) (NUM_VECTORS + GAP_VECTORS + 1));

          final List<Pair<RID, Float>> afterRebuild = index.findNeighborsFromVector(embedding(IN_SESSION_DOC_ID), 5,
              64);
          assertThat(afterRebuild.stream().map(Pair::getFirst))
              .as("and the in-session vector must still be found once it is folded into the graph proper, not only "
                  + "while it sat in the delta buffer")
              .contains(inSessionDocRid);
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The same reopen, with several searchers hitting the index at once instead of one.
     * <p>
     * The reuse decision is made under {@code graphBuildLock} but the graph is published later, in
     * {@code reuseStalePrefixGraph()}, after that mutex has been released and re-acquired. The latch is therefore
     * cleared at PUBLISH time, not at decision time: clearing it at decision time would leave a window in which
     * {@code graphState} is already MUTABLE (this session wrote before it searched), the latch is false, and
     * {@code graphIndex} is still null - and a second search thread landing in it would skip
     * {@code ensureGraphAvailable()} entirely, read the null graph as "small" and run the whole synchronous rebuild
     * this fix exists to remove, on a race rather than on a promotion (PR #6784 review).
     * <p>
     * What is pinned here is what survives that window either way: every concurrent searcher gets a correct answer,
     * exactly one reuse is published no matter how many threads reached the decision, and each pending vector is
     * still buffered exactly once - the dedup in {@code reuseStalePrefixGraph()} has to hold when two callers race
     * for the publish lock, not only when one runs alone.
     */
    @Test
    void concurrentSearchersOnAWroteFirstReopenStillPublishExactlyOneReuse() throws Exception {
      buildStalePersistedGraphFixture();

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          populate(db, IN_SESSION_DOC_ID, IN_SESSION_DOC_ID + 1);
          final RID inSessionDocRid = ridOf(db, IN_SESSION_DOC_ID);

          // The async rebuild the reuse dispatches resets the very gauges asserted below - deltaVectorsCount,
          // mutationsSinceRebuild, graphState, graphRebuildCount - so the permits are saturated for the whole
          // observation window. Without that, whether these assertions see the state the reuse LEFT or the state the
          // rebuild ALREADY CONSUMED is decided by how quickly this JVM hands out the sole vector-rebuild permit; the
          // block used to pass only because a preceding test's rebuild happened to be starving it. Both acquire sites
          // are tryAcquire on background threads and no search path takes a permit, so this cannot stall the queries.
          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            final int searchers = 8;
            final CyclicBarrier startTogether = new CyclicBarrier(searchers);
            final List<Callable<List<Pair<RID, Float>>>> tasks = new ArrayList<>(searchers);
            for (int i = 0; i < searchers; i++)
              tasks.add(() -> {
                startTogether.await(30, TimeUnit.SECONDS);
                return index.findNeighborsFromVector(embedding(IN_SESSION_DOC_ID), 5, 64);
              });

            final ExecutorService pool = Executors.newFixedThreadPool(searchers);
            try {
              for (final Future<List<Pair<RID, Float>>> future : pool.invokeAll(tasks))
                assertThat(future.get(60, TimeUnit.SECONDS).stream().map(Pair::getFirst))
                    .as("every concurrent searcher must find the vector this session wrote before searching, whether "
                        + "it published the reuse, waited for it, or was served from the delta buffer alone")
                    .contains(inSessionDocRid);
            } finally {
              pool.shutdownNow();
            }

            assertThat(index.getStats().get("stalePrefixGraphReuses"))
                .as("exactly one reuse must be published however many threads reached the decision - the losers must "
                    + "discard their work on the graphIndex double-check, not publish a second time")
                .isEqualTo(1L);
            assertThat(PendingDeltaVectors.of(index))
                .as("and the gap must still be buffered exactly once under contention: %d from the crashed session "
                    + "plus the 1 this session wrote", GAP_VECTORS)
                .isEqualTo((long) (GAP_VECTORS + 1));
            assertThat(index.getStats().get("mutationsSinceRebuild"))
                .as("counted exactly once each, for the same reason")
                .isEqualTo((long) (GAP_VECTORS + 1));
          } finally {
            // Released before the convergence wait below: that wait is precisely for the rebuild these permits park.
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          }


          // Teardown hygiene, not an assertion about the fix: the reuse kicks off an async rebuild, and dropping the
          // database out from under it leaves it persisting a graph into files that are going away.
          Awaitility.await("the async rebuild kicked off by the stale-prefix reuse settles before the drop")
              .atMost(ASYNC_REBUILD_TIMEOUT)
              .pollInterval(Duration.ofMillis(200))
              .untilAsserted(() -> assertThat(index.getStats().get("asyncRebuildInProgress"))
                  .as("asyncRebuildInProgress, with graphRebuildCount=%s: this stays 1 for as long as the rebuild "
                          + "is running OR parked on the JVM-wide REBUILD_SEMAPHORE (whose own permit wait is "
                          + "10 minutes by default), so a timeout here means the wait was too short, not that "
                          + "anything hung", index.getStats().get("graphRebuildCount"))
                  .isZero());
        } finally {
          db.drop();
        }
      }
    }

    /**
     * Sessions 1 and 2, shared by both tests: build and persist a graph over exactly {@code NUM_VECTORS} and close
     * cleanly, then write {@code GAP_VECTORS} more committed records and CRASH instead of closing, so the persisted
     * graph and its manifest stay exactly as session 1 left them - describing only the first {@code NUM_VECTORS}
     * records - while the live vector count grows. Same fixture as {@code StalePrefixReuse}, which
     * is deliberate: these tests differ from that one only in what the reopening session does.
     */
    private void buildStalePersistedGraphFixture() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db, 0, NUM_VECTORS);
          final LSMVectorIndex index = vectorIndex(db);
          index.buildVectorGraphNow();
          assertThat(index.getStats().get("graphState")).as("precondition: graph must be built and IMMUTABLE first")
              .isEqualTo(1L); // GraphState.IMMUTABLE
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          populate(db, NUM_VECTORS, NUM_VECTORS + GAP_VECTORS);
          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    private static void populate(final Database db, final int fromInclusive, final int toExclusive) {
      db.transaction(() -> {
        if (db.getSchema().existsType("Doc"))
          return;
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\", \"maxConnections\": " + MAX_CONNECTIONS
            + ", \"beamWidth\": " + BEAM_WIDTH + " }");
      });

      db.begin();
      for (int i = fromInclusive; i < toExclusive; i++) {
        db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
        if ((i - fromInclusive) % 1000 == 999) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }

    /** Deterministic per-id embedding, so any record's own vector can be reconstructed independently at query time. */
    private static float[] embedding(final int id) {
      final Random random = new Random(0x6655L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static RID ridOf(final Database db, final int id) {
      try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
        assertThat(rs.hasNext()).as("the record must exist").isTrue();
        return rs.next().getProperty("@rid");
      }
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #7045: a parse failure mid-page must not drop the rest of the page at compaction.
   * <p>
   * Regression test for issue #7045: a parse failure in the middle of a vector index page dropped every entry from the
   * failing one to the end of that page, silently, and a compaction then rewrote the data file from that truncated
   * set and reclaimed the source - so the vectors it lacked were gone from every later search while the index kept
   * reporting itself healthy. The parse is now strict when it feeds a compaction: the failure propagates, the
   * compaction is aborted before any file is created or replaced, and the lenient load/rebuild paths keep their
   * recovery fallbacks.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TruncatedPageCompaction {
    private static final String DB_PATH     = "./target/databases/TruncatedPageCompaction";
    private static final int    DIMENSIONS  = 8;
    private static final int    NUM_VECTORS = 300;

    @AfterEach
    void cleanUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void compactionAbortsInsteadOfShippingATruncatedLiveSet() throws Exception {
      FileUtils.deleteRecursively(new File(DB_PATH));
      final Random rng = new Random(7045);

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 100_000);
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 0);

          db.transaction(() -> {
            final DocumentType t = db.getSchema().createDocumentType("Doc");
            t.createProperty("id", Type.INTEGER);
            t.createProperty("embedding", Type.ARRAY_OF_FLOATS);
          });
          db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA "
              + "{ \"dimensions\": " + DIMENSIONS + ", \"similarity\": \"EUCLIDEAN\" }");

          for (int batch = 0; batch < 3; batch++) {
            final int from = batch * NUM_VECTORS / 3;
            db.transaction(() -> {
              for (int i = from; i < from + NUM_VECTORS / 3; i++)
                db.newDocument("Doc").set("id", i).set("embedding", randomVector(rng)).save();
            });
          }

          final DatabaseInternal internal = (DatabaseInternal) db;
          final LSMVectorIndex lsm = vectorIndex(db);
          final int fileIdBefore = lsm.getFileId();
          final int totalPages = lsm.getTotalPages();
          final int pageSize = lsm.getPageSize();

          assertThat(parse(internal, fileIdBefore, totalPages, pageSize, false))
              .as("precondition: every entry is readable before the corruption").hasSize(NUM_VECTORS);

          final int corruptedEntry = NUM_VECTORS / 2;
          corruptEntry(internal, fileIdBefore, pageSize, corruptedEntry);

          // The lenient parse is what the issue describes: the page is cut short at the corrupted entry and the parse
          // reports fewer entries than the page holds, with nothing but a WARNING to tell.
          final int lenientlyParsed = parse(internal, fileIdBefore, totalPages, pageSize, false).size();
          assertThat(lenientlyParsed).as("the corruption must cut the page short, or this test proves nothing")
              .isLessThan(NUM_VECTORS);

          // The strict parse, the one a compaction runs, must refuse instead.
          assertThatThrownBy(() -> parse(internal, fileIdBefore, totalPages, pageSize, true))
              .isInstanceOf(IndexException.class)
              .hasMessageContaining("page 0")
              .hasMessageContaining("of the page not read");

          // And the compaction itself must abort, leaving the source file - and every vector in it - in place.
          assertThat(lsm.scheduleCompaction()).isTrue();
          assertThatThrownBy(lsm::compact)
              .as("a compaction fed a truncated live set must fail rather than reclaim the source file")
              .hasStackTraceContaining("Error parsing page 0");

          assertThat(lsm.getFileId()).as("the source data file must not have been replaced").isEqualTo(fileIdBefore);
          assertThat(lsm.countEntries()).as("no vector may have been dropped by the aborted compaction")
              .isEqualTo(NUM_VECTORS);
          assertThat(lsm.scheduleCompaction()).as("the aborted compaction must hand its scheduling slot back")
              .isTrue();
        } finally {
          db.drop();
        }
      }
    }

    private static List<LSMVectorIndexPageParser.VectorEntry> parse(final DatabaseInternal db, final int fileId,
        final int totalPages, final int pageSize, final boolean strict) {
      final List<LSMVectorIndexPageParser.VectorEntry> entries = new ArrayList<>();
      LSMVectorIndexPageParser.parsePages(db, fileId, totalPages, pageSize, false, strict, entries::add);
      return entries;
    }

    /**
     * Corrupts entry {@code entryIndex} of page 0 the way a damaged page would: its quantization type byte claims an
     * INT8 payload as long as the whole page, so the parse of the next entry starts past the end of the page.
     */
    private static void corruptEntry(final DatabaseInternal db, final int fileId, final int pageSize, final int entryIndex) {
      db.transaction(() -> {
        final MutablePage page;
        try {
          page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
        assertThat(page.readInt(LSMVectorIndex.OFFSET_NUM_ENTRIES)).as("every entry must be on page 0")
            .isEqualTo(NUM_VECTORS);

        int offset = LSMVectorIndex.HEADER_BASE_SIZE;
        for (int i = 0; i < entryIndex; i++) {
          offset = skipIdsAndDeletedFlag(page, offset);
          offset = LSMVectorIndexPageParser.skipQuantizationData(page, offset);
        }
        offset = skipIdsAndDeletedFlag(page, offset);
        page.writeByte(offset, (byte) VectorQuantizationType.INT8.ordinal());
        page.writeInt(offset + 1, pageSize);
      });
    }

    private static int skipIdsAndDeletedFlag(final MutablePage page, int offset) {
      offset += (int) page.readNumberAndSize(offset)[1]; // vector id
      offset += (int) page.readNumberAndSize(offset)[1]; // bucket id
      offset += (int) page.readNumberAndSize(offset)[1]; // position
      return offset + 1; // deleted flag
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      final TypeIndex idx = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
      return (LSMVectorIndex) idx.getIndexesOnBuckets()[0];
    }

    private static float[] randomVector(final Random rng) {
      final float[] v = new float[DIMENSIONS];
      for (int i = 0; i < DIMENSIONS; i++)
        v[i] = (float) rng.nextGaussian();
      return v;
    }
  }

  /**
   * Regression for #7190: a vector the graph build could not link is still findable after reopen.
   * <p>
   * A vector the graph build could not link must still be findable after the database is reopened (issue #7190).
   * <p>
   * A Vamana build occasionally leaves a node with a full out-degree and no in-edges (issue #5615): it is physically in
   * the graph and no beam search can return it at any {@code efSearch}. The session that built the graph re-queues
   * those vectors into the delta buffer, which is in memory only - so after {@code close()} they used to be gone from
   * every search while {@code totalVectors} still counted them, with nothing in the log and k results still coming
   * back, so no caller could tell. The set is now recorded in the graph manifest and re-queued by the load path.
   * <p>
   * <b>Why duplicate vectors.</b> The reporter measured one orphan per 50,000 random vectors, which is not something a
   * test can rely on. Diversity pruning refuses to link a node whose neighbours are all closer to each other than to
   * it, so drawing every vector from a handful of distinct points orphans nearly the whole graph, every time, at any
   * {@code maxConnections} - the same state the report describes, deterministically and at a size a test can afford.
   * The preconditions below assert the orphans were actually produced, so the test cannot pass by not reproducing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class UnreachableVectorsSurviveReopen {
    private static final String DB_ROOT    = "target/test-databases/UnreachableVectorsSurviveReopen";
    private static final int    DIMENSIONS = 8;
    private static final int    COUNT      = 1_000;
    /** Distinct vector values the corpus is drawn from: what makes the build orphan almost every node. */
    private static final int    DISTINCT   = 8;
    /**
     * Neighbours asked for. Small on purpose: it has to stay under how many nodes the crippled graph can still reach
     * (about 33 here), because a search the graph leaves short of k falls back to a brute-force scan that reads every
     * vector and would mask the defect. The reporter's 50,000-vector index had no such fallback for k=10 either.
     */
    private static final int    K          = 10;

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @Test
    void vectorsTheBuildLeftUnreachableAreStillFoundAfterAReopen() {
      final long unreachable = buildAndPersistFixture();

      // Reopen: the graph on disk covers every live vector, so it is published as up to date. The nodes its build
      // could not link have to be re-queued into the delta scan, or they are unreachable for good.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        configure(db);
        try {
          final List<Integer> found = idsNear(db, embedding(0), K);
          final LSMVectorIndex index = vectorIndex(db);

          // The defect, first and at the level a caller sees it. The query IS the vector of group 0, and 125 records
          // hold exactly that vector, so every one of the K nearest neighbours must be one of them - at distance 0.
          // Before this fix the graph walk could reach only a handful of them, the rest of the answer was filled with
          // records from other groups, and k results still came back, so nothing said anything was wrong.
          assertThat(found).as("k results must still come back").hasSize(K);
          assertThat(found).as("and be distinct records").doesNotHaveDuplicates();
          assertThat(found.stream().filter(id -> id % DISTINCT != 0).toList())
              .as("every one of the %d nearest neighbours of an exact match must itself be an exact match; the "
                  + "records that are only reachable through the delta scan are exactly what used to be missing "
                  + "after a reopen", K)
              .isEmpty();

          assertThat(index.getStats().get("unreachableGraphNodes"))
              .as("the manifest must carry the ordinals the build could not link, so this session knows about them")
              .isEqualTo(unreachable);
          assertThat(index.getStats().get("deltaVectorsCount"))
              .as("...and they must be queued into the delta scan, which is the only way a search can return them")
              .isEqualTo(unreachable);

          // Re-queueing an orphan is not a mutation: it is already in the persisted graph, and a rebuild does not
          // remove it. Counting it would rebuild this index on every open and, through flush(), on every close.
          assertThat(index.getStats().get("mutationsSinceRebuild"))
              .as("an orphan is not pending work: counting it would make every open trigger a rebuild")
              .isZero();
          assertThat(index.getStats().get("graphState"))
              .as("and must not promote the graph to MUTABLE, which is what makes close() rebuild")
              .isEqualTo(1L); // GraphState.IMMUTABLE
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("nothing here justifies a rebuild")
              .isZero();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // A second reopen must behave identically: the manifest write on close must not have dropped the set.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        configure(db);
        try {
          assertThat(idsNear(db, embedding(0), K).stream().filter(id -> id % DISTINCT != 0).toList())
              .as("the set has to survive every close, not only the one that built the graph")
              .isEmpty();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    /**
     * The same guarantee on the other load path: a session that adds vectors after the reopen reuses the persisted
     * graph as a stale prefix (issue #6655), and the nodes THAT graph leaves unreachable are ordinals inside the
     * prefix - covered by neither the gap the reuse queues nor anything in memory.
     */
    @Test
    void aStalePrefixReuseAlsoRequeuesWhatItsGraphLeavesUnreachable() {
      final long unreachable = buildAndPersistFixture();

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        configure(db);
        try {
          // One new vector, so the persisted graph is a prefix of the live set rather than up to date.
          db.transaction(() -> db.newDocument("Doc").set("id", COUNT).set("vector", embedding(COUNT)).save());

          final List<Integer> found = idsNear(db, embedding(0), K);
          final LSMVectorIndex index = vectorIndex(db);

          assertThat(index.getStats().get("stalePrefixGraphReuses"))
              .as("precondition: the search reused the persisted graph as a stale prefix")
              .isEqualTo(1L);
          assertThat(index.getStats().get("deltaVectorsCount"))
              .as("the prefix's unreachable nodes AND the one-vector gap must both be queued")
              .isEqualTo(unreachable + 1);
          assertThat(index.getStats().get("mutationsSinceRebuild"))
              .as("only the gap counts as a pending mutation - the orphans are already in the graph")
              .isEqualTo(1L);
          assertThat(found.stream().filter(id -> id % DISTINCT != 0).toList())
              .as("and the exact matches only the delta scan can supply must be in the answer")
              .isEmpty();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    /**
     * The counterpart of persisting the set: it describes one graph generation, so a build that publishes NO graph -
     * every vector deleted - must leave nothing behind for {@code getStats()} to report against a graph that is not
     * there.
     */
    @Test
    void aRebuildThatPublishesNoGraphReportsNoUnreachableNodes() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        configure(db);
        try {
          insert(db);
          final LSMVectorIndex index = vectorIndex(db);
          index.buildVectorGraphNow();
          assertThat(index.getStats().get("unreachableGraphNodes"))
              .as("precondition: this build has to leave nodes unreachable, or there is nothing to clear")
              .isPositive();

          db.transaction(() -> db.command("sql", "DELETE FROM Doc"));
          index.buildVectorGraphNow();

          assertThat(index.getStats().get("graphNodeCount"))
              .as("precondition: with every vector gone the build publishes no graph at all")
              .isZero();
          assertThat(index.getStats().get("unreachableGraphNodes"))
              .as("the previous generation's orphans describe a graph that is no longer resident")
              .isZero();
        } finally {
          db.drop();
        }
      }
    }

    /**
     * @return how many nodes the build left unreachable, asserted to be more than none so nothing above can pass by
     * failing to reproduce the state under test
     */
    private long buildAndPersistFixture() {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        configure(db);
        try {
          insert(db);
          final LSMVectorIndex index = vectorIndex(db);
          index.buildVectorGraphNow();

          final long unreachable = index.getStats().get("unreachableGraphNodes");
          assertThat(unreachable)
              .as("precondition: the build has to leave nodes unreachable, or this test proves nothing. Duplicate "
                  + "vectors make that deterministic - see the class javadoc")
              .isPositive();
          assertThat(index.getStats().get("deltaVectorsCount"))
              .as("precondition: the building session serves them from the delta scan")
              .isEqualTo(unreachable);
          assertThat(idsNear(db, embedding(0), K).stream().filter(id -> id % DISTINCT != 0).toList())
              .as("precondition: with that delta buffer in memory, the building session answers with exact matches")
              .isEmpty();
          return unreachable;
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    private static List<Integer> idsNear(final Database db, final float[] query, final int k) {
      final StringBuilder vector = new StringBuilder();
      for (int d = 0; d < DIMENSIONS; d++)
        vector.append(d == 0 ? "" : ", ").append(query[d]);

      final List<Integer> ids = new ArrayList<>(k);
      try (final ResultSet rs = db.query("sql",
          "SELECT id FROM (SELECT expand(vectorNeighbors('Doc[vector]', [" + vector + "], " + k + ")))")) {
        while (rs.hasNext())
          ids.add(rs.next().getProperty("id"));
      }
      return ids;
    }

    private static void configure(final Database db) {
      // This test is about what the delta buffer holds after a reopen, so nothing must drain it behind the
      // assertions: no mutation-count trigger, no ratio trigger, no delta-scan budget trigger, no inactivity timer.
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MAX_DELTA_SCAN_RATIO, 0f);
      db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 600_000);
    }

    private static void insert(final Database db) {
      db.transaction(() -> {
        if (db.getSchema().existsType("Doc"))
          return;
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"EUCLIDEAN\" }");
      });

      db.begin();
      for (int i = 0; i < COUNT; i++) {
        db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
        if (i % 500 == 499) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }

    /** Vector of a record: one of {@link #DISTINCT} distinct points, which is what makes the build orphan nodes. */
    private static float[] embedding(final int id) {
      final Random random = new Random(id % DISTINCT);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #7842: deleting a vector does not condemn the persisted graph.
   * <p>
   * Regression test for issue #7842: deleting a vector must not condemn the persisted graph.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class DeleteKeepsPersistedGraph {
    private static final String DB_ROOT     = "target/test-databases/DeleteKeepsPersistedGraph";
    private static final int    DIMENSIONS  = 32;
    private static final int    NUM_VECTORS = 2_000;
    private static final int    DELETED     = 5;
    private static final int    GAP_VECTORS = 200;

    // A convergence wait, not a latency bound: the async rebuild has to wait out whatever else in this JVM holds the
    // sole LSMVectorIndex.REBUILD_SEMAPHORE permit before it can even start (see the @Tag("vector") note in
    // CLAUDE.md), so this is sized for the worst case. A generous value cannot turn a passing run red.
    private static final Duration ASYNC_REBUILD_TIMEOUT = Duration.ofMinutes(4);

    private String dbPath;

    @BeforeEach
    void setUp(final TestInfo testInfo) {
      dbPath = DB_ROOT + "-" + testInfo.getTestMethod().orElseThrow().getName();
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(dbPath));
    }

    @Test
    void reopenAfterDeletesReusesThePersistedGraphInsteadOfRebuildingItFromScratch() throws Exception {
      // Session 1: build and persist a graph over exactly NUM_VECTORS records, then close cleanly.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          final LSMVectorIndex index = vectorIndex(db);
          index.buildVectorGraphNow();
          assertThat(index.getStats().get("graphState")).as("precondition: graph built and IMMUTABLE").isEqualTo(1L);
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // Session 2: delete a handful of records, then crash rather than close, so nothing rebuilds the graph and the
      // persisted pages stay exactly as session 1 left them - with tombstones now recorded against some of them.
      final List<Integer> deletedIds = new ArrayList<>();
      for (int i = 0; i < DELETED; i++)
        deletedIds.add(i * 37);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          db.begin();
          for (final int id : deletedIds)
            db.command("sql", "DELETE FROM Doc WHERE id = ?", id);
          db.commit();

          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // Session 3: reopen. The persisted graph still holds NUM_VECTORS nodes, DELETED of which now answer for
      // tombstoned vectors. Before this fix that alone made the whole graph UNUSABLE and the first search paid a
      // full synchronous rebuild.
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("persistedGraphNodeCount"))
              .as("precondition: the persisted graph must still describe every record session 1 built it over")
              .isEqualTo((long) NUM_VECTORS);
          // Read BEFORE the first search: a rebuild republishes the location index and drops the tombstone set with
          // it, so on the pre-fix code this same read after the search answers 0.
          assertThat(index.getStats().get("deletedVectors"))
              .as("precondition: the tombstones must have survived the crash, otherwise nothing is being tested")
              .isEqualTo((long) DELETED);

          final int liveId = 1; // not in deletedIds
          final RID liveRid = ridOf(db, liveId);

          final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(liveId), 5, 64);

          assertThat(index.getStats().get("graphRebuildCount"))
              .as("no full graph rebuild may run just because vectors were deleted")
              .isZero();
          assertThat(index.getStats().get("graphNodeCount"))
              .as("the persisted graph must be reused whole, tombstoned nodes included")
              .isEqualTo((long) NUM_VECTORS);
          assertThat(results.stream().map(Pair::getFirst))
              .as("a live record must still be found by its own embedding").contains(liveRid);

          // And the deleted ones must not come back.
          for (final int id : deletedIds) {
            final List<Pair<RID, Float>> hits = index.findNeighborsFromVector(embedding(id), 5, 64);
            try (final ResultSet rs = db.query("sql", "SELECT count(*) as cnt FROM Doc WHERE id = ?", id)) {
              assertThat(rs.next().<Long>getProperty("cnt")).as("record %d must be gone", id).isZero();
            }
            assertThat(hits).as("a deleted vector must never be returned").allSatisfy(
                hit -> assertThat(db.lookupByRID(hit.getFirst(), true).asDocument().<Integer>get("id")).isNotEqualTo(id));
          }
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The same reopen with vectors ALSO added after the persisted build: deletions and insertions together, which the
     * recompute path could not express at all - the stale-prefix reuse of issue #6655 was gated on there being no
     * deletions, so any tombstone sent the whole index back to a full rebuild.
     */
    @Test
    void reopenAfterDeletesAndInsertsReusesThePersistedGraphAsAPrefix() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          vectorIndex(db).buildVectorGraphNow();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      final int gapId = NUM_VECTORS + 1;
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          db.begin();
          db.command("sql", "DELETE FROM Doc WHERE id = ?", 0);
          for (int i = NUM_VECTORS; i < NUM_VECTORS + GAP_VECTORS; i++)
            db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
          db.commit();

          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          // Leave only the absolute floor of 100 in play, so GAP_VECTORS genuinely crosses the threshold and the
          // async fold-in this test waits for is triggered BY the policy rather than in spite of it (issue #7183).
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

          final LSMVectorIndex index = vectorIndex(db);
          final RID gapRid = ridOf(db, gapId);

          // The async rebuild the reuse dispatches resets the very gauges asserted below, so park the sole JVM-wide
          // permit for the observation window - the same reason StalePrefixReuse does.
          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(gapId), 5, 64);

            assertThat(index.getStats().get("graphRebuildCount"))
                .as("no synchronous full rebuild may run for a delete plus some inserts").isZero();
            assertThat(index.getStats().get("stalePrefixGraphReuses"))
                .as("the persisted graph must have been reused as a prefix").isEqualTo(1L);
            assertThat(index.getStats().get("graphReusesWithTombstonedNodes"))
                .as("and that reuse must be recorded as one carrying tombstoned nodes").isEqualTo(1L);
            assertThat(index.getStats().get("mutationsSinceRebuild"))
                .as("the prefix path must charge the tombstoned node too, not only the %d gap vectors - it reaches "
                    + "mutationsSinceSerialize through ReuseCandidate rather than inline, so it is a separate route "
                    + "from the one the no-gap case takes", GAP_VECTORS)
                .isGreaterThan((long) GAP_VECTORS);
            assertThat(results.stream().map(Pair::getFirst))
                .as("a vector written after the persisted build must be searchable from the first query")
                .contains(gapRid);
          } finally {
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          }

          Awaitility.await("the async rebuild the prefix reuse dispatched completes")
              .atMost(ASYNC_REBUILD_TIMEOUT)
              .pollInterval(Duration.ofMillis(200))
              .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount")).isEqualTo(1L));

          assertThat(index.getStats().get("graphNodeCount"))
              .as("the rebuild folds the gap in and the tombstone out")
              .isEqualTo((long) (NUM_VECTORS + GAP_VECTORS - 1));
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The tombstones are not free, they are DEFERRED: the graph's dead nodes are charged to the ordinary mutation
     * counter, so the existing threshold, its heap admission check and the JVM-wide rebuild permit decide when they
     * are folded out - instead of the first search after a reopen paying for all of them synchronously.
     */
    @Test
    void tombstonedNodesAreChargedToTheOrdinaryRebuildSchedule() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          vectorIndex(db).buildVectorGraphNow();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      final int manyDeletes = 150; // above the absolute mutation floor of 100 the configuration below leaves in play
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          db.begin();
          db.command("sql", "DELETE FROM Doc WHERE id < ?", manyDeletes);
          db.commit();

          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          db.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_REBUILD_GRAPH_RATIO, 0f);

          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("mutationsSinceRebuild"))
              .as("precondition: a freshly reopened session has counted nothing yet").isZero();

          LSMVectorIndex.acquireAllRebuildPermitsForTest();
          try {
            index.findNeighborsFromVector(embedding(NUM_VECTORS - 1), 5, 64);

            assertThat(index.getStats().get("graphRebuildCount"))
                .as("the search must be answered off the persisted graph, not off a rebuild of it").isZero();
            assertThat(index.getStats().get("graphReusesWithTombstonedNodes")).isEqualTo(1L);
            assertThat(index.getStats().get("mutationsSinceRebuild"))
                .as("the dead nodes are what the graph owes, so they must be counted as pending work")
                .isGreaterThanOrEqualTo((long) manyDeletes);
            assertThat(index.getStats().get("graphState"))
                .as("a graph that owes a compaction is MUTABLE, which is what the rebuild policies gate on")
                .isEqualTo(2L);
          } finally {
            LSMVectorIndex.releaseAllRebuildPermitsForTest();
          }

          Awaitility.await("the deferred compaction of the tombstoned nodes completes")
              .atMost(ASYNC_REBUILD_TIMEOUT)
              .pollInterval(Duration.ofMillis(200))
              .untilAsserted(() -> assertThat(index.getStats().get("graphRebuildCount"))
                  .as("graphRebuildCount, with asyncRebuildInProgress=%s",
                      index.getStats().get("asyncRebuildInProgress"))
                  .isEqualTo(1L));

          assertThat(index.getStats().get("graphNodeCount"))
              .as("once it runs, the compaction leaves only the live vectors in the graph")
              .isEqualTo((long) (NUM_VECTORS - manyDeletes));
        } finally {
          db.drop();
        }
      }
    }

    /**
     * PRODUCT quantization is the one case this fix deliberately does NOT cover: the PQ codes are addressed by the
     * same ordinal and are produced wholesale by the rebuild, so reusing a graph whose ordinal space has holes would
     * pair it with a codebook built over a dense one. That guard is a single {@code quantizationType == PRODUCT} test
     * at the top of the reuse path, nothing fails loudly if a refactor drops it, and what it would produce is a
     * silently wrong ranking rather than a crash - so it is pinned here.
     */
    @Test
    void aProductQuantizedIndexStillRebuildsAfterDeletes() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db, "PRODUCT");
          vectorIndex(db).buildVectorGraphNow();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          db.begin();
          db.command("sql", "DELETE FROM Doc WHERE id = ?", 0);
          db.commit();

          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getMetadata().quantizationType)
              .as("precondition: the guard under test only applies to PRODUCT").isEqualTo(VectorQuantizationType.PRODUCT);
          assertThat(index.getStats().get("deletedVectors"))
              .as("precondition: read before the first search, which is what discards the tombstone set").isEqualTo(1L);

          index.findNeighborsFromVector(embedding(1), 5, 64);

          assertThat(index.getStats().get("graphReusesWithTombstonedNodes"))
              .as("a PRODUCT-quantized index must never take the tombstone-tolerant reuse path").isZero();
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("it must pay the full rebuild instead, which is what keeps its codebook and its graph on the same "
                  + "dense ordinal space")
              .isEqualTo(1L);
        } finally {
          db.drop();
        }
      }
    }

    /**
     * The RID at each ordinal is recorded, rather than the vector id alone, for exactly one reason: a compaction
     * renumbers the whole live set densely from 0 (issue #5870), so an id that survives a crash between the
     * renumbering and the graph persist is live, in range, and answers for a DIFFERENT record. An id-only check - the
     * manifest fingerprint, which is what the no-deletions path still uses - cannot see that, and reusing the graph
     * would return wrong-but-plausible neighbours rather than failing.
     * <p>
     * Reproduced by leaving behind exactly what that crash leaves: a sidecar recorded next to these very graph pages,
     * over these very ordinals, whose RIDs belong to other records. The index must refuse the graph and rebuild.
     */
    @Test
    void aRecordedMapWhoseRidsNoLongerMatchIsRefusedRatherThanReused() throws Exception {
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.create();
        try {
          populate(db);
          vectorIndex(db).buildVectorGraphNow();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      final int[] ordinalToVectorId;
      final Path graphPath;
      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          index.findNeighborsFromVector(embedding(1), 5, 64); // resolves the persisted graph and its ordinal map
          ordinalToVectorId = index.getOrdinalToVectorIdForTest().clone();
          graphPath = index.getGraphFilePathForTest();

          db.begin();
          db.command("sql", "DELETE FROM Doc WHERE id = ?", 0);
          db.commit();

          ((DatabaseInternal) db).kill();
          db.close();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }

      // Every ordinal now claims a RID one bucket position away from the record it actually describes - the shape a
      // renumbered id space leaves, without needing to win the race that produces it.
      final LSMVectorIndexGraphManifest manifest = new LSMVectorIndexGraphManifest(graphPath.toString());
      final LSMVectorIndexOrdinalMapFile.Content recorded = manifest.readOrdinalMap();
      assertThat(recorded).as("precondition: a map must have been recorded for there to be one to invalidate")
          .isNotNull();
      final Map<Integer, RID> shifted = new HashMap<>();
      for (int ordinal = 0; ordinal < recorded.size(); ordinal++)
        shifted.put(recorded.vectorIds()[ordinal],
            new RID(recorded.bucketIds()[ordinal], recorded.positions()[ordinal] + 1));
      new LSMVectorIndexOrdinalMapFile(graphPath.toString()).write(ordinalToVectorId, shifted::get);

      try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          final RID liveRid = ridOf(db, 1);

          final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(1), 5, 64);

          assertThat(index.getStats().get("graphReusesWithTombstonedNodes"))
              .as("a map whose RIDs no longer resolve to the records it names must never be trusted").isZero();
          assertThat(index.getStats().get("graphRebuildCount"))
              .as("the graph must be rebuilt from scratch instead, which is the conservative direction").isEqualTo(1L);
          assertThat(results.stream().map(Pair::getFirst))
              .as("and the answer must still be right after that rebuild").contains(liveRid);
        } finally {
          db.drop();
        }
      }
    }

    private static void populate(final Database db) {
      populate(db, null);
    }

    private static void populate(final Database db, final String quantization) {
      db.transaction(() -> {
        final var type = db.getSchema().createDocumentType("Doc");
        type.createProperty("id", Type.INTEGER);
        type.createProperty("vector", Type.ARRAY_OF_FLOATS);
        db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
            + ", \"similarity\": \"COSINE\""
            + (quantization != null ? ", \"quantization\": \"" + quantization + "\"" : "") + " }");
      });

      db.begin();
      for (int i = 0; i < NUM_VECTORS; i++) {
        db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
        if (i % 1000 == 999) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
    }

    private static float[] embedding(final int id) {
      final Random random = new Random(0x7842L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static RID ridOf(final Database db, final int id) {
      try (final ResultSet rs = db.query("sql", "SELECT @rid FROM Doc WHERE id = ?", id)) {
        assertThat(rs.hasNext()).as("record %d must exist", id).isTrue();
        return rs.next().getProperty("@rid");
      }
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc")
          .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #9071: COMPACT INDEX running while other threads write.
   * <p>
   * Issue #9071: four threads insert, update and delete records of a type with a COSINE LSM_VECTOR index while a fifth one runs
   * COMPACT INDEX every 250 ms. A record committed between the moment the compaction read the live set from the pages and the
   * moment it swapped the rewritten file in was in neither: the old file was dropped and the new one never held it, so
   * vectorNeighbors() with the record's own vector did not return it, before and after a reopen, until REBUILD INDEX.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class CompactionDuringConcurrentWrites extends TestHelper {
    private static final int DIM     = 8;
    private static final int WRITERS = 4;

    @Test
    void everyCommittedRecordIsFoundAfterCompactionsRanBetweenCommits() throws Exception {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE P");
        database.command("sql", "CREATE PROPERTY P.pid LONG");
        database.command("sql", "CREATE PROPERTY P.emb ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON P (pid) UNIQUE");
        database.command("sql", "CREATE INDEX ON P (emb) LSM_VECTOR METADATA { \"dimensions\": " + DIM
            + ", \"similarity\": \"COSINE\", \"mutationsBeforeRebuild\": 150 }");
      });

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicInteger compactions = new AtomicInteger();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final List<Thread> threads = new ArrayList<>();
      for (int t = 0; t < WRITERS; t++) {
        final int writer = t;
        threads.add(new Thread(() -> {
          try {
            final RID[] ring = new RID[8];
            for (long s = 0; !stop.get(); s++) {
              final long fs = s;
              final RID[] mine = new RID[1];
              database.transaction(() -> {
                final MutableDocument d = database.newDocument("P").set("pid", writer * 1_000_000L + fs).set("emb", vector(writer, fs, 0));
                d.save();
                mine[0] = d.getIdentity();
                if (fs % 5 == 4)
                  database.lookupByRID(ring[(int) ((fs - 3) % 8)], true).asDocument().modify().set("emb", vector(writer, fs - 3, 1)).save();
                if (fs % 7 == 6)
                  database.lookupByRID(ring[(int) ((fs - 4) % 8)], true).delete();
              }, false, 100);
              ring[(int) (s % 8)] = mine[0];
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }));
      }
      threads.add(new Thread(() -> {
        try {
          while (!stop.get()) {
            try (final ResultSet rs = database.command("sql", "COMPACT INDEX `P[emb]`")) {
              while (rs.hasNext())
                rs.next();
              compactions.incrementAndGet();
            } catch (final TimeoutException | IndexException e) {
              // a compaction the writers did not leave room for is retried by the next round
            }
            Thread.sleep(250);
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }));

      threads.forEach(Thread::start);
      final long deadline = System.currentTimeMillis() + 60_000;
      Thread.sleep(12_000);
      // keep the writers going until at least one compaction has run among them, so a slow runner cannot pass without exercising the fix
      while (compactions.get() == 0 && System.currentTimeMillis() < deadline)
        Thread.sleep(250);
      stop.set(true);
      for (final Thread t : threads)
        t.join();

      assertThat(failure.get()).as("a writer failed").isNull();
      assertThat(compactions.get()).as("compactions that ran while the writers were committing").isGreaterThan(0);
      assertThat(missing()).as("records missing before the close").isEmpty();

      reopenDatabase();
      assertThat(missing()).as("records missing after the reopen").isEmpty();
    }

    private List<String> missing() {
      final List<String> missing = new ArrayList<>();
      final Iterator<Record> it = database.iterateType("P", false);
      while (it.hasNext()) {
        final Document d = (Document) it.next();
        final Object e = d.get("emb");
        final float[] q;
        if (e instanceof float[] floats)
          q = floats;
        else {
          final List<?> l = (List<?>) e;
          q = new float[l.size()];
          for (int i = 0; i < q.length; i++)
            q[i] = ((Number) l.get(i)).floatValue();
        }
        boolean found = false;
        try (final ResultSet rs = database.query("sql", "SELECT expand(vectorNeighbors('P[emb]', ?, 10, 400))", q)) {
          while (rs.hasNext()) {
            final Result r = rs.next();
            if (r.getIdentity().isPresent() && r.getIdentity().get().equals(d.getIdentity()))
              found = true;
          }
        }
        if (!found)
          missing.add(d.getIdentity() + " pid=" + d.get("pid"));
      }
      return missing;
    }

    private static float[] vector(final long t, final long s, final int gen) {
      final float[] v = new float[DIM];
      for (int i = 0; i < DIM; i++) {
        long z = (t * 1000003L + s) * 31 + gen * 7919L + i * 104729L + 12345;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        z ^= z >>> 31;
        v[i] = (float) ((z >>> 11) * (1.0 / (1L << 53)) * 2 - 1);
      }
      return v;
    }
  }

  /**
   * Regression for #9241: a search during COMPACT INDEX sees the records added since the last graph build.
   * <p>
   * Issue #9241: while COMPACT INDEX builds its graph, a search missed every record added since the last graph build (the swap
   * emptied the delta buffer, and the new graph is published only when the build ends) and, once deletes made the rewrite
   * renumber the ids, nearly every record (the resident graph's ordinal map still held the old ids).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  class SearchDuringCompaction extends TestHelper {
    private static final int DIM    = 32;
    private static final int BASE   = 12_000;
    private static final int RECENT = 100;

    @Test
    void recentRecordsAreFoundWhileTheCompactionBuilds() throws Exception {
      final List<RID> all = load(0);
      assertThat(searchMisses(all.subList(BASE, all.size()), BASE)).isEmpty();
      searchWhileCompacting(all, BASE);
    }

    @Test
    void recordsAreFoundWhileACompactionThatRenumbersTheIdsBuilds() throws Exception {
      final List<RID> all = load(600);
      searchWhileCompacting(all, BASE);
    }

    /** Creates the type, builds a graph over BASE records, deletes some, then adds RECENT records one per transaction. */
    private List<RID> load(final int deletes) {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE V");
        database.command("sql", "CREATE PROPERTY V.id LONG");
        database.command("sql", "CREATE PROPERTY V.emb ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON V (emb) LSM_VECTOR METADATA { \"dimensions\": " + DIM
            + ", \"similarity\": \"COSINE\", \"mutationsBeforeRebuild\": 1000000 }");
      });
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < BASE; i++) {
          final var d = database.newDocument("V").set("id", (long) i).set("emb", vector(i));
          d.save();
          rids.add(d.getIdentity());
        }
      });
      try (final ResultSet rs = database.command("sql", "COMPACT INDEX `V[emb]`")) {
        while (rs.hasNext())
          rs.next();
      }
      if (deletes > 0)
        database.transaction(() -> {
          // the first records: none of them is among the ones searched
          for (int i = 0; i < deletes; i++)
            database.lookupByRID(rids.get(i), true).delete();
        });
      for (int i = BASE; i < BASE + RECENT; i++) {
        final int id = i;
        database.transaction(() -> {
          final var d = database.newDocument("V").set("id", (long) id).set("emb", vector(id));
          d.save();
          rids.add(d.getIdentity());
        });
      }
      return rids;
    }

    private void searchWhileCompacting(final List<RID> rids, final int firstRecent) throws Exception {
      // the 100 recent records and 100 older ones, none of them deleted
      final List<RID> probes = new ArrayList<>(rids.subList(firstRecent, rids.size()));
      probes.addAll(rids.subList(2000, 2000 + RECENT));
      final int[] probeIds = new int[probes.size()];
      for (int i = 0; i < RECENT; i++)
        probeIds[i] = firstRecent + i;
      for (int i = 0; i < RECENT; i++)
        probeIds[RECENT + i] = 2000 + i;

      final AtomicBoolean done = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread compactor = new Thread(() -> {
        try (final ResultSet rs = database.command("sql", "COMPACT INDEX `V[emb]`")) {
          while (rs.hasNext())
            rs.next();
        } catch (final Throwable e) {
          failure.set(e);
        } finally {
          done.set(true);
        }
      });
      compactor.start();

      int probesDuringTheCompaction = 0;
      final List<String> missing = new ArrayList<>();
      while (!done.get()) {
        for (int i = 0; i < probes.size(); i++) {
          if (!done.get())
            probesDuringTheCompaction++;
          if (!found(probeIds[i], probes.get(i)))
            missing.add("id=" + probeIds[i]);
          else if (!found(probeIds[i], probes.get(i), probes))
            // an allow-list (the ordinal map is read by vector id) holding the probes only
            missing.add("filtered id=" + probeIds[i]);
        }
      }
      compactor.join();

      assertThat(failure.get()).as("the compaction failed").isNull();
      assertThat(probesDuringTheCompaction).as("searches that ran while the compaction was building").isGreaterThan(0);
      assertThat(missing).as("records not found with their own vector while the compaction was running").isEmpty();
      assertThat(searchMisses(probes, probeIds)).as("records not found after the compaction").isEmpty();
    }

    private List<String> searchMisses(final List<RID> probes, final int firstId) {
      final int[] ids = new int[probes.size()];
      for (int i = 0; i < ids.length; i++)
        ids[i] = firstId + i;
      return searchMisses(probes, ids);
    }

    private List<String> searchMisses(final List<RID> probes, final int[] ids) {
      final List<String> missing = new ArrayList<>();
      for (int i = 0; i < probes.size(); i++)
        if (!found(ids[i], probes.get(i)))
          missing.add("id=" + ids[i]);
      return missing;
    }

    private boolean found(final int id, final RID rid) {
      return found(id, rid, null);
    }

    private boolean found(final int id, final RID rid, final List<RID> allowed) {
      final String sql = allowed == null ? "SELECT expand(vectorNeighbors('V[emb]', ?, 10, 200))" :
          "SELECT expand(vectorNeighbors('V[emb]', ?, 10, ?))";
      final Object[] args = allowed == null ? new Object[] { vector(id) } :
          new Object[] { vector(id), Map.of("efSearch", 200, "filter", allowed) };
      try (final ResultSet rs = database.query("sql", sql, args)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          if (r.getIdentity().isPresent() && r.getIdentity().get().equals(rid))
            return true;
        }
      }
      return false;
    }

    private static float[] vector(final long seed) {
      final float[] v = new float[DIM];
      for (int i = 0; i < DIM; i++) {
        long z = seed * 1000003L + i * 104729L + 12345;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        z ^= z >>> 31;
        v[i] = (float) ((z >>> 11) * (1.0 / (1L << 53)) * 2 - 1);
      }
      return v;
    }
  }
}
