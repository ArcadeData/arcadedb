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
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.WALFile;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * A follower that applies the page images of a leader's commits (what a Raft follower does with every entry: the WAL bytes of the
 * commit, applied as pages, with no index operation replayed) answers every vector search like the leader and like a brute-force
 * model, and holds the same locations, tombstones, id allocator and search counters. The leader inserts past a thousand vectors (a
 * graph that the later writes do not rebuild) and then writes through the delta buffer: new rows across several pages, deletes of
 * rows in the graph and in the buffer, rewrites of a row once and twice in one transaction, a vector removed from a record, and a
 * transaction that mixes all of them. After each commit the follower is compared with the leader and with the model.
 */
class VectorReplicaApplyOracleTest extends TestHelper {
  private static final int    DIM        = 8;
  private static final int    GRAPH_ROWS = 1100; // per bucket: from this size on a search does not rebuild the graph before it answers
  private static final int    BULK_ROWS  = 1700; // per bucket: enough entries to cross into the third data page of the index
  private static final int    PAGE_SIZE  = 4_096;  // small pages: the entries of the index spread over many of them
  private static final String FOLLOWER   = "target/databases/VectorReplicaApplyOracle-follower";

  private final Map<RID, float[]> model     = new LinkedHashMap<>();
  private final List<RID>         graphRids = new ArrayList<>();
  private final List<RID>         buffered  = new ArrayList<>();
  private final Set<RID>          gone      = new HashSet<>();
  private       Random            rnd;
  private       String            quantization;
  private       int               buckets;
  private       DatabaseFactory   followerFactory;
  private       DatabaseInternal  follower;

  @AfterEach
  void closeFollower() {
    if (follower != null && follower.isOpen())
      follower.close();
    if (followerFactory != null)
      followerFactory.close();
    FileUtils.deleteRecursively(new File(FOLLOWER));
  }

  private float[] randomVector() {
    final float[] v = new float[DIM];
    for (int i = 0; i < DIM; i++)
      v[i] = (float) rnd.nextGaussian();
    return v;
  }

  private static void createSchema(final Database db, final String quant, final int buckets) {
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
      db.command("sql", "CREATE PROPERTY Doc.name STRING");
      db.command("sql", "CREATE PROPERTY Doc.vector ARRAY_OF_FLOATS");
      ((TypeLSMVectorIndexBuilder) db.getSchema().buildTypeIndex("Doc", new String[] { "vector" }).withLSMVectorType()
          .withPageSize(PAGE_SIZE)).withDimensions(DIM).withQuantization(VectorQuantizationType.valueOf(quant)).withSimilarity("COSINE")
          .create();
    });
  }

  private static LSMVectorIndex[] indexes(final Database db) {
    final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Doc[vector]");
    return Arrays.stream(typeIndex.getIndexesOnBuckets()).map(i -> (LSMVectorIndex) i).toArray(LSMVectorIndex[]::new);
  }

  /** The WAL bytes of a commit as the follower decodes them: txId, timestamp, page count, segment size, then every page segment */
  private static WALFile.WALTransaction decode(final byte[] data) {
    final ByteBuffer buf = ByteBuffer.wrap(data);
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();
    tx.txId = buf.getLong();
    tx.timestamp = buf.getLong();
    tx.forceApply = tx.txId < 0;
    final int pageCount = buf.getInt();
    buf.getInt();
    tx.pages = new WALFile.WALPage[pageCount];
    for (int i = 0; i < pageCount; i++) {
      final WALFile.WALPage page = new WALFile.WALPage();
      page.fileId = buf.getInt();
      page.pageNumber = buf.getInt();
      page.changesFrom = buf.getInt();
      page.changesTo = buf.getInt();
      page.currentPageVersion = buf.getInt();
      page.currentPageSize = buf.getInt();
      final byte[] content = new byte[page.changesTo - page.changesFrom + 1];
      buf.get(content);
      page.currentContent = new Binary(content);
      tx.pages[i] = page;
    }
    return tx;
  }

  /** commits {@code work} on the leader, then applies the pages of that commit to the follower, as a follower applies a Raft entry */
  private void commitAndReplicate(final Runnable work) {
    final DatabaseInternal leader = (DatabaseInternal) database;
    leader.begin();
    final TransactionContext tx = leader.getTransaction();
    final byte[] wal;
    final Map<Integer, Integer> bucketDelta;
    try {
      work.run();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
      wal = phase1.result.toByteArray();
      bucketDelta = new HashMap<>(tx.getBucketRecordDelta());
      tx.commit2ndPhase(phase1);
    } finally {
      if (leader.isTransactionActive())
        leader.rollback();
    }
    assertThat(follower.getTransactionManager().applyChanges(decode(wal), bucketDelta, false)).as("the entry advanced a page").isTrue();
  }

  private RID insert() {
    final float[] v = randomVector();
    final RID rid = database.newDocument("Doc").set("name", "n").set("vector", v).save().getIdentity();
    model.put(rid, v);
    // the slot of a deleted record is handed out again
    gone.remove(rid);
    return rid;
  }

  private void rewrite(final RID rid) {
    final float[] v = randomVector();
    database.lookupByRID(rid, true).asDocument(true).modify().set("vector", v).save();
    model.put(rid, v);
  }

  private void delete(final RID rid) {
    database.lookupByRID(rid, true).asDocument(true).delete();
    model.remove(rid);
    gone.add(rid);
  }

  private void removeVector(final RID rid) {
    final MutableDocument doc = database.lookupByRID(rid, true).asDocument(true).modify();
    doc.remove("vector");
    doc.save();
    model.remove(rid);
    gone.add(rid);
  }

  private void start(final String quant, final int buckets) {
    this.buckets = buckets;
    quantization = quant;
    rnd = new Random(91);
    FileUtils.deleteRecursively(new File(FOLLOWER));
    followerFactory = new DatabaseFactory(FOLLOWER);
    follower = (DatabaseInternal) followerFactory.create();
    // no rebuild may fold the delta buffer into the graph while the test looks at it
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    follower.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 1_000_000);
    createSchema(database, quant, buckets);
    createSchema(follower, quant, buckets);
    assertThat(indexes(follower)).hasSameSizeAs(indexes(database));
    for (int i = 0; i < indexes(follower).length; i++)
      assertThat(indexes(follower)[i].getFileId()).as("the files of both nodes carry the same ids").isEqualTo(indexes(database)[i].getFileId());

    commitAndReplicate(() -> {
      for (int i = 0; i < GRAPH_ROWS * buckets; i++)
        graphRids.add(insert());
    });
    for (final LSMVectorIndex index : indexes(database))
      index.buildVectorGraphNow();
    for (final LSMVectorIndex index : indexes(follower))
      index.buildVectorGraphNow();
  }

  private static double distance(final float[] a, final float[] b) {
    double dot = 0, na = 0, nb = 0;
    for (int i = 0; i < a.length; i++) {
      dot += (double) a[i] * b[i];
      na += (double) a[i] * a[i];
      nb += (double) b[i] * b[i];
    }
    return 1.0 - dot / Math.sqrt(na * nb);
  }

  private double tolerance() {
    return quantization.equals("NONE") ? 2e-4 : 0.05;
  }

  private List<Pair<RID, Float>> search(final Database db, final float[] q, final int k, final List<RID> allow) {
    final List<Pair<RID, Float>> answer = new ArrayList<>();
    final String options = allow == null ? "{efSearch: 300}" : "{efSearch: 300, filter: ?}";
    final Object[] args = allow == null ? new Object[] { q, k } : new Object[] { q, k, allow };
    try (final ResultSet rs = db.query("sql",
        "SELECT @rid AS rid, distance FROM (SELECT expand(vector.neighbors('Doc[vector]', ?, ?, " + options + ")))", args)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        answer.add(new Pair<>(((Identifiable) r.getProperty("rid")).getIdentity(), r.<Number>getProperty("distance").floatValue()));
      }
    }
    return answer;
  }

  /** the follower holds what the leader holds: counters, id allocator, and the location of every id and every record */
  private void assertFollowerHoldsWhatTheLeaderHolds(final String where, final boolean counters) {
    final LSMVectorIndex[] leaders = indexes(database);
    final LSMVectorIndex[] followers = indexes(follower);
    for (int b = 0; b < leaders.length; b++) {
      final LSMVectorIndex l = leaders[b];
      final LSMVectorIndex f = followers[b];
      assertThat(f.countEntries()).as("%s: entries of bucket index %d", where, b).isEqualTo(l.countEntries());
      if (counters) {
        final Map<String, Long> ls = l.getStats();
        final Map<String, Long> fs = f.getStats();
        for (final String counter : List.of("activeVectors", "deletedVectors", "totalVectors", "deltaVectorsCount", "mutationsSinceRebuild",
            "graphState"))
          assertThat(fs.get(counter)).as("%s: %s of bucket index %d", where, counter, b).isEqualTo(ls.get(counter));
      }
      final VectorLocationIndex ll = l.residentLocationsForTest();
      final VectorLocationIndex fl = f.residentLocationsForTest();
      assertThat(fl.getNextId()).as("%s: id allocator of bucket index %d", where, b).isEqualTo(ll.getNextId());
      for (int id = 0; id < ll.getNextId(); id++) {
        assertThat(fl.isLive(id)).as("%s: id %d live on bucket index %d", where, id, b).isEqualTo(ll.isLive(id));
        assertThat(fl.isDeleted(id)).as("%s: id %d deleted on bucket index %d", where, id, b).isEqualTo(ll.isDeleted(id));
        if (ll.isLive(id)) {
          assertThat(fl.getRid(id)).as("%s: record of id %d on bucket index %d", where, id, b).isEqualTo(ll.getRid(id));
          assertThat(fl.getOffsetAndFlag(id)).as("%s: file offset of id %d on bucket index %d", where, id, b)
              .isEqualTo(ll.getOffsetAndFlag(id));
        }
      }
    }
  }

  /** the follower answers like the model: every live record once at the distance to its current vector, nothing else, and a deleted one never */
  private void assertFollowerAnswersLikeTheModel(final String where) {
    final double tolerance = tolerance();
    final float[] q = randomVector();
    final List<Pair<RID, Float>> all = search(follower, q, model.size() + 10, null);
    final Set<RID> seen = new HashSet<>();
    for (final Pair<RID, Float> hit : all) {
      assertThat(seen.add(hit.getFirst())).as("%s: %s once", where, hit.getFirst()).isTrue();
      assertThat(model).as("%s: only live records answer, not %s", where, hit.getFirst()).containsKey(hit.getFirst());
      assertThat((double) hit.getSecond()).as("%s: distance of %s is to its current vector", where, hit.getFirst())
          .isCloseTo(distance(q, model.get(hit.getFirst())), within(tolerance));
    }
    assertThat(seen).as("%s: an exhaustive search finds every live record", where).isEqualTo(model.keySet());
    for (final RID deleted : gone)
      assertThat(seen).as("%s: and not the deleted %s", where, deleted).doesNotContain(deleted);

    // the rows written since the graph was built are found by their own vector and by an allow-list, which is answered exactly
    final List<RID> allow = new ArrayList<>();
    for (final RID rid : buffered)
      if (model.containsKey(rid) && allow.size() < 40)
        allow.add(rid);
    for (final RID rid : graphRids.subList(0, 6))
      if (model.containsKey(rid))
        allow.add(rid);
    final List<Pair<RID, Float>> filtered = search(follower, q, allow.size() + 5, allow);
    assertThat(filtered.stream().map(Pair::getFirst).toList()).as("%s: the allow-list is answered exactly", where)
        .containsExactlyInAnyOrderElementsOf(allow);
    for (final Pair<RID, Float> hit : filtered)
      assertThat((double) hit.getSecond()).as("%s: distance of allowed %s", where, hit.getFirst())
          .isCloseTo(distance(q, model.get(hit.getFirst())), within(tolerance));
    for (final RID rid : allow.subList(0, Math.min(allow.size(), 12))) {
      final List<Pair<RID, Float>> own = search(follower, model.get(rid), 3, null);
      assertThat(own.stream().map(Pair::getFirst)).as("%s: %s finds itself by its own vector", where, rid).contains(rid);
    }
  }

  private void assertFollower(final String where) {
    assertFollowerHoldsWhatTheLeaderHolds(where, true);
    assertFollowerAnswersLikeTheModel(where);
  }

  @ParameterizedTest
  @CsvSource({ "NONE,1", "INT8,1", "NONE,3" })
  void aFollowerThatAppliesTheLeadersPagesHoldsAndAnswersWhatTheLeaderDoes(final String quant, final int buckets) {
    start(quant, buckets);
    assertFollower("after the first thousand rows");

    // rows past the graph, across several pages of the index
    commitAndReplicate(() -> {
      for (int i = 0; i < 12; i++)
        buffered.add(insert());
    });
    assertFollower("after 12 buffered rows");
    commitAndReplicate(() -> {
      for (int i = 0; i < BULK_ROWS * buckets; i++)
        buffered.add(insert());
    });
    for (final LSMVectorIndex index : indexes(follower))
      assertThat(index.getTotalPages()).as("the entries of the follower's index spread over several pages").isGreaterThan(3);
    assertFollower("after the bulk insert");

    // deletes of rows in the graph and in the buffer, and a record that loses its vector
    commitAndReplicate(() -> {
      delete(graphRids.get(0));
      delete(graphRids.get(500));
      delete(buffered.get(1));
      delete(buffered.get(buffered.size() - 1));
      removeVector(graphRids.get(7));
    });
    assertFollower("after the deletes");

    // a graph row rewritten, a buffered row rewritten once, and one rewritten twice in the same transaction
    commitAndReplicate(() -> {
      rewrite(graphRids.get(10));
      rewrite(graphRids.get(20));
      rewrite(buffered.get(2));
      rewrite(buffered.get(3));
      rewrite(buffered.get(3));
    });
    assertFollower("after the rewrites");

    // every kind of write in one transaction, including a row that is inserted and deleted again in it
    commitAndReplicate(() -> {
      for (int i = 0; i < 5; i++)
        buffered.add(insert());
      delete(buffered.get(buffered.size() - 1));
      delete(graphRids.get(30));
      rewrite(graphRids.get(40));
      rewrite(buffered.get(4));
      delete(buffered.get(5));
    });
    assertFollower("after the mixed transaction");

    // a transaction that the leader rolls back reaches no follower, and the next one applies on top of the last
    database.begin();
    database.lookupByRID(graphRids.get(50), true).asDocument(true).delete();
    database.lookupByRID(graphRids.get(51), true).asDocument(true).modify().set("vector", randomVector()).save();
    database.rollback();
    assertFollower("after a rolled back transaction");
    commitAndReplicate(() -> {
      rewrite(graphRids.get(60));
      buffered.add(insert());
    });
    assertFollower("after the commit that follows the rollback");

    // the follower restarts: its locations are read back from the pages it holds, and it answers the same
    follower.close();
    follower = (DatabaseInternal) followerFactory.open();
    assertFollowerHoldsWhatTheLeaderHolds("after the follower restarted", false);
    assertFollowerAnswersLikeTheModel("after the follower restarted");
  }

  /** records of which every second one carries a vector, so a rebuild finds fewer vectors in the pages than records in the bucket and scans them */
  private void loadHalfVectors() {
    commitAndReplicate(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument doc = database.newDocument("Doc").set("name", "n");
        if (i % 2 == 0) {
          final float[] v = randomVector();
          doc.set("vector", v);
          model.put(doc.save().getIdentity(), v);
        } else
          doc.save();
      }
    });
  }

  private void rebuildFollowerWith(final Runnable afterPagesParsed, final Runnable afterLiveSetRead) {
    final LSMVectorIndex f = indexes(follower)[0];
    f.setAfterPagesParsedForTest(afterPagesParsed);
    f.setAfterLiveSetReadForTest(afterLiveSetRead);
    try {
      f.buildVectorGraphNow();
    } finally {
      f.setAfterPagesParsedForTest(null);
      f.setAfterLiveSetReadForTest(null);
    }
  }

  private void assertFollowerAgreesAfterTheRebuild(final RID deleted, final float[] probe) {
    final LSMVectorIndex f = indexes(follower)[0];
    assertThat(f.residentLocationsForTest().getVectorIdsForRid(deleted)).as("the deleted record has no live id").isEmpty();
    assertThat(f.countEntries()).as("one vector per record that carries one").isEqualTo(indexes(database)[0].countEntries())
        .isEqualTo(model.size());
    assertThat(search(follower, probe, model.size() + 10, null).stream().map(Pair::getFirst))
        .as("the follower answers the live records only").containsExactlyInAnyOrderElementsOf(model.keySet());
  }

  /**
   * The case of issue #13148 on a follower: a record replicated while the follower's rebuild runs, and its delete replicated before the
   * rebuild publishes. Nothing is left of the record on the leader or on the follower.
   */
  @Test
  void aRecordReplicatedAndDeletedWhileTheFollowerRebuildsLeavesNoVector() {
    start("NONE", 1);
    loadHalfVectors();
    final RID[] inserted = new RID[1];
    final float[] vector = randomVector();
    rebuildFollowerWith(() -> inAnotherThread(() -> commitAndReplicate(() -> {
      inserted[0] = database.newDocument("Doc").set("vector", vector).save().getIdentity();
      model.put(inserted[0], vector);
    })), () -> inAnotherThread(() -> commitAndReplicate(() -> delete(inserted[0]))));

    assertFollowerAgreesAfterTheRebuild(inserted[0], vector);
  }

  /** a page whose entries the follower already holds (an entry applied twice, a node's own commit echoed back) changes nothing */
  @Test
  void aPageWhoseEntriesTheFollowerAlreadyHoldsLeavesItsSearchStateAlone() throws Exception {
    start("NONE", 1);
    final LSMVectorIndex f = indexes(follower)[0];
    assertThat(f.getStats().get("graphState")).as("precondition: the graph is IMMUTABLE (1)").isEqualTo(1L);
    assertThat(f.getStats().get("mutationsSinceRebuild")).as("precondition: the build paid every write off").isZero();

    for (int page = 0; page < f.getTotalPages(); page++) {
      final ImmutablePage image = follower.getPageManager().getImmutablePage(new PageId(follower, f.getFileId(), page), f.getPageSize(),
          false, false);
      f.applyReplicatedPageUpdate(image.modify());
    }

    assertThat(f.getStats().get("graphState")).as("nothing changed, the graph stays IMMUTABLE").isEqualTo(1L);
    assertThat(f.getStats().get("mutationsSinceRebuild")).as("and nothing is charged").isZero();
    assertThat(f.getStats().get("deltaVectorsCount")).isZero();
    assertFollowerHoldsWhatTheLeaderHolds("after the pages were applied twice", true);
    assertFollowerAnswersLikeTheModel("after the pages were applied twice");
  }

  /**
   * A follower nobody searches still folds what it replicated into its graph: the replicated page arms the inactivity rebuild as a
   * local write does, so the delta buffer of such a node stays bounded and a follower promoted to leader takes over a graph that
   * holds everything it replicated.
   */
  @Test
  void aFollowerNobodySearchesRebuildsItsGraphAfterTheQuietPeriod() throws Exception {
    start("NONE", 1);
    follower.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_MUTATIONS_BEFORE_REBUILD, 10);
    follower.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_INACTIVITY_REBUILD_TIMEOUT_MS, 300);
    final LSMVectorIndex f = indexes(follower)[0];

    // The replicated page arms the inactivity rebuild, which folds the delta buffer into the graph. Every rebuild takes a permit of the
    // JVM-wide semaphore (the small-graph arm re-arms itself when it cannot get one), so holding all the permits keeps the rebuild
    // from running before the buffered rows are observed, whatever the test thread's scheduling does to the 300 ms window.
    final Semaphore permits = LSMVectorIndex.rebuildSemaphoreForTesting();
    assertThat(permits.tryAcquire(30, TimeUnit.SECONDS)).as("a rebuild the setup started returns its permit").isTrue();
    final int held = 1 + permits.drainPermits();
    try {
      commitAndReplicate(() -> {
        for (int i = 0; i < 50; i++)
          buffered.add(insert());
      });
      assertThat(f.getStats().get("deltaVectorsCount")).as("the replicated rows wait in the delta buffer").isEqualTo(50L);
    } finally {
      permits.release(held);
    }

    final long deadline = System.currentTimeMillis() + 60_000;
    while (f.getStats().get("deltaVectorsCount") != 0L && System.currentTimeMillis() < deadline)
      Thread.sleep(100);

    assertThat(f.getStats().get("deltaVectorsCount")).as("the quiet period folded the rows into the graph").isZero();
    assertThat(f.getStats().get("mutationsSinceRebuild")).isZero();
    assertFollowerAnswersLikeTheModel("after the follower rebuilt on its own");
  }

  private static void inAnotherThread(final Runnable work) {
    final Throwable[] failure = new Throwable[1];
    final Thread thread = new Thread(() -> {
      try {
        work.run();
      } catch (final Throwable t) {
        failure[0] = t;
      }
    });
    thread.start();
    try {
      thread.join(60_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertThat(thread.isAlive()).as("the writer finished").isFalse();
    assertThat(failure[0]).as("the writer failed").isNull();
  }
}
