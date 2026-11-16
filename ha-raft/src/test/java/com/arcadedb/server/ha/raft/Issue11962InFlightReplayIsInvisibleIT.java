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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A commit's vectors are visible to searches only once the commit is (issue #11962). The commit replay used to publish them
 * into the index's location index and delta buffer during phase 1, before the transaction's pages and records were, which on
 * a Raft leader is the whole quorum round trip. A search in that window answered the RID of a record nobody could read yet,
 * or of one whose commit was then refused; and an update had already tombstoned the record's committed vector, so the record
 * went missing from searches by it. The replay now writes its pages and defers what it publishes to other readers to the
 * commit's conclusion: published when it commits, dropped when it rolls back.
 * <p>
 * Each test searches from {@link RaftReplicatedDatabase#TEST_PRE_DISPATCH_HOOK}, on another thread as a search on another
 * connection would, while the commit sits between its replay and its dispatch to Raft.
 */
class Issue11962InFlightReplayIsInvisibleIT extends BaseRaftHATest {
  private static final int DIMENSIONS = 16;
  // Past the size below which a search rebuilds a graph that took a write on its own thread: that rebuild commits, and waits
  // for the file lock the commit in flight holds, so a search of a smaller graph cannot run inside the window at all
  private static final int BASE       = 1_200;

  private final Random             random    = new Random(119622);
  private final Map<RID, float[]>  committed = new LinkedHashMap<>();

  @Override
  protected int getServerCount() {
    return 3;
  }

  @AfterEach
  void clearHook() {
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = null;
  }

  @Test
  void aVectorWhoseCommitIsInFlightIsNotAnswered() {
    final Database db = leaderDatabase();
    final LSMVectorIndex index = createIndex(db, "Inserted");

    final float[] inserted = vector();
    final WindowSearch window = searchDuringTheNextCommit(db, index, inserted, BASE + 10, false);
    final RID[] rid = new RID[1];
    db.transaction(() -> rid[0] = db.newDocument("Inserted").set("vector", inserted).save().getIdentity());

    assertThat(window.result()).as("the vector of a commit still in flight is not answered (issue #11962)")
        .doesNotContain(rid[0]).hasSize(BASE);
    assertThat(neighbours(db, index, inserted, 1)).as("and is answered once the commit completes").containsExactly(rid[0]);
  }

  @Test
  void anUpdateInFlightLeavesTheCommittedVectorAnswered() {
    final Database db = leaderDatabase();
    final LSMVectorIndex index = createIndex(db, "Updated");
    final Map.Entry<RID, float[]> target = committed.entrySet().iterator().next();

    final float[] update = vector();
    final WindowSearch window = searchDuringTheNextCommit(db, index, target.getValue(), 1, false);
    db.transaction(() -> target.getKey().asDocument(true).modify().set("vector", update).save());

    assertThat(window.result())
        .as("while the update is in flight the record is still found by the vector it committed (issue #11962)")
        .containsExactly(target.getKey());
    assertThat(neighbours(db, index, update, 1)).as("and by its new vector once the update completes")
        .containsExactly(target.getKey());
  }

  @Test
  void aRefusedCommitIsNeverAnswered() {
    final Database db = leaderDatabase();
    final LSMVectorIndex index = createIndex(db, "Refused");

    final float[] refused = vector();
    final RID[] rid = new RID[1];
    final WindowSearch window = searchDuringTheNextCommit(db, index, refused, BASE + 10, true);
    assertThatThrownBy(() -> db.transaction(() -> rid[0] = db.newDocument("Refused").set("vector", refused).save().getIdentity()))
        .as("precondition: the commit is refused after its replay").isNotNull();

    assertThat(window.result()).as("the vector of a commit that is then refused was never answered (issue #11962)")
        .doesNotContain(rid[0]).hasSize(BASE);
    assertThat(neighbours(db, index, refused, BASE + 10)).as("nor is it after the rollback").doesNotContain(rid[0])
        .hasSize(BASE);
  }

  /**
   * A full rebuild that read the pages while the commit was in flight, and publishes what it read after the commit
   * published its vector. The vector's id was allocated by the replay, before the rebuild took its snapshot of the id
   * sequence, and the pages the rebuild read did not hold it yet: the rebuild has to carry the publication over into the
   * location index it publishes, or the committed vector loses its location and no search finds it.
   */
  @Test
  void aCommitPublishedWhileARebuildReadThePagesSurvivesItsPublish() throws Exception {
    final Database db = leaderDatabase();
    final LSMVectorIndex index = createIndex(db, "Rebuilt");

    final CountDownLatch pagesRead = new CountDownLatch(1);
    final CountDownLatch commitPublished = new CountDownLatch(1);
    final AtomicReference<Throwable> buildFailure = new AtomicReference<>();
    final Thread[] builder = new Thread[1];
    final Method afterLiveSetRead = LSMVectorIndex.class.getDeclaredMethod("setAfterLiveSetReadForTest", Runnable.class);
    afterLiveSetRead.setAccessible(true);

    final AtomicBoolean armed = new AtomicBoolean(true);
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = name -> {
      if (!getDatabaseName().equals(name) || !armed.getAndSet(false))
        return;
      // The rebuild starts once the replay has allocated, reads the pages, and is held before it publishes what it read
      builder[0] = new Thread(() -> {
        try {
          afterLiveSetRead.invoke(index, (Runnable) () -> {
            pagesRead.countDown();
            try {
              commitPublished.await(60, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
          index.buildVectorGraphNow();
        } catch (final Throwable t) {
          buildFailure.set(t);
        } finally {
          pagesRead.countDown();
        }
      });
      builder[0].start();
      try {
        pagesRead.await(60, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    };

    final float[] inserted = vector();
    final RID[] rid = new RID[1];
    try {
      db.transaction(() -> rid[0] = db.newDocument("Rebuilt").set("vector", inserted).save().getIdentity());
    } finally {
      commitPublished.countDown();
      if (builder[0] != null)
        builder[0].join(60_000);
      afterLiveSetRead.invoke(index, (Runnable) null);
    }

    assertThat(builder[0]).as("precondition: a rebuild ran inside the commit window").isNotNull();
    assertThat(buildFailure.get()).as("precondition: the rebuild completed").isNull();
    assertThat(neighbours(db, index, inserted, 1))
        .as("the vector published while the rebuild read the pages keeps its location through the rebuild's publish (issue #11962)")
        .containsExactly(rid[0]);
  }

  private Database leaderDatabase() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }

  private LSMVectorIndex createIndex(final Database db, final String typeName) {
    db.transaction(() -> db.getSchema().createDocumentType(typeName, 1).createProperty("vector", Type.ARRAY_OF_FLOATS));
    db.command("sql", "CREATE INDEX ON " + typeName + " (vector) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS
        + ", similarity: 'EUCLIDEAN', quantization: 'INT8'}");
    db.transaction(() -> {
      for (int i = 0; i < BASE; i++) {
        final float[] v = vector();
        committed.put(db.newDocument(typeName).set("vector", v).save().getIdentity(), v);
      }
    });
    waitForAllServers();
    final LSMVectorIndex index = (LSMVectorIndex) db.getSchema().getType(typeName).getPolymorphicIndexByProperties("vector")
        .getIndexesOnBuckets()[0];
    index.buildVectorGraphNow();
    return index;
  }

  /** What a search answered from inside a commit window, read once the commit concluded. */
  private record WindowSearch(AtomicReference<List<RID>> found, AtomicReference<Throwable> failure,
                              AtomicReference<String> fired) {
    List<RID> result() {
      assertThat(failure.get()).as("the search inside the commit window failed").isNull();
      assertThat(found.get()).as("precondition: a search ran inside the commit window (%s)", fired.get()).isNotNull();
      return found.get();
    }
  }

  /**
   * Arms the hook for the next commit of the test database, once: a search on another thread, then, when {@code refuse}, a
   * refusal of the commit on the committing thread, which rolls the transaction back.
   */
  private WindowSearch searchDuringTheNextCommit(final Database db, final LSMVectorIndex index, final float[] query, final int k,
      final boolean refuse) {
    final WindowSearch window = new WindowSearch(new AtomicReference<>(), new AtomicReference<>(),
        new AtomicReference<>("the hook did not fire"));
    final AtomicBoolean armed = new AtomicBoolean(true);
    RaftReplicatedDatabase.TEST_PRE_DISPATCH_HOOK = name -> {
      if (!getDatabaseName().equals(name) || !armed.getAndSet(false))
        return;
      final Thread searcher = new Thread(() -> {
        try {
          window.found().set(neighbours(db, index, query, k));
        } catch (final Throwable t) {
          window.failure().set(t);
        }
      });
      searcher.start();
      try {
        searcher.join(60_000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      window.fired().set("fired on thread " + Thread.currentThread().getName() + ", searcher alive after the join: "
          + searcher.isAlive() + (searcher.isAlive() ? " at " + Arrays.toString(searcher.getStackTrace()) : ""));
      if (refuse)
        throw new TransactionException("refused by the test after the replay");
    };
    return window;
  }

  /** A search in a transaction of its own, as a query on a connection runs, rolled back since it writes nothing. */
  private static List<RID> neighbours(final Database db, final LSMVectorIndex index, final float[] query, final int k) {
    db.begin();
    try {
      return index.findNeighborsFromVector(query, k).stream().map(Pair::getFirst).toList();
    } finally {
      db.rollback();
    }
  }

  private float[] vector() {
    final float[] v = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      v[d] = random.nextFloat();
    return v;
  }
}
