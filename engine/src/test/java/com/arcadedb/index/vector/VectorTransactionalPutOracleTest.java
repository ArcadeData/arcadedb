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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.engine.BasePage;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Oracle tests of the writes of a vector index, held to brute-force exact search over a model of the live set.
 * <p>
 * A transaction does not call {@code put}: it queues the operation, and the commit hands the vector indexes their ADDs through
 * {@code putBatch}. {@code put} applies a vector at once - the id, the page entry, the delta buffer entry, the graph state, the
 * counters - and is reached through {@code putReplay}, which these tests call from inside a transaction, as the replay of a
 * write-ahead log does, so that its pages commit with the transaction around it. A record the index is handed a vector for this
 * way carries no vector property: INT8 reads the vector back from the index's own pages, so those tests reopen the database;
 * NONE reads it back from the document, so there the answers are checked in the session of the write only.
 */
@Tag("vector")
class VectorTransactionalPutOracleTest extends VectorReuseOracleFixture {
  /** The size from which a search answers from the graph and the delta buffer, instead of rebuilding the graph first. */
  private static final int GRAPH_ROWS = 1000;
  /** No timer folds the buffer into the graph while a test looks at it. */
  private static final String NO_TIMER = ",\"inactivityRebuildTimeoutMs\":0";

  private void graphOf(final String quant, final int rows) {
    createIndex(quant, NO_TIMER);
    insertRows(rows);
    index().buildVectorGraphNow();
    assertThat(stat("graphState")).as("a graph is built and nothing is pending").isEqualTo(1L);
    assertThat(stat("deltaVectorsCount")).isZero();
    assertThat(stat("mutationsSinceRebuild")).isZero();
  }

  /** Saves a record without a vector and hands the index the vector of that record by {@code putReplay}, in the open transaction. */
  private RID directPut(final float[] vector) {
    final RID rid = database.newDocument("Doc").set("id", model.size()).save().getIdentity();
    index().putReplay(new Object[] { vector }, new RID[] { rid });
    model.put(rid, vector);
    return rid;
  }

  private List<RID> directPuts(final int count) {
    final List<RID> rids = new ArrayList<>(count);
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids.add(directPut(randomVector()));
    });
    return rids;
  }

  private void assertEachFindsItself(final String where, final List<RID> rids) {
    for (final RID rid : rids)
      assertThat(search(model.get(rid), 1)).as("%s: %s by its own vector", where, rid).extracting(p -> p.getFirst())
          .containsExactly(rid);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void aDirectPutIsInTheIndexAtOnceAndInItsCountersAndAnswers(final String quant) {
    graphOf(quant, GRAPH_ROWS);
    final long rebuildsBefore = stat("graphRebuildCount");
    final long insertsBefore = stat("insertOperations");
    final List<RID> added = new ArrayList<>();

    database.transaction(() -> {
      for (int i = 0; i < 12; i++)
        added.add(directPut(randomVector()));

      // Before the commit: put does not wait for it
      assertThat(stat("graphState")).as("the first put moved the graph to MUTABLE").isEqualTo(2L);
      assertThat(stat("deltaVectorsCount")).as("every put is in the buffer a search merges").isEqualTo(12L);
      assertThat(stat("mutationsSinceRebuild")).as("and is a mutation owed to the next rebuild").isEqualTo(12L);
      assertThat(stat("totalVectors")).isEqualTo(GRAPH_ROWS + 12);
      assertThat(stat("activeVectors")).isEqualTo(GRAPH_ROWS + 12);
      assertThat(index().countEntries()).isEqualTo(GRAPH_ROWS + 12);
    });

    assertThat(stat("insertOperations")).as("every put is counted, and so is the save of the record without a vector")
        .isEqualTo(insertsBefore + 2 * 12);
    assertThat(stat("avgInsertLatencyMs")).as("a latency is a time taken: not a sum of clock readings").isBetween(0L, 3_600_000L);
    assertEachFindsItself("committed", added);
    assertMatchesModel("committed", 41, 4);
    assertThat(stat("graphRebuildCount")).as("answered by the graph and the buffer, with no rebuild").isEqualTo(rebuildsBefore);

    if ("INT8".equals(quant)) {
      reopenDatabase();

      assertThat(stat("totalVectors")).isEqualTo(GRAPH_ROWS + 12);
      assertThat(index().countEntries()).isEqualTo(GRAPH_ROWS + 12);
      assertEachFindsItself("reopened", added);
      assertMatchesModel("reopened", 41, 4);
    }
  }

  /**
   * The first put of a session finds the index with its locations unread and its graph unloaded: it has to read the ids of the
   * earlier vectors before it takes one, or it hands out an id that is already a record's.
   */
  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void theFirstDirectPutOfASessionNeverReusesAVectorIdOfAnEarlierOne(final String quant) {
    graphOf(quant, GRAPH_ROWS);
    reopenDatabase();

    // Nothing of the index has been asked for yet
    final List<RID> added = directPuts(6);

    assertThat(stat("graphState")).as("a put moves a graph no search has loaded yet to MUTABLE too").isEqualTo(2L);
    assertThat(stat("totalVectors")).as("none of the earlier vectors was superseded").isEqualTo(GRAPH_ROWS + 6);
    assertEachFindsItself("after the first put", added);
    assertMatchesModel("after the first put", 41, 4);

    if ("INT8".equals(quant)) {
      reopenDatabase();

      assertThat(stat("totalVectors")).isEqualTo(GRAPH_ROWS + 6);
      assertEachFindsItself("reopened", added);
      assertMatchesModel("reopened", 41, 4);
    }
  }

  /** The write lock of a put is given back: no other thread is left waiting for it, and the thread of the put no longer owns it. */
  @Test
  void aDirectPutReleasesTheLockOfTheIndex() throws Exception {
    graphOf("INT8", GRAPH_ROWS);
    final ReentrantReadWriteLock lock = lockOf(index());

    final List<RID> added = directPuts(2);

    assertThat(lock.isWriteLocked()).as("the write lock taken by the put").isFalse();
    assertThat(lock.getWriteHoldCount()).isZero();
    assertEachFindsItself("after the puts", added);
  }

  /**
   * A load of the locations that could not read every entry leaves the index unable to tell which vector ids are taken: it
   * refuses new vectors, a put included, until it is rebuilt.
   */
  @Test
  void aDirectPutIsRefusedWhenTheLoadOfTheLocationsWasIncomplete() throws IOException {
    graphOf("INT8", 300);
    final String path = ((DatabaseInternal) database).getFileManager().getFile(index().getFileId()).getFilePath();
    database.close();
    try (final RandomAccessFile file = new RandomAccessFile(path, "rw")) {
      // The page says it holds more entries than a page can
      file.seek(BasePage.PAGE_HEADER_SIZE + LSMVectorIndex.OFFSET_NUM_ENTRIES);
      file.writeInt(Integer.MAX_VALUE);
    }
    database = factory.open();

    assertThatThrownBy(() -> database.transaction(() -> directPut(randomVector()))).isInstanceOf(IndexException.class)
        .hasMessageContaining("refuses new vectors");
  }

  /** A put arms the inactivity rebuild, which folds it into the graph with no search to do it. */
  @Test
  void aDirectPutArmsTheInactivityRebuildThatFoldsItIntoTheGraph() {
    graphOf("INT8", 200);
    assertThat(stat("graphRebuildCount")).isEqualTo(1L);
    // The timer is off while the fixture loads, so it cannot rebuild what the test builds; it is on for the put under test
    index().getMetadata().inactivityRebuildTimeoutMs = 200;

    final List<RID> added = directPuts(3);

    Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
      assertThat(stat("graphRebuildCount")).as("the timer rebuilt the graph").isEqualTo(2L);
      assertThat(stat("mutationsSinceRebuild")).isZero();
      assertThat(stat("deltaVectorsCount")).as("and the buffer is in it, but for the nodes the build left unreachable")
          .isEqualTo(stat("unreachableGraphNodes"));
    });
    assertThat(stat("graphNodeCount")).isEqualTo(203L);
    assertEachFindsItself("rebuilt by the timer", added);
    assertMatchesModel("rebuilt by the timer", 10, 4);
  }

  /**
   * The latency of a put is the time the put took, lock wait included: a put held up by another holder of the index lock for
   * 50 ms took at least that, however fast the rest of it is.
   */
  @Test
  void theInsertLatencyIsTheTimeAPutTookIncludingTheTimeItWaitedForTheIndexLock() throws Exception {
    graphOf("INT8", 50);
    reopenDatabase();
    assertThat(index().getStats().get("insertOperations")).as("the counters of a new session start at zero").isZero();
    final ReentrantReadWriteLock lock = lockOf(index());
    final CountDownLatch held = new CountDownLatch(1);
    final Throwable[] failure = new Throwable[1];
    final Thread holder = new Thread(() -> {
      lock.writeLock().lock();
      try {
        held.countDown();
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!lock.hasQueuedThreads() && System.nanoTime() < deadline)
          Thread.sleep(1);
        // The put is waiting for the lock: it has been for as long as this sleeps
        Thread.sleep(50);
      } catch (final Throwable t) {
        failure[0] = t;
      } finally {
        lock.writeLock().unlock();
      }
    }, "vector-put-oracle-lock-holder");
    holder.setDaemon(true);
    holder.start();
    assertThat(held.await(60, TimeUnit.SECONDS)).isTrue();

    final List<RID> added = directPuts(1);
    holder.join(60_000);

    assertThat(failure[0]).isNull();
    // The save of the record without a vector is counted as well, and takes no time: the 50 ms are spread over the operations
    final long operations = stat("insertOperations");
    assertThat(operations).isBetween(1L, 2L);
    assertThat(stat("avgInsertLatencyMs")).as("the put waited 50 ms").isBetween(50L / operations, 3_600_000L);
    assertEachFindsItself("after the wait", added);
  }

  private static ReentrantReadWriteLock lockOf(final LSMVectorIndex index) throws ReflectiveOperationException {
    final Field field = LSMVectorIndex.class.getDeclaredField("lock");
    field.setAccessible(true);
    return (ReentrantReadWriteLock) field.get(index);
  }

  /** What a commit leaves, for the records of the ordinary write path: saved with their vector, replayed by the commit. */
  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void committedWritesAreInTheDeltaBufferTheCountersTheAnswersAndTheNextOpen(final String quant) {
    graphOf(quant, GRAPH_ROWS);

    final List<RID> added = insertRows(12);

    assertThat(stat("graphState")).isEqualTo(2L);
    assertThat(stat("deltaVectorsCount")).isEqualTo(12L);
    assertThat(stat("mutationsSinceRebuild")).isEqualTo(12L);
    assertThat(index().countEntries()).isEqualTo(GRAPH_ROWS + 12);
    assertEachFindsItself("committed", added);
    assertMatchesModel("committed", 41, 4);

    reopenDatabase();

    assertThat(stat("totalVectors")).isEqualTo(GRAPH_ROWS + 12);
    assertEachFindsItself("reopened", added);
    assertMatchesModel("reopened", 41, 4);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NONE", "INT8" })
  void theIndexCursorOfAGetIsTheExactTopKAndDefaultsToTen(final String quant) {
    createIndex(quant);
    final IndexCursor empty = index().get(new Object[] { randomVector() });
    assertThat(empty.hasNext()).as("an empty index answers nothing").isFalse();

    insertRows(300);
    index().buildVectorGraphNow();
    final float[] q = randomVector();
    final List<Double> exact = exactDistances(q);

    final IndexCursor byDefault = index().get(new Object[] { q });
    final List<RID> defaults = new ArrayList<>();
    while (byDefault.hasNext())
      defaults.add(byDefault.next().getIdentity());
    assertThat(defaults).as("no limit is a limit of ten").hasSize(10);
    for (int rank = 0; rank < defaults.size(); rank++)
      assertThat(distance(q, model.get(defaults.get(rank)))).as("rank %d", rank).isCloseTo(exact.get(rank), within(tolerance()));

    final IndexCursor limited = index().get(new Object[] { q }, 3);
    final List<RID> three = new ArrayList<>();
    while (limited.hasNext())
      three.add(limited.next().getIdentity());
    assertThat(three).hasSize(3);
    for (int rank = 0; rank < 3; rank++)
      assertThat(distance(q, model.get(three.get(rank)))).as("limited rank %d", rank).isCloseTo(exact.get(rank), within(tolerance()));
  }

  /**
   * The removal of a vector is queued under the key of the vector it removes, whenever that can be read from what the caller
   * passed, so that the queue drops the addition it cancels; with no such key it is queued under a placeholder that equals no
   * key, and the pair stays. Either way the commit leaves the record out of the index.
   */
  @Test
  void aRemovalIsQueuedUnderTheKeyOfTheVectorItRemovesWheneverThatCanBeRead() {
    graphOf("NONE", 300);
    final Object[][] cases = {
        // key given to remove(), whether it cancels the addition queued for the same vector
        { "a ComparableVector of the vector", Boolean.TRUE },
        { "the float array of the vector", Boolean.TRUE },
        { "a ComparableVector of another width", Boolean.FALSE },
        { "a string", Boolean.FALSE },
        { "no key at all", Boolean.FALSE } };
    for (final Object[] c : cases) {
      final float[] v = randomVector();
      database.begin();
      final RID rid = database.newDocument("Doc").set("id", -1).save().getIdentity();
      index().put(new Object[] { v }, new RID[] { rid });
      final TransactionIndexContext queue = ((DatabaseInternal) database).getTransaction().getIndexChanges();
      assertThat(queue.getTotalEntriesByIndex(index().getName())).as("%s: the addition", c[0]).isEqualTo(1);

      final Object[] keys;
      switch ((String) c[0]) {
      case "a ComparableVector of the vector" -> keys = new Object[] { new ComparableVector(v) };
      case "the float array of the vector" -> keys = new Object[] { v };
      case "a ComparableVector of another width" -> keys = new Object[] { new ComparableVector(new float[] { 1f, 2f }) };
      case "a string" -> keys = new Object[] { "not a vector" };
      default -> keys = new Object[] { null };
      }
      index().remove(keys, rid);

      assertThat(queue.getTotalEntriesByIndex(index().getName())).as("%s: the queue after the removal", c[0])
          .isEqualTo(((Boolean) c[1]) ? 1 : 2);
      database.commit();

      assertThat(stat("totalVectors")).as("%s: the record was never indexed", c[0]).isEqualTo(300L);
      assertThat(stat("activeVectors")).isEqualTo(300L);
    }
    assertMatchesModel("after the cancelled writes", 10, 4);
  }

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }
}
