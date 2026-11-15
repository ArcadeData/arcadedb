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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.utility.Pair;
import com.arcadedb.utility.RWLockContext;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11836: a vector search takes the database read lock before its index's lock and holds it for the whole search, the
 * order every writer of the index takes them in (a commit applies its index changes under the database read lock, a schema
 * change holds the database write lock). The search used to take the index lock first and the database read lock inside the
 * walk, for every vector it read. That deadlocked it with a DROP INDEX of its index, and, through the fair database lock, with
 * a schema change of any other type that queued behind a commit to the index.
 * <p>
 * The first two tests stop a real search inside its walk, line up the other side of the old cycle, and ask whether the read the
 * walk makes next would get the database lock. When it would not, the search is failed instead of left to wait, so the threads
 * of a failing run unwind rather than hang. The third checks the order on every search entry point.
 */
class Issue11836VectorSearchLockOrderTest extends TestHelper {
  private static final int  DIMENSIONS   = 8;
  private static final int  VECTORS      = 200;
  private static final long HANG_SECONDS = 60;

  private final Random random = new Random(11836);

  @Test
  void aDropOfTheIndexWaitsForASearchInsteadOfDeadlockingWithIt() throws Exception {
    final LSMVectorIndex index = indexedVectors("V", "");
    final StoppingAllowList allowList = new StoppingAllowList(index, "V");
    final Party<List<Pair<RID, Float>>> search = new Party<>("search",
        () -> index.findNeighborsFromVector(vector(), 10, -1, allowList));
    search.start();
    allowList.awaitStopped();

    final Party<Object> drop = new Party<>("drop", () -> {
      database.command("sql", "DROP INDEX `V[v]`").close();
      return null;
    });
    drop.start();
    // The drop waits either for the database lock the search holds or for the index lock it holds
    awaitParked("the drop waits for the search", () -> lockOf(index).hasQueuedThread(drop.thread) || queuedOnDatabaseLock(drop.thread));

    allowList.resume();
    search.finish();
    drop.finish();

    assertThat(allowList.walkCouldRead).as("the walk of a search can read through the database while a drop of its index waits")
        .isTrue();
    assertThat(search.failure).isNull();
    assertThat(search.result).hasSize(10);
    assertThat(drop.failure).isNull();
    assertThat(database.getSchema().existsIndex("V[v]")).isFalse();
  }

  @Test
  void aSchemaChangeOfAnotherTypeQueuedBehindACommitToTheIndexDoesNotDeadlockASearch() throws Exception {
    database.command("sql", "CREATE DOCUMENT TYPE O");
    database.command("sql", "CREATE PROPERTY O.x INTEGER");
    final LSMVectorIndex index = indexedVectors("V", "");
    final StoppingAllowList allowList = new StoppingAllowList(index, "V");
    final Party<List<Pair<RID, Float>>> search = new Party<>("search",
        () -> index.findNeighborsFromVector(vector(), 10, -1, allowList));

    // The commit holds the stripe of the database lock that the search reads under, so the schema change queues on that stripe:
    // the database lock is fair, and a reader that does not hold its stripe yet waits behind a queued writer
    final float[] inserted = vector();
    Party<Object> insert;
    do
      insert = new Party<>("insert", () -> {
        database.transaction(() -> database.newVertex("V").set("v", inserted).save());
        return null;
      });
    while (stripeOf(insert.thread) != stripeOf(search.thread));

    search.start();
    allowList.awaitStopped();

    final Party<Object> committing = insert;
    committing.start();
    awaitParked("the commit waits for the index lock the search holds", () -> lockOf(index).hasQueuedThread(committing.thread));

    final Party<Object> schemaChange = new Party<>("schema change", () -> {
      database.command("sql", "CREATE INDEX ON O (x) NOTUNIQUE").close();
      return null;
    });
    schemaChange.start();
    awaitParked("the schema change waits for the commit", () -> queuedOnDatabaseLock(schemaChange.thread));

    allowList.resume();
    search.finish();
    committing.finish();
    schemaChange.finish();

    assertThat(allowList.walkCouldRead).as(
        "the walk of a search can read through the database while a schema change waits for a commit that waits for the search")
        .isTrue();
    assertThat(search.failure).isNull();
    assertThat(search.result).hasSize(10);
    assertThat(committing.failure).isNull();
    assertThat(schemaChange.failure).isNull();
    assertThat(database.countType("V", false)).isEqualTo(VECTORS + 1);
    assertThat(database.getSchema().existsIndex("O[x]")).isTrue();
  }

  @Test
  void everySearchTakesTheIndexLockUnderTheDatabaseReadLock() {
    final LSMVectorIndex exact = indexedVectors("V", "");
    final LSMVectorIndex productQuantized = indexedVectors("P", ", quantization: 'PRODUCT', pqClusters: 16");
    assertThat(productQuantized.isPQSearchAvailable()).isTrue();
    final RecordingLock exactLock = recordingLockOf(exact);
    final RecordingLock productQuantizedLock = recordingLockOf(productQuantized);

    assertThat(exact.findNeighborsFromVector(vector(), 10)).hasSize(10);
    assertTakenUnderTheDatabaseReadLock(exactLock, "the exact search");

    assertThat(exact.findNeighborsFromVectorGrouped(vector(), 3, 2, -1, null, rid -> rid.getPosition() % 5)).isNotEmpty();
    assertTakenUnderTheDatabaseReadLock(exactLock, "the grouped search");

    assertThat(exact.get(new Object[] { vector() }, 10).hasNext()).isTrue();
    assertTakenUnderTheDatabaseReadLock(exactLock, "the lookup of the index API");

    assertThat(productQuantized.findNeighborsFromVectorApproximate(vector(), 10)).hasSize(10);
    assertTakenUnderTheDatabaseReadLock(productQuantizedLock, "the approximate search");
  }

  /** {@code VECTORS} vectors of a single-bucket type and their index, its graph built by a first search. */
  private LSMVectorIndex indexedVectors(final String type, final String metadata) {
    database.command("sql", "CREATE VERTEX TYPE " + type + " BUCKETS 1");
    database.command("sql", "CREATE PROPERTY " + type + ".v ARRAY_OF_FLOATS");
    database.transaction(() -> {
      for (int i = 0; i < VECTORS; i++)
        database.newVertex(type).set("v", vector()).save();
    });
    database.command("sql",
        "CREATE INDEX ON " + type + " (v) LSM_VECTOR METADATA {dimensions: " + DIMENSIONS + ", similarity: 'EUCLIDEAN'" + metadata
            + "}");
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(type + "[v]")).getSubIndexes()
        .getFirst();
    assertThat(index.findNeighborsFromVector(vector(), 10)).hasSize(10);
    return index;
  }

  private float[] vector() {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = (float) random.nextGaussian();
    return vector;
  }

  /**
   * The lock of an index, recording for every acquisition by the test thread that is not a re-entry whether the thread holds the
   * database read lock at that moment.
   */
  private final class RecordingLock extends ReentrantReadWriteLock {
    private final Thread        owner        = Thread.currentThread();
    private final List<Boolean> acquisitions = new CopyOnWriteArrayList<>();
    private final ReadLock      read         = new ReadLock(this) {
      @Override
      public void lock() {
        record();
        super.lock();
      }
    };
    private final WriteLock     write        = new WriteLock(this) {
      @Override
      public void lock() {
        record();
        super.lock();
      }
    };

    private void record() {
      if (Thread.currentThread() == owner && getReadHoldCount() == 0 && !isWriteLockedByCurrentThread()) {
        final ReentrantReadWriteLock stripe = stripeOf(owner);
        acquisitions.add(stripe.getReadHoldCount() > 0 || stripe.isWriteLockedByCurrentThread());
      }
    }

    @Override
    public ReadLock readLock() {
      return read;
    }

    @Override
    public WriteLock writeLock() {
      return write;
    }
  }

  /** Swaps the lock of {@code index}, idle, for a {@link RecordingLock}. */
  private RecordingLock recordingLockOf(final LSMVectorIndex index) {
    final RecordingLock recording = new RecordingLock();
    try {
      final Field field = LSMVectorIndex.class.getDeclaredField("lock");
      field.setAccessible(true);
      field.set(index, recording);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
    return recording;
  }

  private static void assertTakenUnderTheDatabaseReadLock(final RecordingLock lock, final String what) {
    assertThat(lock.acquisitions).as("%s takes the index lock", what).isNotEmpty();
    assertThat(lock.acquisitions).as("%s takes the index lock under the database read lock it took first", what).containsOnly(true);
    lock.acquisitions.clear();
  }

  /**
   * The allow-list of a search that lets every record of {@code type} through. It stops the search at the first lookup its walk
   * makes under the index lock, until {@link #resume()}. Then asks whether the next read of the walk would get the database read
   * lock, and fails the search when it would not.
   */
  private final class StoppingAllowList extends HashSet<RID> {
    private final    ReentrantReadWriteLock indexLock;
    private final    CountDownLatch         stopped = new CountDownLatch(1);
    private final    CountDownLatch         resumed = new CountDownLatch(1);
    private volatile Boolean                walkCouldRead;

    StoppingAllowList(final LSMVectorIndex index, final String type) {
      indexLock = lockOf(index);
      for (final Iterator<Record> it = database.iterateType(type, false); it.hasNext(); )
        add(it.next().getIdentity());
    }

    @Override
    public boolean contains(final Object rid) {
      if (stopped.getCount() > 0 && indexLock.getReadHoldCount() > 0) {
        stopped.countDown();
        try {
          assertThat(resumed.await(HANG_SECONDS, TimeUnit.SECONDS)).as("the test resumes the search").isTrue();
          walkCouldRead = databaseReadLockWithoutWaitingForever();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          walkCouldRead = false;
        }
        if (!walkCouldRead)
          throw new IllegalStateException("The walk would wait for the database lock forever: search abandoned");
      }
      return super.contains(rid);
    }

    void awaitStopped() throws InterruptedException {
      assertThat(stopped.await(HANG_SECONDS, TimeUnit.SECONDS)).as("the search reaches its walk").isTrue();
    }

    void resume() {
      resumed.countDown();
    }
  }

  /** What the next read of the walk does, the read lock of this thread's stripe, with a timeout instead of forever. */
  private boolean databaseReadLockWithoutWaitingForever() throws InterruptedException {
    final ReentrantReadWriteLock stripe = stripeOf(Thread.currentThread());
    if (!stripe.readLock().tryLock(1, TimeUnit.SECONDS))
      return false;
    stripe.readLock().unlock();
    return true;
  }

  /** A thread of the scenario, with what it answered or threw. */
  private static final class Party<T> {
    private final    Thread    thread;
    private volatile T         result;
    private volatile Throwable failure;

    private Party(final String name, final Callable<T> work) {
      thread = new Thread(() -> {
        try {
          result = work.call();
        } catch (final Throwable t) {
          failure = t;
        }
      }, name);
      thread.setDaemon(true);
    }

    private void start() {
      thread.start();
    }

    private void finish() throws InterruptedException {
      thread.join(TimeUnit.SECONDS.toMillis(HANG_SECONDS));
      assertThat(thread.isAlive()).as("%s is still running: the threads of the scenario deadlocked", thread.getName()).isFalse();
    }
  }

  private static void awaitParked(final String what, final BooleanSupplier parked) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(HANG_SECONDS);
    while (!parked.getAsBoolean() && System.nanoTime() < deadline)
      Thread.sleep(5);
    assertThat(parked.getAsBoolean()).as(what).isTrue();
  }

  private boolean queuedOnDatabaseLock(final Thread thread) {
    return Arrays.stream(databaseLockStripes()).anyMatch(stripe -> stripe.hasQueuedThread(thread));
  }

  private ReentrantReadWriteLock stripeOf(final Thread thread) {
    final ReentrantReadWriteLock[] stripes = databaseLockStripes();
    return stripes[(int) thread.threadId() & (stripes.length - 1)];
  }

  private ReentrantReadWriteLock[] databaseLockStripes() {
    try {
      final Field stripes = RWLockContext.class.getDeclaredField("stripes");
      stripes.setAccessible(true);
      return (ReentrantReadWriteLock[]) stripes.get(((DatabaseInternal) database).getEmbedded());
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private static ReentrantReadWriteLock lockOf(final LSMVectorIndex index) {
    try {
      final Field field = LSMVectorIndex.class.getDeclaredField("lock");
      field.setAccessible(true);
      return (ReentrantReadWriteLock) field.get(index);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
