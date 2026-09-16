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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The measurement that decided issue #7657, and the pin that keeps the decision from being undone by accident.
 * <p>
 * #7410 established that a TimeSeries append commits its own transaction whatever the caller has open, and
 * corrected the javadoc that said otherwise ({@code Issue7410AppendTransactionScopeTest} pins that). #7657 then
 * asked the deferred question: should the append instead <i>join</i> the enclosing transaction, so that
 * {@code INSERT INTO <timeseries type>} becomes atomic with its own statement's transaction? The answer recorded
 * on {@code TimeSeriesShard.appendSamples} is no, and these two tests are why - together they are an experiment
 * rather than an appeal to the surrounding comments.
 * <p>
 * {@link #concurrentAppendsToOneShardFromOpenTransactionsAllSucceed()} measures the property the current design
 * delivers. {@link #twoTransactionsHoldingTheSameHeaderPageCannotBothCommit()} measures what that property rests
 * on, by reproducing the shape the alternative would have: it drives the mutable bucket from two transactions the
 * shard does not own and shows that the second commit cannot land. Every append writes page 0 - the header
 * carries the sample count and the min/max timestamps - so two appends to one shard that are staged in separate
 * transactions and committed by their callers always contend for the same page version.
 * <p>
 * Today they never are staged that way: {@code TimeSeriesShard.appendLock} is held across the shard's whole
 * {@code begin}/{@code commit} cycle, so the two appends are serialized and the second reads the version the
 * first published. An append that joined the caller's transaction could not be serialized that way - the commit
 * would belong to the caller, and holding a shard lock until an arbitrary user transaction ends is not something
 * the shard can do - which is what turns the first test's outcome into the second's.
 */
class Issue7657AppendStaysSelfCommittingTest extends TestHelper {

  /** One shard, so every append in this class contends for the same page 0 without depending on round-robin. */
  private static final int SHARDS = 1;

  private static final int THREADS            = 4;
  private static final int APPENDS_PER_THREAD = 5;

  /**
   * Concurrent ingest into one shard, every append made from a thread that already has a transaction open.
   * <p>
   * This is the workload the self-committing design is for, and the one the alternative would put at risk: all
   * {@code THREADS * APPENDS_PER_THREAD} appends succeed, none of them raises, and all of the samples are
   * readable afterwards even though every calling transaction was rolled back. The rollback is deliberate - it
   * makes the test say both things at once, that the appends did not conflict and that they were never part of
   * the transactions they were made from.
   */
  @Test
  void concurrentAppendsToOneShardFromOpenTransactionsAllSucceed() throws Exception {
    final TimeSeriesEngine engine = createEngine("concurrent_append");
    try {
      final CountDownLatch start = new CountDownLatch(1);
      final CountDownLatch done  = new CountDownLatch(THREADS);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      for (int t = 0; t < THREADS; t++) {
        final int threadIndex = t;
        final Thread worker = new Thread(() -> {
          try {
            start.await();
            database.begin();
            for (int i = 0; i < APPENDS_PER_THREAD; i++)
              engine.appendSamples(new long[] { timestampFor(threadIndex, i) }, new Object[][] { { (double) i } });
            // Rolling back proves in the same breath that the samples below owe nothing to this transaction.
            database.rollback();
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          } finally {
            done.countDown();
          }
        }, "issue7657-append-" + t);
        worker.setDaemon(true);
        worker.start();
      }

      start.countDown();
      assertThat(done.await(60, TimeUnit.SECONDS))
          .as("every appending thread finished")
          .isTrue();

      assertThat(failure.get())
          .as("concurrent appends to a single shard all succeed: no caller is handed a page-0 conflict to "
              + "deal with. (That they never even contend is the shard's doing, not this assertion's - a "
              + "conflict absorbed by appendSamples' own retry loop would look the same from here. "
              + "twoTransactionsHoldingTheSameHeaderPageCannotBothCommit is where the mechanism is measured.)")
          .isNull();

      assertThat(timestampsOf(engine))
          .as("#7657: every append committed itself, so all of them survive the rollback of the transaction "
              + "they were made from")
          .containsExactlyInAnyOrderElementsOf(expectedTimestamps());
    } finally {
      engine.close();
    }
  }

  /**
   * The shape an append that joined the enclosing transaction would have, and the reason #7657 was decided
   * against it.
   * <p>
   * Both threads stage a modification of the same shard's page 0 in a transaction the shard does not own, and
   * only then does either commit - exactly what would happen if {@code TimeSeriesShard.appendSamples} wrote
   * through the caller's transaction and returned, leaving the commit to the caller. The second commit raises
   * {@link ConcurrentModificationException}, and there is nowhere left to absorb it: the shard's three-attempt
   * retry loop retries its <i>own</i> commit, and a commit that belongs to the caller could only be retried by
   * replaying the caller's whole transaction.
   * <p>
   * Written as an assertion about the pair rather than about a particular thread, because which of the two
   * commits lands first is a race - what is not a race is that exactly one of them can.
   */
  @Test
  void twoTransactionsHoldingTheSameHeaderPageCannotBothCommit() throws Exception {
    final TimeSeriesEngine engine = createEngine("joined_tx_shape");
    try {
      final TimeSeriesBucket bucket = engine.getShard(0).getMutableBucket();

      final CountDownLatch staged = new CountDownLatch(2);
      final CountDownLatch done   = new CountDownLatch(2);
      final AtomicInteger  commits = new AtomicInteger();
      final AtomicInteger  conflicts = new AtomicInteger();
      final AtomicReference<Throwable> unexpected = new AtomicReference<>();

      for (int t = 0; t < 2; t++) {
        final long timestamp = 90_000L + t;
        final Thread worker = new Thread(() -> {
          try {
            database.begin();
            // Stage the page-0 write in THIS thread's transaction, and do not commit it here. That is the
            // whole of the difference: the write is made, the publication is somebody else's business.
            bucket.appendSamples(bucket.newRowSource(new long[] { timestamp }, new Object[][] { { 1.0 } }));
            staged.countDown();
            if (!staged.await(60, TimeUnit.SECONDS))
              throw new IllegalStateException("the other transaction never staged its write");
            try {
              database.commit();
              commits.incrementAndGet();
            } catch (final ConcurrentModificationException e) {
              conflicts.incrementAndGet();
              if (database.isTransactionActive())
                database.rollback();
            }
          } catch (final Throwable e) {
            unexpected.compareAndSet(null, e);
          } finally {
            done.countDown();
          }
        }, "issue7657-joined-" + t);
        worker.setDaemon(true);
        worker.start();
      }

      assertThat(done.await(60, TimeUnit.SECONDS))
          .as("both transactions finished")
          .isTrue();
      assertThat(unexpected.get())
          .as("the only failure this test expects is the page-version conflict it is measuring")
          .isNull();

      assertThat(commits.get())
          .as("exactly one of the two transactions can publish its version of page 0. A 2 here means the two "
              + "stopped contending - something serialized them, and the premise of the #7657 decision would "
              + "need re-checking; a 0 means neither could commit, which is a different bug entirely")
          .isEqualTo(1);
      assertThat(conflicts.get())
          .as("#7657: the other loses its whole transaction to a ConcurrentModificationException. An append "
              + "that joined the caller's transaction would hand this failure to the caller, where the shard's "
              + "own retry loop cannot reach it. A 0 here is the same finding as a 2 above, seen from the "
              + "other side")
          .isEqualTo(1);
    } finally {
      engine.close();
    }
  }

  /** Distinct per (thread, append) so a lost or duplicated append shows up as a missing or extra timestamp. */
  private static long timestampFor(final int threadIndex, final int appendIndex) {
    return 10_000L + threadIndex * 1_000L + appendIndex;
  }

  private static List<Long> expectedTimestamps() {
    final List<Long> expected = new ArrayList<>(THREADS * APPENDS_PER_THREAD);
    for (int t = 0; t < THREADS; t++)
      for (int i = 0; i < APPENDS_PER_THREAD; i++)
        expected.add(timestampFor(t, i));
    return expected;
  }

  /**
   * Every sample timestamp the engine holds, over a range wider than any test here writes. Wrapped in a
   * transaction of its own for the reason {@code Issue7410AppendTransactionScopeTest.timestampsOf} records:
   * reading straight after a {@code rollback()} happens to work, but only through an internal detail of how
   * {@code popIfNotLastTransaction()} retains the sole transaction context.
   */
  private List<Long> timestampsOf(final TimeSeriesEngine engine) throws Exception {
    database.begin();
    try {
      return engine.query(0L, 1_000_000L, null, null).stream().map(row -> (Long) row[0]).toList();
    } finally {
      database.commit();
    }
  }

  /**
   * Builds a single-shard engine directly, as {@code Issue7410AppendTransactionScopeTest} does. The constructor
   * initialises the shard's header page in a transaction of its own, so it is wrapped in one here.
   */
  private TimeSeriesEngine createEngine(final String typeName) throws Exception {
    final List<ColumnDefinition> cols = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    database.begin();
    final TimeSeriesEngine engine = new TimeSeriesEngine((DatabaseInternal) database, typeName, cols, SHARDS);
    database.commit();
    return engine;
  }
}
