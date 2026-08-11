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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5872: {@code releaseBackgroundResources()} shuts down the dedicated graph-build
 * {@code ForkJoinPool} with {@code shutdownNow()} to cancel an in-progress build, but that only cancels tasks it
 * still finds queued and unstarted. Once insertion is under way, the building thread is parked in an EXTERNAL
 * (non-pool) {@code ForkJoinTask.join()} on the submitted insertion task, waiting on that exact task's own
 * completion status - a status {@code shutdownNow()} never sets for a task that has already started, and that
 * {@code Thread.interrupt()} on the joiner cannot force either, since an external joiner ignores interruption
 * while parked there. The joiner can then outlive close() indefinitely, even once every pool worker has gone
 * idle: the original report reproduced a build thread still alive 60 seconds after {@code db.close()}.
 * <p>
 * Same shape as issue #5577's repro (4000 vectors, 128 dimensions, {@code maxConnections=64},
 * {@code beamWidth=400}) so insertion spans several seconds and is still genuinely under way when
 * {@code releaseBackgroundResources()} runs.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class LSMVectorIndexCloseCancelsInFlightBuildTest {
  private static final String DB_ROOT = "target/test-databases/LSMVectorIndexCloseCancelsInFlightBuildTest";
  private static final int    DIMENSIONS      = 128;
  private static final int    NUM_VECTORS     = 4_000;
  private static final int    MAX_CONNECTIONS = 64;
  private static final int    BEAM_WIDTH      = 400;

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
  void releaseBackgroundResourcesUnblocksAnExternalJoinerParkedOnTheInsertionTask() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        populate(db);

        final LSMVectorIndex index = vectorIndex(db);

        // Fires once the build has reported genuine mid-insertion progress, i.e. it is provably still running
        // (not queued, not finished) at the moment releaseBackgroundResources() is about to be called below.
        final CountDownLatch buildUnderWay = new CountDownLatch(1);
        final AtomicReference<Throwable> unexpectedFailure = new AtomicReference<>();

        final Thread builder = new Thread(() -> {
          try {
            index.buildVectorGraphNow((phase, processedNodes, totalNodes, vectorAccesses) -> {
              if ("building".equals(phase) && processedNodes > 0 && processedNodes < totalNodes)
                buildUnderWay.countDown();
            });
          } catch (final CancellationException expected) {
            // This is the outcome releaseBackgroundResources() forces below; not a failure.
          } catch (final Throwable t) {
            unexpectedFailure.set(t);
          }
        }, "test-vector-graph-builder");
        builder.setDaemon(true);
        builder.start();

        assertThat(buildUnderWay.await(30, TimeUnit.SECONDS))
            .as("the build must have reported genuine mid-insertion progress before releaseBackgroundResources() "
                + "runs, otherwise this test is not exercising an in-flight build at all")
            .isTrue();

        // This is the call under test. Before the fix for issue #5872 it can leave `builder` parked in
        // ForkJoinTask.join() long after every pool worker has gone idle - the original report reproduced a
        // build thread still alive 60s later. Bound the wait here well under that.
        index.releaseBackgroundResources();

        builder.join(10_000);
        assertThat(builder.isAlive())
            .as("the build thread must not outlive releaseBackgroundResources(): a caller parked in "
                + "ForkJoinTask.join() on the cancelled insertion task must be unblocked, not left hanging")
            .isFalse();

        assertThat(unexpectedFailure.get())
            .as("the build thread must have exited via cancellation, not an unrelated error")
            .isNull();
      } finally {
        // No flush()/rebuild-on-close: the build above was deliberately cancelled mid-flight, so the graph was
        // never persisted, and drop() is what the codebase uses to tear a test database down without paying for
        // that.
        db.drop();
      }
    }
  }

  /**
   * The test above exercises the common case, where {@code shutdownNow()} drains the pool inside its own 5s
   * budget and the forced {@code cancel()} in {@code releaseBackgroundResources()} never actually has to run.
   * This test drives that fallback directly: a worker that deliberately swallows interruption (standing in for
   * JVector's {@code GraphIndexBuilder} not observing it, see {@code getOrCreateGraphBuildPool()}'s javadoc)
   * never lets the pool terminate, so {@code awaitTermination(5, SECONDS)} times out and
   * {@code releaseBackgroundResources()} must fall back to cancelling {@code graphBuildActiveTask} directly.
   * Injecting the pool/task fields directly keeps this deterministic and fast to set up, instead of depending on
   * a real JVector build timing out in a way this test cannot control.
   */
  @Test
  void releaseBackgroundResourcesCancelsAStuckInsertionTaskAfterTheShutdownTimeout() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": 4, "
              + "\"similarity\": \"COSINE\" }");
        });
        final LSMVectorIndex index = vectorIndex(db);

        final ForkJoinPool pool = new ForkJoinPool(1);
        final CountDownLatch stop = new CountDownLatch(1);
        final CountDownLatch workerStarted = new CountDownLatch(1);
        final ForkJoinTask<?> stuckTask = pool.submit(() -> {
          workerStarted.countDown();
          while (stop.getCount() > 0) {
            try {
              stop.await(50, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException ignored) {
              // Deliberately swallow: this is standing in for JVector code that does not check interruption.
            }
          }
        });

        try {
          assertThat(workerStarted.await(5, TimeUnit.SECONDS)).as("the stuck worker must have started").isTrue();

          final Field poolField = LSMVectorIndex.class.getDeclaredField("graphBuildPool");
          poolField.setAccessible(true);
          poolField.set(index, pool);
          final Field taskField = LSMVectorIndex.class.getDeclaredField("graphBuildActiveTask");
          taskField.setAccessible(true);
          taskField.set(index, stuckTask);

          final AtomicReference<Throwable> unexpectedFailure = new AtomicReference<>();
          final Thread joiner = new Thread(() -> {
            try {
              stuckTask.join();
            } catch (final CancellationException expected) {
              // This is the outcome releaseBackgroundResources() forces below; not a failure.
            } catch (final Throwable t) {
              unexpectedFailure.set(t);
            }
          }, "test-stuck-task-joiner");
          joiner.setDaemon(true);
          joiner.start();
          // Give the joiner time to actually park inside join() before racing it against
          // releaseBackgroundResources() below - a join() called after the task is already cancelled would
          // pass trivially without ever exercising the wakeup this test targets.
          Thread.sleep(200);

          // This is the call under test. It must fall back to cancelling stuckTask once awaitTermination(5s)
          // times out, since the worker above never lets the pool terminate on its own.
          index.releaseBackgroundResources();

          joiner.join(10_000);
          assertThat(joiner.isAlive())
              .as("the external joiner must not outlive releaseBackgroundResources(): once shutdownNow() fails "
                  + "to drain the pool within its own budget, the fallback cancel() must still unblock a caller "
                  + "parked in ForkJoinTask.join(), not leave it hanging")
              .isFalse();
          assertThat(unexpectedFailure.get())
              .as("the joiner must have exited via cancellation, not an unrelated error")
              .isNull();
        } finally {
          stop.countDown();
          pool.shutdownNow();
        }
      } finally {
        db.drop();
      }
    }
  }

  private static void populate(final Database db) {
    final Random rnd = new Random(7);
    db.transaction(() -> {
      final var type = db.getSchema().createDocumentType("Doc");
      type.createProperty("id", Type.INTEGER);
      type.createProperty("vector", Type.ARRAY_OF_FLOATS);
      db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"maxConnections\": " + MAX_CONNECTIONS
          + ", \"beamWidth\": " + BEAM_WIDTH + " }");
    });

    db.begin();
    for (int i = 0; i < NUM_VECTORS; i++) {
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = rnd.nextFloat();
      db.newDocument("Doc").set("id", i).set("vector", vector).save();
      if (i % 1000 == 999) {
        db.commit();
        db.begin();
      }
    }
    db.commit();
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
