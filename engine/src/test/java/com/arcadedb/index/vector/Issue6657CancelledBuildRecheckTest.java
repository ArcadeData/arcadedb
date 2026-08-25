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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the gap review found in issue #6657's own fix: {@code markCloseTimeRebuildDeferred()} skips
 * the manifest write when {@code graphBuildLock.tryLock()} fails, reasoning that a build holding the lock is
 * "about to run its own completion write, which clears this same flag on its own". That is only true if the build
 * actually COMPLETES. When {@code close()} instead CANCELS an in-flight async rebuild -
 * {@code releaseBackgroundResources()} does exactly that, and runs right after {@code flush()} - the cancelled
 * build's {@code CancellationException} path never touches the manifest, so without a backstop the flag would be
 * left wherever it was before this close (typically {@code false}), even though a rebuild is now genuinely owed:
 * a false negative in the exact "why is my first query after reopen slow" moment the stat exists to answer.
 * <p>
 * Deterministic rather than racing a real JVector build's timing (which {@code LSMVectorIndexCloseCancelsInFlightBuildTest}
 * already shows is a legitimate but non-trivial thing to pin reliably): a helper thread holds
 * {@code graphBuildLock} directly, standing in for "a build is genuinely in progress", and releases it only after
 * {@code flush()} has already observed it contended - simulating the point at which a real cancellation would have
 * freed the lock without ever writing the manifest. The test then checks that the SECOND attempt, inside
 * {@code releaseBackgroundResources()}, catches what the first one had to skip.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6657CancelledBuildRecheckTest {
  private static final String DB_ROOT     = "target/test-databases/Issue6657CancelledBuildRecheckTest";
  private static final int    DIMENSIONS  = 8;
  private static final int    NUM_VECTORS = 1_005; // at/above ASYNC_REBUILD_MIN_GRAPH_SIZE (1000)

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
  void releaseBackgroundResourcesRecordsTheDeferralThatFlushHadToSkipBecauseABuildHeldTheLock() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        final Random rnd = new Random(13);
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Doc").set("id", i).set("vector", randomVector(rnd)).save();
          if (i % 500 == 499) {
            db.commit();
            db.begin();
          }
        }
        db.commit();

        final LSMVectorIndex index = vectorIndex(db);
        assertThat(index.getStats().get("graphState"))
            .as("precondition: MUTABLE, otherwise flush() never reaches the deferral branch at all")
            .isEqualTo(2L); // GraphState.MUTABLE
        assertThat(index.getStats().get("totalVectors"))
            .as("precondition: at/above the async-rebuild threshold, the exact branch the reviewed race is about")
            .isGreaterThanOrEqualTo(1000L);

        final ReentrantLock graphBuildLock = graphBuildLock(index);
        final CountDownLatch lockHeld = new CountDownLatch(1);
        final CountDownLatch releaseLock = new CountDownLatch(1);
        final Thread lockHolder = new Thread(() -> {
          graphBuildLock.lock();
          try {
            lockHeld.countDown();
            releaseLock.await();
          } catch (final InterruptedException ignored) {
            // test teardown path only
          } finally {
            graphBuildLock.unlock();
          }
        }, "test-graph-build-lock-holder");
        lockHolder.setDaemon(true);
        lockHolder.start();

        assertThat(lockHeld.await(5, TimeUnit.SECONDS))
            .as("the helper thread must genuinely hold graphBuildLock before flush() runs, otherwise this test "
                + "does not exercise the contended tryLock() at all")
            .isTrue();

        // flush()'s ASYNC_REBUILD_MIN_GRAPH_SIZE branch: needsBuild is true, but markCloseTimeRebuildDeferred()'s
        // tryLock() must fail while the helper thread holds the lock - standing in for a real async rebuild still
        // running at this exact point in close().
        index.flush();
        assertThat(index.getStats().get("closeTimeRebuildPending"))
            .as("flush() must have skipped the write while the lock was held - this is the state a cancelled "
                + "build would otherwise leave behind uncorrected")
            .isEqualTo(0L);

        // Stand-in for cancellation freeing the lock without ever writing the manifest (a real CancellationException
        // path does not call writeGraphManifest()/markGraphManifestUnusable() either).
        releaseLock.countDown();
        lockHolder.join(5_000);
        assertThat(lockHolder.isAlive()).as("the helper thread must have released the lock before proceeding").isFalse();

        // This is the call under test: its tail recheck must catch what flush() had to skip, now that the lock is
        // free and graphState still says a build is genuinely owed.
        index.releaseBackgroundResources();

        assertThat(index.getStats().get("closeTimeRebuildPending"))
            .as("releaseBackgroundResources()'s recheck must record the deferral flush() could not - this is the "
                + "false-negative the review comment identified")
            .isEqualTo(1L);
      } finally {
        if (db.isOpen())
          db.drop();
      }
    }
  }

  private static ReentrantLock graphBuildLock(final LSMVectorIndex index) throws ReflectiveOperationException {
    final Field field = LSMVectorIndex.class.getDeclaredField("graphBuildLock");
    field.setAccessible(true);
    return (ReentrantLock) field.get(index);
  }

  private static float[] randomVector(final Random rnd) {
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = rnd.nextFloat();
    return vector;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc")
        .getPolymorphicIndexByProperties("vector").getIndexesOnBuckets()[0];
  }
}
