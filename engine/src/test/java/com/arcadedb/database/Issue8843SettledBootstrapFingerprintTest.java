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
package com.arcadedb.database;

import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression tests for issue #8843: the HA bootstrap fingerprint was computed over an OPEN database without waiting for
 * the commits still queued in the asynchronous page flush, so two fingerprints of one unchanged copy could differ.
 * {@link BootstrapFingerprint#computeSettled} waits, bounded, for the pages pending at the call to reach the disk.
 * <p>
 * The in-flight window is made deterministic by holding the flush of the database with
 * {@code PageManager.suspendFlushAndExecute} on a background thread: the pages of a commit made inside the callback stay
 * in the pipeline until the hold is released, so the test decides when they land instead of racing the flush thread.
 */
class Issue8843SettledBootstrapFingerprintTest {

  private static final String DB_PATH = "./target/databases/test-8843-settled-fingerprint";

  private LocalDatabase   database;
  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = (LocalDatabase) new DatabaseFactory(DB_PATH).create();
    database.getSchema().createDocumentType("Seed");
    assertThat(database.getPageManager().waitAllPagesOfDatabaseAreFlushed(database)).isTrue();
    executor = Executors.newFixedThreadPool(2);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    executor.shutdownNow();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    if (database != null && database.isOpen())
      database.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * The pages of the last commit are still in the pipeline when the fingerprint is asked for. The settled fingerprint
   * waits for them, so it is the digest of the copy with the commit on disk - the one every later sample of the same
   * unchanged copy reads - and not the one a plain {@link BootstrapFingerprint#compute(File)} took at the same moment.
   */
  @Test
  void theSettledFingerprintWaitsForTheLastCommitToReachTheDisk() throws Exception {
    final CountDownLatch committed = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final AtomicReference<String> rawInFlight = new AtomicReference<>();
    final Future<?> hold = executor.submit(() -> {
      database.getPageManager().suspendFlushAndExecute(database, () -> {
        database.transaction(() -> database.newDocument("Seed").set("k", 1).save());
        rawInFlight.set(BootstrapFingerprint.compute(new File(DB_PATH)));
        committed.countDown();
        assertThat(release.await(60, TimeUnit.SECONDS)).isTrue();
      });
      return null;
    });
    assertThat(committed.await(60, TimeUnit.SECONDS)).isTrue();

    final AtomicReference<Thread> samplerThread = new AtomicReference<>();
    final Future<String> sampled = executor.submit(() -> {
      samplerThread.set(Thread.currentThread());
      return BootstrapFingerprint.computeSettled(database, 60_000L);
    });

    // RELEASE ONLY ONCE THE SAMPLER IS PARKED ON THE DRAIN (OR HAS ALREADY ANSWERED, WHICH IS THE BUG): RELEASING EARLIER
    // WOULD LET A SAMPLER THAT NEVER WAITS READ THE SETTLED FILES BY LUCK
    awaitParkedOrDone(samplerThread, sampled);
    release.countDown();
    hold.get(60, TimeUnit.SECONDS);

    final String settledNow = BootstrapFingerprint.computeSettled(database, 60_000L);
    assertThat(sampled.get(60, TimeUnit.SECONDS))
        .as("the fingerprint taken while the commit was in flight is the copy as it stands once the commit landed")
        .isEqualTo(settledNow);
    assertThat(rawInFlight.get())
        .as("control: a plain compute while the pages were held reads the files without the commit. If this fails the "
            + "hold no longer keeps the pages off the disk and the test above proves nothing")
        .isNotEqualTo(settledNow);
  }

  /**
   * A flush that cannot drain - here held for the whole test, in production a wedged disk or a long backup suspension -
   * does not hold the caller beyond the bound it passed: the fingerprint is answered from the files as they are.
   */
  @Test
  void theWaitForTheFlushIsBounded() throws Exception {
    final CountDownLatch committed = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Future<?> hold = executor.submit(() -> {
      database.getPageManager().suspendFlushAndExecute(database, () -> {
        database.transaction(() -> database.newDocument("Seed").set("k", 1).save());
        committed.countDown();
        assertThat(release.await(60, TimeUnit.SECONDS)).isTrue();
      });
      return null;
    });
    assertThat(committed.await(60, TimeUnit.SECONDS)).isTrue();

    try {
      final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
      final String fingerprint = BootstrapFingerprint.computeSettled(database, 200L);
      stopwatch.assertGaveUpWithin(30_000L, "a 200 ms bound on the flush wait from a wait held until the test releases it");
      assertThat(fingerprint).hasSize(64);
    } finally {
      release.countDown();
      hold.get(60, TimeUnit.SECONDS);
    }
  }

  /** Nothing in flight: no wait, and the same digest as a plain compute. */
  @Test
  void aSettledCopyHashesExactlyLikeAPlainCompute() {
    assertThat(BootstrapFingerprint.computeSettled(database, 60_000L))
        .isEqualTo(BootstrapFingerprint.compute(new File(DB_PATH)));
  }

  private static void awaitParkedOrDone(final AtomicReference<Thread> thread, final Future<?> future)
      throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 60_000L;
    while (!future.isDone() && System.currentTimeMillis() < deadline) {
      final Thread t = thread.get();
      // Any waiting state counts, not only the drain: a brief wait elsewhere (class loading, logging) can only release
      // the hold early, which weakens the test towards passing on an unfixed reader, never towards failing a fixed one.
      if (t != null && (t.getState() == Thread.State.TIMED_WAITING || t.getState() == Thread.State.WAITING))
        return;
      Thread.sleep(5);
    }
    if (!future.isDone())
      fail("the sampler neither parked on the flush drain nor answered within 60 s");
  }
}
