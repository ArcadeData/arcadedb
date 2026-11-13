/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Unit tests for {@link BootstrapFingerprint}, the helper that backs the HA bootstrap path
 * (issue #4147). The contract these tests pin down:
 * <ul>
 *   <li>Determinism: same files → same digest, regardless of insertion order.</li>
 *   <li>Sensitivity: a one-byte change in any included file flips the digest.</li>
 *   <li>WAL/lock files do NOT affect the digest (lastTxId is the recency signal, not the WAL bytes).</li>
 *   <li>Subdirectories are not traversed (avoids accidentally pulling in external-bucket dirs).</li>
 *   <li>Empty / missing directory yields the canonical empty-content digest.</li>
 * </ul>
 */
class BootstrapFingerprintTest {

  @Test
  void emptyDirectoryYieldsStableDigest(@TempDir final Path dir) {
    final String f1 = BootstrapFingerprint.compute(dir.toFile());
    final String f2 = BootstrapFingerprint.compute(dir.toFile());
    assertThat(f1).hasSize(64);
    assertThat(f1).isEqualTo(f2);
  }

  @Test
  void missingDirectoryYieldsSameDigestAsEmpty(@TempDir final Path tmp) {
    final File missing = new File(tmp.toFile(), "does-not-exist");
    assertThat(BootstrapFingerprint.compute(missing))
        .isEqualTo(BootstrapFingerprint.compute(tmp.toFile()));
  }

  @Test
  void identicalContentTwoDirectoriesSameDigest(@TempDir final Path dirA, @TempDir final Path dirB) throws Exception {
    Files.writeString(dirA.resolve("v_0.bucket"), "alpha", StandardCharsets.UTF_8);
    Files.writeString(dirA.resolve("schema.json"), "{ \"types\": [] }", StandardCharsets.UTF_8);

    Files.writeString(dirB.resolve("v_0.bucket"), "alpha", StandardCharsets.UTF_8);
    Files.writeString(dirB.resolve("schema.json"), "{ \"types\": [] }", StandardCharsets.UTF_8);

    assertThat(BootstrapFingerprint.compute(dirA.toFile()))
        .isEqualTo(BootstrapFingerprint.compute(dirB.toFile()));
  }

  @Test
  void contentChangeFlipsDigest(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "alpha", StandardCharsets.UTF_8);
    final String before = BootstrapFingerprint.compute(dir.toFile());

    Files.writeString(dir.resolve("v_0.bucket"), "beta", StandardCharsets.UTF_8);
    final String after = BootstrapFingerprint.compute(dir.toFile());

    assertThat(after).isNotEqualTo(before);
  }

  @Test
  void walFilesAreExcluded(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "data", StandardCharsets.UTF_8);
    final String before = BootstrapFingerprint.compute(dir.toFile());

    // WAL rotation is a normal in-flight detail and must not move the fingerprint - the recency
    // signal is the persisted lastTxId, not the WAL bytes.
    Files.writeString(dir.resolve("txlog_0.wal"), "transient", StandardCharsets.UTF_8);
    Files.writeString(dir.resolve("txlog_1.wal"), "transient2", StandardCharsets.UTF_8);
    final String after = BootstrapFingerprint.compute(dir.toFile());

    assertThat(after).isEqualTo(before);
  }

  @Test
  void subdirectoriesAreNotTraversed(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "data", StandardCharsets.UTF_8);
    final String before = BootstrapFingerprint.compute(dir.toFile());

    final Path subdir = dir.resolve("external");
    Files.createDirectory(subdir);
    Files.writeString(subdir.resolve("anything.bucket"), "external content", StandardCharsets.UTF_8);

    assertThat(BootstrapFingerprint.compute(dir.toFile())).isEqualTo(before);
  }

  @Test
  void filenameIsPartOfTheDigest(@TempDir final Path dirA, @TempDir final Path dirB) throws Exception {
    // Same content under different file names must produce different digests, otherwise an
    // accidental file rename would falsely look like the same database.
    Files.writeString(dirA.resolve("v_0.bucket"), "payload", StandardCharsets.UTF_8);
    Files.writeString(dirB.resolve("v_1.bucket"), "payload", StandardCharsets.UTF_8);

    assertThat(BootstrapFingerprint.compute(dirA.toFile()))
        .isNotEqualTo(BootstrapFingerprint.compute(dirB.toFile()));
  }

  @Test
  void unincludedExtensionsAreIgnored(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "data", StandardCharsets.UTF_8);
    final String before = BootstrapFingerprint.compute(dir.toFile());

    Files.writeString(dir.resolve("readme.txt"), "operator notes", StandardCharsets.UTF_8);
    Files.writeString(dir.resolve("metrics.lock"), "lockfile", StandardCharsets.UTF_8);
    assertThat(BootstrapFingerprint.compute(dir.toFile())).isEqualTo(before);
  }

  @Test
  void customSuffixListIsRespected(@TempDir final Path dir) throws Exception {
    Files.writeString(dir.resolve("v_0.bucket"), "data", StandardCharsets.UTF_8);
    Files.writeString(dir.resolve("notes.md"), "operator notes", StandardCharsets.UTF_8);

    // With the default suffix list, .md is excluded.
    final String defaultDigest = BootstrapFingerprint.compute(dir.toFile());
    // With a list that includes .md, the digest must include it.
    final String mdAwareDigest = BootstrapFingerprint.compute(dir.toFile(), List.of(".bucket", ".md"));

    assertThat(mdAwareDigest).isNotEqualTo(defaultDigest);
  }

  /**
   * Regression tests for issue #8843: the HA bootstrap fingerprint was computed over an OPEN database without waiting for
   * the commits still queued in the asynchronous page flush, so two fingerprints of one unchanged copy could differ.
   * {@link BootstrapFingerprint#computeSettled} waits, bounded, for the pages pending at the call to reach the disk.
   * <p>
   * The in-flight window is made deterministic by holding the flush of the database with
   * {@code PageManager.suspendFlushAndExecute} on a background thread: the pages of a commit made inside the callback stay
   * in the pipeline until the hold is released, so the test decides when they land instead of racing the flush thread.
   */
  @Nested
  class SettledBootstrapFingerprint {

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
}
