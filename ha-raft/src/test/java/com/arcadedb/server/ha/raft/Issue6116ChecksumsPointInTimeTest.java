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
package com.arcadedb.server.ha.raft;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6116, part 3: {@code GET /api/v1/ha/snapshot/{db}/checksums} was the last reader in the product still
 * freezing the data files with {@code PageManager.suspendFlushAndExecute} after #6075 migrated the full backup, the
 * HA verify and the HA snapshot ship. On a large database it CRCs every byte of every file, so the freeze it held
 * throttled the leader's committers for its whole duration.
 * <p>
 * Two properties are defended here. That the answer is still a genuine point in time, byte-for-byte the value a peer
 * on the fallback path computes - otherwise a migrated leader would report every file as differing. And the reason
 * for the change: the flush suspension is no longer held for the read, only for the window's t0 barrier, measured
 * against the fallback path in the same test so the comparison does not depend on this machine's timing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6116ChecksumsPointInTimeTest {
  private static final String DATABASE_PATH = "target/databases/checksums-point-in-time";
  private static final String TYPE          = "Doc";
  /** Big enough that CRC-ing every file lasts long enough to be sampled, small enough to build in well under a second. */
  private static final int    RECORDS       = 100_000;

  @BeforeEach
  @AfterEach
  void clean() {
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  /**
   * The migrated path must produce exactly what the frozen-file path produced: a follower still on the old code CRCs
   * its files raw, and the two are compared for equality.
   */
  @Test
  void theSnapshotChecksumsAreByteForByteTheFrozenFileOnes() throws Exception {
    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final File dbDir = new File(db.getDatabasePath());

      final Map<String, Long> live = SnapshotManager.computeFileChecksums(dbDir);
      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        final Map<String, Long> viaSnapshot = SnapshotManager.computeFileChecksums(dbDir, snapshot);
        assertThat(viaSnapshot).isEqualTo(live);
      }
    }
  }

  /**
   * The point-in-time property, with the mutation check that keeps it from passing vacuously: the very same rewrite
   * that leaves the window's answer untouched must be visible to a live read of the same directory.
   */
  @Test
  void theChecksumsStayAtT0WhileTheDatabaseIsRewritten() throws Exception {
    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final File dbDir = new File(db.getDatabasePath());

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        final Map<String, Long> t0 = SnapshotManager.computeFileChecksums(dbDir, snapshot);
        assertThat(t0).isNotEmpty();

        database.transaction(() -> database.iterateType(TYPE, false).forEachRemaining(
            record -> record.asDocument().modify().set("payload", "rewritten-" + "y".repeat(300)).save()));
        db.getPageManager().waitAllPagesOfDatabaseAreFlushed(db);

        assertThat(SnapshotManager.computeFileChecksums(dbDir, snapshot))
            .as("the window must keep answering with its t0 image").isEqualTo(t0);
        assertThat(SnapshotManager.computeFileChecksums(dbDir))
            .as("a live read of the same directory must have changed, or the assertion above proves nothing")
            .isNotEqualTo(t0);

        // THE SCRATCH SPILL FILE OF THE OPEN WINDOW LIVES IN THIS VERY DIRECTORY AND IS NOT PART OF THE DATABASE:
        // CHECKSUMMING IT WOULD REPORT A FILE NO PEER HAS
        assertThat(t0.keySet()).noneMatch(name -> name.endsWith("." + PageSnapshot.SHADOW_FILE_EXT));
      }
    }
  }

  /**
   * A page file that appears in the directory but not in the window was created after t0 - index compaction does
   * exactly this during a backup, and it does it WITHOUT the database write lock, so it can happen at any point
   * while this endpoint is reading. It must be left out, not CRC'd live: a torn checksum of a file being written is
   * worse than an absent one in a map whose only purpose is to be compared with another node's.
   * <p>
   * Deciding that by extension rather than by asking the FileManager for its current file list is what makes this
   * race-free: a name set captured before the directory listing can already be out of date by the time the listing
   * runs.
   */
  @Test
  void aPageFileThatAppearsAfterT0IsLeftOutWhileANonPageFileIsStillRead() throws Exception {
    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final File dbDir = new File(db.getDatabasePath());

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        // APPEARING UNDER THE READER, AFTER t0, EXACTLY AS A COMPACTED INDEX FILE DOES
        Files.writeString(new File(dbDir, "Latecomer_9.1.64." + LocalBucket.BUCKET_EXT).toPath(), "post-t0 bytes");
        Files.writeString(new File(dbDir, "operator-notes.txt").toPath(), "not a page file");

        final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(dbDir, snapshot);

        assertThat(checksums).doesNotContainKey("Latecomer_9.1.64." + LocalBucket.BUCKET_EXT);
        assertThat(checksums).as("a file the snapshot never covered is still read live, as it always was")
            .containsKey("operator-notes.txt");
        // THE CONTROL: WITHOUT A WINDOW THERE IS NO POINT IN TIME TO BE OUTSIDE OF, SO BOTH ARE READ
        assertThat(SnapshotManager.computeFileChecksums(dbDir))
            .containsKey("Latecomer_9.1.64." + LocalBucket.BUCKET_EXT);
      }
    }
  }

  /**
   * The reason the change exists. The suspend-and-freeze path is measured in the same run as the control, so the
   * comparison is not a bet on absolute timings.
   */
  @Test
  void computingTheChecksumsNoLongerSuspendsPageFlushing() throws Exception {
    // NOT "never suspends": OPENING THE WINDOW ITSELF PARKS THE FLUSH THREAD FOR THE t0 BARRIER, WHICH IS BOUNDED
    // AND MEASURED IN MILLISECONDS. WHAT CHANGES IS THE SHAPE - A BRIEF BARRIER INSTEAD OF A FREEZE HELD FOR THE
    // WHOLE READ - SO THE ASSERTION IS ON THE FRACTION OF THE COMPUTATION SPENT SUSPENDED, WITH THE SAME THRESHOLDS
    // Issue6075SnapshotBackupIT USES FOR THE BACKUP. THE FALLBACK RUN IS THE CONTROL: IT PROVES THIS MACHINE'S
    // SAMPLER SEES SUSPENSIONS AT ALL
    final double fallback = suspendedFractionWhileComputingChecksums(false);
    final double withSnapshot = suspendedFractionWhileComputingChecksums(true);

    assertThat(fallback).as("the fallback path must still freeze the files for the whole read").isGreaterThan(0.9);
    assertThat(withSnapshot).as("the snapshot path must not hold the flush suspension for the read (fallback was %f)",
        fallback).isLessThan(0.5);
  }

  /**
   * Whichever path it took, the handler answers with one checksum per non-transient file, and the answer is the
   * same on both - the endpoint's contract does not change with the setting.
   */
  @Test
  void bothPathsAnswerWithTheSameFileSet() throws Exception {
    final SnapshotHttpHandler handler = new SnapshotHttpHandler(null);
    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
      final JSONObject viaSnapshot = handler.computeChecksums(db);

      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);
      final JSONObject viaSuspension = handler.computeChecksums(db);

      assertThat(viaSnapshot.keySet()).isNotEmpty().isEqualTo(viaSuspension.keySet());
      for (final String name : viaSnapshot.keySet())
        assertThat(viaSnapshot.getLong(name)).as(name).isEqualTo(viaSuspension.getLong(name));
    } finally {
      handler.close();
    }
  }

  /**
   * Runs the handler's checksum computation with a writer committing throughout, and returns the fraction of the
   * samples taken during it that found page flushing suspended.
   * <p>
   * The writer pauses between transactions on purpose. It has to be committing - otherwise the fallback path has
   * nothing to freeze and the comparison is empty - but a writer running flat out makes the t0 barrier RETRY (a
   * commit landing between its full queue drain and the flush-thread suspension), which stretches the one
   * suspension the snapshot path does take until it is comparable to the whole read. That would be measuring the
   * barrier, which {@code PageSnapshotOverheadBenchmark} measures properly, instead of what is being asserted here.
   */
  private double suspendedFractionWhileComputingChecksums(final boolean snapshot) throws Exception {
    clean();
    GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(snapshot);

    final SnapshotHttpHandler handler = new SnapshotHttpHandler(null);
    try (final Database database = createDatabase()) {
      final DatabaseInternal db = (DatabaseInternal) database;

      final AtomicBoolean running = new AtomicBoolean(true);
      final AtomicReference<Exception> writerFailure = new AtomicReference<>();
      final CountDownLatch warmedUp = new CountDownLatch(1);
      final AtomicLong samples = new AtomicLong();
      final AtomicLong suspendedSamples = new AtomicLong();

      final Thread writer = new Thread(() -> {
        for (int id = 0; running.get(); id++) {
          try {
            final int current = id;
            database.transaction(
                () -> database.newDocument(TYPE).set("id", current).set("payload", "concurrent-" + current).save());
            warmedUp.countDown();
            Thread.sleep(2);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (final Exception e) {
            writerFailure.compareAndSet(null, e);
            return;
          }
        }
      }, "checksums-writer");
      writer.setDaemon(true);

      // SPINS RATHER THAN SLEEPING BETWEEN SAMPLES, AND IS ALREADY SPINNING BEFORE THE COMPUTATION STARTS. CRC-ING
      // A FEW TENS OF MEGABYTES OFF A WARM PAGE CACHE TAKES MILLISECONDS: A 1 ms SAMPLER WOULD TAKE A HANDFUL OF
      // SAMPLES, AND A SAMPLER STARTED AT THE SAME MOMENT AS THE COMPUTATION WOULD CHARGE ITS OWN THREAD-START
      // LATENCY TO THE UNSUSPENDED SIDE - WORTH A FIFTH OF THE WHOLE WINDOW AT THESE DURATIONS
      final AtomicBoolean computing = new AtomicBoolean(false);
      final Thread sampler = new Thread(() -> {
        while (running.get()) {
          if (computing.get()) {
            samples.incrementAndGet();
            if (db.getPageManager().isPageFlushingSuspended(db))
              suspendedSamples.incrementAndGet();
          }
          Thread.onSpinWait();
        }
      }, "checksums-suspension-sampler");
      sampler.setDaemon(true);

      writer.start();
      sampler.start();
      try {
        assertThat(warmedUp.await(30, TimeUnit.SECONDS)).isTrue();

        computing.set(true);
        final JSONObject checksums = handler.computeChecksums(db);
        computing.set(false);
        assertThat(checksums.keySet()).isNotEmpty();
      } finally {
        computing.set(false);
        running.set(false);
        writer.join(30_000);
        sampler.join(10_000);
      }

      assertThat(writerFailure.get()).isNull();
      assertThat(samples.get()).as("the computation must last long enough to sample meaningfully").isGreaterThan(200);
      return (double) suspendedSamples.get() / samples.get();
    } finally {
      handler.close();
    }
  }

  private Database createDatabase() {
    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createDocumentType(TYPE);
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("id", i).set("payload", "x".repeat(300)).save();
    });
    ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    return database;
  }
}
