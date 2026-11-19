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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.PageSnapshot;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.TransactionManager;
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.olap.ContractionHierarchy;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileFilter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Point-in-time checksums and verify of a database: transient files, node-local files and the verify read-lock window.
 */
class DatabaseVerifyAndChecksumsTest {

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
   */
  @Nested
  class ChecksumsPointInTime {

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

    // Regression for #6116: the checksums endpoint reads a point-in-time view instead of freezing the data files
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

    // Regression for #6116: the checksums endpoint reads a point-in-time view instead of freezing the data files
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

    // Regression for #6116: the checksums endpoint reads a point-in-time view instead of freezing the data files
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

    // Regression for #6116: the checksums endpoint reads a point-in-time view instead of freezing the data files
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

      // THE CONTROL'S THRESHOLD IS DELIBERATELY LOOSE (issue #6168, item 4). Its only job is to prove this machine's
      // sampler sees suspensions at all, and it is a SAMPLED FRACTION: the handler does real work either side of the
      // suspension window, and on a 4-CPU runner a spinning sampler does not distribute its samples evenly over the
      // computation. At 0.9 the control - not the behaviour under test - was what failed CI, at 0.8953. Anything
      // comfortably above the 0.5 the real assertion allows still separates "frozen for the read" from "not frozen",
      // which is the whole of what this comparison has to establish.
      assertThat(fallback).as("the fallback path must still freeze the files for the whole read").isGreaterThan(0.75);
      assertThat(withSnapshot).as("the snapshot path must not hold the flush suspension for the read (fallback was %f)",
          fallback).isLessThan(0.5);
    }

    // Regression for #6116: the checksums endpoint reads a point-in-time view instead of freezing the data files
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

    // Regression for #6116: the checksums endpoint reads a point-in-time view instead of freezing the data files
    /**
     * #7459, on the live endpoint rather than on the scan in isolation: the reachability check for the skip list.
     * {@code SnapshotHttpHandler.computeChecksums} is what {@code GET /api/v1/ha/snapshot/{db}/checksums} and the
     * cluster-wide {@code /api/v1/cluster/checksums} comparison call, and it has TWO branches - the point-in-time
     * window and the suspend-and-freeze fallback. Both reach the same directory scan, so both are driven here with
     * real scratch names sitting in a real database directory.
     * <p>
     * {@code operator-notes.txt} is the control: a file the scan has no business skipping. Without it this test would
     * still pass against a scan that skipped everything.
     */
    @Test
    void neitherPathChecksumsTheScratchFilesLeftInTheDatabaseDirectory() throws Exception {
      final SnapshotHttpHandler handler = new SnapshotHttpHandler(null);
      try (final Database database = createDatabase()) {
        final DatabaseInternal db = (DatabaseInternal) database;
        final File dbDir = new File(db.getDatabasePath());

        Files.writeString(new File(dbDir, "schema.json.1234567890.tmp").toPath(), "{}");
        Files.writeString(new File(dbDir, "weather_shard_0.ts.sealed.tmp").toPath(), "sealed");
        Files.writeString(new File(dbDir, "weather_shard_0.ts.sealed.incoming").toPath(), "half an install");
        Files.writeString(new File(dbDir, "weather_shard_1.ts.sealed.parts").toPath(), "three slices of seven");
        Files.writeString(new File(dbDir, ArcadeDBServer.SNAPSHOT_PENDING_FILE).toPath(), "");
        Files.writeString(new File(dbDir, "operator-notes.txt").toPath(), "not scratch, and not a page file");

        GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
        final JSONObject viaSnapshot = handler.computeChecksums(db);

        GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);
        final JSONObject viaSuspension = handler.computeChecksums(db);

        for (final JSONObject answer : new JSONObject[] { viaSnapshot, viaSuspension }) {
          assertThat(answer.keySet()).as("the control proves the scan is still reading the directory")
              .contains("operator-notes.txt");
          assertThat(answer.keySet()).as("no node-local scratch file may be reported to a peer")
              .doesNotContain("schema.json.1234567890.tmp", "weather_shard_0.ts.sealed.tmp",
                  "weather_shard_0.ts.sealed.incoming", "weather_shard_1.ts.sealed.parts",
                  ArcadeDBServer.SNAPSHOT_PENDING_FILE);
        }
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

  /**
   * Issue #8018 on the live endpoints, which is where the two halves of the issue have to be answered rather than in
   * the scan in isolation.
   * <p>
   * <b>#7955</b> - an index-compaction temporary is the one scratch file in a database directory that is a fully
   * REGISTERED component file: {@code PaginatedComponent}'s constructor calls {@code FileManager.getOrCreateFile}, so
   * a {@code temp_*} file is in {@code getFiles()}, in every page snapshot window opened while the compaction runs,
   * and in a directory listing. Only the node that happens to be compacting has one, so every endpoint that exists to
   * compare two nodes has to leave it out. That is two endpoints, not one: {@code /checksums} reaches it through the
   * directory listing and {@code /cluster/verify} through the registry, and the fixture below builds the temporary the
   * way compaction itself does - {@code LSMTreeIndexMutable.createNewForCompaction()} is the method
   * {@code LSMTreeIndexCompactor} line 83 calls - so neither test is asserting about a file of its own invention.
   * <p>
   * <b>#7956</b> - a file that disappears between {@code listFiles} and the {@code FileInputStream} used to take the
   * whole {@code /checksums} answer down as a 500, which reported the node as ERROR to the cluster comparison at
   * exactly the moment an operator was using it to decide whether a follower had diverged. The answer now survives and
   * names what it could not cover.
   *
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8018">issue #8018</a>
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7955">issue #7955</a>
   * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7956">issue #7956</a>
   */
  @Nested
  class ChecksumTransientFiles {

    private static final String DATABASE_PATH = "target/databases/checksums-transient-files";
    private static final String TYPE          = "Doc";
    private static final String INDEX_PROPERTY = "id";
    private static final int    RECORDS       = 2_000;

    private final SnapshotHttpHandler      checksumsHandler = new SnapshotHttpHandler(null);
    private final PostVerifyDatabaseHandler verifyHandler   = new PostVerifyDatabaseHandler(null, null);

    @BeforeEach
    @AfterEach
    void clean() {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      FileUtils.deleteRecursively(new File(DATABASE_PATH));
    }

    @AfterEach
    void closeHandlers() {
      checksumsHandler.close();
      verifyHandler.close();
    }

    // Regression for #8018: transient index-compaction files are not compared by the live endpoints
    /**
     * #7955 on {@code GET /api/v1/ha/snapshot/{db}/checksums}, driven through BOTH of its branches: the point-in-time
     * window and the suspend-and-freeze fallback. They fail differently without the fix - with a window the temporary
     * lands in the map with a consistent but node-local checksum, without one it is CRC'd live while compaction is
     * writing it - and the same skip settles both, which is only visible if both are run.
     * <p>
     * The already-published index file is the control: it has the same stem and the same directory, and it is exactly
     * what the temporary becomes once {@code removeTempSuffix} renames it, so a skip that reached it would be caught
     * here.
     */
    @Test
    void neitherChecksumsBranchReportsAnIndexCompactionTemporary() throws Exception {
      try (final Database database = createDatabaseWithIndex()) {
        final DatabaseInternal db = (DatabaseInternal) database;
        final String tempName = startCompactionTemporary(db);

        GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
        final JSONObject viaSnapshot = checksumsHandler.computeChecksums(db);
        GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);
        final JSONObject viaSuspension = checksumsHandler.computeChecksums(db);

        for (final JSONObject answer : new JSONObject[] { viaSnapshot, viaSuspension }) {
          assertThat(answer.keySet())
              .as("a compaction temporary exists only on the node that is compacting: reporting it is a false divergence")
              .doesNotContain(tempName);
          assertThat(answer.keySet())
              .as("the control: the published index file of the same index must still be compared")
              .anyMatch(name -> name.endsWith(".umtidx") || name.endsWith(".uctidx"));
        }
      }
    }

    // Regression for #8018: transient index-compaction files are not compared by the live endpoints
    /**
     * #7955 on {@code POST /api/v1/cluster/verify/{database}}, the twin endpoint. It reads the REGISTERED files rather
     * than the directory, so none of the scratch families the directory scan skips can reach it - and the compaction
     * temporary is the single exception, because it is registered. Both of its branches are driven for the same
     * reason as above: one enumerates the window, the other {@code FileManager.getFiles()}.
     */
    @Test
    void neitherVerifyBranchReportsAnIndexCompactionTemporary() throws Exception {
      try (final Database database = createDatabaseWithIndex()) {
        final DatabaseInternal db = (DatabaseInternal) database;
        final String tempName = startCompactionTemporary(db);

        GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);
        final JSONObject viaSnapshot = new JSONObject();
        verifyHandler.computeLocalChecksums(db, viaSnapshot, new JSONArray());

        GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);
        final JSONObject viaSuspension = new JSONObject();
        verifyHandler.computeLocalChecksums(db, viaSuspension, new JSONArray());

        for (final JSONObject answer : new JSONObject[] { viaSnapshot, viaSuspension }) {
          assertThat(answer.keySet()).as("the fixture must have produced a comparable answer at all").isNotEmpty();
          assertThat(answer.keySet())
              .as("the verify reads the registry, and the compaction temporary is the one scratch file IN the registry")
              .doesNotContain(tempName);
        }
      }
    }

    // Regression for #8018: transient index-compaction files are not compared by the live endpoints
    /**
     * The fixture's own proof, kept apart from the assertions it underwrites. Without it a skip that happened to be
     * unreachable - a temporary that was never created, never registered, never in the directory - would make both
     * tests above pass while proving nothing at all.
     */
    @Test
    void theFixtureReallyProducesARegisteredTemporaryInTheDatabaseDirectory() throws Exception {
      try (final Database database = createDatabaseWithIndex()) {
        final DatabaseInternal db = (DatabaseInternal) database;
        final String tempName = startCompactionTemporary(db);

        assertThat(PaginatedComponent.isTemporaryFileName(tempName))
            .as("'%s' must be what compaction actually names its output", tempName).isTrue();
        assertThat(new File(db.getDatabasePath(), tempName)).as("it is on disk, so the directory scan lists it").exists();
        assertThat(db.getFileManager().getFiles().stream().filter(f -> f != null).map(f -> f.getFileName()))
            .as("and it is registered, so the verify handler and every page snapshot window carry it")
            .contains(tempName);
      }
    }

    // Regression for #8018: transient index-compaction files are not compared by the live endpoints
    /**
     * #7956 through the real handler. The listing-then-open gap is between two syscalls and cannot be provoked from
     * outside, so the directory handed to the handler lists itself with the JDK's own call and then deletes one of the
     * files it is about to return - which is exactly what TimeSeries retention does in that window, holding no lock
     * that would stop it.
     * <p>
     * Both branches are driven: the fallback one is the branch #7956 named as the exposure, and the window one shares
     * the same scan.
     */
    @Test
    void aFileThatVanishesDuringTheScanIsNamedInTheAnswerRatherThanFailingIt() throws Exception {
      try (final Database database = createDatabaseWithIndex()) {
        final DatabaseInternal db = (DatabaseInternal) database;

        for (final boolean pageSnapshot : new boolean[] { true, false }) {
          GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(pageSnapshot);
          final String doomed = "weather_shard_" + pageSnapshot + ".ts.sealed";
          Files.writeString(new File(db.getDatabasePath(), doomed).toPath(), "dropped by retention mid-scan");
          // The control: a file the scan has no business dropping. Without it this test would pass against a scan
          // that gave up on everything after the first failure.
          final String survivor = "operator-notes-" + pageSnapshot + ".txt";
          Files.writeString(new File(db.getDatabasePath(), survivor).toPath(), "not scratch, and not a page file");

          final JSONObject answer = checksumsHandler.computeChecksums(db, vanishingDirectory(db.getDatabasePath(), doomed));

          assertThat(answer.has(SnapshotHttpHandler.UNREADABLE_FILES_KEY))
              .as("pageSnapshotEnabled=%s: the answer must say which file it does not cover", pageSnapshot).isTrue();
          assertThat(toList(answer.getJSONArray(SnapshotHttpHandler.UNREADABLE_FILES_KEY))).containsExactly(doomed);
          assertThat(answer.keySet())
              .as("pageSnapshotEnabled=%s: and the rest of the answer must survive", pageSnapshot)
              .contains(survivor);
        }
      }
    }

    // Regression for #8018: transient index-compaction files are not compared by the live endpoints
    /**
     * The reserved key is absent, not empty, when nothing went wrong - so the ordinary answer keeps exactly the flat
     * shape it has always had and no existing client sees a new key. The two assertions belong together: the second
     * one is what says the first is about a complete answer rather than about a broken one.
     */
    @Test
    void aCompleteAnswerCarriesNoReservedKeyAtAll() throws Exception {
      try (final Database database = createDatabaseWithIndex()) {
        final DatabaseInternal db = (DatabaseInternal) database;

        final JSONObject answer = checksumsHandler.computeChecksums(db);

        assertThat(answer.keySet()).isNotEmpty().doesNotContain(SnapshotHttpHandler.UNREADABLE_FILES_KEY);
        for (final String name : answer.keySet())
          assertThat(name).as("every key of a complete answer is a file name").doesNotContain("/");
      }
    }

    /**
     * A directory that behaves exactly like the real one except that it drops {@code doomed} in the gap between the
     * listing and the read. {@code File} is subclassed rather than mocked: the listing IS the JDK's, the deletion IS
     * the filesystem's, and only the interleaving is arranged.
     */
    private static File vanishingDirectory(final String path, final String doomed) {
      return new File(path) {
        @Override
        public File[] listFiles(final FileFilter filter) {
          final File[] listed = super.listFiles(filter);
          assertThat(new File(this, doomed).delete())
              .as("the fixture must really enter the gap, or the test proves nothing").isTrue();
          return listed;
        }
      };
    }

    /**
     * Creates the compaction output the way compaction does, and returns its file name. This is the constructor
     * {@code LSMTreeIndexCompactor} calls at line 83 before it starts streaming pages into it; not calling
     * {@code removeTempSuffix()} afterwards is what leaves the database in the state an in-flight compaction has it
     * in.
     */
    private static String startCompactionTemporary(final DatabaseInternal db) throws Exception {
      // A property index is a TypeIndex over one LSMTreeIndex per bucket; compaction runs on those, not on the wrapper.
      final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName(TYPE + "[" + INDEX_PROPERTY + "]");
      final LSMTreeIndex index = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
      final LSMTreeIndexCompacted temporary = index.getMutableIndex().createNewForCompaction();
      return temporary.getComponentFile().getFileName();
    }

    private static List<String> toList(final JSONArray array) {
      final List<String> names = new ArrayList<>(array.length());
      for (int i = 0; i < array.length(); i++)
        names.add(array.getString(i));
      return names;
    }

    private Database createDatabaseWithIndex() {
      final Database database = new DatabaseFactory(DATABASE_PATH).create();
      database.getSchema().createDocumentType(TYPE).createProperty(INDEX_PROPERTY, Integer.class)
          .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE).set(INDEX_PROPERTY, i).set("payload", "x".repeat(300)).save();
      });
      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      return database;
    }
  }

  /**
   * Issue #9638: {@code statistics.json} (and its pre-25.2.1 predecessor {@code cached-count.json}), a Graph Analytical
   * View's persisted CSR ({@code gav-<view>.csr}) and its contraction-hierarchy orders ({@code *.cchorder}) are written
   * per node - the GAV files only on the leader - and none of them is shipped by a snapshot, so the
   * {@code /api/v1/ha/snapshot/{database}/checksums} answer reported them as a data divergence on a healthy cluster.
   * {@code database.lck} was covered only because it happens to be empty.
   */
  @Nested
  class NodeLocalFilesChecksum {

    /** The reporter's probe, against the production method. */
    // Regression for #9638: node-local files are left out of the checksums
    @Test
    void nodeLocalEngineSideFilesAreNotChecksummed(@TempDir final Path dir) throws Exception {
      Files.writeString(dir.resolve("schema.json"), "{\"types\":[]}");
      Files.writeString(dir.resolve("Item_0.1.65536.v0.bucket"), "pages");
      Files.writeString(dir.resolve("statistics.json"), "{\"Item_0\":{\"count\":41},\":v\":2}");
      Files.writeString(dir.resolve("cached-count.json"), "{\"Item_0\":41}");
      Files.writeString(dir.resolve("gav-Routes.csr"), "GAVC-payload");
      Files.writeString(dir.resolve("gav-Routes.csr.cost.Road.cchorder"), "CCHO-payload");
      // NOT EMPTY ON PURPOSE: AN EMPTY FILE CRCS TO 0 ON EVERY NODE, WHICH IS HOW IT HID BEFORE
      Files.writeString(dir.resolve("database.lck"), "x");

      final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(dir.toFile());

      assertThat(checksums).containsOnlyKeys("schema.json", "Item_0.1.65536.v0.bucket");
    }

    // Regression for #9638: node-local files are left out of the checksums
    /**
     * The skip is by shape, not by substring: a bucket whose type happens to be named {@code gav-...} or
     * {@code statistics...} is data, and a short map reads as agreement.
     */
    @Test
    void dataFilesThatMerelyLookLikeNodeLocalOnesAreStillChecksummed(@TempDir final Path dir) throws Exception {
      Files.writeString(dir.resolve("gav-Routes_0.1.65536.v0.bucket"), "a bucket of a type named gav-Routes");
      Files.writeString(dir.resolve("statistics_0.2.65536.v0.bucket"), "a bucket of a type named statistics");
      Files.writeString(dir.resolve("Routes.csr"), "a user file without the gav- prefix");

      final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(dir.toFile());

      assertThat(checksums).containsOnlyKeys("gav-Routes_0.1.65536.v0.bucket", "statistics_0.2.65536.v0.bucket", "Routes.csr");
    }

    // Regression for #9638: node-local files are left out of the checksums
    /**
     * The documented trade-off of matching by shape: a stray file that takes the engine's own shape is left out too. Pinned
     * so that narrowing or widening the shape is a decision, not an accident.
     */
    @Test
    void aStrayFileTakingTheGavShapeIsLeftOut(@TempDir final Path dir) throws Exception {
      Files.writeString(dir.resolve("schema.json"), "{\"types\":[]}");
      Files.writeString(dir.resolve("gav-notes.csr"), "not written by the engine");

      assertThat(SnapshotManager.computeFileChecksums(dir.toFile())).containsOnlyKeys("schema.json");
    }

    // Regression for #9638: node-local files are left out of the checksums
    /**
     * The issue's three-node scenario stated as the property: the leader closed cleanly last with a GAV persisted, one
     * follower closed an hour ago with older statistics, the other got its copy by snapshot install and has neither.
     */
    @Test
    void threeHealthyNodesAgreeWhateverTheirNodeLocalState(@TempDir final Path tempDir) throws Exception {
      final Path leader = Files.createDirectory(tempDir.resolve("leader"));
      final Path restarted = Files.createDirectory(tempDir.resolve("restarted"));
      final Path installed = Files.createDirectory(tempDir.resolve("installed"));

      for (final Path node : new Path[] { leader, restarted, installed }) {
        Files.writeString(node.resolve("schema.json"), "{\"types\":[]}");
        Files.writeString(node.resolve("configuration.json"), "{}");
        Files.writeString(node.resolve("Item_0.1.65536.v0.bucket"), "pages");
      }

      Files.writeString(leader.resolve("statistics.json"), "{\"Item_0\":{\"count\":41},\":v\":2}");
      Files.writeString(leader.resolve("gav-Routes.csr"), "GAVC-payload");
      Files.writeString(leader.resolve("gav-Routes.csr.cost.Road.cchorder"), "CCHO-payload");
      Files.writeString(leader.resolve("database.lck"), "x");
      Files.writeString(restarted.resolve("statistics.json"), "{\"Item_0\":{\"count\":12},\":v\":2}");
      Files.writeString(restarted.resolve("gav-Routes.csr"), "GAVC-stale-payload-from-when-it-was-leader");
      Files.writeString(restarted.resolve("cached-count.json"), "{\"Item_0\":3}");

      final Map<String, Long> expected = SnapshotManager.computeFileChecksums(leader.toFile());
      assertThat(SnapshotManager.computeFileChecksums(restarted.toFile())).isEqualTo(expected);
      assertThat(SnapshotManager.computeFileChecksums(installed.toFile())).isEqualTo(expected);
    }

    // Regression for #9638: node-local files are left out of the checksums
    /**
     * The guard the issue asks for over the engine's side-file writers. Rather than another list of names, it drives a
     * REAL database through the writers - a graph, a Graph Analytical View with a contraction hierarchy, a clean close -
     * and requires every file the directory then holds to be either something a snapshot ships (and so something two
     * nodes must agree on) or something the checksum answer leaves out. A new engine side file that is neither fails
     * here, at the build that introduces it, instead of on an operator's diff of two healthy nodes.
     */
    @Test
    void everyFileARealDatabaseWritesIsEitherShippedOrLeftOutOfTheChecksums(@TempDir final Path tempDir) throws Exception {
      final String path = tempDir.resolve("db").toString();
      final List<String> seenWhileOpen = new ArrayList<>();

      try (final DatabaseFactory factory = new DatabaseFactory(path)) {
        final Database database = factory.create();
        try {
          database.getSchema().createVertexType("Junction").createProperty("i", Type.INTEGER);
          database.getSchema().createEdgeType("ROAD").createProperty("distance", Type.DOUBLE);
          database.transaction(() -> {
            final MutableVertex[] junctions = new MutableVertex[16];
            for (int i = 0; i < junctions.length; i++)
              junctions[i] = database.newVertex("Junction").set("i", i).save();
            for (int i = 0; i + 1 < junctions.length; i++) {
              junctions[i].newEdge("ROAD", junctions[i + 1]).set("distance", 1.0 + i).save();
              junctions[i + 1].newEdge("ROAD", junctions[i]).set("distance", 2.0 + i).save();
            }
          });

          final GraphAnalyticalView view = GraphAnalyticalView.builder(database).withName("Routes").withEdgeTypes("ROAD")
              .withUpdateMode(GraphAnalyticalView.UpdateMode.SYNCHRONOUS).withContractionHierarchy("distance").build();
          assertThat(view.awaitReady(60, TimeUnit.SECONDS)).isTrue();
          final ContractionHierarchy cch = view.getContractionHierarchy("distance", "ROAD");
          assertThat(cch).isNotNull();
          assertThat(cch.awaitReady(false, 60, TimeUnit.SECONDS)).isTrue();

          // #9751: LSM VECTOR INDEXES, THEIR GRAPHS BUILT, WRITE THE GRAPH COMPONENT AND THREE PLAIN SIDE FILES BESIDE IT -
          // ONE WITHOUT AND ONE WITH PRODUCT QUANTIZATION, SINCE ONLY THE LATTER WRITES A PQ
          VectorSnapshotShipTest.SideFilesChecksum.createVectorIndex(database, "Doc", "NONE");
          VectorSnapshotShipTest.SideFilesChecksum.createVectorIndex(database, "PqDoc", "PRODUCT");

          // #9751: AND A TIMESERIES TYPE WITH A SEALED STORE, THE OTHER WRITER OF RAW FILES INTO THE DIRECTORY
          database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (id STRING) FIELDS (value DOUBLE) SHARDS 1");
          database.transaction(() -> {
            for (int i = 0; i < 100; i++)
              database.command("sql", "INSERT INTO Reading SET ts = ?, id = 's1', value = ?", 1_700_000_000_000L + i * 1000L,
                  (double) i);
          });
          ((LocalTimeSeriesType) database.getSchema().getType("Reading")).getEngine().compactAll();

          for (final File file : listFiles(path))
            seenWhileOpen.add(file.getName());
          assertThat(seenWhileOpen).as("the open database holds its lock file, so the guard reaches it").contains("database.lck");
          assertUnshippedFilesAreLeftOut(path);
        } finally {
          database.close();
        }
      }

      final List<String> afterClose = new ArrayList<>();
      for (final File file : listFiles(path))
        afterClose.add(file.getName());
      // REACHABILITY: THE CLOSE MUST HAVE WRITTEN THE FILES THE ISSUE IS ABOUT, OR THIS TEST PROVES NOTHING
      assertThat(afterClose).as("the clean close writes the statistics file").contains(LocalSchema.STATISTICS_FILE_NAME);
      assertThat(afterClose).as("the clean close persists the view's CSR").contains("gav-Routes.csr");
      assertThat(afterClose).as("and the contraction hierarchy's order").anyMatch(n -> n.startsWith("gav-Routes.csr.") && n.endsWith(".cchorder"));
      assertThat(afterClose).as("the vector index persisted its graph").anyMatch(n -> n.endsWith(".vecgraph"));
      assertThat(afterClose).as("and its manifest").anyMatch(n -> n.endsWith(".vecgraphfp"));
      assertThat(afterClose).as("and its ordinal map").anyMatch(n -> n.endsWith(".vecordmap"));
      assertThat(afterClose).as("and its product quantization").anyMatch(n -> n.endsWith(".vecpq"));
      assertThat(afterClose).as("the TimeSeries compaction sealed a store").anyMatch(n -> n.endsWith(TimeSeriesSealedStore.FILE_EXTENSION));
      assertThat(afterClose).as("the clean close wrote the recency marker").contains(TransactionManager.LAST_TX_ID_FILE_NAME);

      assertUnshippedFilesAreLeftOut(path);
    }

    private static void assertUnshippedFilesAreLeftOut(final String path) throws Exception {
      final Map<String, Long> checksums = SnapshotManager.computeFileChecksums(new File(path));
      for (final File file : listFiles(path)) {
        final String name = file.getName();
        if (isShippedBySnapshot(name))
          continue;
        assertThat(checksums).as("'%s' is not shipped by a snapshot, so it is node-local and must not be in the checksum answer "
            + "- if it IS replicated content, ship it; if it is not, declare it in NodeLocalFiles", name).doesNotContainKey(name);
      }
      for (final String name : checksums.keySet())
        assertThat(isShippedBySnapshot(name)).as("'%s' is in the checksum answer but no snapshot ships it", name).isTrue();
    }

    /**
     * What {@code SnapshotHttpHandler} puts in the archive: the registered page files ({@code addPageFilesToZip}), the
     * sealed stores ({@code addSealedStoresToZip}), the two files of {@code addConfigurationToZip} and the synthesized
     * recency marker. A mirror of those methods, not a call into them: if the handler starts or stops shipping a file,
     * update this helper with it.
     */
    private static boolean isShippedBySnapshot(final String name) {
      return (LocalDatabase.isComponentFileName(name) && SnapshotHttpHandler.isShippedPageFile(name))
          || name.endsWith(TimeSeriesSealedStore.FILE_EXTENSION)
          || name.equals(LocalDatabase.CONFIGURATION_FILE_NAME)
          || name.equals(LocalSchema.SCHEMA_FILE_NAME)
          // SYNTHESIZED INTO THE ARCHIVE, BUT LEFT OUT OF THE CHECKSUMS ALL THE SAME: A LIVE NODE REWRITES IT ON ITS OWN WAL
          // ROTATIONS AND CLOSES, SO TWO HEALTHY PEERS HOLD DIFFERENT VALUES (#9751)
          || name.equals(TransactionManager.LAST_TX_ID_FILE_NAME);
    }

    private static File[] listFiles(final String path) {
      final File[] files = new File(path).listFiles(File::isFile);
      assertThat(files).isNotNull();
      return files;
    }
  }

  /**
   * Issue #7634: the HA verify holds the database read lock only while it opens its point-in-time window and lists the
   * TimeSeries sealed stores, not while it CRCs every byte of every file.
   * <p>
   * The verify's twin of #6114 (full backup) and #7456/#7671 (snapshot ship). On the window path the page bytes come from
   * the window, so the read lock only blocked the node's DDL for the whole verify. The fallback path, which reads the live
   * files under a flush suspension, keeps the lock, and one test here pins that.
   */
  @Nested
  class VerifyWindowHoldsNoReadLock {

    private static final String DATABASE_PATH    = "target/databases/verify-window-read-lock-7634";
    private static final String DOC_TYPE         = "Doc";
    private static final String TS_TYPE          = "Reading";
    private static final String AFTER_T0_TYPE    = "CreatedWhileVerifying";
    private static final long   BASE_TS          = 1_700_000_000_000L;
    private static final int    SAMPLES          = 5_000;
    /** Budget for something expected to happen. Generous: a wider bound cannot turn a passing run red. */
    private static final long   WAIT_MS          = 30_000L;
    /** A wait that is EXPECTED to expire: it IS the assertion, and a stall can only make it more true. */
    private static final long   BLOCKED_PROBE_MS = 2_000L;

    private final PostVerifyDatabaseHandler handler = new PostVerifyDatabaseHandler(null, null);

    @BeforeEach
    void clean() {
      PostVerifyDatabaseHandler.whileChecksummingForTesting = null;
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.reset();
      FileUtils.deleteRecursively(new File(DATABASE_PATH));
    }

    /** One method, so the order is fixed: close the handler, then reset the seam and the setting, then delete. */
    @AfterEach
    void tearDown() {
      handler.close();
      clean();
    }

    // Regression for #7634: the verify holds the database read lock only while opening its window
    /**
     * The headline claim: a {@code CREATE TYPE} completes while the verify is CRC-ing the window. The assertion is
     * logical rather than a stopwatch - a DDL that returns before the verify is released cannot have been queued behind a
     * lock the verify holds for its whole duration.
     */
    @Test
    void theWindowPathHoldsNoDatabaseReadLockWhileChecksumming() throws Exception {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

      try (final Database database = createDatabase()) {
        final DatabaseInternal db = (DatabaseInternal) database;
        final AtomicBoolean sawWindow = new AtomicBoolean();
        final AtomicBoolean ddlCompletedInsideTheVerify = new AtomicBoolean();
        final AtomicReference<Throwable> ddlFailure = new AtomicReference<>();

        PostVerifyDatabaseHandler.whileChecksummingForTesting = snapshot -> {
          sawWindow.set(snapshot != null);
          final Thread ddl = startDdl(database, ddlFailure);
          ddlCompletedInsideTheVerify.set(joined(ddl, WAIT_MS));
        };

        final JSONObject checksums = new JSONObject();
        handler.computeLocalChecksums(db, checksums, new JSONArray());

        assertThat(ddlFailure.get()).isNull();
        assertThat(sawWindow.get()).as("the fixture must have taken the window path").isTrue();
        assertThat(ddlCompletedInsideTheVerify.get())
            .as("DDL must run alongside the verify's CRC pass, not queue behind a read lock held for all of it")
            .isTrue();
        assertThat(database.getSchema().existsType(AFTER_T0_TYPE)).isTrue();
        assertThat(checksums.keySet()).as("the verify must still have produced its answer").isNotEmpty();
      }
    }

    // Regression for #7634: the verify holds the database read lock only while opening its window
    /**
     * The mirror image, and the reason the two branches are not one: with the window disabled the verify freezes the
     * live files, and there the read lock IS still held, so a concurrent DDL parks until the verify releases it.
     */
    @Test
    void theFallbackPathStillHoldsTheDatabaseReadLock() throws Exception {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);

      try (final Database database = createDatabase()) {
        final DatabaseInternal db = (DatabaseInternal) database;
        final AtomicBoolean sawWindow = new AtomicBoolean(true);
        final AtomicBoolean ddlCompletedInsideTheVerify = new AtomicBoolean(true);
        final AtomicReference<Throwable> ddlFailure = new AtomicReference<>();
        final AtomicReference<Thread> ddlThread = new AtomicReference<>();
        final AtomicReference<AssertionError> neverParked = new AtomicReference<>();

        PostVerifyDatabaseHandler.whileChecksummingForTesting = snapshot -> {
          sawWindow.set(snapshot != null);
          final Thread ddl = startDdl(database, ddlFailure);
          ddlThread.set(ddl);
          try {
            // WITHOUT THIS THE "DID NOT COMPLETE" ASSERTION BELOW IS VACUOUS: A THREAD THAT WAS NEVER SCHEDULED ALSO
            // DOES NOT COMPLETE
            awaitParkedOnTheWriteLock(ddl);
          } catch (final AssertionError e) {
            neverParked.set(e);
          }
          ddlCompletedInsideTheVerify.set(joined(ddl, BLOCKED_PROBE_MS));
        };

        handler.computeLocalChecksums(db, new JSONObject(), new JSONArray());

        assertThat(ddlThread.get()).isNotNull();
        assertThat(joined(ddlThread.get(), WAIT_MS)).as("the DDL must complete once the verify releases the lock").isTrue();
        assertThat(neverParked.get()).isNull();
        assertThat(ddlFailure.get()).isNull();
        assertThat(sawWindow.get()).as("the fixture must have taken the flush-suspension path").isFalse();
        assertThat(ddlCompletedInsideTheVerify.get())
            .as("the fallback reads the live files, so the read lock still pins them and the DDL waits for the verify")
            .isFalse();
        assertThat(database.getSchema().existsType(AFTER_T0_TYPE)).isTrue();
      }
    }

    // Regression for #7634: the verify holds the database read lock only while opening its window
    /**
     * What the read lock is still for on the window path: the sealed-store LISTING is taken in the same read-locked frame
     * as the window's t0. A TimeSeries type created while the verify CRCs - which the lock no longer excludes - must not
     * have its sealed store in the answer, because its pages are not in the window. Listing after the lock is released
     * would pair them.
     */
    @Test
    void theWindowPathListsTheSealedStoresAtT0() throws Exception {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

      try (final Database database = createDatabase()) {
        final DatabaseInternal db = (DatabaseInternal) database;
        final Set<String> sealedAtT0 = sealedFileNames(db);
        assertThat(sealedAtT0).as("the fixture must have sealed something, or this proves nothing").isNotEmpty();

        final AtomicReference<Set<String>> sealedDuringVerify = new AtomicReference<>();
        final AtomicReference<Throwable> ddlFailure = new AtomicReference<>();
        final AtomicBoolean ddlCompletedInsideTheVerify = new AtomicBoolean();
        PostVerifyDatabaseHandler.whileChecksummingForTesting = snapshot -> {
          // ON ANOTHER THREAD, SO A REGRESSION THAT PUTS THE READ LOCK BACK FAILS THIS TEST INSTEAD OF HANGING IT ON A
          // READ-TO-WRITE UPGRADE
          final Thread ddl = new Thread(() -> {
            try {
              database.command("sql", "CREATE TIMESERIES TYPE " + AFTER_T0_TYPE
                  + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
              sealedDuringVerify.set(sealedFileNames(db));
            } catch (final Throwable t) {
              ddlFailure.compareAndSet(null, t);
            }
          }, "issue7634-ts-ddl");
          ddl.setDaemon(true);
          ddl.start();
          ddlCompletedInsideTheVerify.set(joined(ddl, WAIT_MS));
        };

        final JSONObject checksums = new JSONObject();
        assertThat(handler.computeLocalChecksums(db, checksums, new JSONArray()))
            .as("a healthy database must report full sealed-store coverage").isTrue();

        assertThat(ddlFailure.get()).isNull();
        assertThat(ddlCompletedInsideTheVerify.get()).as("the TimeSeries DDL must run alongside the verify").isTrue();
        final Set<String> createdDuringVerify = new HashSet<>(sealedDuringVerify.get());
        createdDuringVerify.removeAll(sealedAtT0);
        assertThat(createdDuringVerify)
            .as("the DDL inside the verify must actually have created a sealed store, or this proves nothing")
            .isNotEmpty();

        assertThat(checksums.keySet()).containsAll(sealedAtT0);
        assertThat(checksums.keySet())
            .as("a sealed store created after t0 has no pages in the window and must not be in the answer")
            .doesNotContainAnyElementsOf(createdDuringVerify);
      }
    }

    // Regression for #7634: the verify holds the database read lock only while opening its window
    /**
     * The residual risk the narrowed lock accepts, pinned: a sealed store listed at t0 and gone before the CRC pass reads
     * it - a {@code DROP TYPE} the lock no longer excludes - is REPORTED as not covered, never silently left out of an
     * answer that still claims full coverage.
     */
    @Test
    void aSealedStoreGoneAfterT0IsReportedAsNotCovered() throws Exception {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

      try (final Database database = createDatabase()) {
        final DatabaseInternal db = (DatabaseInternal) database;
        final Set<String> sealedAtT0 = sealedFileNames(db);
        assertThat(sealedAtT0).as("the fixture must have sealed something, or this proves nothing").isNotEmpty();

        final File[] sealed = TimeSeriesSealedStore.listSealedFiles(new File(db.getDatabasePath()));
        final File moved = new File(sealed[0].getParentFile().getParentFile(), sealed[0].getName() + ".moved-7634");
        final AtomicBoolean removed = new AtomicBoolean();
        PostVerifyDatabaseHandler.whileChecksummingForTesting = snapshot -> removed.set(sealed[0].renameTo(moved));

        final JSONObject checksums = new JSONObject();
        try {
          assertThat(handler.computeLocalChecksums(db, checksums, new JSONArray()))
              .as("a sealed store listed at t0 and gone before it was read must make the answer report incomplete coverage")
              .isFalse();
        } finally {
          PostVerifyDatabaseHandler.whileChecksummingForTesting = null;
          // NOT ASSERTED: A FAILING RESTORE MUST NOT MASK THE ASSERTION ABOVE, AND tearDown() DELETES THE DIRECTORY ANYWAY
          if (moved.exists())
            moved.renameTo(sealed[0]);
        }
        assertThat(removed.get()).as("the fixture must actually have removed the listed sealed store").isTrue();
        assertThat(checksums.keySet()).doesNotContain(sealed[0].getName());
      }
    }

    // Regression for #7634: the verify holds the database read lock only while opening its window
    /**
     * The other thing the removed lock used to exclude: a {@code close()} (or {@code DROP DATABASE}) landing while the
     * window is read. It still cannot, but for a different reason: since #7458 a close waits for every open snapshot
     * window to be released, without holding a lock. So the close parks until the verify is done, and the verify's
     * answer is the full one.
     */
    @Test
    void aCloseDuringTheWindowPassWaitsForTheVerify() throws Exception {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(true);

      final Database database = createDatabase();
      try {
        final DatabaseInternal db = (DatabaseInternal) database;
        final Set<String> sealedAtT0 = sealedFileNames(db);
        final AtomicReference<Thread> closerThread = new AtomicReference<>();
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final AtomicReference<AssertionError> neverParked = new AtomicReference<>();
        final AtomicBoolean closedInsideTheVerify = new AtomicBoolean(true);
        PostVerifyDatabaseHandler.whileChecksummingForTesting = snapshot -> {
          final Thread closer = new Thread(() -> {
            try {
              database.close();
            } catch (final Throwable t) {
              closeFailure.compareAndSet(null, t);
            }
          }, "issue7634-close");
          closer.setDaemon(true);
          closer.start();
          closerThread.set(closer);
          try {
            awaitParkedIn(closer, "PageManager", "beginDatabaseClose");
          } catch (final AssertionError e) {
            neverParked.set(e);
          }
          closedInsideTheVerify.set(joined(closer, BLOCKED_PROBE_MS));
        };

        final JSONObject checksums = new JSONObject();
        assertThat(handler.computeLocalChecksums(db, checksums, new JSONArray())).isTrue();

        assertThat(neverParked.get()).isNull();
        assertThat(closedInsideTheVerify.get()).as("the close must wait for the window the verify is reading").isFalse();
        assertThat(joined(closerThread.get(), WAIT_MS)).as("the close must complete once the verify is done").isTrue();
        assertThat(closeFailure.get()).isNull();
        assertThat(checksums.keySet()).as("the verify's answer must be the full one").containsAll(sealedAtT0);
        assertThat(checksums.keySet().stream().anyMatch(n -> !n.endsWith(TimeSeriesSealedStore.FILE_EXTENSION)))
            .as("the page files must be in the answer too").isTrue();
      } finally {
        if (database.isOpen())
          database.close();
      }
    }

    // Regression for #7634: the verify holds the database read lock only while opening its window
    /**
     * The narrow case the close wait does not reach: a window that fails for its own reasons (shadow cap, I/O) is closed
     * before the fallback runs, so a pending close can complete in between. Pinned here: the fallback then fails loudly
     * on the closed database, rather than skipping every page file as "cannot be checksummed" and answering a checksum
     * set silently short of all of them.
     */
    @Test
    void aVerifyThatFallsBackOntoAClosedDatabaseFailsInsteadOfAnsweringShort() throws Exception {
      GlobalConfiguration.PAGE_SNAPSHOT_ENABLED.setValue(false);

      final Database database = createDatabase();
      final DatabaseInternal db = (DatabaseInternal) database;
      database.close();

      final JSONObject checksums = new JSONObject();
      Throwable failure = null;
      try {
        handler.computeLocalChecksums(db, checksums, new JSONArray());
      } catch (final Throwable t) {
        failure = t;
      }

      assertThat(failure)
          .as("a verify of a closed database must fail, not answer a checksum set short of its page files")
          .isInstanceOf(DatabaseIsClosedException.class);
      assertThat(checksums.keySet()).as("nothing may have been put in the answer").isEmpty();
    }

    // ------------------------------------------------------------------------------------------------- HELPERS

    private static Thread startDdl(final Database database, final AtomicReference<Throwable> failure) {
      final Thread ddl = new Thread(() -> {
        try {
          database.getSchema().createDocumentType(AFTER_T0_TYPE);
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      }, "issue7634-ddl");
      ddl.setDaemon(true);
      ddl.start();
      return ddl;
    }

    private static boolean joined(final Thread thread, final long millis) {
      try {
        thread.join(millis);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return !thread.isAlive();
    }

    /** Blocks until {@code thread} is parked acquiring a write lock, or fails. */
    private static void awaitParkedOnTheWriteLock(final Thread thread) {
      awaitParked(thread, frame -> frame.getClassName().endsWith("ReentrantReadWriteLock$WriteLock"), "on a write lock");
    }

    /** Blocks until {@code thread} is waiting inside {@code className.methodName}, or fails. */
    private static void awaitParkedIn(final Thread thread, final String className, final String methodName) {
      awaitParked(thread, frame -> frame.getClassName().endsWith(className) && methodName.equals(frame.getMethodName()),
          "in " + className + "." + methodName);
    }

    /**
     * Blocks until {@code thread} is WAITING with a stack frame matching {@code frameMatches}, or fails. The discriminator
     * a "did it stay blocked?" assertion needs: a thread that was never scheduled does not complete either.
     */
    private static void awaitParked(final Thread thread, final Predicate<StackTraceElement> frameMatches,
        final String where) {
      final long deadline = System.currentTimeMillis() + WAIT_MS;
      while (System.currentTimeMillis() < deadline) {
        final Thread.State state = thread.getState();
        if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
          for (final StackTraceElement frame : thread.getStackTrace())
            if (frameMatches.test(frame))
              return;
        if (state == Thread.State.TERMINATED)
          break;
        try {
          Thread.sleep(10);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      throw new AssertionError("thread '" + thread.getName() + "' never parked " + where + " (state=" + thread.getState()
          + "); the block assertion that follows would have been vacuous");
    }

    private static Set<String> sealedFileNames(final DatabaseInternal db) {
      final Set<String> names = new HashSet<>();
      for (final File file : TimeSeriesSealedStore.listSealedFiles(new File(db.getDatabasePath())))
        names.add(file.getName());
      return names;
    }

    private Database createDatabase() throws Exception {
      final Database database = new DatabaseFactory(DATABASE_PATH).create();
      database.getSchema().createDocumentType(DOC_TYPE);
      database.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          database.newDocument(DOC_TYPE).set("id", i).set("payload", "x".repeat(200)).save();
      });

      database.command("sql",
          "CREATE TIMESERIES TYPE " + TS_TYPE + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS 1");
      final long[] timestamps = new long[SAMPLES];
      final Object[] hosts = new Object[SAMPLES];
      final Object[] values = new Object[SAMPLES];
      for (int i = 0; i < SAMPLES; i++) {
        timestamps[i] = BASE_TS + i * 1_000L;
        hosts[i] = "host_" + (i % 4);
        values[i] = (double) i;
      }
      final var engine = ((LocalTimeSeriesType) database.getSchema().getType(TS_TYPE)).getEngine();
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      engine.compactAll();

      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      return database;
    }
  }
}
