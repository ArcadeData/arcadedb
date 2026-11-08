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
package com.arcadedb.engine;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class PageSnapshotWindowTest {

  @Nested
  class SnapshotConfigurationCapture extends TestHelper {

    @Test
    void theWindowCarriesBothConfigurationFilesAsOfT0() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalSchema schema = (LocalSchema) database.getSchema();

      schema.createDocumentType("AtT0");

      final String liveSchemaAtT0 = Files.readString(schema.getConfigurationFile().toPath(), StandardCharsets.UTF_8);
      final String previousSchemaAtT0 = Files.readString(
          new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME).toPath(), StandardCharsets.UTF_8);

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        final List<PageSnapshot.SnapshotConfigFile> captured = snapshot.getConfigurationFiles();

        assertThat(captured).extracting(PageSnapshot.SnapshotConfigFile::fileName)
            .as("configuration.json first, then schema.json, then schema.prev.json - the order a consumer archives "
                + "them in. The previous copy joined the window in #7637, so a restored database keeps the "
                + "corruption fallback its source had")
            .containsExactly(LocalDatabase.CONFIGURATION_FILE_NAME, LocalSchema.SCHEMA_FILE_NAME,
                LocalSchema.SCHEMA_PREV_FILE_NAME);

        assertThat(schemaOf(captured)).as("the window must serve the schema exactly as it was at t0")
            .isEqualTo(liveSchemaAtT0);

        // NOW MOVE THE LIVE SCHEMA ON, WHICH IS THE DDL THE READ LOCK USED TO EXCLUDE
        schema.createDocumentType("AfterT0");

        assertThat(Files.readString(schema.getConfigurationFile().toPath(), StandardCharsets.UTF_8))
            .as("the DDL must really have rewritten the live file").isNotEqualTo(liveSchemaAtT0);

        assertThat(schemaOf(snapshot.getConfigurationFiles()))
            .as("the window keeps serving t0 regardless of what the live schema does afterwards")
            .isEqualTo(liveSchemaAtT0);

        final JSONObject capturedTypes = new JSONObject(schemaOf(snapshot.getConfigurationFiles())).getJSONObject("types");
        assertThat(capturedTypes.keySet()).contains("AtT0");
        assertThat(capturedTypes.keySet())
            .as("a type created after t0 has no pages in this window, so it must not be in its schema either")
            .doesNotContain("AfterT0");

        // AND THE PREVIOUS COPY IS PINNED AT t0 TOO, not merely present in the captured set (issue #7637). This is
        // where that claim is decidable: the DDL above ran while this window was open, so the live schema.prev.json
        // now holds the generation the window is serving as its PRIMARY. A window that re-read the file instead of
        // capturing it would serve that newer copy - a fallback describing a page set the archive does not contain,
        // which is the whole reason the capture is inside the barrier rather than after it.
        final String capturedPrevious = new String(
            configurationFile(snapshot, LocalSchema.SCHEMA_PREV_FILE_NAME).content(), StandardCharsets.UTF_8);
        assertThat(capturedPrevious).isEqualTo(previousSchemaAtT0);
        assertThat(Files.readString(new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME).toPath(),
            StandardCharsets.UTF_8))
            .as("the DDL must really have moved the live previous copy on, or this proves nothing")
            .isNotEqualTo(capturedPrevious);
      }
    }

    /**
     * The bytes the consumer streams are the captured ones, not a re-read of the file: the stream is what the backup
     * and the HA snapshot ship actually write into their archive.
     */
    @Test
    void theCapturedStreamServesTheT0BytesAfterTheFileHasMovedOn() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalSchema schema = (LocalSchema) database.getSchema();

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        final PageSnapshot.SnapshotConfigFile capturedSchema = configurationFile(snapshot, LocalSchema.SCHEMA_FILE_NAME);

        schema.createDocumentType("WrittenAfterTheWindowOpened");

        final String streamed;
        try (final InputStream in = capturedSchema.newInputStream()) {
          streamed = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }

        assertThat(streamed).isEqualTo(new String(capturedSchema.content(), StandardCharsets.UTF_8));
        assertThat(capturedSchema.size()).isEqualTo(capturedSchema.content().length);
        assertThat(new JSONObject(streamed).getJSONObject("types").keySet())
            .doesNotContain("WrittenAfterTheWindowOpened");
      }
    }

    /**
     * Overlapping windows each hold their own point in time for the configuration too, exactly as they do for the
     * pages (challenge C3 of #6075).
     */
    @Test
    void overlappingWindowsEachKeepTheirOwnConfiguration() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalSchema schema = (LocalSchema) database.getSchema();

      schema.createDocumentType("First");

      try (final PageSnapshot older = db.getPageManager().openSnapshot(db)) {
        schema.createDocumentType("Second");

        try (final PageSnapshot newer = db.getPageManager().openSnapshot(db)) {
          assertThat(new JSONObject(schemaOf(older.getConfigurationFiles())).getJSONObject("types").keySet())
              .contains("First").doesNotContain("Second");
          assertThat(new JSONObject(schemaOf(newer.getConfigurationFiles())).getJSONObject("types").keySet())
              .contains("First", "Second");
        }
      }
    }

    /**
     * A configuration file that does not exist at t0 is simply absent, never an empty entry a restore would extract.
     * Both optional files are removed here, not just {@code configuration.json}: {@code schema.prev.json} is legitimately
     * missing on a database whose schema has never been re-saved, and joining the captured set in #7637 must not have
     * turned that into a zero-length entry either.
     */
    @Test
    void aMissingConfigurationFileIsOmittedRatherThanCapturedEmpty() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final LocalDatabase local = (LocalDatabase) db.getEmbedded();

      FileUtils.deleteFile(local.getConfigurationFile());
      assertThat(local.getConfigurationFile()).doesNotExist();

      final File previousSchema = new File(database.getDatabasePath(), LocalSchema.SCHEMA_PREV_FILE_NAME);
      FileUtils.deleteFile(previousSchema);
      assertThat(previousSchema).doesNotExist();

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        assertThat(snapshot.getConfigurationFiles()).extracting(PageSnapshot.SnapshotConfigFile::fileName)
            .containsExactly(LocalSchema.SCHEMA_FILE_NAME);
      }
    }

    private static String schemaOf(final List<PageSnapshot.SnapshotConfigFile> files) {
      return new String(configurationFile(files, LocalSchema.SCHEMA_FILE_NAME).content(), StandardCharsets.UTF_8);
    }

    private static PageSnapshot.SnapshotConfigFile configurationFile(final PageSnapshot snapshot, final String name) {
      return configurationFile(snapshot.getConfigurationFiles(), name);
    }

    private static PageSnapshot.SnapshotConfigFile configurationFile(
        final List<PageSnapshot.SnapshotConfigFile> files, final String name) {
      return files.stream().filter(f -> f.fileName().equals(name)).findFirst()
          .orElseThrow(() -> new AssertionError("the window carries no '" + name + "'"));
    }
  }

  @Nested
  class SnapshotBarrierAndCap extends TestHelper {

    private static final String TYPE          = "Doc";
    private static final long   ONE_MEGABYTE  = 1024L * 1024L;
    private static final int    WRITER_THREADS = 4;
    private static final int    BARRIER_ROUNDS = 12;

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("id", Integer.class);
      type.createProperty("payload", String.class);

      database.transaction(() -> {
        for (int i = 0; i < 4_000; i++)
          database.newDocument(TYPE).set("id", i).set("payload", "initial-" + "x".repeat(200)).save();
      });
      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    }

    /**
     * The default cap is no longer a number of megabytes at all: it is resolved when the window opens, to the size the
     * page files actually occupy at t0 - which is the ceiling the shadow provably cannot exceed, since it holds one
     * pre-image per page that existed at t0 and pages appended later need none.
     */
    @Test
    void theAutomaticCapIsTheSizeThePagesOccupyAtT0() {
      final DatabaseInternal db = (DatabaseInternal) database;

      assertThat(GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.getValueAsLong())
          .as("the default must be the automatic marker, not a number of MB").isEqualTo(-1L);

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        long t0Size = 0;
        for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
          t0Size += file.size();

        assertThat(t0Size).as("the fixture has to have written something for this to mean anything").isPositive();
        assertThat(snapshot.getShadowMaxSizeInBytes()).isEqualTo(t0Size);
        // THE POINT OF THE CHANGE: THE CAP TRACKS THE DATABASE, IT IS NOT THE OLD FLAT 1 GB THAT HAPPENS TO BE BIG
        // ENOUGH FOR A TEST FIXTURE AND TOO SMALL FOR A REAL DATABASE
        assertThat(snapshot.getShadowMaxSizeInBytes()).isNotEqualTo(1024L * ONE_MEGABYTE);
      }
    }

    /**
     * The automatic cap must never resolve to {@link PageShadow}'s "0 means uncapped" sentinel by accident. The free
     * space is halved with integer division, so a volume down to its last byte computes {@code 1 / 2 == 0} - which
     * would disable the cap outright in exactly the disk-almost-full case it exists for, the inverse of its purpose.
     * <p>
     * Asserted against the arithmetic directly rather than through a real volume: no test can fill a disk, and the
     * method is pure precisely so this edge is reachable.
     */
    @Test
    void theAutomaticCapNeverDegeneratesIntoTheUncappedSentinel() {
      final ContextConfiguration configuration = database.getConfiguration();
      // 64 KB OF PAGES AT t0, SO THE PROVABLE CEILING IS NOT ITSELF ZERO
      final List<PageSnapshot.SnapshotFile> files = List.of(
          new PageSnapshot.SnapshotFile(0, null, 65_536, 1, "one.bucket"));

      // NO RAM BUDGET AT ALL, SO THE FREE-SPACE TERM IS THE ONLY ONE LEFT AND THE SENTINEL EDGE IS REACHABLE. With the
      // default 64 MB budget the shadow would live entirely in RAM here and the cap would never consult the disk at
      // all (#6132), which is a different property, asserted in PageSnapshotWindowTest.SnapshotBarrierFollowups
      configuration.setValue(GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM, 0);

      for (final long usable : new long[] { 1L, 2L, 3L })
        assertThat(PageManager.snapshotMaxShadowSize(configuration, files, usable))
            .as("a nearly-full spill volume (%d usable bytes) must still cap the shadow", usable).isPositive();

      // AND THE ORDINARY CASES STILL COMPUTE WHAT THEY SHOULD
      assertThat(PageManager.snapshotMaxShadowSize(configuration, files, Long.MAX_VALUE))
          .as("with room to spare the cap is the provable ceiling").isEqualTo(65_536L);
      assertThat(PageManager.snapshotMaxShadowSize(configuration, files, 40_000L))
          .as("with less room than the database, and no RAM budget to claim, the cap is half the free space")
          .isEqualTo(20_000L);
      assertThat(PageManager.snapshotMaxShadowSize(configuration, files, 0L))
          .as("an unreadable free-space figure is not 'no room': fall back to the provable ceiling")
          .isEqualTo(65_536L);
      assertThat(PageManager.snapshotMaxShadowSize(configuration, List.of(), Long.MAX_VALUE))
          .as("an empty t0 page set can never need a pre-image, so uncapped is the honest answer").isZero();
    }

    /** A number set by hand still means exactly what it used to, and 0 still means "do not cap at all". */
    @Test
    void anExplicitCapIsStillAbsoluteMegabytesAndZeroIsStillUncapped() {
      final DatabaseInternal db = (DatabaseInternal) database;

      GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.setValue(7);
      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        assertThat(snapshot.getShadowMaxSizeInBytes()).isEqualTo(7 * ONE_MEGABYTE);
      }

      GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.setValue(0);
      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        assertThat(snapshot.getShadowMaxSizeInBytes()).isZero();
      }
    }

    /**
     * The shadow spills where it is told to, not necessarily next to the database it is protecting: a shadow can grow
     * to the size of the database, so on a volume sized for the data alone the two compete for space.
     */
    @Test
    void theShadowSpillsIntoTheConfiguredDirectoryAndCleansUpAfterItself(@TempDir final Path spillDir) throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      // NO RAM BUDGET AT ALL, SO THE FIRST CAPTURED PRE-IMAGE GOES STRAIGHT TO THE SPILL FILE
      GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.setValue(0);
      GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH.setValue(spillDir.toString());

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        rewriteEveryRecord("spilled");

        assertThat(snapshot.getShadowSpilledBytes()).as("the whole shadow must have gone to disk").isPositive();
        assertThat(shadowFilesIn(spillDir.toFile())).as("the spill file must live in the configured directory")
            .isNotEmpty();
        assertThat(shadowFilesIn(new File(db.getDatabasePath())))
            .as("nothing may be left in the database directory").isEmpty();
      }

      assertThat(shadowFilesIn(spillDir.toFile())).as("the spill file is scratch and is removed with the window")
          .isEmpty();
    }

    /**
     * The spill directory is created if it is not there yet, and a path that cannot be one degrades to the database
     * directory instead of failing the backup. Both branches run BEFORE the barrier takes its locks (they are
     * unbounded blocking filesystem calls), so they are also the reason the cap arithmetic that consumes them is pure.
     */
    @Test
    void theSpillDirectoryIsCreatedOnDemandAndAnUnusableOneDegradesToTheDatabaseDirectory(@TempDir final Path tempDir)
        throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.setValue(0);

      final File notYetThere = tempDir.resolve("nested/spill").toFile();
      assertThat(notYetThere).doesNotExist();
      GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH.setValue(notYetThere.getAbsolutePath());

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        rewriteEveryRecord("created-on-demand");
        assertThat(snapshot.getShadowSpilledBytes()).isPositive();
        assertThat(shadowFilesIn(notYetThere)).as("the directory must have been created and used").isNotEmpty();
      }

      // A PLAIN FILE CANNOT BE A DIRECTORY, SO THIS EXERCISES THE FALLBACK. A MISCONFIGURED SPILL PATH MUST COST A
      // WARNING AND THE OLD LOCATION, NEVER THE BACKUP ITSELF
      final File notADirectory = tempDir.resolve("this-is-a-file").toFile();
      assertThat(notADirectory.createNewFile()).isTrue();
      GlobalConfiguration.PAGE_SNAPSHOT_SPILL_PATH.setValue(notADirectory.getAbsolutePath());

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        rewriteEveryRecord("fallen-back");
        assertThat(snapshot.getStatus()).as("a misconfigured spill path must not break the window")
            .isEqualTo(PageSnapshot.STATUS.ACTIVE);
        assertThat(snapshot.getShadowSpilledBytes()).isPositive();
        assertThat(shadowFilesIn(new File(db.getDatabasePath())))
            .as("the shadow must have fallen back to the database directory").isNotEmpty();
      }
    }

    /**
     * The property the whole barrier exists for, asserted end to end and on CONTENT rather than on a counter: a record
     * committed immediately before the window opens, while four threads commit continuously around it, must be
     * findable in the point-in-time image of the page files, and no barrier may report itself unable to prove the
     * pipeline was empty.
     * <p>
     * This is the integration guard, not the proof - the microsecond-wide race the retry loop used to lose cannot be
     * provoked on demand, and the mechanism that closes it is pinned deterministically by
     * {@link #publicationCannotReachTheFlushPipelineWhileThePageManagerLockIsHeld()}.
     */
    @Test
    void theLastCommitBeforeTheBarrierIsAlwaysInThePointInTimeImage() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
      final List<Thread> writers = startWriters(stop, writerFailure);

      try {
        final long inexactBefore = pageManager.getStats().snapshotBarriersInexact;

        for (int round = 0; round < BARRIER_ROUNDS; round++) {
          final String marker = "BARRIER-MARKER-" + round + "-" + System.nanoTime();

          // COMMITTED FROM THIS THREAD, THEN THE WINDOW IS OPENED WITH NO PAUSE IN BETWEEN: THE WRITERS ARE STILL
          // COMMITTING THROUGHOUT, SO THIS IS THE EXACT RACE THE RETRY LOOP USED TO LOSE
          database.transaction(() -> database.newDocument(TYPE).set("id", -1).set("payload", marker).save());

          try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
            assertThat(containsMarker(snapshot, marker))
                .as("round %d: the transaction committed just before t0 must be in the point-in-time image", round)
                .isTrue();
          }
        }

        assertThat(pageManager.getStats().snapshotBarriersInexact)
            .as("no commit can leave the pipeline non-empty at t0 any more, so no barrier may report itself inexact")
            .isEqualTo(inexactBefore);
      } finally {
        stop.set(true);
        for (final Thread writer : writers)
          writer.join(30_000);
      }

      // A WRITER THAT DIED WOULD HAVE MADE THE ASSERTIONS ABOVE PASS AGAINST AN IDLE DATABASE, WHICH IS THE ONE
      // CONDITION UNDER WHICH THE OLD BARRIER ALSO PASSED
      assertThat(writerFailure.get()).isNull();
    }

    /**
     * The premise the retry-free barrier rests on, pinned deterministically: {@link PageManager#publishPages} holds
     * the global page-manager lock across BOTH halves of publication - the synchronous page write and the
     * {@code scheduleFlushOfPages} enqueue - so while the barrier holds that lock no committer can put a page on disk
     * or into the flush pipeline, and the drain it performs there is guaranteed to converge.
     * <p>
     * This is the assertion to keep: the end-to-end race the retry loop used to lose is a microsecond-wide window that
     * cannot be provoked on demand, whereas a refactor that moved the enqueue out from under this lock would silently
     * bring it back and is caught here every run.
     */
    @Test
    void publicationCannotReachTheFlushPipelineWhileThePageManagerLockIsHeld() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();
      assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

      // A REAL PAGE OF A REAL BUCKET, PUBLISHED THE WAY A COMMIT'S SECOND PHASE PUBLISHES ITS PAGES. NOT A WHOLE
      // TRANSACTION: THAT WOULD ALSO BLOCK ON THE VALIDATION HALF OF updatePages, WHICH TAKES THE SAME LOCK, AND THE
      // TEST WOULD PASS EVEN IF THE ENQUEUE HAD BEEN MOVED OUT FROM UNDER IT - WHICH IS THE ONE REGRESSION IT EXISTS
      // TO CATCH
      final int fileId = db.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final PageId pageId = new PageId(db, fileId, 0);
      final MutablePage page = pageManager.getImmutablePage(pageId, file.getPageSize(), false, true).modify();

      final AtomicBoolean published = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread publisher = new Thread(() -> {
        try {
          pageManager.publishPages(List.of(page), null, true);
          published.set(true);
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue6125-blocked-publisher");
      publisher.setDaemon(true);

      pageManager.executeInLock(() -> {
        publisher.start();
        // LONG ENOUGH THAT A PUBLICATION THAT COULD RUN WOULD HAVE FINISHED MANY TIMES OVER
        Thread.sleep(500);
        assertThat(published.get()).as("publication must not complete while the page-manager lock is held").isFalse();
        assertThat(pageManager.getFlushThread().hasPendingPagesOfDatabase(db))
            .as("and no page of it may have reached the flush pipeline either - that is what lets the t0 barrier's "
                + "drain converge with this lock held, instead of retrying").isFalse();
        return null;
      });

      publisher.join(30_000);
      assertThat(failure.get()).isNull();
      // THE PAIRED ASSERTION: THE PUBLICATION WAS BLOCKED, NOT BROKEN, SO THIS CANNOT PASS BY NOTHING EVER HAPPENING
      assertThat(published.get()).as("releasing the lock must let the very same publication through").isTrue();
    }

    /**
     * The other half of holding the JVM-wide lock across the barrier: everything done under it has to be BOUNDED, or
     * the lock that makes the drain converge becomes a way for one sick disk to stall every committer in the process.
     * <p>
     * The review of #6126 caught exactly this: the residual drain was capped but the wait for the in-flight flush
     * batch was not, and that wait polls until a synchronous {@code file.write} returns. Here an in-flight batch is
     * fabricated and never completes - the shape a wedged disk presents - and the barrier has to give the window up
     * within its budget and let go of the lock, rather than hold it until the write lands.
     */
    @Test
    void aFlushThatNeverCompletesFailsTheBarrierInsteadOfHoldingTheGlobalLockForever() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();
      final PageManagerFlushThread flushThread = pageManager.getFlushThread();
      assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

      final int fileId = db.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final MutablePage page = pageManager.getImmutablePage(new PageId(db, fileId, 0), file.getPageSize(), false, true)
          .modify();

      // AN IN-FLIGHT BATCH OF THIS DATABASE THAT NEVER FINISHES. NOT IN pageIndex, SO BOTH DRAINS STILL SEE AN EMPTY
      // PIPELINE AND THE BARRIER REACHES THE WAIT UNDER TEST. THE FLUSH THREAD ONLY TOUCHES nextPagesToFlush WHEN IT
      // POLLS A REAL BATCH, AND THIS DATABASE IS QUIET, SO THE FABRICATED VALUE STANDS
      flushThread.nextPagesToFlush.set(new PageManagerFlushThread.PagesToFlush(new ArrayList<>(List.of(page))));
      try {
        final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
        // THE REASON IS THE PROPERTY UNDER TEST, NOT THE ELAPSED TIME: openSnapshot HAS TWO OPPOSITE-MEANING TIMEOUTS
        // BEHIND THE SAME EXCEPTION TYPE (#6394), AND UNDER A FULL-SUITE RUN trySuspendUntil CAN GENUINELY LOSE ITS
        // OWN RACE FIRST (ANOTHER SUSPENDER STILL RESUMING) - A DURATION-BASED ASSERTION CANNOT TELL THAT APART FROM
        // THE waitForCurrentFlushToCompleteUntil TIMEOUT THIS TEST FABRICATES, AND FLAKES WHEN IT GUESSES WRONG
        assertThatThrownBy(() -> pageManager.openSnapshot(db))
            .as("the barrier must abandon the window rather than wait out a flush that never lands")
            .isInstanceOf(PageSnapshotException.class)
            .extracting(e -> ((PageSnapshotException) e).getReason())
            .as("the fabricated never-completing flush must be diagnosed as the flush timeout, not the unrelated "
                + "suspend-timeout race")
            .isEqualTo(PageSnapshotException.Reason.FLUSH_TIMEOUT);

        // THE UPPER BOUND IS STILL A REAL PROPERTY: FAR BELOW THE FOREVER THE UNBOUNDED WAIT WOULD HAVE TAKEN, AND
        // DISCOUNTED FOR JVM STALLS (#6260) RATHER THAN WIDENED
        stopwatch.assertGaveUpWithin(60_000L, "the barrier's own budget from waiting out a flush that never lands");

        // AND IT MUST HAVE LET GO: A FAILED BARRIER THAT KEPT THE GLOBAL LOCK WOULD BE WORSE THAN THE HANG IT REPLACES
        final AtomicBoolean acquired = new AtomicBoolean();
        final Thread other = new Thread(() -> pageManager.executeInLock(() -> {
          acquired.set(true);
          return null;
        }), "issue6125-lock-probe");
        other.setDaemon(true);
        other.start();
        other.join(10_000);
        assertThat(acquired.get()).as("the page-manager lock must be free again after a failed barrier").isTrue();

        // THE SUSPENSION IT TOOK ON THE WAY IN MUST BE RELEASED TOO, OR THE DATABASE STAYS FROZEN FOR EVERY WRITER
        assertThat(pageManager.isPageFlushingSuspended(db)).isFalse();
      } finally {
        flushThread.nextPagesToFlush.set(null);
      }

      // THE PAIRED ASSERTION: WITH THE FABRICATED BATCH GONE, THE VERY SAME CALL SUCCEEDS - SO THE FAILURE ABOVE WAS
      // THE WAIT UNDER TEST AND NOT A DATABASE LEFT BROKEN BY THE FIXTURE
      try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
        assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      }
    }

    /**
     * The one stall the snapshot path still has is now reported rather than only logged when it goes wrong.
     * <p>
     * The timer measures the barrier itself: it starts inside the per-database monitor, so a second caller queueing
     * behind a barrier on the same database is not charged for the wait, and a call refused before the barrier is
     * entered at all is neither timed nor counted. Neither of those is asserted here - the first needs two threads
     * racing on one database and the second needs a page manager that is not running, which no test can arrange
     * without breaking the JVM-wide singleton this and every other test class shares.
     */
    @Test
    void theBarrierIsTimed() {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();

      final PageManager.PPageManagerStats before = pageManager.getStats();

      for (int i = 0; i < 3; i++)
        pageManager.openSnapshot(db).close();

      final PageManager.PPageManagerStats after = pageManager.getStats();
      assertThat(after.snapshotBarriers).isEqualTo(before.snapshotBarriers + 3);
      assertThat(after.snapshotBarrierMillis).isGreaterThanOrEqualTo(before.snapshotBarrierMillis);
      assertThat(after.snapshotBarrierMaxMillis).isGreaterThanOrEqualTo(before.snapshotBarrierMaxMillis);
    }

    /**
     * A cap breach and a disk failure both end a window and both push its consumer onto the path that throttles
     * writers, but one is answered by tuning and the other by looking at the disk, so they are counted apart.
     */
    @Test
    void aCapBreachIsCountedAsAnOverflowAndNotAsAFailure() {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();

      // ENOUGH RECORDS TO SPREAD OVER FAR MORE PAGES THAN THE 1 MB CAP BELOW CAN HOLD PRE-IMAGES FOR
      database.transaction(() -> {
        for (int i = 0; i < 20_000; i++)
          database.newDocument(TYPE).set("id", 500_000 + i).set("payload", "z".repeat(500)).save();
      });
      pageManager.waitAllPagesOfDatabaseAreFlushed(db);

      final PageManager.PPageManagerStats before = pageManager.getStats();

      GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM.setValue(1);
      GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE.setValue(1);

      try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
        for (int round = 0; round < 5 && snapshot.getStatus() == PageSnapshot.STATUS.ACTIVE; round++)
          rewriteEveryRecord("overflow-" + round);

        assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.OVERFLOWED);
      }

      final PageManager.PPageManagerStats after = pageManager.getStats();
      assertThat(after.snapshotWindowsOverflowed).isEqualTo(before.snapshotWindowsOverflowed + 1);
      assertThat(after.snapshotWindowsFailed).as("nothing about the disk went wrong here")
          .isEqualTo(before.snapshotWindowsFailed);
      assertThat(after.snapshotWindowsInvalidated).as("the total stays the sum of the two reasons")
          .isEqualTo(before.snapshotWindowsInvalidated + 1);
    }

    // ------------------------------------------------------------------------------------------------------ HELPERS

    private List<Thread> startWriters(final AtomicBoolean stop, final AtomicReference<Throwable> failure) {
      final List<Thread> writers = new ArrayList<>(WRITER_THREADS);
      for (int t = 0; t < WRITER_THREADS; t++) {
        final int writerId = t;
        final Thread writer = new Thread(() -> {
          try {
            for (int i = 0; !stop.get(); i++) {
              final int id = writerId * 1_000_000 + i;
              // RETRIES: CONCURRENT WRITERS PACK INTO THE SAME PAGES, AND A LOST MVCC RACE IS NOISE HERE, NOT THE
              // FAILURE THE TEST IS LOOKING FOR
              database.transaction(() -> database.newDocument(TYPE).set("id", id).set("payload", "load-" + id).save(),
                  false, 10);
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue6125-writer-" + t);
        writer.setDaemon(true);
        writer.start();
        writers.add(writer);
      }
      return writers;
    }

    /** Streams the whole point-in-time image of every page file, looking for the marker's UTF-8 bytes. */
    private boolean containsMarker(final PageSnapshot snapshot, final String marker) throws Exception {
      final byte[] needle = marker.getBytes(StandardCharsets.UTF_8);
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
        if (streamContains(snapshot, file.fileId(), needle))
          return true;
      return false;
    }

    private boolean streamContains(final PageSnapshot snapshot, final int fileId, final byte[] needle) throws Exception {
      // THE WINDOW BEFORE THE BUFFER IS CARRIED OVER SO A MARKER STRADDLING TWO READS IS STILL FOUND
      final byte[] buffer = new byte[64 * 1024 + needle.length];
      int carried = 0;
      try (final InputStream in = snapshot.newInputStream(fileId)) {
        for (int read = in.read(buffer, carried, buffer.length - carried); read > 0;
            read = in.read(buffer, carried, buffer.length - carried)) {
          final int available = carried + read;
          if (indexOf(buffer, available, needle) >= 0)
            return true;
          carried = Math.min(available, needle.length - 1);
          System.arraycopy(buffer, available - carried, buffer, 0, carried);
        }
      }
      return false;
    }

    private static int indexOf(final byte[] haystack, final int length, final byte[] needle) {
      outer:
      for (int i = 0; i <= length - needle.length; i++) {
        for (int j = 0; j < needle.length; j++)
          if (haystack[i + j] != needle[j])
            continue outer;
        return i;
      }
      return -1;
    }

    private static File[] shadowFilesIn(final File directory) {
      final File[] found = directory.listFiles((d, name) -> name.endsWith("." + PageSnapshot.SHADOW_FILE_EXT));
      return found != null ? found : new File[0];
    }

    private void rewriteEveryRecord(final String marker) {
      database.transaction(() -> database.iterateType(TYPE, false).forEachRemaining(record -> {
        final MutableDocument doc = record.asDocument().modify();
        doc.set("payload", marker + "-" + "y".repeat(200));
        doc.save();
      }));
      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    }
  }

  @Nested
  class SnapshotBarrierFollowups extends TestHelper {

    private static final String TYPE = "Doc";

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("id", Integer.class);
      type.createProperty("payload", String.class);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

      database.transaction(() -> {
        for (int i = 0; i < 4_000; i++)
          database.newDocument(TYPE).set("id", i).set("payload", "initial-" + "x".repeat(200)).save();
      });
      ((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
    }

    /**
     * Item 1, and the whole warrant for the syscall being gone: the in-memory count is not an estimate that is usually
     * right, it is the same number the filesystem holds.
     * <p>
     * The argument it pins is short - a paginated component file is extended by exactly one operation, a whole-page
     * write at a known page number, and nothing truncates it - but "should agree" is precisely what needed proving
     * before the syscall could be removed, because being wrong here is SILENT: a count too low truncates the archive,
     * one too high reads past the end of the file. Asserted at t0, which is when the snapshot reads it, and after each
     * of the three events that could plausibly break it.
     */
    @Test
    void theInMemoryPageCountAgreesWithTheFilesystemAtT0() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      assertEveryPageCountAgreesAtT0("straight after the fixture");

      // A FRESHLY APPENDED PAGE: the counter has to follow the write that extended the file, not the next reopen
      database.transaction(() -> {
        for (int i = 0; i < 20_000; i++)
          database.newDocument(TYPE).set("id", 100_000 + i).set("payload", "grown-" + "y".repeat(400)).save();
      });
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      assertEveryPageCountAgreesAtT0("after appending pages");

      // COMPACTION: the case the issue was most worried about, because PaginatedComponent's own counter is monotonic
      // and would over-report a file that shrank. The file-level counter cannot: compaction builds a NEW file and
      // drops the old one, and a new file is seeded from its real length when it is opened
      final Index[] indexes = db.getSchema().getIndexes();
      assertThat(indexes).as("the fixture has to have an index for the compaction step to mean anything").isNotEmpty();
      for (final Index index : indexes)
        ((IndexInternal) index).compact();
      db.getPageManager().waitAllPagesOfDatabaseAreFlushed(database);
      assertEveryPageCountAgreesAtT0("after compacting the index");

      // AND A REOPEN, WHICH IS THE ONE MOMENT THE COUNTER IS SEEDED FROM THE FILESYSTEM RATHER THAN MAINTAINED
      reopenDatabase();
      assertEveryPageCountAgreesAtT0("after reopening the database");
    }

    /**
     * Item 2. A barrier that gives up before publishing a window is the one snapshot outcome nothing could see: the
     * invalidation counters all require a window to exist, and the consumer falls back to suspend-and-freeze and still
     * completes, so a backup that has quietly gone back to throttling every writer on the server reported success.
     * <p>
     * The wedged in-flight batch is fabricated exactly as {@code PageSnapshotWindowTest.SnapshotBarrierAndCap} does it. Which of
     * the two failing steps wins is NOT asserted - a full-suite run has other databases actively resuming, so
     * {@code trySuspendUntil} can genuinely lose its own race first (#6394) - so the reason the exception reports is
     * read out and the matching split counter is the one checked. The total is checked either way, which is the
     * reading an operator alerts on.
     */
    @Test
    void aBarrierThatGivesUpBeforePublishingAWindowIsCounted() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();
      final PageManagerFlushThread flushThread = pageManager.getFlushThread();
      assertThat(pageManager.waitAllPagesOfDatabaseAreFlushed(db)).isTrue();

      final int fileId = db.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId();
      final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(fileId);
      final MutablePage page = pageManager.getImmutablePage(new PageId(db, fileId, 0), file.getPageSize(), false, true)
          .modify();

      final PageManager.PPageManagerStats before = pageManager.getStats();

      // AN IN-FLIGHT BATCH OF THIS DATABASE THAT NEVER FINISHES: THE SHAPE A WEDGED DISK PRESENTS
      flushThread.nextPagesToFlush.set(new PageManagerFlushThread.PagesToFlush(new ArrayList<>(List.of(page))));
      final AtomicReference<PageSnapshotException.Reason> reason = new AtomicReference<>();
      try {
        try {
          pageManager.openSnapshot(db).close();
          fail("the fabricated never-completing flush must fail the barrier");
        } catch (final PageSnapshotException e) {
          reason.set(e.getReason());
        }
      } finally {
        flushThread.nextPagesToFlush.set(null);
      }

      final PageManager.PPageManagerStats after = pageManager.getStats();

      assertThat(after.snapshotBarriersFailed)
          .as("a barrier that never published a window must be counted, or nothing an operator can alert on records it")
          .isEqualTo(before.snapshotBarriersFailed + 1);
      assertThat(after.snapshotBarriers)
          .as("and it is still a barrier: the timer already covered it, which is what made a failure indistinguishable")
          .isEqualTo(before.snapshotBarriers + 1);

      if (reason.get() == PageSnapshotException.Reason.FLUSH_TIMEOUT) {
        assertThat(after.snapshotBarriersFailedFlush).isEqualTo(before.snapshotBarriersFailedFlush + 1);
        assertThat(after.snapshotBarriersFailedSuspend).isEqualTo(before.snapshotBarriersFailedSuspend);
      } else if (reason.get() == PageSnapshotException.Reason.SUSPEND_TIMEOUT) {
        assertThat(after.snapshotBarriersFailedSuspend).isEqualTo(before.snapshotBarriersFailedSuspend + 1);
        assertThat(after.snapshotBarriersFailedFlush).isEqualTo(before.snapshotBarriersFailedFlush);
      }

      // THE PAIRED ASSERTION: WITH THE FABRICATED BATCH GONE THE VERY SAME CALL SUCCEEDS AND COUNTS NOTHING AS FAILED,
      // SO THE COUNTER ABOVE MOVED FOR THE FAILURE AND NOT FOR EVERY BARRIER
      try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
        assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      }
      assertThat(pageManager.getStats().snapshotBarriersFailed).isEqualTo(after.snapshotBarriersFailed);
    }

    /**
     * Item 3. The cap covers RAM plus spill, and sizing it from the free space alone put it BELOW
     * {@code arcadedb.pageSnapshotMaxRAM} on any spill volume with less than twice the RAM budget free - so a shadow
     * that would have lived entirely in memory and never written a byte to that volume was invalidated anyway, and its
     * backup fell back to throttling every writer over space it was never going to use.
     * <p>
     * Pure arithmetic, like the sentinel edge it sits next to: no test can fill a disk, and the method is
     * package-private precisely so these cases are reachable directly.
     */
    @Test
    void theAutomaticCapNeverFallsBelowTheRamBudget() {
      final ContextConfiguration configuration = database.getConfiguration();
      final long oneMegabyte = 1024L * 1024L;

      // A 256 MB DATABASE AT t0 AND A 64 MB RAM BUDGET, THE DEFAULT
      final List<PageSnapshot.SnapshotFile> files = List.of(
          new PageSnapshot.SnapshotFile(0, null, 64 * 1024, 4 * 1024, "one.bucket"));  // 4096 pages of 64 KB = 256 MB
      configuration.setValue(GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM, 64);

      assertThat(PageManager.snapshotMaxShadowSize(configuration, files, 40 * oneMegabyte))
          .as("40 MB free on the spill volume used to cap the shadow at 20 MB - below a RAM budget that needs no disk")
          .isEqualTo(64 * oneMegabyte);
      assertThat(PageManager.snapshotMaxShadowSize(configuration, files, 0L))
          .as("an unreadable free-space figure still falls back to the provable ceiling")
          .isEqualTo(256 * oneMegabyte);

      // THE DISK STILL WINS WHEN IT HAS MORE TO OFFER THAN THE RAM BUDGET: THIS RAISES THE FLOOR, IT DOES NOT REPLACE
      // THE FREE-SPACE TERM
      assertThat(PageManager.snapshotMaxShadowSize(configuration, files, 300 * oneMegabyte))
          .as("half of 300 MB is more than the RAM budget, so it is what the cap uses").isEqualTo(150 * oneMegabyte);

      // AND THE PROVABLE CEILING IS STILL THE OUTER BOUND: A RAM BUDGET LARGER THAN THE WHOLE DATABASE CANNOT RAISE
      // THE CAP ABOVE ONE PRE-IMAGE PER PAGE THAT EXISTED AT t0
      configuration.setValue(GlobalConfiguration.PAGE_SNAPSHOT_MAX_RAM, 4_096);
      assertThat(PageManager.snapshotMaxShadowSize(configuration, files, 40 * oneMegabyte))
          .as("the RAM budget raises the floor, it does not invent pre-images that cannot exist")
          .isEqualTo(256 * oneMegabyte);

      // AN EXPLICIT CAP IS UNTOUCHED BY ALL OF THIS
      configuration.setValue(GlobalConfiguration.PAGE_SNAPSHOT_MAX_SIZE, 7);
      assertThat(PageManager.snapshotMaxShadowSize(configuration, files, 40 * oneMegabyte))
          .as("a number set by hand still means exactly what it says").isEqualTo(7 * oneMegabyte);
    }

    // ------------------------------------------------------------------------------------------------------ HELPERS

    /**
     * Opens a real snapshot window - so the counts are read at the very moment {@code buildSnapshot} reads them, with
     * the pipeline drained and the flush thread suspended - and checks every page file against its own channel.
     */
    private void assertEveryPageCountAgreesAtT0(final String when) throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      int checked = 0;
      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        for (final PageSnapshot.SnapshotFile file : snapshot.getFiles()) {
          final PaginatedComponentFile paginated = file.file();
          assertThat((long) file.pageCount())
              .as("%s: the t0 page count of '%s' must be what the file holds", when, file.fileName())
              .isEqualTo(paginated.getTotalPagesFromChannel());
          assertThat(paginated.getTotalPages())
              .as("%s: the in-memory counter of '%s' must be what the file holds", when, file.fileName())
              .isEqualTo(paginated.getTotalPagesFromChannel());
          checked++;
        }
      }

      // A SNAPSHOT OVER NO FILES WOULD MAKE EVERY ASSERTION ABOVE VACUOUS
      assertThat(checked).as("%s: the snapshot must cover the database's page files", when).isGreaterThan(1);
    }
  }

  @Nested
  class CloseWaitsForSnapshotWindow extends TestHelper {
    private static final String TYPE = "Doc";

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("id", Integer.class);
      database.transaction(() -> {
        for (int i = 0; i < 500; i++)
          database.newDocument(TYPE).set("id", i).save();
      });
    }

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Test
    void closeWaitsUntilTheWindowIsReleased() throws Exception {
      assertCloseWaitsForTheWindow(false);
    }

    @Test
    void dropWaitsUntilTheWindowIsReleased() throws Exception {
      assertCloseWaitsForTheWindow(true);
    }

    @Test
    void noNewWindowOpensWhileACloseIsWaiting() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();

      final PageSnapshot first = pageManager.openSnapshot(db);
      final Thread closer = new Thread(db::close, "closer");
      try {
        closer.start();

        // THE CLOSE HAS NO WAY TO SIGNAL THAT IT IS WAITING, SO PROBE FOR ITS EFFECT: ONCE THE CLOSE IS WAITING, A NEW
        // WINDOW IS REFUSED. UNTIL THEN A WINDOW STILL OPENS (AND IS RELEASED AT ONCE, SO THE CLOSE IS NOT DELAYED)
        PageSnapshotException refusal = null;
        final long deadline = System.currentTimeMillis() + 30_000;
        while (refusal == null && System.currentTimeMillis() < deadline) {
          try (final PageSnapshot probe = pageManager.openSnapshot(db)) {
            assertThat(probe.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
          } catch (final PageSnapshotException e) {
            refusal = e;
          }
          Thread.sleep(10);
        }

        assertThat(refusal).as("a window must be refused once the close is waiting").isNotNull();
        assertThat(refusal.getReason()).isEqualTo(PageSnapshotException.Reason.CLOSING);
        assertThat(closer.isAlive()).as("the close must still be waiting for the first window").isTrue();
        assertThat(db.isOpen()).isTrue();
      } finally {
        first.close();
        closer.join(30_000);
      }

      assertThat(closer.isAlive()).isFalse();
      assertThat(db.isOpen()).isFalse();
    }

    /**
     * A window leaves the registry as the FIRST step of its close and is fully released (shadow closed, retained files
     * deleted) only at the end of it. The close must count it until the end: this drives the two steps by hand, so the
     * gap between them is as wide as the test wants it.
     */
    @Test
    void closeWaitsForAReleaseStillInProgress() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();

      final PageSnapshot snapshot = pageManager.openSnapshot(db);
      // THE FIRST STEP OF PageSnapshot.close(): OUT OF THE REGISTRY, NOT RELEASED YET
      pageManager.unregisterSnapshot(snapshot);
      assertThat(pageManager.isSnapshotWindowOpen(db)).isFalse();

      final Thread closer = new Thread(db::close, "closer");
      try {
        closer.start();
        closer.join(1_000);
        assertThat(closer.isAlive()).as("the close must wait for the release, not only for the unregistration").isTrue();
        assertThat(db.isOpen()).isTrue();
      } finally {
        // THE REST OF PageSnapshot.close(): NOTHING LEFT TO UNREGISTER, THE SHADOW AND THE RETAINED FILES ARE RELEASED
        // AND ONLY THEN IS THE WAITING CLOSE TOLD
        snapshot.close();
        closer.join(30_000);
      }

      assertThat(closer.isAlive()).isFalse();
      assertThat(db.isOpen()).isFalse();
    }

    /** An interrupt does not cut the wait short: the close still waits for the window, and the interrupt survives it. */
    @Test
    void interruptedCloseStillWaitsForTheWindowAndKeepsTheInterrupt() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();

      final PageSnapshot snapshot = pageManager.openSnapshot(db);
      final AtomicReference<Boolean> interruptedAfterClose = new AtomicReference<>();
      final Thread closer = new Thread(() -> {
        db.close();
        interruptedAfterClose.set(Thread.currentThread().isInterrupted());
      }, "closer");
      try {
        closer.start();
        closer.join(500);
        assertThat(closer.isAlive()).isTrue();

        closer.interrupt();
        closer.join(1_000);
        assertThat(closer.isAlive()).as("the interrupt must not let the close shut the files under the window").isTrue();
        assertThat(db.isOpen()).isTrue();
        snapshot.checkValid();
      } finally {
        snapshot.close();
        closer.join(30_000);
      }

      assertThat(closer.isAlive()).isFalse();
      assertThat(db.isOpen()).isFalse();
      assertThat(interruptedAfterClose.get()).as("the interrupt is restored for the caller").isTrue();
    }

    @Test
    void windowOnAClosedDatabaseIsRefused() {
      final DatabaseInternal db = (DatabaseInternal) database;
      db.close();

      // THE ACCESSOR ON THE DATABASE REFUSES A CLOSED ONE; THE PAGE MANAGER IS JVM-WIDE AND ANSWERS FOR IT
      assertThatThrownBy(() -> PageManager.INSTANCE.openSnapshot(db)).isInstanceOf(PageSnapshotException.class)
          .satisfies(e -> assertThat(((PageSnapshotException) e).getReason()).isEqualTo(PageSnapshotException.Reason.CLOSING));
      assertThat(PageManager.INSTANCE.isSnapshotWindowOpen(db)).isFalse();
    }

    private void assertCloseWaitsForTheWindow(final boolean drop) throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();

      final PageSnapshot snapshot = pageManager.openSnapshot(db);
      assertThat(snapshot.getFiles()).isNotEmpty();

      final CountDownLatch started = new CountDownLatch(1);
      final AtomicReference<Throwable> closerFailure = new AtomicReference<>();
      final Thread closer = new Thread(() -> {
        started.countDown();
        try {
          if (drop)
            db.drop();
          else
            db.close();
        } catch (final Throwable e) {
          closerFailure.set(e);
        }
      }, drop ? "dropper" : "closer");

      try {
        closer.start();
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        // GIVE THE CLOSE EVERY CHANCE TO GO AHEAD: IT MUST NOT
        closer.join(1_000);
        assertThat(closer.isAlive()).as("the " + closer.getName() + " must wait for the open window").isTrue();
        assertThat(db.isOpen()).as("the database must stay open while the window is").isTrue();
        assertThat(pageManager.isSnapshotWindowOpen(db)).isTrue();

        // THE WINDOW STILL SERVES ITS t0 IMAGE WHILE THE CLOSE IS WAITING - THIS IS THE BACKUP FINISHING ITS READ
        long bytes = 0;
        for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
          try (final InputStream in = snapshot.newInputStream(file.fileId())) {
            final byte[] buffer = new byte[8192];
            for (int read; (read = in.read(buffer)) > 0; )
              bytes += read;
          }
        assertThat(bytes).isPositive();
        snapshot.checkValid();
        assertThat(closer.isAlive()).isTrue();
      } finally {
        snapshot.close();
        closer.join(30_000);
      }

      assertThat(closer.isAlive()).as("the " + closer.getName() + " must complete once the window is released").isFalse();
      assertThat(closerFailure.get()).isNull();
      assertThat(db.isOpen()).isFalse();
      assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.CLOSED);
    }
  }

  @Nested
  class SnapshotSurvivesInterruptClosedChannel extends TestHelper {

    private static final String TYPE = "Doc";

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("id", Integer.class);
      type.createProperty("payload", String.class);

      database.transaction(() -> {
        for (int i = 0; i < 1_000; i++)
          database.newDocument(TYPE).set("id", i).set("payload", "initial").save();
      });
    }

    @Test
    void preImageCaptureReopensAChannelAnInterruptClosedInsteadOfInvalidatingTheWindow() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();

      try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
        final Map<Integer, Long> t0Checksums = checksums(snapshot);

        // THE FLUSH IS HELD BACK WHILE THE REWRITE COMMITS, SO THE CHANNELS CAN BE CLOSED AFTER THE COMMIT READ ITS PAGES
        // (A READ REOPENS THE CHANNEL ON ITS OWN) AND BEFORE THE FLUSH CAPTURES THE PRE-IMAGES OF THE PAGES IT OVERWRITES
        pageManager.suspendFlushAndExecute(db, () -> {
          database.transaction(() -> database.iterateType(TYPE, false).forEachRemaining(record -> {
            final MutableDocument doc = record.asDocument().modify();
            // THE SAME LENGTH AS "initial": EVERY RECORD IS REWRITTEN IN PLACE, SO EVERY PAGE THE FLUSH WRITES EXISTED AT t0
            // AND NEEDS A PRE-IMAGE. A GROWING RECORD WOULD ALSO WRITE A NEW PAGE, AND IF THAT ONE WERE FLUSHED FIRST ITS
            // OWN WRITE WOULD REOPEN THE CHANNEL BEFORE ANY CAPTURE RAN
            doc.set("payload", "INITIAL");
            doc.save();
          }));
          // ANOTHER THREAD'S INTERRUPT CLOSED EVERY CHANNEL: THE FLUSH CAPTURES ITS PRE-IMAGES THROUGH A CLOSED CHANNEL
          closeEveryChannel(snapshot);
        });
        pageManager.waitAllPagesOfDatabaseAreFlushed(db);

        assertThat(snapshot.getShadowedPages()).as("the rewrite must have captured pre-images, or this test proves nothing")
            .isPositive();
        assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
        assertThat(checksums(snapshot)).isEqualTo(t0Checksums);
      }
    }

    @Test
    void snapshotStreamReopensAChannelAnInterruptClosedInsteadOfFailingTheTransfer() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;

      try (final PageSnapshot snapshot = db.getPageManager().openSnapshot(db)) {
        final Map<Integer, Long> t0Checksums = checksums(snapshot);

        closeEveryChannel(snapshot);

        // EVERY FILE IS READ BACK THROUGH THE SNAPSHOT'S INPUT STREAM, EACH ONE STARTING ON A CLOSED CHANNEL
        assertThat(checksums(snapshot)).isEqualTo(t0Checksums);
        assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      }
    }

    private static Map<Integer, Long> checksums(final PageSnapshot snapshot) throws IOException {
      final Map<Integer, Long> result = new HashMap<>();
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
        result.put(file.fileId(), snapshot.calculateChecksum(file.fileId()));
      return result;
    }

    /** What a ClosedByInterruptException does to the file: the channel is closed, while the file itself stays open. */
    private static void closeEveryChannel(final PageSnapshot snapshot) throws ReflectiveOperationException, IOException {
      final Field field = PaginatedComponentFile.class.getDeclaredField("channel");
      field.setAccessible(true);
      for (final PageSnapshot.SnapshotFile file : snapshot.getFiles())
        ((FileChannel) field.get(file.file())).close();
    }
  }

  @Nested
  class SnapshotWindowLeakDiagnostics extends TestHelper {
    private static final String TYPE = "Doc";

    @Override
    protected void beginTest() {
      final DocumentType type = database.getSchema().createDocumentType(TYPE);
      type.createProperty("id", Integer.class);
      database.transaction(() -> {
        for (int i = 0; i < 500; i++)
          database.newDocument(TYPE).set("id", i).save();
      });
    }

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      return false;
    }

    @Test
    void aBarrierThatThrowsAfterPublishingTheWindowReleasesIt() {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();
      final PageManagerFlushThread flushThread = pageManager.getFlushThread();

      // THE LAST STEP OF THE BARRIER, AFTER THE WINDOW IS REGISTERED, IS RESUMING THE FLUSH IT SUSPENDED. A RESUME
      // THAT FAILS THERE USED TO PROPAGATE OUT OF openSnapshot WITH THE WINDOW STILL REGISTERED AND NO HANDLE RETURNED
      flushThread.testHookAfterDeferredBacklogSnapshot = () -> {
        throw new IllegalStateException("simulated failure resuming the flush after the barrier");
      };
      try {
        assertThatThrownBy(() -> pageManager.openSnapshot(db)).isInstanceOf(PageSnapshotException.class);
      } finally {
        flushThread.testHookAfterDeferredBacklogSnapshot = null;
      }

      assertThat(pageManager.countSnapshotWindows(db))
          .as("the window the failed barrier published must have been released, or every later close of the database "
              + "waits for it forever").isZero();
      assertThat(pageManager.isSnapshotWindowOpen(db)).isFalse();
      assertThat(pageManager.isPageFlushingSuspended(db)).isFalse();

      // THE PAIRED ASSERTION: THE SAME CALL SUCCEEDS ONCE THE FAULT IS GONE, AND THE DATABASE STILL CLOSES
      try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
        assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      }
    }

    @Test
    void aBarrierThatFailsWithAnErrorAfterPublishingTheWindowReleasesItAndRethrowsTheError() {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();
      final PageManagerFlushThread flushThread = pageManager.getFlushThread();

      // AN Error IS NOT A FAILED BARRIER TO FALL BACK FROM: openSnapshot WRAPS ONLY Exception INTO PageSnapshotException,
      // SO THE Error ITSELF REACHES THE CALLER - BUT THE WINDOW MUST STILL BE RELEASED ON THE WAY OUT
      final AssertionError failure = new AssertionError("simulated error resuming the flush after the barrier");
      flushThread.testHookAfterDeferredBacklogSnapshot = () -> {
        throw failure;
      };
      try {
        assertThatThrownBy(() -> pageManager.openSnapshot(db)).isSameAs(failure);
      } finally {
        flushThread.testHookAfterDeferredBacklogSnapshot = null;
      }

      assertThat(pageManager.countSnapshotWindows(db)).isZero();
      assertThat(pageManager.isSnapshotWindowOpen(db)).isFalse();
      assertThat(pageManager.isPageFlushingSuspended(db)).isFalse();

      try (final PageSnapshot snapshot = pageManager.openSnapshot(db)) {
        assertThat(snapshot.getStatus()).isEqualTo(PageSnapshot.STATUS.ACTIVE);
      }
    }

    @Test
    void aCloseWaitingPastTheThresholdNamesTheWindowAndTheStackThatOpenedIt() throws Exception {
      GlobalConfiguration.PAGE_SNAPSHOT_TRACK_OPENERS.setValue(true);
      final String report;
      try {
        report = closeAndCaptureTheReport();
      } finally {
        GlobalConfiguration.PAGE_SNAPSHOT_TRACK_OPENERS.reset();
      }

      assertThat(report).contains(database.getName());
      assertThat(report).contains("thread '" + Thread.currentThread().getName() + "'");
      assertThat(report).contains("ACTIVE");
      // THE STACK IS WHAT TELLS WHICH CODE PATH LEAKED THE WINDOW: IT HAS TO NAME THE METHOD THAT OPENED IT
      assertThat(report).contains("openTheWindowFromHere");
      assertThat(report).endsWith(System.lineSeparator());
    }

    @Test
    void withoutTrackingTheReportStillNamesTheWindowAndSaysHowToGetTheStack() throws Exception {
      GlobalConfiguration.PAGE_SNAPSHOT_TRACK_OPENERS.setValue(false);
      final String report;
      try {
        report = closeAndCaptureTheReport();
      } finally {
        GlobalConfiguration.PAGE_SNAPSHOT_TRACK_OPENERS.reset();
      }

      assertThat(report).contains("thread '" + Thread.currentThread().getName() + "'");
      assertThat(report).doesNotContain("openTheWindowFromHere");
      assertThat(report).contains(GlobalConfiguration.PAGE_SNAPSHOT_TRACK_OPENERS.getKey());
      assertThat(report).endsWith(System.lineSeparator());
    }

    @Test
    void theWaitingLineEndsWithALineSeparator() throws Exception {
      final List<String> messages = new CopyOnWriteArrayList<>();
      final String waiting = runACloseBehindAWindow(messages, Long.MAX_VALUE, "is waiting for");
      assertThat(waiting).endsWith(System.lineSeparator());
    }

    // ------------------------------------------------------------------------------------------------------ HELPERS

    private String closeAndCaptureTheReport() throws Exception {
      return runACloseBehindAWindow(new CopyOnWriteArrayList<>(), 0L, "still open");
    }

    /**
     * Opens a window, starts a close of the database behind it with the report threshold set to
     * {@code reportAfterMillis}, waits for a logged line containing {@code needle}, then releases the window and lets the
     * close finish.
     */
    private String runACloseBehindAWindow(final List<String> messages, final long reportAfterMillis, final String needle)
        throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final PageManager pageManager = db.getPageManager();
      final long previousThreshold = pageManager.closeWaitReportMillis;
      final Logger previousLogger = LogManager.instance().getLogger();

      LogManager.instance().setLogger(new CapturingLogger(previousLogger, messages));
      pageManager.closeWaitReportMillis = reportAfterMillis;

      final PageSnapshot snapshot = openTheWindowFromHere(pageManager, db);
      final AtomicReference<Throwable> closerFailure = new AtomicReference<>();
      final Thread closer = new Thread(() -> {
        try {
          db.close();
        } catch (final Throwable e) {
          closerFailure.set(e);
        }
      }, "issue10657-closer");

      String found = null;
      try {
        closer.start();
        for (int i = 0; i < 1_000 && found == null; i++) {
          for (final String message : messages)
            if (message.contains(needle)) {
              found = message;
              break;
            }
          if (found == null)
            Thread.sleep(10);
        }
      } finally {
        snapshot.close();
        closer.join(30_000);
        pageManager.closeWaitReportMillis = previousThreshold;
        LogManager.instance().setLogger(previousLogger);
      }

      assertThat(closer.isAlive()).as("the close must complete once the window is released").isFalse();
      assertThat(closerFailure.get()).isNull();
      assertThat(found).as("the waiting close must have logged a line containing '%s', logged: %s", needle, messages)
          .isNotNull();
      return found;
    }

    private static PageSnapshot openTheWindowFromHere(final PageManager pageManager, final DatabaseInternal db) {
      return pageManager.openSnapshot(db);
    }

    /** Records every PageManager message as the formatted text it would publish, and forwards everything. */
    private static final class CapturingLogger implements Logger {
      private final Logger       delegate;
      private final List<String> messages;

      private CapturingLogger(final Logger delegate, final List<String> messages) {
        this.delegate = delegate;
        this.messages = messages;
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
          final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
          final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
          final Object arg17) {
        record(requester, message,
            new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15,
                arg16, arg17 });
        delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
            arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        record(requester, message, args);
        delegate.log(requester, level, message, exception, context, args);
      }

      @Override
      public void flush() {
        delegate.flush();
      }

      private void record(final Object requester, final String message, final Object[] args) {
        if (!(requester instanceof PageManager) || message == null)
          return;
        messages.add(args != null && args.length > 0 ? message.formatted(args) : message);
      }
    }
  }

  @Nested
  class PageShadowSpillReopen {
    private static final int PAGE_SIZE = 4_096;

    @TempDir
    File tempDir;

    @Test
    void aSpillWriteReopensAChannelAnInterruptClosed() throws Exception {
      final File spillFile = new File(tempDir, "shadow.spill");
      // NO RAM BUDGET: EVERY PRE-IMAGE SPILLS
      try (final PageShadow shadow = new PageShadow(spillFile, 0, 0)) {
        assertThat(shadow.store(PageShadow.key(1, 0), page(1), PAGE_SIZE)).isTrue();

        closeSpillChannel(shadow);

        // A PAGE OVERSIZED ON PURPOSE: THE RETRY MUST REWRITE EXACTLY length BYTES, NOT THE WHOLE ARRAY
        final byte[] oversized = Arrays.copyOf(page(2), PAGE_SIZE * 2);
        Arrays.fill(oversized, PAGE_SIZE, oversized.length, (byte) 0x7F);
        assertThat(shadow.store(PageShadow.key(1, 1), oversized, PAGE_SIZE)).isTrue();
        assertThat(shadow.store(PageShadow.key(1, 2), page(3), PAGE_SIZE)).isTrue();

        assertThat(shadow.getSpilledBytes()).isEqualTo(3L * PAGE_SIZE);
        assertThat(spillFile.length()).isEqualTo(3L * PAGE_SIZE);
        assertPreImage(shadow, 0, 1);
        assertPreImage(shadow, 1, 2);
        assertPreImage(shadow, 2, 3);
      }
      assertThat(spillFile).doesNotExist();
    }

    @Test
    void aSpilledReadReopensAChannelAnInterruptClosed() throws Exception {
      final File spillFile = new File(tempDir, "shadow.spill");
      try (final PageShadow shadow = new PageShadow(spillFile, 0, 0)) {
        for (int i = 0; i < 4; i++)
          assertThat(shadow.store(PageShadow.key(7, i), page(i + 10), PAGE_SIZE)).isTrue();

        closeSpillChannel(shadow);

        // READ INTO THE MIDDLE OF A LARGER BUFFER, AS PageSnapshot DOES FOR A RUN OF PAGES: THE RETRY MUST RESTORE BOTH
        // THE OFFSET AND THE LENGTH OF THE DESTINATION RANGE
        final byte[] run = new byte[PAGE_SIZE * 4];
        for (int i = 0; i < 4; i++)
          assertThat(shadow.read(PageShadow.key(7, i), run, i * PAGE_SIZE, PAGE_SIZE)).isTrue();
        for (int i = 0; i < 4; i++)
          assertThat(Arrays.copyOfRange(run, i * PAGE_SIZE, (i + 1) * PAGE_SIZE)).isEqualTo(page(i + 10));
      }
    }

    /**
     * Many threads that all find the same channel closed must share ONE reopen (no leaked descriptors, no channel replaced
     * under a thread that is already using the reopened one), and every one of them must still get its bytes.
     */
    @Test
    void concurrentReadersAndWritersOfAClosedChannelShareOneReopen() throws Exception {
      final File spillFile = new File(tempDir, "shadow.spill");
      try (final PageShadow shadow = new PageShadow(spillFile, 0, 0)) {
        final int pages = 8;
        for (int i = 0; i < pages; i++)
          assertThat(shadow.store(PageShadow.key(3, i), page(i + 20), PAGE_SIZE)).isTrue();

        closeSpillChannel(shadow);

        final int threads = 16;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread[] workers = new Thread[threads];
        for (int t = 0; t < threads; t++) {
          final int id = t;
          workers[t] = new Thread(() -> {
            try {
              start.await();
              if (id % 2 == 0) {
                final byte[] dst = new byte[PAGE_SIZE];
                assertThat(shadow.read(PageShadow.key(3, id % pages), dst, 0, PAGE_SIZE)).isTrue();
                assertThat(dst).isEqualTo(page(id % pages + 20));
              } else
                assertThat(shadow.store(PageShadow.key(4, id), page(id + 40), PAGE_SIZE)).isTrue();
            } catch (final Throwable e) {
              failure.compareAndSet(null, e);
            }
          });
          workers[t].start();
        }
        start.countDown();
        for (final Thread worker : workers)
          worker.join();

        assertThat(failure.get()).isNull();
        assertThat(reopens(shadow)).isEqualTo(1);
        for (int t = 1; t < threads; t += 2) {
          final byte[] dst = new byte[PAGE_SIZE];
          assertThat(shadow.read(PageShadow.key(4, t), dst, 0, PAGE_SIZE)).isTrue();
          assertThat(dst).isEqualTo(page(t + 40));
        }
      }
    }

    @Test
    void anInterruptedThreadCanStillSpillAndReadAndKeepsItsInterruptFlag() throws Exception {
      final File spillFile = new File(tempDir, "shadow.spill");
      try (final PageShadow shadow = new PageShadow(spillFile, 0, 0)) {
        assertThat(shadow.store(PageShadow.key(1, 0), page(1), PAGE_SIZE)).isTrue();

        // A REAL INTERRUPT: WITH THE CHANNEL PROTECTED IT IS IGNORED, WITHOUT THE PROTECTION (NO --add-opens) IT CLOSES THE
        // CHANNEL AND THE REOPEN TAKES OVER. EITHER WAY THE CAPTURE AND THE READ SUCCEED, AND THE FLAG SURVIVES FOR THE
        // CALLER
        Thread.currentThread().interrupt();
        try {
          assertThat(shadow.store(PageShadow.key(1, 1), page(2), PAGE_SIZE)).isTrue();
          final byte[] dst = new byte[PAGE_SIZE];
          assertThat(shadow.read(PageShadow.key(1, 1), dst, 0, PAGE_SIZE)).isTrue();
          assertThat(dst).isEqualTo(page(2));
        } finally {
          assertThat(Thread.interrupted()).as("the interrupt must be restored for the caller").isTrue();
        }

        // AND THE CHANNEL IS USABLE BY EVERY OTHER THREAD AFTERWARDS
        final Thread other = new Thread(() -> {
          try {
            assertPreImage(shadow, 0, 1);
            assertPreImage(shadow, 1, 2);
            assertThat(shadow.store(PageShadow.key(1, 2), page(3), PAGE_SIZE)).isTrue();
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        });
        final Throwable[] failure = new Throwable[1];
        other.setUncaughtExceptionHandler((t, e) -> failure[0] = e);
        other.start();
        other.join();
        assertThat(failure[0]).isNull();
        assertPreImage(shadow, 2, 3);
      }
    }

    @Test
    void aClosedShadowNeverReopensNorRecreatesTheSpillFile() throws Exception {
      final File spillFile = new File(tempDir, "shadow.spill");
      final PageShadow shadow = new PageShadow(spillFile, 0, 0);
      assertThat(shadow.store(PageShadow.key(1, 0), page(1), PAGE_SIZE)).isTrue();
      shadow.close();
      assertThat(spillFile).doesNotExist();

      assertThat(shadow.store(PageShadow.key(1, 1), page(2), PAGE_SIZE)).isFalse();
      assertThat(shadow.read(PageShadow.key(1, 0), new byte[PAGE_SIZE], 0, PAGE_SIZE)).isFalse();
      assertThat(spillFile).doesNotExist();
    }

    private static void assertPreImage(final PageShadow shadow, final int pageNumber, final int seed) throws Exception {
      final byte[] dst = new byte[PAGE_SIZE];
      assertThat(shadow.read(PageShadow.key(1, pageNumber), dst, 0, PAGE_SIZE)).isTrue();
      assertThat(dst).isEqualTo(page(seed));
    }

    private static byte[] page(final int seed) {
      final byte[] content = new byte[PAGE_SIZE];
      for (int i = 0; i < PAGE_SIZE; i++)
        content[i] = (byte) (seed * 31 + i);
      return content;
    }

    private static int reopens(final PageShadow shadow) throws Exception {
      final Field field = PageShadow.class.getDeclaredField("reopens");
      field.setAccessible(true);
      synchronized (shadow) {
        return field.getInt(shadow);
      }
    }

    /** What a ClosedByInterruptException does to the spill file: the channel is closed while the shadow stays open. */
    private static void closeSpillChannel(final PageShadow shadow) throws Exception {
      final Field field = PageShadow.class.getDeclaredField("spillChannel");
      field.setAccessible(true);
      ((FileChannel) field.get(shadow)).close();
    }
  }

  @Nested
  class SchemaReloadKeepsPageReservations extends TestHelper {

    private static final String TYPE = "Issue9835Item";

    @Override
    protected void beginTest() {
      database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
    }

    /**
     * The issue's repro: the first record of an empty bucket makes its transaction create page 0, which stays in flight
     * across the reload. The reload used to re-seed the counter to 0 and forget the page's owner, so the next insert
     * created page 0 too and got the same RID.
     */
    @Test
    void aReloadDoesNotReissueAPageCreatedByARunningTransaction() throws Exception {
      final LocalBucket before = bucket();

      final InFlightWriter writer = new InFlightWriter(1);
      writer.start();
      writer.awaitWritten();

      // Not vacuous: the writer really holds a brand-new page across the reload
      assertThat(before.getInFlightNewPagesForTesting()).isEqualTo(1);
      final int reservedBefore = before.reservedPageCounter.get();
      assertThat(reservedBefore).isGreaterThan(before.getCommittedPageCount());

      reloadSchema();

      final LocalBucket after = bucket();
      assertThat(after).as("the full load must have replaced the bucket instance").isNotSameAs(before);
      assertThat(after.reservedPageCounter.get()).as("reservation counter after the reload").isGreaterThanOrEqualTo(reservedBefore);
      assertThat(after.getInFlightNewPagesForTesting()).as("pages in flight after the reload").isEqualTo(1);
      // SHARED, not copied: a transaction still allocating through the retired instance draws from the same sequence
      assertThat(after.reservedPageCounter).as("the reservation counter").isSameAs(before.reservedPageCounter);

      final List<RID> laterRids = insertAndCommit(3);

      writer.commitAndJoin();

      final int maxRecordsInPage = after.getMaxRecordsInPage();
      final long writerPage = writer.rids.getFirst().getPosition() / maxRecordsInPage;
      for (final RID rid : laterRids) {
        assertThat(rid).as("a later transaction was given the writer's RID").isNotIn(writer.rids);
        assertThat(rid.getPosition() / maxRecordsInPage).as("record %s must not be on the writer's page", rid).isNotEqualTo(writerPage);
      }

      // The writer's own release reaches the instance that is live now, so its page is not held forever
      assertThat(after.getInFlightNewPagesForTesting()).isZero();

      assertAllReadable(writer.rids, "writer");
      assertAllReadable(laterRids, "later");
      database.transaction(() -> assertThat(database.countType(TYPE, false)).isEqualTo(1 + laterRids.size()));
    }

    /**
     * The same failure one level down: a transaction inserting into an EXISTING page reserves its slot (#5279), and the
     * reservation used to live only in the retired instance, so a later transaction claimed the same slot of the same
     * page after the reload.
     */
    @Test
    void aReloadDoesNotReissueASlotReservedByARunningTransaction() throws Exception {
      database.transaction(() -> database.newDocument(TYPE).set("owner", "seed").save());

      final LocalBucket before = bucket();
      final InFlightWriter writer = new InFlightWriter(1);
      writer.start();
      writer.awaitWritten();

      // Not vacuous: the writer went into the committed page 0, not into a page of its own
      assertThat(before.getInFlightNewPagesForTesting()).isZero();
      assertThat(writer.rids.getFirst().getPosition() / before.getMaxRecordsInPage()).isZero();

      reloadSchema();
      assertThat(bucket()).as("the full load must have replaced the bucket instance").isNotSameAs(before);

      final List<RID> laterRids = insertAndCommit(1);

      writer.commitAndJoin();

      for (final RID rid : laterRids)
        assertThat(rid).as("a later transaction was given the writer's RID").isNotIn(writer.rids);

      assertAllReadable(writer.rids, "writer");
      assertAllReadable(laterRids, "later");
      database.transaction(() -> assertThat(database.countType(TYPE, false)).isEqualTo(2 + laterRids.size()));
    }

    /** A second reload while the same transaction is still running must not lose what the first one carried over. */
    @Test
    void twoReloadsInARowStillKeepTheReservation() throws Exception {
      final InFlightWriter writer = new InFlightWriter(1);
      writer.start();
      writer.awaitWritten();

      reloadSchema();
      reloadSchema();

      assertThat(bucket().getInFlightNewPagesForTesting()).isEqualTo(1);
      final List<RID> laterRids = insertAndCommit(2);
      writer.commitAndJoin();

      for (final RID rid : laterRids)
        assertThat(rid).isNotIn(writer.rids);
      assertThat(bucket().getInFlightNewPagesForTesting()).isZero();
      database.transaction(() -> assertThat(database.countType(TYPE, false)).isEqualTo(1 + laterRids.size()));
    }

    private LocalBucket bucket() {
      return (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
    }

    private void reloadSchema() throws Exception {
      ((LocalSchema) database.getSchema().getEmbedded()).load(ComponentFile.MODE.READ_WRITE, true);
    }

    private List<RID> insertAndCommit(final int count) {
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          rids.add(database.newDocument(TYPE).set("owner", "later").set("seq", i).save().getIdentity());
      });
      return rids;
    }

    private void assertAllReadable(final List<RID> rids, final String owner) {
      database.transaction(() -> {
        for (final RID rid : rids)
          assertThat(database.lookupByRID(rid, true).asDocument().getString("owner")).as("owner of %s", rid).isEqualTo(owner);
      });
    }

    /** A transaction that inserts, then stays open until told to commit. */
    private final class InFlightWriter extends Thread {
      private final int                        records;
      private final CountDownLatch             written   = new CountDownLatch(1);
      private final CountDownLatch             mayCommit = new CountDownLatch(1);
      private final AtomicReference<Throwable> failure   = new AtomicReference<>();
      final         List<RID>                  rids      = new ArrayList<>();

      InFlightWriter(final int records) {
        this.records = records;
        setName("issue9835-writer");
      }

      @Override
      public void run() {
        try {
          database.begin();
          for (int i = 0; i < records; i++) {
            final MutableDocument doc = database.newDocument(TYPE).set("owner", "writer").set("seq", i);
            doc.save();
            rids.add(doc.getIdentity());
          }
          written.countDown();
          assertThat(mayCommit.await(30, TimeUnit.SECONDS)).isTrue();
          database.commit();
        } catch (final Throwable e) {
          failure.set(e);
        } finally {
          written.countDown();
          if (database.isTransactionActive())
            database.rollback();
        }
      }

      void awaitWritten() throws InterruptedException {
        assertThat(written.await(30, TimeUnit.SECONDS)).as("the writer did not write").isTrue();
        assertThat(failure.get()).as("the writer failed before the reload").isNull();
      }

      void commitAndJoin() throws InterruptedException {
        mayCommit.countDown();
        join(60_000);
        assertThat(isAlive()).as("the writer hung").isFalse();
        assertThat(failure.get()).as("the writer transaction, which held its reservation across the reload").isNull();
      }
    }
  }

  @Nested
  class InFlightNewPageAllocation extends TestHelper {

    private static final String TYPE = "Issue9694Item";

    @Override
    protected void beginTest() {
      database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
    }

    @Test
    void aConcurrentTransactionDoesNotAllocateOnAPageAnotherTransactionCreated() throws Exception {
      final CountDownLatch creatorWrote = new CountDownLatch(1);
      final CountDownLatch otherCommitted = new CountDownLatch(1);
      final AtomicReference<Throwable> creatorFailure = new AtomicReference<>();
      final AtomicReference<Throwable> otherFailure = new AtomicReference<>();
      final AtomicReference<RID> creatorRid = new AtomicReference<>();
      final List<RID> otherRids = new CopyOnWriteArrayList<>();

      // The creator writes the bucket's first page and keeps its transaction open.
      final Thread creator = new Thread(() -> {
        try {
          database.begin();
          final MutableDocument doc = database.newDocument(TYPE).set("owner", "creator");
          doc.save();
          creatorRid.set(doc.getIdentity());
          creatorWrote.countDown();
          assertThat(otherCommitted.await(30, TimeUnit.SECONDS)).isTrue();
          database.commit();
        } catch (final Throwable e) {
          creatorFailure.set(e);
          creatorWrote.countDown();
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      });

      // The other transaction cannot see that page, so its first record opens a later one. Its second record used to be
      // allocated from the free-space map, where the creator's page is listed first and lies below its horizon.
      final Thread other = new Thread(() -> {
        try {
          assertThat(creatorWrote.await(30, TimeUnit.SECONDS)).isTrue();
          database.begin();
          for (int i = 0; i < 2; i++) {
            final MutableDocument doc = database.newDocument(TYPE).set("owner", "other").set("seq", i);
            doc.save();
            otherRids.add(doc.getIdentity());
          }
          database.commit();
        } catch (final Throwable e) {
          otherFailure.set(e);
        } finally {
          if (database.isTransactionActive())
            database.rollback();
          otherCommitted.countDown();
        }
      });

      creator.start();
      other.start();
      creator.join(60_000);
      other.join(60_000);
      assertThat(creator.isAlive()).as("the creator thread hung").isFalse();
      assertThat(other.isAlive()).as("the other thread hung").isFalse();

      assertThat(otherFailure.get()).as("the other transaction").isNull();
      assertThat(creatorFailure.get()).as("the creator transaction").isNull();

      final int maxRecordsInPage = ((LocalBucket) database.getSchema().getBucketById(creatorRid.get().getBucketId())).getMaxRecordsInPage();
      final long creatorPage = creatorRid.get().getPosition() / maxRecordsInPage;
      for (final RID rid : otherRids)
        assertThat(rid.getPosition() / maxRecordsInPage).as("record %s must not be on the creator's page", rid).isNotEqualTo(creatorPage);

      database.transaction(() -> {
        assertThat(scanCount()).isEqualTo(3);
        assertThat(database.lookupByRID(creatorRid.get(), true).asDocument().getString("owner")).isEqualTo("creator");
        for (final RID rid : otherRids)
          assertThat(database.lookupByRID(rid, true).asDocument().getString("owner")).isEqualTo("other");
      });
    }

    /**
     * A page stays its creator's only while the creator runs: a commit, a rollback and a commit that fails with a
     * conflict all give it back, and a page abandoned by a rollback is an empty page the next insert can fill.
     */
    @Test
    void aCreatedPageIsGivenBackWheneverItsTransactionEnds() throws Exception {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
      database.getSchema().createDocumentType("Issue9694Other");
      database.begin();
      final RID shared = database.newDocument("Issue9694Other").set("v", 0).save().getIdentity();
      database.commit();

      // ROLLBACK: the page the transaction created is never committed
      database.begin();
      database.newDocument(TYPE).set("owner", "rolledBack").save();
      assertThat(bucket.getInFlightNewPagesForTesting()).isEqualTo(1);
      database.rollback();
      assertThat(bucket.getInFlightNewPagesForTesting()).isZero();

      // FAILED COMMIT: a new page plus an update another transaction commits first
      database.begin();
      database.newDocument(TYPE).set("owner", "conflicted").save();
      database.lookupByRID(shared, true).asDocument().modify().set("v", 1).save();
      assertThat(bucket.getInFlightNewPagesForTesting()).isEqualTo(1);
      final Thread concurrent = new Thread(() -> database.transaction(
          () -> database.lookupByRID(shared, true).asDocument().modify().set("v", 2).save()));
      concurrent.start();
      concurrent.join(30_000);
      assertThat(concurrent.isAlive()).as("the concurrent updater hung").isFalse();
      assertThatThrownBy(database::commit).isInstanceOf(ConcurrentModificationException.class);
      assertThat(bucket.getInFlightNewPagesForTesting()).isZero();

      // COMMIT: the abandoned pages are free again, and a committed page is an ordinary one afterwards
      database.begin();
      final RID rid = database.newDocument(TYPE).set("owner", "committed").save().getIdentity();
      database.commit();
      assertThat(bucket.getInFlightNewPagesForTesting()).isZero();

      database.transaction(() -> {
        assertThat(scanCount()).isEqualTo(1);
        assertThat(database.lookupByRID(rid, true).asDocument().getString("owner")).isEqualTo("committed");
      });
    }

    /**
     * The second half of #9694: two transactions inserting into the same page reserve disjoint slots (#5279), and the
     * one that commits second has its inserts replayed on the committed page. When the first one held the LAST slot of
     * the record table, the committed page's record count reached {@code maxRecordsInPage}, and the replay measured the
     * page with the allocator's "can a new record go here?" walker, which gives up on such a page without measuring it.
     * The second transaction's insert - into a slot that is free, on a page with plenty of free bytes - was refused, and
     * the whole transaction retried. Small records fill the 2048 slots of a page long before its bytes, so with several
     * writers on one bucket this was the common case.
     */
    @Test
    void anInsertIsMergedIntoAPageWhoseRecordTableAnotherTransactionFilled() throws Exception {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
      final int maxRecordsInPage = bucket.getMaxRecordsInPage();
      final int prefilled = maxRecordsInPage - 8;

      database.transaction(() -> {
        for (int i = 0; i < prefilled; i++)
          database.newDocument(TYPE).set("seq", i).save();
      });

      final CountDownLatch firstWrote = new CountDownLatch(1);
      final CountDownLatch secondCommitted = new CountDownLatch(1);
      final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
      final AtomicReference<Throwable> secondFailure = new AtomicReference<>();
      final AtomicReference<RID> firstRid = new AtomicReference<>();
      final List<RID> secondRids = new CopyOnWriteArrayList<>();

      // Takes the first free slot of the page and commits last.
      final Thread first = new Thread(() -> {
        try {
          database.begin();
          final MutableDocument doc = database.newDocument(TYPE).set("owner", "first");
          doc.save();
          firstRid.set(doc.getIdentity());
          firstWrote.countDown();
          assertThat(secondCommitted.await(30, TimeUnit.SECONDS)).isTrue();
          database.commit();
        } catch (final Throwable e) {
          firstFailure.set(e);
          firstWrote.countDown();
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
      });

      // Takes every other free slot of the page, up to the last one, and commits first.
      final Thread second = new Thread(() -> {
        try {
          assertThat(firstWrote.await(30, TimeUnit.SECONDS)).isTrue();
          database.begin();
          for (int i = 0; i < 7; i++) {
            final MutableDocument doc = database.newDocument(TYPE).set("owner", "second").set("seq", i);
            doc.save();
            secondRids.add(doc.getIdentity());
          }
          database.commit();
        } catch (final Throwable e) {
          secondFailure.set(e);
        } finally {
          if (database.isTransactionActive())
            database.rollback();
          secondCommitted.countDown();
        }
      });

      first.start();
      second.start();
      first.join(60_000);
      second.join(60_000);
      assertThat(first.isAlive()).as("the first thread hung").isFalse();
      assertThat(second.isAlive()).as("the second thread hung").isFalse();

      assertThat(secondFailure.get()).as("the transaction that filled the record table").isNull();
      // Not vacuous: both transactions really shared page 0, and the second one took its last slot
      assertThat(firstRid.get().getPosition()).isEqualTo(prefilled);
      assertThat(secondRids).hasSize(7);
      assertThat(secondRids.getLast().getPosition()).isEqualTo(maxRecordsInPage - 1);

      assertThat(firstFailure.get()).as("the transaction merged into the full record table").isNull();
      database.transaction(() -> {
        assertThat(scanCount()).isEqualTo(maxRecordsInPage);
        assertThat(database.lookupByRID(firstRid.get(), true).asDocument().getString("owner")).isEqualTo("first");
        for (final RID rid : secondRids)
          assertThat(database.lookupByRID(rid, true).asDocument().getString("owner")).isEqualTo("second");
      });
    }

    /**
     * Found while measuring #9694: when the page the allocator offered had no room left once the slots running
     * transactions reserved were counted, the insert opened a brand-new page - while the free-space map kept offering the
     * same page to the next insert, so the transaction opened one page PER RECORD. The #7615 load (4 async workers on
     * one bucket) spread 9,742 small records over ~100 pages, where one worker needs 5. The rejected page is now dropped
     * from the map and the allocator asked again. The bound counts pages, not time: each worker may hold a partly filled
     * tail page of its own, which is the whole difference from a single worker.
     */
    @Test
    void concurrentInsertsDoNotOpenAPagePerRecord() {
      final int total = 9_742;
      database.async().setParallelLevel(4);
      database.async().setCommitEvery(1_000);
      final AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
      database.async().onError(asyncFailure::set);

      for (int i = 0; i < total; i++)
        database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = :seq", null, Map.of("seq", i));
      database.async().waitCompletion();

      assertThat(asyncFailure.get()).isNull();
      database.transaction(() -> assertThat(scanCount()).isEqualTo(total));

      final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
      // ~5 pages hold the records; one partly filled tail page per worker comes on top of that
      assertThat(bucket.getTotalPages()).as("pages used by %s small records", total).isLessThanOrEqualTo(12);
    }

    /**
     * The page an insert was turned away from is taken out of the free-space map, because the room its committed image
     * shows is promised to running transactions. When those give it back - here by rolling back - the page must be
     * listed again, not left unlisted until the next statistics gather. The release only queues it (transaction
     * teardown must not read pages); the next allocation measures it and lists the room it really has.
     */
    @Test
    void aPageTakenOutOfTheFreeSpaceMapIsListedAgainWhenItsReservationsAreReleased() {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument(TYPE).set("seq", i).save();
      });

      database.begin();
      final RID reserved = database.newDocument(TYPE).set("owner", "reserving").save().getIdentity();
      assertThat(reserved.getPosition() / bucket.getMaxRecordsInPage()).as("the insert reserved a slot on page 0").isZero();

      bucket.delistReservedPage(0);
      assertThat(bucket.getListedFreeSpacePagesForTesting()).doesNotContain(0);

      database.rollback();
      assertThat(bucket.getPagesToRelistForTesting()).as("page 0 queued once its reservations were released").containsExactly(0);

      database.begin();
      try {
        bucket.relistQueuedPagesForTesting();
      } finally {
        database.rollback();
      }
      assertThat(bucket.getPagesToRelistForTesting()).isEmpty();
      assertThat(bucket.getListedFreeSpacePagesForTesting()).as("page 0 after the next allocation measured it").contains(0);
      // Measured, not the whole page: ten small records are on it
      final int usableSpaceInPage = bucket.getPageSize() - BasePage.PAGE_HEADER_SIZE - bucket.contentHeaderSize;
      assertThat(bucket.getFreeSpaceHintForPage(0)).isPositive().isLessThan(usableSpaceInPage);
    }

    /** Counts by scanning the records: {@code countType} and {@code count(*)} read the bucket's cached counter. */
    private long scanCount() {
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + TYPE)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }
}
