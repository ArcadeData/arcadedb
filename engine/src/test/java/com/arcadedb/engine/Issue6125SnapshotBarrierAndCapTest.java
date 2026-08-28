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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.StallAwareStopwatch;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6125, items 1 and 2: the shadow cap sizes itself from the thing it actually bounds, and the t0 barrier is
 * single-pass, exact and measured.
 * <p>
 * The two properties are tested together because they come from the same measurement: the benchmarks #6116 asked for
 * showed the shadow reaching 100% of the database size under a flat-out writer (so no flat cap can be right), and
 * the barrier costing tens of milliseconds because it RETRIED when a commit landed between the flush-queue drain and
 * the flush-thread suspension (so t0 could end up behind the last commit).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6125SnapshotBarrierAndCapTest extends TestHelper {

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
    // all (#6132), which is a different property, asserted in Issue6132SnapshotBarrierFollowupsTest
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
    final int fileId = db.getSchema().getType(TYPE).getBuckets(false).get(0).getFileId();
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

    final int fileId = db.getSchema().getType(TYPE).getBuckets(false).get(0).getFileId();
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
