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
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6132, the three gaps left by #6125 - none a correctness bug, all three places where that PR's own stated
 * invariant is not quite met.
 * <ol>
 * <li><b>The last unbounded filesystem I/O under the barrier's locks.</b> {@code buildSnapshot} ran one
 * {@code channel.size()} syscall per page file - dozens to hundreds on a real database, each with a lock acquisition
 * of its own - while holding the JVM-wide page-manager lock every committer of every database queues behind, and none
 * of them bounded by the barrier's deadline on a stalled filesystem. It is now a field read, and the test that keeps
 * it honest is the one below: the counter and the filesystem must agree, at t0, across appends, compaction and a
 * reopen.</li>
 * <li><b>A barrier that fails is invisible except in the log.</b> Every other snapshot counter is incremented from a
 * window that EXISTS, so a barrier that gave up before publishing one left no trace an operator could alert on -
 * while its consumer fell back to suspend-and-freeze and completed the backup by throttling every writer.</li>
 * <li><b>The automatic cap could fall below the RAM budget</b> and refuse a shadow that would never have touched the
 * disk at all, in exactly the case {@code pageSnapshotSpillPath} exists for.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6132SnapshotBarrierFollowupsTest extends TestHelper {

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
   * The wedged in-flight batch is fabricated exactly as {@code Issue6125SnapshotBarrierAndCapTest} does it. Which of
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
        assertThat(false).as("the fabricated never-completing flush must fail the barrier").isTrue();
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
