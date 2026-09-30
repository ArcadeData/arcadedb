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
import com.arcadedb.engine.timeseries.TimeSeriesSealedStore;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7634: the HA verify holds the database read lock only while it opens its point-in-time window and lists the
 * TimeSeries sealed stores, not while it CRCs every byte of every file.
 * <p>
 * The verify's twin of #6114 (full backup) and #7456/#7671 (snapshot ship). On the window path the page bytes come from
 * the window, so the read lock only blocked the node's DDL for the whole verify. The fallback path, which reads the live
 * files under a flush suspension, keeps the lock, and one test here pins that.
 */
class Issue7634VerifyWindowHoldsNoReadLockTest {
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

  /** Blocks until {@code thread} is waiting inside {@code className.methodName}, or fails. */
  private static void awaitParkedIn(final Thread thread, final String className, final String methodName) {
    final long deadline = System.currentTimeMillis() + WAIT_MS;
    while (System.currentTimeMillis() < deadline) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
        for (final StackTraceElement frame : thread.getStackTrace())
          if (frame.getClassName().endsWith(className) && methodName.equals(frame.getMethodName()))
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
    throw new AssertionError("thread '" + thread.getName() + "' never parked in " + className + "." + methodName
        + " (state=" + thread.getState() + "); the block assertion that follows would have been vacuous");
  }

  /** Blocks until {@code thread} is parked acquiring a write lock, or fails. */
  private static void awaitParkedOnTheWriteLock(final Thread thread) {
    final long deadline = System.currentTimeMillis() + WAIT_MS;
    while (System.currentTimeMillis() < deadline) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
        for (final StackTraceElement frame : thread.getStackTrace())
          if (frame.getClassName().endsWith("ReentrantReadWriteLock$WriteLock"))
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
    throw new AssertionError("thread '" + thread.getName() + "' never parked on a write lock (state=" + thread.getState()
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
