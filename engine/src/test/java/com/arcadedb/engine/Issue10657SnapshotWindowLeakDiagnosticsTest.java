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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10657: a drop waited 42 minutes in {@link PageManager#beginDatabaseClose} for a snapshot window nobody was
 * going to release, and nothing in the log or the thread dump said which window it was or who had opened it.
 * <ul>
 *   <li>A window published by the t0 barrier must not outlive a barrier that throws AFTER publishing it: the caller
 *   never receives the handle, so nobody else can ever close it.</li>
 *   <li>A close that has waited past the report threshold names every window it is still waiting for - the opening
 *   thread, its age, its status and, when {@code arcadedb.pageSnapshotTrackOpeners} is on, the stack that opened it -
 *   on a line that ends with a line separator, so a console that holds back the last partial line still shows it.</li>
 * </ul>
 */
class Issue10657SnapshotWindowLeakDiagnosticsTest extends TestHelper {
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
