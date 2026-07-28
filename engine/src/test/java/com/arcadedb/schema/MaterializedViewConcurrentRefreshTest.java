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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A refresh requested while another is already running used to be dropped, which left the view
 * reflecting a snapshot taken before the requesting transaction committed, with nothing scheduled to
 * ever correct it. Requests are now coalesced onto the running refresh instead.
 */
class MaterializedViewConcurrentRefreshTest extends TestHelper {

  @Test
  void refreshRequestedWhileRunningIsServicedByTheRunningRefresh() {
    final MaterializedViewImpl view = newView("PendingGuardView");

    assertThat(view.tryBeginRefresh()).as("the first caller owns the refresh").isTrue();

    // A second caller arrives while the first is still running.
    assertThat(view.markRefreshPendingIfRunning()).as("the request is handed to the running refresh").isTrue();

    // The owner must therefore make a further pass before releasing.
    assertThat(view.finishRefreshPassAndCheckPending()).as("a pending request forces another pass").isTrue();
    assertThat(view.finishRefreshPassAndCheckPending()).as("no request left, ownership released").isFalse();

    assertThat(view.tryBeginRefresh()).as("ownership was released").isTrue();
    view.endRefresh();
  }

  @Test
  void repeatedRequestsDuringOnePassCollapseIntoASingleFurtherPass() {
    final MaterializedViewImpl view = newView("CoalesceView");

    assertThat(view.tryBeginRefresh()).isTrue();
    for (int i = 0; i < 5; i++)
      assertThat(view.markRefreshPendingIfRunning()).isTrue();

    assertThat(view.finishRefreshPassAndCheckPending()).as("five requests collapse into one pass").isTrue();
    assertThat(view.finishRefreshPassAndCheckPending()).isFalse();
  }

  @Test
  void requestArrivingAfterTheRefreshFinishedIsRunByTheCaller() {
    final MaterializedViewImpl view = newView("NoOwnerView");

    // Nothing is running, so there is nobody to hand the request to and the caller must run it.
    assertThat(view.markRefreshPendingIfRunning()).isFalse();
  }

  /**
   * Drives the exact interleaving of the defect with no timing dependence: a refresh is in flight,
   * a commit asks for another one, and that request must still be there when the in-flight refresh
   * finishes its pass. Dropping it is what left the view stale forever.
   */
  @Test
  void refresherRegistersItsRequestWhenAnotherRefreshIsInFlight() {
    database.transaction(() -> database.getSchema().createDocumentType("InFlightSource"));
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("InFlightView")
        .withQuery("SELECT value FROM InFlightSource")
        .create());

    final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("InFlightView");

    // Stand in for a refresh that is already running.
    assertThat(view.tryBeginRefresh()).isTrue();
    try {
      MaterializedViewRefresher.fullRefresh(database, view);

      assertThat(view.finishRefreshPassAndCheckPending())
          .as("the in-flight refresh must be told to make another pass, not have the request dropped")
          .isTrue();
    } finally {
      view.endRefresh();
    }
  }

  /**
   * A pass that fails must not silently swallow a request registered while it ran: releasing
   * ownership with a plain write would clobber it, leaving the view stale with nobody aware.
   */
  @Test
  void aFailedPassReportsTheRequestItDiscardsInsteadOfClobberingIt() {
    final MaterializedViewImpl view = newView("FailedPassView");

    assertThat(view.tryBeginRefresh()).isTrue();
    assertThat(view.markRefreshPendingIfRunning()).isTrue();

    assertThat(view.releaseRefreshAfterFailure())
        .as("the discarded request must be reported, not silently dropped").isTrue();
    assertThat(view.tryBeginRefresh()).as("ownership must still be released").isTrue();
    view.endRefresh();

    // With no request outstanding there is nothing to report.
    assertThat(view.tryBeginRefresh()).isTrue();
    assertThat(view.releaseRefreshAfterFailure()).isFalse();
  }

  @Tag("slow")
  @Test
  void concurrentWritersLeaveTheViewReflectingEveryCommit() throws Exception {
    database.transaction(() -> database.getSchema().createDocumentType("ConcurrentSource"));

    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("ConcurrentSourceView")
        .withQuery("SELECT value FROM ConcurrentSource")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .create());

    final int threads = 4;
    final int perThread = 40;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    final AtomicReference<Exception> failure = new AtomicReference<>();

    for (int t = 0; t < threads; t++) {
      final int threadId = t;
      new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < perThread; i++) {
            final int value = threadId * perThread + i;
            database.transaction(() -> database.newDocument("ConcurrentSource").set("value", value).save());
          }
        } catch (final Exception e) {
          failure.compareAndSet(null, e);
        } finally {
          done.countDown();
        }
      }, "mv-refresh-writer-" + t).start();
    }

    start.countDown();
    assertThat(done.await(2, TimeUnit.MINUTES)).as("writers should finish").isTrue();
    if (failure.get() != null)
      throw failure.get();

    final int expected = threads * perThread;
    assertThat(countOf("ConcurrentSource")).isEqualTo(expected);

    // The last commit's refresh request is either run by its own caller or coalesced onto the
    // in-flight refresh; either way a pass starts after that commit, so the view converges.
    assertThat(awaitViewCount("ConcurrentSourceView", expected))
        .as("the view must eventually reflect every committed record, not a stale earlier snapshot")
        .isEqualTo(expected);
  }

  /**
   * Builds a view over a type that does not exist. That is fine for the state-machine tests: they
   * exercise the ownership transitions directly and never run an actual refresh.
   */
  private MaterializedViewImpl newView(final String name) {
    return new MaterializedViewImpl(database, name, "SELECT value FROM Foo", name, List.of("Foo"),
        MaterializedViewRefreshMode.MANUAL, true, 0);
  }

  private long countOf(final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM " + typeName)) {
      return rs.next().<Number>getProperty("cnt").longValue();
    }
  }

  private long awaitViewCount(final String viewName, final long expected) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 60_000;
    long count = countOf(viewName);
    while (count != expected && System.currentTimeMillis() < deadline) {
      Thread.sleep(100);
      count = countOf(viewName);
    }
    return count;
  }
}
