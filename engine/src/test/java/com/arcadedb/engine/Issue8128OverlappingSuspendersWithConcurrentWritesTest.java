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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression test for a review finding on the #8111 fix (PR #8128): {@link PageManager#suspendFlushAndExecute}
 * used to run its two drains ({@code waitAllPagesOfDatabaseAreFlushed}, {@code waitPendingPagesOfDatabaseUntil})
 * unconditionally, even when the database was ALREADY suspended by another caller - which #5068's own refcounted
 * design explicitly supports ("every caller... owns its whole window even when the windows overlap").
 * <p>
 * The problem: while a database is suspended, a commit's page is DEFERRED rather than written
 * ({@code PageManagerFlushThread#flushPagesFromQueueToDisk}'s {@code isSuspended(db)} branch) and stays counted
 * as pending in {@code pageIndex} - it is never removed from it - until the LAST suspender releases and flushes
 * the backlog. An overlapping suspender's own drains would therefore see {@code pending() > 0} with NO possible
 * progress until the first suspender finishes, turning a previously-instant, previously-safe overlapping acquire
 * into a stall bounded only by {@code arcadedb.flushAllPagesTimeout} (60 s default) followed by a hard failure.
 * <p>
 * The fix adds a fast path: when the database is already suspended, skip the drains entirely and just bump the
 * refcount, the same instant acquisition the unconditional {@code setSuspended(database, true)} call always did
 * before the #8111 fix existed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8128OverlappingSuspendersWithConcurrentWritesTest extends TestHelper {

  @Test
  void overlappingSuspenderAcquiresInstantlyDespiteADeferredPendingPage() throws Exception {
    final Database db = (Database) database;
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final PageManagerFlushThread flush = pageManager.getFlushThread();

    database.getSchema().createDocumentType("Overlap");

    final CountDownLatch firstInside = new CountDownLatch(1);
    final CountDownLatch releaseFirst = new CountDownLatch(1);

    final Thread first = new Thread(() -> {
      try {
        pageManager.suspendFlushAndExecute(db, () -> {
          firstInside.countDown();
          releaseFirst.await(10, TimeUnit.SECONDS);
        });
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }, "test-suspender-1");

    final AtomicBoolean secondRan = new AtomicBoolean(false);

    assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      first.start();
      firstInside.await();

      // A real commit while suspender #1 holds the freeze: its page is deferred, not written, and stays
      // counted as pending until the last suspender resumes.
      database.transaction(() -> database.newDocument("Overlap").set("id", 1).save());

      // Driven by the real state rather than a fixed sleep: wait until the deferral is actually visible.
      final long deadline = System.currentTimeMillis() + 5_000;
      while (!flush.pageIndex.hasPendingOf(database) && System.currentTimeMillis() < deadline)
        Thread.sleep(5);
      assertThat(flush.pageIndex.hasPendingOf(database))
          .as("the commit during the freeze must have left a page deferred and pending")
          .isTrue();

      // The overlapping suspender must acquire INSTANTLY - the pre-#8111-fix behavior - not stall behind
      // drains that can never make progress while suspender #1 still owns the freeze.
      final long start = System.currentTimeMillis();
      pageManager.suspendFlushAndExecute(db, () -> secondRan.set(true));
      final long elapsed = System.currentTimeMillis() - start;

      assertThat(elapsed)
          .as("an overlapping suspender must acquire instantly, not pay for the #8111 drains a second time "
              + "while they can make no progress")
          .isLessThan(1_000);

      releaseFirst.countDown();
      first.join();
    });

    assertThat(secondRan.get()).isTrue();
    // Once the last suspender resumed, the deferred write must have been flushed and be durable.
    assertThat(flush.pageIndex.hasPendingOf(database)).isFalse();
    assertThat(database.query("sql", "select count(*) as c from Overlap").next().<Long>getProperty("c")).isEqualTo(1L);
  }
}
