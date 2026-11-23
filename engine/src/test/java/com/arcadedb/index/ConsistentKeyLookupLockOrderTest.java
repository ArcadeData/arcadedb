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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.PageManager;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Issue #13112: a commit holds the database read lock while it waits for the publication holds to be released, a read against one
 * committed state holds a publication hold while it loads records (which takes the database read lock), and a schema change queues for
 * the database write lock. The lock is fair, so a reader that does not hold it already is refused while a writer is queued: the commit
 * waited for the hold, the hold owner for the read lock and the writer for the commit's read lock, forever. The read has to take the
 * database read lock BEFORE the hold, as a commit does, so that the lookup inside the hold re-enters a lock it owns.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ConsistentKeyLookupLockOrderTest extends TestHelper {
  /** Long enough for three threads that are not stuck to finish whatever the load of the machine, short for a hang detector. */
  private static final long HANG_DETECTOR_MS = 30_000;

  private RID rid;

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Item");
      rid = database.newDocument("Item").set("k", 1).save().getIdentity();
    });
  }

  /**
   * A reader inside the publication hold loads a record by RID while a commit waits for the hold and a schema change waits for the
   * write lock. The reader and the commit sit on the same stripe of the database lock, so the writer queues on the stripe the reader needs,
   * which is what made the cycle certain instead of a matter of thread ids.
   */
  @Test
  @Timeout(value = 180, unit = TimeUnit.SECONDS)
  void aReadInsideThePublicationHoldDoesNotWaitForTheReadLockBehindAQueuedWriter() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final CountDownLatch holdTaken = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    final AtomicReference<Record> loaded = new AtomicReference<>();
    final List<Throwable> failures = new CopyOnWriteArrayList<>();

    final Thread reader = new Thread(() -> guarded(failures, () -> {
      db.begin();
      try {
        loaded.set(ConsistentKeyLookup.readAgainstOneCommit(db, () -> {
          holdTaken.countDown();
          awaitQuietly(proceed);
          return db.lookupByRID(rid, true);
        }, "lock order test"));
      } finally {
        db.rollback();
      }
    }), "lookup-holding-publications");
    reader.setDaemon(true);

    final Thread committer = threadOnTheStripeOf(reader, () -> guarded(failures, () -> {
      db.begin();
      db.newDocument("Item").set("k", 2).save();
      db.commit();
    }), "committer");

    final Thread schemaWriter = new Thread(() -> guarded(failures, () -> db.getSchema().createDocumentType("Other")), "schema-change");
    schemaWriter.setDaemon(true);

    reader.start();
    assertThat(holdTaken.await(HANG_DETECTOR_MS, TimeUnit.MILLISECONDS)).as("the reader must take the publication hold").isTrue();
    committer.start();
    awaitBlockedIn(committer, "awaitPublicationHoldsReleased");
    schemaWriter.start();
    awaitBlockedIn(schemaWriter, "writeLock");

    proceed.countDown();

    final boolean finished = joinAll(HANG_DETECTOR_MS, reader, committer, schemaWriter);
    if (!finished) {
      final String report = describe(reader, committer, schemaWriter);
      // the cycle is broken by hand so that the threads end and the teardown can drop the database, which needs the write lock
      breakTheHolds(db.getPageManager(), false);
      joinAll(HANG_DETECTOR_MS, reader, committer, schemaWriter);
      breakTheHolds(db.getPageManager(), true);
      fail("Deadlock: the reader holds a publication hold and waits for the database read lock, the commit waits for the hold and the "
          + "schema change for the commit's read lock\n" + report);
    }

    assertThat(failures).isEmpty();
    assertThat(loaded.get()).isNotNull();
    assertThat(loaded.get().getIdentity()).isEqualTo(rid);
    assertThat(database.getSchema().existsType("Other")).isTrue();
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS total FROM Item")) {
      assertThat(rs.next().<Long>getProperty("total")).isEqualTo(2L);
    }
  }

  /**
   * The stripe of a reader is picked by its thread id modulo the stripe count (a power of two, at most 64), so two threads whose ids
   * differ by a multiple of 64 share a stripe whatever the count.
   */
  private static Thread threadOnTheStripeOf(final Thread reference, final Runnable body, final String name) {
    for (int attempt = 0; attempt < 100_000; attempt++) {
      final Thread thread = new Thread(body, name);
      if ((thread.threadId() - reference.threadId()) % 64 == 0) {
        thread.setDaemon(true);
        return thread;
      }
    }
    throw new IllegalStateException("No thread id on the stripe of " + reference.getName());
  }

  private static void guarded(final List<Throwable> failures, final Runnable work) {
    try {
      work.run();
    } catch (final Throwable e) {
      failures.add(e);
    }
  }

  private static void awaitQuietly(final CountDownLatch latch) {
    try {
      latch.await(HANG_DETECTOR_MS * 4, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Waits until {@code thread} is parked or waiting inside a method named {@code method}. */
  private static void awaitBlockedIn(final Thread thread, final String method) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(HANG_DETECTOR_MS);
    while (System.nanoTime() - deadline < 0) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING || state == Thread.State.BLOCKED)
        for (final StackTraceElement frame : thread.getStackTrace())
          if (frame.getMethodName().equals(method))
            return;
      Thread.sleep(5);
    }
    fail("Thread " + thread.getName() + " never blocked in " + method + "\n" + describe(thread));
  }

  private static boolean joinAll(final long budgetMillis, final Thread... threads) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(budgetMillis);
    for (final Thread thread : threads) {
      final long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
      if (remainingMillis > 0)
        thread.join(remainingMillis);
      if (thread.isAlive())
        return false;
    }
    return true;
  }

  private static String describe(final Thread... threads) {
    final StringBuilder report = new StringBuilder();
    for (final Thread thread : threads) {
      report.append(thread.getName()).append(' ').append(thread.getState()).append('\n');
      final StackTraceElement[] stack = thread.getStackTrace();
      for (int i = 0; i < Math.min(stack.length, 14); i++)
        report.append("    at ").append(stack[i]).append('\n');
    }
    return report.toString();
  }

  /**
   * Failure path only: lets the commit stop waiting for a hold that its owner can no longer release, and, once the owner ended, puts
   * the count of holds (process-wide, one page manager serves every database) back to zero.
   */
  private static void breakTheHolds(final PageManager pageManager, final boolean reset) throws ReflectiveOperationException {
    final Field gateField = PageManager.class.getDeclaredField("publicationGate");
    final Field holdsField = PageManager.class.getDeclaredField("publicationHolds");
    gateField.setAccessible(true);
    holdsField.setAccessible(true);
    final Object gate = gateField.get(pageManager);
    synchronized (gate) {
      holdsField.setInt(pageManager, 0);
      if (!reset)
        gate.notifyAll();
    }
  }
}
