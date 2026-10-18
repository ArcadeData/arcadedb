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
package com.arcadedb.server;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.schema.DocumentType;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The conformance check of issue #10235, shared by every surface that goes through the {@link QueryAdmissionGate}: a
 * statement that gives its slot back before a wait - a follower forwarding to the leader, a request waiting on an explicit
 * lock - must find the slot on the thread that runs it, whichever thread the surface admitted the request on. A surface
 * that admits on one thread and runs the statement on another, without carrying the slot there ({@link
 * QueryAdmissionGate#handOff()}), gives nothing back and holds the slot for the whole wait (#10140).
 * <p>
 * The hook is a {@link BeforeRecordCreateListener} on the type the request writes to, so it runs on the thread that runs the
 * statement whatever the protocol: SQL, Cypher, Gremlin, a Redis {@code HSET}, a MongoDB insert or a bulk load. It calls
 * {@link QueryAdmissionGate#releaseCurrentSlot()}, as the statements that wait do, and then blocks until the test has looked
 * at the gate.
 * <p>
 * The check runs with one slot: the request holds it when the hook runs, and the slot must be free while the hook waits.
 */
public final class AdmissionSlotConformance {
  private static final long TIMEOUT_MS = 60_000L;

  private AdmissionSlotConformance() {
  }

  /**
   * Sends {@code request} through the surface under test with the gate enabled, and asserts that the statement it runs gives
   * its slot back from the thread that runs it, and that the request then completes and leaves no slot taken.
   *
   * @param database the database the request writes to, to register the hook on
   * @param typeName the type the request creates a record of: the hook runs on its first record
   * @param surface  the name of the surface, for the messages
   * @param request  sends the request and returns once the surface answered it, throwing if it failed
   */
  public static void assertGivesItsSlotBack(final Database database, final String typeName, final String surface,
      final Callable<?> request) throws Exception {
    final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();
    // NULL WHEN THE SETTING WAS NOT CHANGED: IT IS RESET RATHER THAN WRITTEN BACK AS AN EXPLICIT VALUE
    final Object savedMaxConcurrent = GlobalConfiguration.QUERY_MAX_CONCURRENT.isChanged() ? GlobalConfiguration.QUERY_MAX_CONCURRENT.getValue() : null;
    final Object savedQueueTimeout = GlobalConfiguration.QUERY_QUEUE_TIMEOUT.isChanged() ? GlobalConfiguration.QUERY_QUEUE_TIMEOUT.getValue() : null;
    final DocumentType type = database.getSchema().getType(typeName);
    final Probe probe = new Probe(gate);
    final ExecutorService client = Executors.newSingleThreadExecutor(r -> new Thread(r, "admission-conformance-" + surface));
    try {
      GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
      // A SLOT LEFT TAKEN BY A PREVIOUS REQUEST IS A REFUSAL HERE, NOT A WAIT AS LONG AS THE TEST
      GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(5_000L);
      awaitCondition(() -> gate.getRunning() == 0, surface + ": no slot is taken before the request");

      type.getEvents().registerListener(probe);
      final Future<?> answer = client.submit(request);

      // A REQUEST THAT ENDS WITHOUT REACHING THE HOOK FAILS AT ONCE, WITH ITS OWN ERROR WHEN IT HAS ONE
      final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
      while (!probe.entered.await(10, TimeUnit.MILLISECONDS)) {
        if (answer.isDone()) {
          answer.get();
          fail(surface + ": the request completed without creating a " + typeName + " record");
        }
        if (System.nanoTime() - deadline > 0)
          fail(surface + ": the request never created a " + typeName + " record");
      }

      assertThat(probe.runningAtEntry).as(surface + ": the statement runs admitted, holding the only slot").isEqualTo(1);
      assertThat(probe.released).as(
          surface + ": releaseCurrentSlot() finds the slot on the thread that runs the statement (" + probe.thread + ")").isTrue();
      assertThat(gate.getRunning()).as(surface + ": the slot is free while the statement waits").isZero();
      assertThat(answer.isDone()).as(surface + ": the request is still waiting on the hook").isFalse();

      probe.proceed.countDown();
      try {
        answer.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (final TimeoutException e) {
        fail(surface + ": the request did not complete once the hook let it go");
      }
      awaitCondition(() -> gate.getRunning() == 0, surface + ": the request leaves no slot taken");
    } finally {
      try {
        probe.proceed.countDown();
        type.getEvents().unregisterListener(probe);
        client.shutdownNow();
        // A REQUEST LEFT BEHIND BY A FAILED ASSERTION GIVES ITS SLOT BACK BEFORE THE GATE IS RESET, NOT IN THE NEXT TEST
        client.awaitTermination(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } finally {
        // THE GATE IS JVM-WIDE: LEFT ENABLED, IT WOULD GATE EVERY TEST THAT RUNS AFTER THIS ONE
        restore(GlobalConfiguration.QUERY_MAX_CONCURRENT, savedMaxConcurrent);
        restore(GlobalConfiguration.QUERY_QUEUE_TIMEOUT, savedQueueTimeout);
      }
    }
  }

  private static void restore(final GlobalConfiguration setting, final Object saved) {
    if (saved == null)
      setting.reset();
    else
      setting.setValue(saved);
  }

  private static void awaitCondition(final BooleanSupplier condition, final String what) throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() - deadline > 0)
        fail(what);
      Thread.sleep(10);
    }
  }

  /**
   * Gives the slot back on the first record created, as a statement about to wait does, then waits for the test. Only the
   * first: a record the request creates afterwards, or a retry of its statement, runs without the hold, and the check has
   * already read what the first one saw.
   */
  private static final class Probe implements BeforeRecordCreateListener {
    private final QueryAdmissionGate gate;
    private final AtomicBoolean      armed   = new AtomicBoolean(true);
    private final CountDownLatch     entered = new CountDownLatch(1);
    private final CountDownLatch     proceed = new CountDownLatch(1);
    private volatile int             runningAtEntry;
    private volatile boolean         released;
    private volatile String          thread;

    private Probe(final QueryAdmissionGate gate) {
      this.gate = gate;
    }

    @Override
    public boolean onBeforeCreate(final Record record) {
      if (!armed.compareAndSet(true, false))
        return true;
      thread = Thread.currentThread().getName();
      runningAtEntry = gate.getRunning();
      released = gate.releaseCurrentSlot();
      entered.countDown();
      try {
        proceed.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException e) {
        // ONLY ON TEARDOWN, WHEN THE CHECK HAS ALREADY FAILED OR PASSED: THE FLAG IS NOT RESTORED, BECAUSE THIS SERVER THREAD
        // GOES ON TO WRITE THE RECORD, AND AN INTERRUPTED THREAD WOULD CLOSE THE FILE CHANNEL IT WRITES TO
      }
      return true;
    }
  }
}
