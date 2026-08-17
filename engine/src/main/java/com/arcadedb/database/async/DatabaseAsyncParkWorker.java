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
package com.arcadedb.database.async;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Commits the worker's open transaction batch, reports that the worker has stopped writing, and parks it there until
 * released. One of these per worker is what {@link DatabaseAsyncExecutorImpl#quiesceWorkers()} is made of (issue
 * #6303, item 2).
 * <p>
 * <b>The commit is half the point, not preparation for it.</b> A worker opens a transaction when it starts and keeps
 * it open across up to {@link com.arcadedb.GlobalConfiguration#ASYNC_TX_BATCH_SIZE} tasks, so a worker that has
 * merely stopped taking new work is still holding every record of the current batch uncommitted and invisible to any
 * scan (issue #6281). Parking such a worker would freeze the writes without publishing them, which is the worse half
 * of both worlds: an index build would then scan a bucket that provably does not contain them.
 * <p>
 * <b>And the report is the other half.</b> The pause this replaces was scheduled and forgotten - the build began
 * whether or not any worker had reached it - so a task already queued ahead of the pause could still be writing while
 * the scan ran. {@code parked} is counted down exactly once per task, by whichever of {@link #execute} or
 * {@link #completed} gets there first, so a worker that dies before parking releases the waiter instead of leaving it
 * to time out.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DatabaseAsyncParkWorker implements DatabaseAsyncTask {
  /** Bounds how often the park re-reads the shutdown flag, so a closing executor is not held by a lost release. */
  private static final long          PARK_POLL_MILLIS = 500;
  private final        CountDownLatch parked;
  private final        CountDownLatch release;
  private final        AtomicBoolean  reported         = new AtomicBoolean();

  public DatabaseAsyncParkWorker(final CountDownLatch parked, final CountDownLatch release) {
    this.parked = parked;
    this.release = release;
  }

  @Override
  public void execute(final DatabaseAsyncExecutorImpl.AsyncThread async, final DatabaseInternal database) {
    try {
      if (database.isTransactionActive())
        database.commit();
    } catch (final Exception e) {
      // The batch could not be published. Say so and still park: the quiescing caller has to be released either way,
      // and it is going to scan data this worker's batch was supposed to be part of - a silent skip here is exactly
      // the shape of failure issue #6281 was about.
      LogManager.instance().log(this, Level.SEVERE,
          "Error on committing the pending asynchronous batch of database '%s' before parking its worker", e,
          database.getName());
      async.onError(e);
    } finally {
      signalParked();
    }

    try {
      // Bounded rounds rather than one untimed await: a worker must not be held here past its own executor's
      // shutdown, which is the one release that never arrives from the quiescing caller.
      boolean released = false;
      while (!released && !async.isShutdown())
        released = release.await(PARK_POLL_MILLIS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      // SHUTDOWN IN PROGRESS
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Also reached when the task is dropped without executing - a worker that exited leaves its queue to
   * {@code drainQueueNotifyingWaiters} - so a quiescing caller is released by a dead worker rather than waiting out
   * its whole timeout for one that is never going to park.
   */
  @Override
  public void completed() {
    signalParked();
  }

  private void signalParked() {
    if (reported.compareAndSet(false, true))
      parked.countDown();
  }

  @Override
  public boolean requiresActiveTx() {
    return false;
  }

  @Override
  public String toString() {
    return "ParkWorker";
  }
}
