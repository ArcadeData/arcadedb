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

import com.arcadedb.log.LogManager;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Warms, in the background, the indexes a schema reload re-created cold (issue #10021).
 * <p>
 * <b>Why it exists.</b> A schema reload replaces every component with a freshly built instance. For an {@code LSM_VECTOR}
 * index that instance starts with nothing resident: no location map, no graph, no searcher pool. The next search pays for
 * all of it on its own thread - a full read of the index pages, then a graph load or, worse, a rebuild under the JVM-wide
 * rebuild permit. On a replica every DDL that its incremental refresh declines ends in such a reload (issue #10020), so one
 * unrelated statement turned the following searches of every vector index into a burst of loads and rebuilds.
 * <p>
 * <b>What it does.</b> The warm-up runs the loads on ONE daemon thread, one index after the other, so they never burst
 * and a search that arrives meanwhile joins the load in progress through the index's own one-owner protocol instead of
 * repeating it. It is never run on the apply thread that published the reload: that is the thread the work is being moved
 * off.
 * <p>
 * <b>Debounce and supersession.</b> A request waits {@code delayMs} and is replaced by any newer request that arrives
 * first, so a burst of DDL statements, each ending in a reload, results in one warm-up of the last generation and not in
 * one per statement. A batch already running stops at the next index boundary when a newer request arrives: the
 * indexes left in it are the retired instances of the generation that request re-created. {@link #unfinishedNames()} is how
 * a reload keeps the intent of an index that has been re-created twice before it got warm.
 * <p>
 * It is a thread of its own rather than a pool task on purpose: a {@code DedicatedThreadPool} answers saturation by
 * running the task on the submitter, which here is the apply thread. The pattern is the one the vector index's own async
 * rebuild thread uses. The thread exists only while there is work.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class IndexWarmUpScheduler {

  /** One index to warm: its name, for the log and for {@link #unfinishedNames()}, and the work. */
  record Task(String name, Runnable warmUp) {
  }

  /** How long {@link #close()} waits for a load in flight to end after interrupting it. */
  static final long CLOSE_JOIN_MS = 5_000L;

  private final String databaseName;
  private final Object monitor = new Object();

  // Everything below is guarded by `monitor`
  private List<Task>      pending;
  private long            dueNanos;
  // By identity: two requests may carry a task of the same name, and finishing the older must not forget the newer
  private final Set<Task> unfinished = Collections.newSetFromMap(new IdentityHashMap<>());
  private Thread          worker;
  private boolean         running;
  private boolean         closed;
  // The index the worker is warming right now, for the log of a close it outlives (issue #10156)
  private String          inFlight;
  // Bumped by every request and by close(): a batch that finds it moved is no longer the latest and stops
  private long            generation;

  IndexWarmUpScheduler(final String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * Replaces whatever was waiting with {@code tasks}, to run once {@code delayMs} has passed with no newer request. An
   * empty list cancels the waiting batch. Ignored once closed.
   */
  void request(final List<Task> tasks, final long delayMs) {
    synchronized (monitor) {
      if (closed)
        return;

      generation++;
      if (pending != null)
        unfinished.removeAll(pending);

      if (tasks.isEmpty())
        pending = null;
      else {
        pending = List.copyOf(tasks);
        unfinished.addAll(pending);
        dueNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0L, delayMs));
        if (worker == null) {
          worker = new Thread(this::runWorker, "ArcadeDB-IndexWarmUp-" + databaseName);
          worker.setDaemon(true);
          worker.start();
        }
      }
      monitor.notifyAll();
    }
  }

  /**
   * The names of the indexes whose warm-up was requested and has not finished: the ones waiting, and the ones of the batch
   * running that it has not got to yet. A reload that re-creates an index among them has to ask for it again, whether or not
   * the instance it replaces ever got warm.
   */
  Set<String> unfinishedNames() {
    synchronized (monitor) {
      final Set<String> names = new HashSet<>();
      for (final Task task : unfinished)
        names.add(task.name());
      return names;
    }
  }

  /**
   * Waits until nothing is waiting and nothing is running, and, once closed, until the worker has left. For callers that need
   * the indexes warm before they carry on (a test, a restart sequence): the warm-up exists so that nobody has to.
   *
   * @return {@code false} when the timeout elapsed first
   */
  boolean awaitIdle(final long timeoutMs) {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    synchronized (monitor) {
      // Once closed, idle also means the worker has left: a close that outlived a load reports it gone only after this
      while (pending != null || running || (closed && worker != null)) {
        final long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remainingMs <= 0)
          return false;
        try {
          monitor.wait(remainingMs);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Drops what is waiting, refuses further requests, interrupts the thread so a load in flight ends early, and waits for it to
   * end for at most {@link #CLOSE_JOIN_MS}.
   * <p>
   * The wait is what lets the caller go on to flush and close the files the warm-up reads: the database close calls this before
   * any of that, where every index stops its background work (issue #10156). It is bounded because a load is not obliged to
   * honour an interrupt (a graph build inside JVector does not): a thread that outlives the bound is left to finish on its own,
   * and the index load path it is in is the same one a search thread racing a close is in, which already tolerates it. That is
   * logged, naming the index, since nothing else would tell an operator the database closed with a load still running.
   * <p>
   * Only the first call waits: a later one finds the scheduler closed and returns at once, so the backstop call of a schema
   * close does not charge a second bound for a load that already outlived the first.
   */
  void close() {
    final boolean first;
    synchronized (monitor) {
      first = !closed;
    }
    // Logged by the call that waited, not again by the backstop that finds the same thread still running
    if (!close(CLOSE_JOIN_MS) && first)
      LogManager.instance().log(this, Level.WARNING,
          "The warm-up of index '%s' of database '%s' did not end within %d ms of being interrupted: the database closes with it "
              + "still running", inFlightName(), databaseName, CLOSE_JOIN_MS);
  }

  /**
   * @return whether no worker is left running. Only the first call waits up to {@code joinMs} for it
   */
  boolean close(final long joinMs) {
    final Thread toInterrupt;
    final boolean alreadyClosed;
    synchronized (monitor) {
      alreadyClosed = closed;
      closed = true;
      generation++;
      pending = null;
      unfinished.clear();
      toInterrupt = worker;
      monitor.notifyAll();
    }
    if (toInterrupt == null)
      return true;
    if (alreadyClosed)
      // The first close interrupted it and waited for it already
      return !toInterrupt.isAlive();

    toInterrupt.interrupt();
    // Never join the thread we are on: a task closing the schema it is warming would wait for itself
    if (toInterrupt.equals(Thread.currentThread()))
      return false;
    try {
      toInterrupt.join(Math.max(1L, joinMs));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return !toInterrupt.isAlive();
  }

  private void runWorker() {
    try {
      while (true) {
        final List<Task> batch;
        final long batchGeneration;
        synchronized (monitor) {
          while (true) {
            if (closed || pending == null) {
              // Decided under the monitor that also guards request(): clearing the field later, after the lock is
              // released, would leave a window in which a request sees a worker that has already decided to leave
              worker = null;
              monitor.notifyAll();
              return;
            }
            final long remainingNanos = dueNanos - System.nanoTime();
            if (remainingNanos <= 0)
              break;
            monitor.wait(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
          }
          batch = pending;
          batchGeneration = generation;
          pending = null;
          running = true;
        }

        try {
          runBatch(batch, batchGeneration);
        } finally {
          synchronized (monitor) {
            running = false;
            unfinished.removeAll(batch);
            monitor.notifyAll();
          }
        }
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      synchronized (monitor) {
        if (Thread.currentThread().equals(worker))
          worker = null;
        running = false;
        monitor.notifyAll();
      }
    }
  }

  private void runBatch(final List<Task> batch, final long batchGeneration) {
    final long startNanos = System.nanoTime();
    int warmed = 0;
    for (final Task task : batch) {
      synchronized (monitor) {
        // A newer request (or close) means the rest of this batch names instances no schema publishes any more
        if (generation != batchGeneration)
          break;
        inFlight = task.name();
      }

      try {
        task.warmUp().run();
        ++warmed;
      } catch (final Exception e) {
        // A load that was interrupted because the database is closing is the expected end of the thread, not a failure
        final boolean quiet = Thread.currentThread().isInterrupted() || isClosed();
        LogManager.instance().log(this, quiet ? Level.FINE : Level.WARNING,
            "Could not warm up index '%s' of database '%s' after a schema reload, its first search will load it instead: %s",
            task.name(), databaseName, e.toString());
        if (quiet)
          break;
      } catch (final Error e) {
        // An OutOfMemoryError from a graph load that does not fit ends the worker (the loop above is left, and the outer
        // finally clears it), so say so: the next search loads the index instead, and nothing else would tell an operator why
        LogManager.instance().log(this, Level.SEVERE,
            "Warming up index '%s' of database '%s' after a schema reload failed with %s, the warm-up of the remaining indexes is "
                + "abandoned and their first search will load them instead", task.name(), databaseName, e.toString());
        throw e;
      } finally {
        synchronized (monitor) {
          inFlight = null;
        }
      }
    }

    if (warmed > 0)
      LogManager.instance().log(this, Level.INFO,
          "Warmed up %d of %d index(es) of database '%s' that a schema reload re-created in %d ms", warmed, batch.size(),
          databaseName, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
  }

  /** The index the worker is warming, or a placeholder when it is between two (it may be about to leave). */
  private String inFlightName() {
    synchronized (monitor) {
      return inFlight != null ? inFlight : "(none)";
    }
  }

  private boolean isClosed() {
    synchronized (monitor) {
      return closed;
    }
  }
}
