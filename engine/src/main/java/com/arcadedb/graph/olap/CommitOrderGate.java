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
package com.arcadedb.graph.olap;

import com.arcadedb.log.LogManager;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.logging.Level;

/**
 * Hands the committed changes of a Graph Analytical View to it in the order their transactions committed (issue #10249).
 * <p>
 * A transaction reports its change from an after-commit callback, which runs on the committing thread once the transaction has
 * released its file locks. Two transactions that conflict on a file can only commit one after the other, but nothing orders
 * their callbacks: the second thread can get to the view first. The overlay is not indifferent to that order. A record slot
 * freed by a deleted edge is reused by the next edge created in the bucket, so the same RID names two edges in turn; delivered
 * the other way round, the creation of the new edge finds the old one still in the overlay and replaces it, and the deletion of
 * the old edge that follows withdraws the new one: an edge is missing from the view for good.
 * <p>
 * A transaction takes its place in line while it holds its file locks and cannot be refused any more ({@link #takePlace()},
 * called from {@link com.arcadedb.database.OrderedAfterCommitCallback#onCommitPoint()}), so the line is in commit order for the
 * transactions that conflict, which are the ones whose order matters. Its change is delivered once it is at the head of the
 * line, with the changes of the transactions behind it that were already waiting. The place is released by
 * {@link #deliver} when the transaction committed, or {@link #abandon} when it did not.
 * <p>
 * {@link #deliver} returns once the change has been handed over, whichever thread did it, so a {@code SYNCHRONOUS} commit still
 * returns with its change in the view. The wait is bounded, and a place that stays at the head of the line for that long is
 * evicted, so a transaction that never reported (its thread died between the commit point and the callback) costs the commits
 * behind it one wait and not one wait each, for as long as the view lives. Should that transaction report later, its change is
 * delivered then, out of order, which only a stall of that length can cause.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CommitOrderGate {
  // How often a waiting thread looks again at who is at the head of the line, which can change without waking it
  private static final long RECHECK_MS = 100L;

  private static final int PENDING = 0;
  private static final int READY   = 1;
  private static final int EVICTED = 2;

  private final ConcurrentLinkedQueue<Place> line       = new ConcurrentLinkedQueue<>();
  // One thread delivers at a time, so the changes reach the view one by one in the order of the line; the others leave what
  // they have to it. Not a lock: the delivery runs the view's merge, and a thread must not wait for it while holding anything
  private final AtomicBoolean                delivering = new AtomicBoolean();
  private final Object                       waitLock   = new Object();
  // Read at every wait, so a change of the setting applies at once. 0 or less: the committing thread does not wait
  private final LongSupplier                 maxWaitMs;

  /**
   * @param maxWaitMs how long, in milliseconds, a committing thread waits for the changes ahead of its own, asked at every wait
   *                  (see {@code GlobalConfiguration.GAV_COMMIT_ORDER_MAX_WAIT}); 0 or less does not wait
   */
  CommitOrderGate(final LongSupplier maxWaitMs) {
    this.maxWaitMs = maxWaitMs;
  }

  /** A transaction's place in line. */
  static final class Place {
    // The thread that took the place: the committing one, which is the one that delivers the change
    private final    Thread        owner = Thread.currentThread();
    // PENDING until the transaction reports, then READY (deliver or abandon) or EVICTED (the line moved on without it)
    private final    AtomicInteger state = new AtomicInteger(PENDING);
    // The change to deliver, or null for a place with nothing to deliver. Written before the state leaves PENDING
    private volatile TxDelta       delta;
    private volatile boolean       handled;
  }

  /**
   * Takes the next place in line. Lock-free and never blocks, as the caller holds its transaction's file locks.
   */
  Place takePlace() {
    final Place place = new Place();
    line.add(place);
    return place;
  }

  /**
   * The transaction of {@code place} committed: delivers its change to {@code sink} after the changes ahead of it in line, and
   * returns once that has happened or the wait is over. A null {@code delta} is a transaction with nothing to deliver, which only
   * releases its place. When the wait ends early, because the time is up or the thread is interrupted, the change is not in the
   * view yet but is delivered, in order, by the next thread to pass through the gate. The {@code sink} is the same for every call
   * and must be safe to call from several threads: a change of an evicted place is handed to it beside a delivery in progress.
   */
  void deliver(final Place place, final TxDelta delta, final Consumer<TxDelta> sink) {
    place.delta = delta;
    if (!place.state.compareAndSet(PENDING, READY)) {
      // Evicted while it was late: the line moved on without it. Its change goes in now, after the ones that overtook it. Not
      // through drain(): it may run beside a delivery in progress, which the view's merge (synchronized, as it always was for
      // commits that reach it from several threads) allows
      try {
        if (delta != null)
          sink.accept(delta);
      } catch (final RuntimeException e) {
        LogManager.instance().log(this, Level.WARNING, "Cannot deliver a committed change to the Graph Analytical View", e);
      } finally {
        place.handled = true;
      }
      return;
    }
    drain(sink);
    awaitHandled(place, sink);
  }

  /**
   * The transaction of {@code place} did not commit: releases its place so the changes behind it are delivered.
   */
  void abandon(final Place place, final Consumer<TxDelta> sink) {
    if (place.state.compareAndSet(PENDING, READY))
      drain(sink);
  }

  /** The number of places not yet released, for tests. */
  int pending() {
    return line.size();
  }

  private void drain(final Consumer<TxDelta> sink) {
    while (true) {
      if (!delivering.compareAndSet(false, true))
        // Whoever is delivering looks at the head again before it stops, and finds this place ready
        return;
      boolean any = false;
      try {
        Place head;
        while ((head = line.peek()) != null && head.state.get() != PENDING) {
          line.poll();
          try {
            final TxDelta delta = head.state.get() == READY ? head.delta : null;
            if (delta != null)
              sink.accept(delta);
          } catch (final RuntimeException e) {
            // One change the view cannot take must not hold back the ones behind it
            LogManager.instance().log(this, Level.WARNING, "Cannot deliver a committed change to the Graph Analytical View", e);
          } finally {
            head.handled = true;
            any = true;
          }
        }
      } finally {
        delivering.set(false);
        // Also when an Error escapes the sink: the places already handled must not leave their callers waiting
        if (any)
          synchronized (waitLock) {
            waitLock.notifyAll();
          }
      }
      // A place may have become ready between the last look at the head and the release: it was left to this thread
      final Place head = line.peek();
      if (head == null || head.state.get() == PENDING)
        return;
    }
  }

  private boolean isHeldBackByThisThread() {
    final Place head = line.peek();
    return head != null && head.state.get() == PENDING && Thread.currentThread().equals(head.owner);
  }

  private void awaitHandled(final Place place, final Consumer<TxDelta> sink) {
    final long waitMs = maxWaitMs.getAsLong();
    if (waitMs <= 0)
      return;
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMs);
    while (!place.handled) {
      // Also takes over the delivery a thread left half done when an Error escaped the view: the places it did not reach are ready
      drain(sink);
      if (place.handled)
        return;
      if (isHeldBackByThisThread())
        // A commit made by a callback of a transaction this thread is still finishing: that transaction is ahead in line
        // and delivers when the callback returns, so waiting here would be waiting for itself. The change is delivered then
        return;
      final long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
      if (remainingMs <= 0) {
        if (!evictHead(place, sink, waitMs))
          return;
        // The line moved: wait for what is left in front of this place
        deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMs);
        continue;
      }
      synchronized (waitLock) {
        if (place.handled)
          return;
        try {
          waitLock.wait(Math.min(remainingMs, RECHECK_MS));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  /**
   * Gives up on the place at the head of the line, which has held back {@code waiter} for the whole wait, and delivers what was
   * waiting behind it.
   *
   * @return whether a place was evicted
   */
  private boolean evictHead(final Place waiter, final Consumer<TxDelta> sink, final long waitMs) {
    final Place head = line.peek();
    if (head == null || head == waiter || !head.state.compareAndSet(PENDING, EVICTED))
      return false;
    LogManager.instance().log(this, Level.WARNING,
        "A transaction did not report its change to the Graph Analytical View in %d ms after it committed: the commits behind it go"
            + " ahead, and its change is delivered out of order if it comes (setting arcadedb.gavCommitOrderMaxWait)", waitMs);
    drain(sink);
    return true;
  }
}
