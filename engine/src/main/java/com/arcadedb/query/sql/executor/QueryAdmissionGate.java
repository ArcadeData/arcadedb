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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.QueryAdmissionException;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Decides when a query a remote client sent starts, across every database of the JVM (issue #9518): at once when fewer than
 * {@link GlobalConfiguration#QUERY_MAX_CONCURRENT} queries are running and the heap the running queries hold reserved is
 * below {@link GlobalConfiguration#QUERY_ADMISSION_HEAP_WATERMARK} of the {@link QueryHeapBudget}, otherwise after
 * waiting in a queue, in arrival order. Without it a burst of heavy queries all starts together and the ones that come
 * last are refused by the heap budget (503), so a client retrying them loses the order it sent them in.
 * <p>
 * <b>A gate, not an executor.</b> It owns no thread: the query runs on the thread that asked to be admitted, which parks
 * while it waits. The engine already runs several pools sized on the cores, and a per-database async executor whose
 * threads multiply with the open databases, so a query pool would only add to that budget. The HTTP worker that received
 * the request is the one that waits, and {@link GlobalConfiguration#QUERY_QUEUE_MAX_SIZE} bounds how many of them can.
 * <p>
 * <b>The wait happens before the query starts, never in the middle of it.</b> A query that already holds part of the
 * heap budget and waits for the others to give theirs back is hold-and-wait: enough of them deadlock each other. Here a
 * waiting query holds nothing. Running queries keep growing after they were admitted, so the watermark is headroom and
 * not a guarantee: the heap budget stays the safety net that refuses the query that would exceed it.
 * <p>
 * <b>Arrival order.</b> Only the head of the queue can be admitted, and a query that arrives while others wait queues
 * behind them even when it could start at once. Each waiter parks on its own condition, so a query that ends wakes the
 * head alone rather than every waiter. The heap is given back by queries the gate never sees end - a query run by the
 * embedded API, a scan's buffers released mid-query - so the head re-checks every {@link #POLL_NANOS} on its own; that
 * also picks up a raised {@link GlobalConfiguration#QUERY_MAX_CONCURRENT} without waiting for a query to end. When no
 * admitted query is running, the head starts whatever the heap: a budget held by work outside the gate must not stall
 * the queue until every waiter times out.
 * <p>
 * <b>Only the outermost query takes a slot.</b> A query started on a thread that already holds one - a SQL function or an
 * MCP tool running a query, a script calling another language - shares it: queueing would make it wait for the slot its own
 * caller holds, which deadlocks as soon as every slot is taken. The slot goes back when the last ticket that shares it is
 * closed.
 * <p>
 * <b>Who goes through it.</b> Every protocol that executes the requests of a remote client, at the point where the request
 * starts: HTTP, Postgres, Bolt, Redis, gRPC, Gremlin Server, MCP and MongoDB. A protocol that runs its requests on a
 * thread shared by other connections cannot park it, so it admits with no wait and answers "busy" instead. The embedded API
 * does not go through it: the server's own work (security, schema, replication, triggers) runs queries the same way, and
 * making it wait behind client queries could stall the server.
 * <p>
 * The settings are read on every admission, so a change applies to the next query. With
 * {@link GlobalConfiguration#QUERY_MAX_CONCURRENT} at 0, {@link #admit()} returns at once and counts nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class QueryAdmissionGate {
  /** How often the head of the queue re-checks the heap and the slots without being woken. */
  static final long POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

  private static final QueryAdmissionGate INSTANCE = new QueryAdmissionGate();

  /** What {@link #admit()} returns when the gate is disabled: it holds no slot, so closing it gives nothing back. */
  private static final Ticket NOT_GATED = new Ticket(null, null);

  private final ReentrantLock          lock  = new ReentrantLock();
  // ONE CONDITION PER WAITING QUERY, IN ARRIVAL ORDER. GUARDED BY lock
  private final ArrayDeque<Condition>  queue = new ArrayDeque<>();
  // WRITTEN UNDER lock, READ WITHOUT IT BY THE METRICS
  private volatile int                 running;
  private volatile int                 queued;
  private final LongAdder              admitted             = new LongAdder();
  private final LongAdder              admittedAfterWaiting = new LongAdder();
  private final LongAdder              refused              = new LongAdder();
  private final LongAdder              heapDeferrals        = new LongAdder();
  private final LongAdder              waitNanos            = new LongAdder();
  // THE SLOT THE CURRENT THREAD HOLDS: A QUERY STARTED FROM INSIDE AN ADMITTED ONE TAKES A TICKET ON IT INSTEAD OF QUEUEING
  private final ThreadLocal<Slot>      currentSlot          = new ThreadLocal<>();

  // PACKAGE-PRIVATE FOR THE TESTS, WHICH NEED A GATE OF THEIR OWN: THE COUNTERS AND THE QUEUE OF THE JVM-WIDE ONE ARE SHARED
  QueryAdmissionGate() {
  }

  /** The gate the queries of every remote client go through. */
  public static QueryAdmissionGate getInstance() {
    return INSTANCE;
  }

  /** Whether {@link GlobalConfiguration#QUERY_MAX_CONCURRENT} enables the gate. */
  public boolean isEnabled() {
    return GlobalConfiguration.QUERY_MAX_CONCURRENT.getValueAsInteger() > 0;
  }

  /**
   * Admits a query, waiting in the queue up to {@link GlobalConfiguration#QUERY_QUEUE_TIMEOUT}. The caller runs the query
   * and closes the ticket when it is done with it, results included.
   *
   * @throws QueryAdmissionException when the query waited too long, found the queue full, or was interrupted while waiting
   */
  public Ticket admit() {
    return admit(GlobalConfiguration.QUERY_QUEUE_TIMEOUT.getValueAsLong());
  }

  /**
   * Admits a query, waiting in the queue up to {@code timeoutMs}: 0 or a negative value refuses a query that cannot start
   * at once.
   *
   * @throws QueryAdmissionException when the query waited too long, found the queue full, or was interrupted while waiting
   */
  public Ticket admit(final long timeoutMs) {
    final int maxConcurrent = GlobalConfiguration.QUERY_MAX_CONCURRENT.getValueAsInteger();
    if (maxConcurrent <= 0)
      return NOT_GATED;

    // STARTED FROM INSIDE A QUERY THIS THREAD ALREADY RUNS: IT SHARES THAT SLOT. QUEUEING WOULD WAIT FOR THE SLOT ITS OWN CALLER
    // HOLDS, A DEADLOCK AS SOON AS EVERY SLOT IS TAKEN. ONLY WHILE THE SLOT IS STILL HELD: A TICKET CLOSED ON ANOTHER THREAD MAY
    // HAVE GIVEN IT BACK
    final Slot held = currentSlot.get();
    if (held != null) {
      if (!held.released.get())
        for (int n = held.tickets.get(); n > 0; n = held.tickets.get())
          if (held.tickets.compareAndSet(n, n + 1))
            return new Ticket(this, held);
      // GIVEN BACK BY A TICKET CLOSED ON ANOTHER THREAD: DROP THE STALE REFERENCE THIS POOLED THREAD STILL HOLDS
      currentSlot.remove();
    }

    lock.lock();
    try {
      // A QUERY THAT ARRIVES WHILE OTHERS WAIT QUEUES BEHIND THEM EVEN WHEN IT COULD START: ARRIVAL ORDER
      if (queue.isEmpty() && blocker(maxConcurrent) == Blocker.NONE)
        return admitLocked();
      return waitForTurn(timeoutMs);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Gives back, before the request ends, the slot the current thread holds: for a request about to wait on another server
   * - a follower forwarding a write to the leader - which does no work here while it waits, and which would otherwise hold
   * a slot of this server for as long as the leader takes. It also keeps the wait from closing a cycle where the servers
   * share one JVM, and so one gate: the follower holding the last slot while the leader waits for it. The tickets of the
   * request still close normally, and give nothing back a second time. A query the thread starts afterwards takes a slot of
   * its own. Does nothing when the thread holds no slot.
   */
  public void releaseCurrentSlot() {
    final Slot held = currentSlot.get();
    if (held != null && held.released.compareAndSet(false, true)) {
      currentSlot.remove();
      release();
    }
  }

  /** Queries admitted and not finished yet. */
  public int getRunning() {
    return running;
  }

  /** Queries waiting in the queue right now. */
  public int getQueued() {
    return queued;
  }

  /** Queries admitted since the JVM started, at once or after waiting. Nothing is counted while the gate is disabled. */
  public long getAdmitted() {
    return admitted.sum();
  }

  /** Queries admitted after waiting in the queue since the JVM started. */
  public long getAdmittedAfterWaiting() {
    return admittedAfterWaiting.sum();
  }

  /** Queries refused since the JVM started: they waited too long, found the queue full or were interrupted. */
  public long getRefused() {
    return refused.sum();
  }

  /**
   * How many times the head of the queue had a free slot but found the heap the running queries hold above the
   * watermark, since the JVM started. Growing while queries wait says the heap, not the number of slots, holds them back.
   */
  public long getHeapDeferrals() {
    return heapDeferrals.sum();
  }

  /** The time the admitted queries spent waiting in the queue since the JVM started, in nanoseconds. */
  public long getTotalWaitNanos() {
    return waitNanos.sum();
  }

  /**
   * What a query gets from {@link #admit()}, to close once the query and its results are done with. The tickets of one slot -
   * the query that took it and every query started from inside it on the same thread - give it back together, when the last
   * of them is closed, whichever order they are closed in and whichever thread closes them.
   */
  public static final class Ticket implements AutoCloseable {
    private final QueryAdmissionGate gate;
    private final Slot               slot;
    // ATOMIC: A TICKET MAY BE CLOSED ON ANOTHER THREAD THAN THE ONE THAT TOOK IT, AND TWO RACING CLOSES MUST COUNT ONCE
    private final AtomicBoolean      closed = new AtomicBoolean();

    private Ticket(final QueryAdmissionGate gate, final Slot slot) {
      this.gate = gate;
      this.slot = slot;
    }

    /** Gives the slot back once no other ticket holds it, and lets the head of the queue start. Closing it again does nothing. */
    @Override
    public void close() {
      if (slot == null || !closed.compareAndSet(false, true))
        return;
      if (slot.tickets.decrementAndGet() == 0) {
        if (gate.currentSlot.get() == slot)
          gate.currentSlot.remove();
        if (slot.released.compareAndSet(false, true))
          gate.release();
      }
    }
  }

  /** One admitted slot, shared by the tickets of the queries the thread that took it starts while it holds it. */
  private static final class Slot {
    private final AtomicInteger tickets = new AtomicInteger(1);
    // SET ONCE, BY THE LAST TICKET CLOSED OR BY releaseCurrentSlot(), WHICHEVER COMES FIRST: THE SLOT GOES BACK EXACTLY ONCE
    private final AtomicBoolean released = new AtomicBoolean();
  }

  private enum Blocker {NONE, SLOTS, HEAP}

  /** What keeps a query from starting now. Called with the lock held. */
  private Blocker blocker(final int maxConcurrent) {
    if (running >= maxConcurrent)
      return Blocker.SLOTS;
    // NOTHING ADMITTED IS RUNNING: THE HEAP IS HELD BY WORK OUTSIDE THE GATE, WHICH THE QUEUE MUST NOT WAIT FOR
    if (running == 0)
      return Blocker.NONE;

    final int watermark = GlobalConfiguration.QUERY_ADMISSION_HEAP_WATERMARK.getValueAsInteger();
    if (watermark <= 0)
      return Blocker.NONE;
    final long limit = QueryHeapBudget.getLimitBytes();
    if (limit <= 0)
      return Blocker.NONE;
    return QueryHeapBudget.getReservedBytes() < limit / 100 * Math.min(watermark, 100) ? Blocker.NONE : Blocker.HEAP;
  }

  private Ticket admitLocked() {
    running++;
    admitted.increment();
    final Slot slot = new Slot();
    currentSlot.set(slot);
    return new Ticket(this, slot);
  }

  /** Queues the query and parks it until it is the head of the queue and can start. Called with the lock held. */
  private Ticket waitForTurn(final long timeoutMs) {
    final int maxQueued = GlobalConfiguration.QUERY_QUEUE_MAX_SIZE.getValueAsInteger();
    if (timeoutMs <= 0 || maxQueued <= 0)
      throw refuse("it cannot start at once and does not wait (" + GlobalConfiguration.QUERY_QUEUE_TIMEOUT.getKey() + "=" + timeoutMs
          + ", " + GlobalConfiguration.QUERY_QUEUE_MAX_SIZE.getKey() + "=" + maxQueued + ")");
    if (queue.size() >= maxQueued)
      throw refuse("the queue already holds " + queue.size() + " queries (" + GlobalConfiguration.QUERY_QUEUE_MAX_SIZE.getKey() + ")");

    final Condition turn = lock.newCondition();
    queue.addLast(turn);
    queued = queue.size();

    final long start = System.nanoTime();
    final long deadline = start + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    boolean admittedNow = false;
    try {
      while (true) {
        final boolean head = queue.peekFirst() == turn;
        if (head) {
          // RE-READ: A SETTING CHANGED WHILE WAITING APPLIES TO THE QUERIES ALREADY IN THE QUEUE TOO
          final int maxConcurrent = GlobalConfiguration.QUERY_MAX_CONCURRENT.getValueAsInteger();
          final Blocker blocker = maxConcurrent <= 0 ? Blocker.NONE : blocker(maxConcurrent);
          if (blocker == Blocker.NONE) {
            queue.pollFirst();
            queued = queue.size();
            admittedNow = true;
            admittedAfterWaiting.increment();
            waitNanos.add(System.nanoTime() - start);
            return admitLocked();
          }
          if (blocker == Blocker.HEAP)
            heapDeferrals.increment();
        }

        final long remaining = deadline - System.nanoTime();
        if (remaining <= 0)
          throw refuse("it waited " + timeoutMs + "ms in the queue (" + GlobalConfiguration.QUERY_QUEUE_TIMEOUT.getKey() + ")");

        // ONLY THE HEAD POLLS: THE OTHERS WAIT TO BECOME IT, WHICH SIGNALS THEM
        turn.awaitNanos(head ? Math.min(remaining, POLL_NANOS) : remaining);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw refuse("it was interrupted while waiting in the queue");
    } finally {
      if (!admittedNow) {
        queue.remove(turn);
        queued = queue.size();
      }
      // THE NEW HEAD RE-CHECKS AT ONCE: A SIGNAL THIS WAITER CONSUMED BEFORE LEAVING WOULD OTHERWISE BE LOST, AND ONE THAT
      // WAS ADMITTED MAY HAVE LEFT ROOM FOR THE NEXT ONE TOO
      signalHead();
    }
  }

  private void release() {
    lock.lock();
    try {
      running--;
      signalHead();
    } finally {
      lock.unlock();
    }
  }

  /** Called with the lock held. */
  private void signalHead() {
    final Condition head = queue.peekFirst();
    if (head != null)
      head.signal();
  }

  /** Called with the lock held. */
  private QueryAdmissionException refuse(final String reason) {
    refused.increment();
    final long limit = QueryHeapBudget.getLimitBytes();
    return new QueryAdmissionException("Query not started because " + reason + ": " + running + " running ("
        + GlobalConfiguration.QUERY_MAX_CONCURRENT.getKey() + "=" + GlobalConfiguration.QUERY_MAX_CONCURRENT.getValueAsInteger() + "), "
        + queue.size() + " waiting, heap budget " + (limit > 0 ? (QueryHeapBudget.getReservedBytes() * 100 / limit) + "% reserved" : "disabled")
        + ". Retry later");
  }
}
