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
package com.arcadedb.utility;

import com.arcadedb.log.LogManager;

import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

/**
 * Per-resource lock manager with <b>FIFO-fair</b> hand-off.
 * <p>
 * A resource is owned by a {@code REQUESTER} (a thread, an HTTP session id, ...), not by the
 * acquiring thread, so a lock may legitimately be acquired on one thread and released on another
 * (e.g. session-scoped explicit locks). Ownership is therefore tracked by requester, while waiting
 * threads are parked/unparked individually via {@link LockSupport}.
 * <p>
 * <b>Fairness.</b> When a held resource is released, ownership is handed off to the <i>oldest</i>
 * waiter (queue head) rather than waking every waiter to race for it. This eliminates the
 * thundering-herd starvation of the previous {@code CountDownLatch}-based design, where a waiter
 * could keep losing the wake-up race for the whole timeout and spuriously fail on a hot resource
 * (e.g. a single-bucket type under concurrent write load) even though the lock was being acquired
 * and released continuously. Under contention the cost per release drops from O(waiters) wake-ups to
 * a single {@code unpark}.
 * <p>
 * Deadlock avoidance is the caller's responsibility (acquire resources in a globally consistent
 * order); this class does not change that contract - it only changes how a single contended resource
 * is handed among its waiters.
 * <p>
 * Semantics preserved from the historical implementation:
 * <ul>
 *   <li>{@link #tryLock} returns {@link LOCK_STATUS#YES} on acquisition, {@link LOCK_STATUS#ALREADY_ACQUIRED}
 *       when the same requester already owns the resource (non-counting: a single {@link #unlock}
 *       releases it), and {@link LOCK_STATUS#NO} when the wait times out or the thread is interrupted.</li>
 *   <li>{@code timeout > 0} waits up to that many milliseconds; {@code timeout <= 0} waits indefinitely
 *       until the lock is acquired (used by index compaction).</li>
 *   <li>{@link #unlock} is a no-op for a resource that is not held, and throws {@link LockException}
 *       when the requester is not the owner.</li>
 *   <li>{@link #close} releases every resource and wakes all waiters.</li>
 * </ul>
 */
public class LockManager<RESOURCE, REQUESTER> {
  public enum LOCK_STATUS {NO, YES, ALREADY_ACQUIRED}

  private final ConcurrentHashMap<RESOURCE, ResourceLock<REQUESTER>> lockManager = new ConcurrentHashMap<>(256);

  /**
   * State of one resource: its current owner and the FIFO queue of waiting requesters. All fields are
   * guarded by the monitor of the instance. {@code removed} marks the instance as detached from the
   * map (freed with no waiters, or closed) so a thread that captured it just before removal retries
   * instead of operating on a stale node.
   */
  private static final class ResourceLock<R> {
    R                         owner;
    long                      when;
    final ArrayDeque<Waiter<R>> queue = new ArrayDeque<>();
    boolean                   removed;
  }

  /**
   * One waiting requester. {@code granted} is set by the releasing thread when ownership is handed to
   * this waiter; {@code cancelled} is set when the waiter gives up (timeout/interrupt/close) - cancelled
   * waiters are skipped (and dropped) lazily during hand-off, so cancellation is O(1).
   */
  private static final class Waiter<R> {
    final R      requester;
    final Thread thread;
    boolean      granted;
    boolean      cancelled;

    Waiter(final R requester, final Thread thread) {
      this.requester = requester;
      this.thread = thread;
    }
  }

  public LOCK_STATUS tryLock(final RESOURCE resource, final REQUESTER requester, final long timeout) {
    if (resource == null)
      throw new IllegalArgumentException("Resource to lock is null");

    if (requester == null)
      throw new IllegalArgumentException("Requester is null");

    final Thread current = Thread.currentThread();

    // --- Registration: take the free resource, detect re-entrancy, or enqueue as a waiter. ---
    ResourceLock<REQUESTER> rl = null;
    Waiter<REQUESTER> waiter = null;
    for (; ; ) {
      final ResourceLock<REQUESTER> candidate = lockManager.computeIfAbsent(resource, k -> new ResourceLock<>());
      synchronized (candidate) {
        if (candidate.removed)
          // Captured a node that was detached from the map between computeIfAbsent and here; retry.
          continue;

        if (candidate.owner == null) {
          // Free: acquire immediately.
          candidate.owner = requester;
          candidate.when = System.currentTimeMillis();
          return LOCK_STATUS.YES;
        }

        if (candidate.owner.equals(requester)) {
          LogManager.instance().log(this, Level.FINE, "Resource '%s' already locked by requester '%s'", resource, candidate.owner);
          return LOCK_STATUS.ALREADY_ACQUIRED;
        }

        // Held by someone else: join the FIFO queue and wait below.
        waiter = new Waiter<>(requester, current);
        candidate.queue.addLast(waiter);
        rl = candidate;
      }
      break;
    }

    // --- Wait: parked until handed ownership (granted), the deadline passes, interrupted, or closed. ---
    final long deadlineNanos = timeout > 0 ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout) : 0L;
    boolean interrupted = false;
    try {
      for (; ; ) {
        synchronized (rl) {
          if (waiter.granted)
            // Ownership was handed to us by a releasing thread: we are now the owner and must unlock later.
            return LOCK_STATUS.YES;
          if (rl.removed) {
            // The manager was closed while we waited.
            waiter.cancelled = true;
            return LOCK_STATUS.NO;
          }
          if (interrupted) {
            waiter.cancelled = true;
            return LOCK_STATUS.NO;
          }
          if (timeout > 0 && deadlineNanos - System.nanoTime() <= 0) {
            waiter.cancelled = true;
            return LOCK_STATUS.NO;
          }
        }

        // Park OUTSIDE the monitor so a releasing thread can hand off without blocking. A handoff (or
        // close) calls unpark; park may also return spuriously - the loop re-checks state either way.
        if (timeout > 0)
          LockSupport.parkNanos(deadlineNanos - System.nanoTime());
        else
          LockSupport.park();

        if (Thread.interrupted())
          interrupted = true;
      }
    } finally {
      if (interrupted)
        current.interrupt();
    }
  }

  public void unlock(final RESOURCE resource, final REQUESTER requester) {
    if (resource == null)
      throw new IllegalArgumentException("Resource to unlock is null");

    final ResourceLock<REQUESTER> rl = lockManager.get(resource);
    if (rl == null)
      // Not held.
      return;

    Waiter<REQUESTER> next = null;
    synchronized (rl) {
      if (rl.removed || rl.owner == null)
        // Already released by a concurrent unlock/close.
        return;

      if (!rl.owner.equals(requester))
        throw new LockException(
            "Cannot unlock resource '" + resource + "' because owner '" + rl.owner + "' <> requester '" + requester + "'");

      // Hand off to the oldest live waiter, skipping (and dropping) any that gave up.
      Waiter<REQUESTER> head;
      while ((head = rl.queue.pollFirst()) != null && head.cancelled)
        ; // discard cancelled waiters

      if (head != null) {
        rl.owner = head.requester;
        rl.when = System.currentTimeMillis();
        head.granted = true;
        next = head;
      } else {
        // No waiters: free the resource and detach the node from the map atomically with marking it removed.
        rl.owner = null;
        rl.removed = true;
        lockManager.remove(resource, rl);
      }
    }

    if (next != null)
      LockSupport.unpark(next.thread);
  }

  public void close() {
    for (final Iterator<Map.Entry<RESOURCE, ResourceLock<REQUESTER>>> it = lockManager.entrySet().iterator(); it.hasNext(); ) {
      final ResourceLock<REQUESTER> rl = it.next().getValue();
      it.remove();

      final List<Thread> toWake;
      synchronized (rl) {
        rl.removed = true;
        rl.owner = null;
        if (rl.queue.isEmpty())
          continue;
        toWake = new ArrayList<>(rl.queue.size());
        for (final Waiter<REQUESTER> w : rl.queue) {
          w.cancelled = true;
          toWake.add(w.thread);
        }
        rl.queue.clear();
      }

      // Wake waiters outside the monitor; each observes removed==true and returns NO.
      for (final Thread t : toWake)
        LockSupport.unpark(t);
    }
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<RESOURCE, ResourceLock<REQUESTER>> entry : lockManager.entrySet()) {
      final ResourceLock<REQUESTER> rl = entry.getValue();
      final REQUESTER owner;
      final long when;
      final int waiters;
      synchronized (rl) {
        owner = rl.owner;
        when = rl.when;
        waiters = rl.queue.size();
      }
      sb.append("\n- '").append(entry.getKey()).append("', owner='").append(owner)
          .append("' on ").append(DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(DateUtils.millisToLocalDateTime(when, null)))
          .append(" (").append(waiters).append(" waiters)");
    }
    return sb.toString();
  }
}
