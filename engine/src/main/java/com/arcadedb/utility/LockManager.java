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
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;

/**
 * Per-resource lock manager with FIFO-fair hand-off. Releasing a resource hands ownership to the
 * oldest waiter (a single {@link LockSupport} unpark) instead of waking every waiter to race for it,
 * which removes the thundering-herd starvation of the previous {@code CountDownLatch} design (a waiter
 * could lose the wake-up race for the whole timeout and spuriously time out on a hot resource).
 * Ownership is keyed by {@code REQUESTER} (thread, HTTP session id, ...), not the acquiring thread, so
 * a lock may be acquired on one thread and released on another. Deadlock avoidance remains the
 * caller's responsibility (acquire in a globally consistent order).
 * <p>
 * Semantics: {@code timeout > 0} waits that many ms then returns {@link LOCK_STATUS#NO}; {@code timeout <= 0}
 * waits indefinitely (index compaction); re-acquiring by the same owner is {@link LOCK_STATUS#ALREADY_ACQUIRED}
 * (non-counting - one {@link #unlock} releases); {@link #unlock} is a no-op when not held and throws
 * {@link LockException} on a non-owner; {@link #close} frees all and wakes all waiters with NO.
 * <p>
 * The uncontended fast path takes the per-resource monitor (and creates the node when the resource is
 * free) rather than a single lock-free CAS; negligible for commit-level locking.
 * <p>
 * <b>Spin-then-park hand-off</b> (issue #10501, on when {@code spinNanos > 0}). A hand-off to a parked waiter makes it the
 * owner while it is still parked, so the resource does nothing until the OS schedules that thread: when the lock is held
 * for less time than a wake-up takes (a commit holds a file lock for tens of microseconds), the waiters queue up behind
 * the wake-ups rather than behind the work, a lock convoy. Two changes remove it and keep the FIFO order:
 * <ul>
 * <li>the waiter at the HEAD of the queue, and only that one, spins on its {@code granted} flag for up to
 * {@code spinNanos} before it parks, so a hand-off to it costs a cache-line transfer instead of a wake-up;</li>
 * <li>a release that hands the lock to the head also wakes the waiter right behind it, the new head, so its wake-up
 * overlaps the new owner's work instead of following it.</li>
 * </ul>
 * The spin is adaptive: each resource keeps a moving average of how long its owners hold it, measured from the moment an
 * owner RUNS (a slow wake-up never inflates it), and the head parks at once when that average exceeds {@code spinNanos}:
 * a lock held across an fsync costs no spinning. At most one thread per contended resource spins at any time.
 * {@code spinNanos = 0}, the default, is the plain park/unpark hand-off.
 * <p>
 * Measured (#10501): the spin pays only where the hold is short CPU work. Embedded relaxed inserts into one type
 * (txWalFlush=0, 8-50 threads): +15-19% commits/s for +35-40% CPU per commit, the hand-off falling from ~6us to ~0.3us.
 * It does nothing for the Redis HSET workloads and for group commit, where the holder sits in the WAL append's
 * {@code pwrite}, blocked behind the shared flush (200-330us held), and the adaptive rule keeps the spin off. Hence off by
 * default and opt-in through {@code arcadedb.commitLockSpin}. The hand-off and hold diagnostics are kept either way.
 */
public class LockManager<RESOURCE, REQUESTER> {
  public enum LOCK_STATUS {NO, YES, ALREADY_ACQUIRED}

  // Weight of the newest sample in the moving average of the hold time, as a right shift: 1/8
  private static final int HOLD_AVERAGE_SHIFT = 3;

  private final ConcurrentHashMap<RESOURCE, ResourceLock<REQUESTER>> lockManager = new ConcurrentHashMap<>(256);
  // 0 = no spinning. Forced to 0 on a single core, where a spinning waiter can only delay the owner it waits for
  private final long                                                 spinNanos;
  // Hand-offs taken by a waiter that was spinning / that had parked (or never got to spin), and spins that ran out
  private final LongAdder                                            spinHandOffs = new LongAdder();
  private final LongAdder                                            parkHandOffs = new LongAdder();
  private final LongAdder                                            spinsExpired = new LongAdder();
  // Sum, over every hand-off, of the time from the release to the new owner running
  private final LongAdder                                            handOffNanos = new LongAdder();

  /** Lock manager with the plain park/unpark hand-off (no spin). */
  public LockManager() {
    this(0L);
  }

  /**
   * @param spinNanos how long the head waiter of a resource spins before it parks. 0 disables the spin and the early wake
   *                  of the next head, leaving the plain FIFO park/unpark hand-off
   */
  public LockManager(final long spinNanos) {
    if (spinNanos < 0)
      throw new IllegalArgumentException("Spin time cannot be negative: " + spinNanos);
    this.spinNanos = Runtime.getRuntime().availableProcessors() > 1 ? spinNanos : 0L;
  }

  public long getSpinNanos() {
    return spinNanos;
  }

  /** Hand-offs a waiter took while it was spinning: each one is an OS wake-up the spin saved. */
  public long getSpinHandOffs() {
    return spinHandOffs.sum();
  }

  /** Hand-offs a waiter took after it had parked, or without spinning at all. */
  public long getParkHandOffs() {
    return parkHandOffs.sum();
  }

  /** Spins that ran out before the hand-off came, each followed by a park: CPU the spin burnt for nothing. */
  public long getSpinsExpired() {
    return spinsExpired.sum();
  }

  /**
   * Total time the hand-offs took, from the release to the new owner running, in nanoseconds. Divided by the hand-offs
   * ({@link #getSpinHandOffs()} + {@link #getParkHandOffs()}) it is the average time a contended resource stays idle
   * between two owners.
   */
  public long getHandOffNanos() {
    return handOffNanos.sum();
  }

  /**
   * State of one resource: its current owner and the FIFO queue of waiting requesters. All fields are
   * guarded by the monitor of the instance. {@code removed} marks the instance as detached from the
   * map (freed with no waiters, or closed) so a thread that captured it just before removal retries
   * instead of operating on a stale node.
   * <p>
   * For the spin: {@code removed} is volatile so a spinning head sees a {@link #close} without the monitor;
   * {@code runningSinceNanos} is written by the owner itself, outside the monitor, once it runs (the unlock may come
   * from another thread, hence volatile); {@code averageHoldNanos}, guarded by the monitor, is the moving average of the
   * hold time, seeded by its first sample ({@code holdSampled}). A node lives only while its resource is held, so a new
   * node has no history: until its first release, its head waiter spins.
   * The hold is tracked with the spin off too, for {@link #statsSnapshot()}.
   */
  private static final class ResourceLock<R> {
    R                           owner;
    long                        when;
    final ArrayDeque<Waiter<R>> queue = new ArrayDeque<>();
    volatile boolean            removed;
    volatile long               runningSinceNanos;
    long                        averageHoldNanos;
    boolean                     holdSampled;
  }

  /**
   * One waiting requester. {@code granted} is set by the releasing thread when ownership is handed to
   * this waiter. A waiter that gives up removes itself from the queue under the monitor, so the queue
   * only ever holds live waiters. Volatile because the head waiter spins on it without the monitor.
   * {@code grantedAtNanos} is written before {@code granted}, so the waiter that reads {@code granted} sees it.
   * {@code spinning} is set while the waiter spins: a release that finds it set does not unpark the waiter, which would
   * cost the releaser a system call and leave a permit that makes the waiter's next park, anywhere, return at once. The
   * waiter clears it and reads {@code granted} once more before it parks, the release writes {@code granted} and then
   * reads {@code spinning} (all volatile): either the waiter sees the grant or the release sees it parking and unparks it.
   */
  private static final class Waiter<R> {
    final R          requester;
    final Thread     thread;
    long             grantedAtNanos;
    volatile boolean granted;
    volatile boolean spinning;

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
      // computeIfAbsent allocates a ResourceLock even when the resource is free: acceptable because this
      // is commit-level locking (once per transaction), not a tight loop - do not reuse this manager on a
      // hot path without revisiting that. Lock ordering: computeIfAbsent holds the CHM bin lock while the
      // mapping function runs and returns before we take the per-node monitor below, so the bin lock is
      // always strictly outer - no inversion. Do not move the synchronized(candidate) inside the lambda.
      final ResourceLock<REQUESTER> candidate = lockManager.computeIfAbsent(resource, k -> new ResourceLock<>());
      synchronized (candidate) {
        if (candidate.removed)
          // Captured a node that was detached from the map between computeIfAbsent and here; retry.
          // This could only spin indefinitely under continuous close() churn; close() is a one-shot
          // shutdown call, so in practice the retry resolves on the next iteration.
          continue;

        if (candidate.owner == null) {
          // Free: acquire immediately.
          candidate.owner = requester;
          candidate.when = System.currentTimeMillis();
          candidate.runningSinceNanos = System.nanoTime();
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
        final boolean spin;
        synchronized (rl) {
          // 'removed' (close) is checked before 'granted' so a waiter that wins the grant-vs-close
          // race abandons the grant and returns NO: the node is gone from the map, so it could not be
          // released anyway. (close() has already emptied the queue.)
          if (rl.removed)
            return LOCK_STATUS.NO;
          if (waiter.granted) {
            // Handed ownership by a releasing thread: we own it now and must unlock later.
            parkHandOffs.increment();
            return granted(rl, waiter);
          }
          if (interrupted || (timeout > 0 && deadlineNanos - System.nanoTime() <= 0)) {
            // Give up: leave the queue immediately so abandoned waiters never accumulate between
            // unlocks. ArrayDeque.remove is O(n), but the queue is bounded by concurrent waiters and
            // give-ups are rare under fair hand-off, so the scan is cheap for commit-level locking.
            rl.queue.remove(waiter);
            return LOCK_STATUS.NO;
          }
          // Only the head spins, and only while the owners hold the resource for less than the spin budget
          spin = spinNanos > 0 && rl.averageHoldNanos <= spinNanos && rl.queue.peekFirst() == waiter;
        }

        if (spin && spinUntilGranted(rl, waiter, timeout > 0, deadlineNanos)) {
          // Granted while spinning. 'granted' was read before 'removed', the order of the checks above: a close() that
          // lands after both reads is no different from one landing right after this method returned YES
          if (rl.removed)
            return LOCK_STATUS.NO;
          spinHandOffs.increment();
          return granted(rl, waiter);
        }

        // Park OUTSIDE the monitor so a releasing thread can hand off without blocking. A handoff (or
        // close) calls unpark; park may also return spuriously - the loop re-checks state either way.
        if (timeout > 0)
          // Math.max(1L, ...) guards the case where the deadline already passed between the check above
          // and here: parkNanos(<=0) returns immediately (correct, just an extra loop), the clamp keeps
          // the intent explicit and always parks for a positive duration.
          LockSupport.parkNanos(Math.max(1L, deadlineNanos - System.nanoTime()));
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

  /**
   * Spins until the waiter is handed the resource, the resource is closed, the spin budget or the caller's deadline
   * ({@code deadlineNanos}, only when {@code hasDeadline}) runs out, or the thread is interrupted. Returns true only when
   * handed the resource.
   */
  private boolean spinUntilGranted(final ResourceLock<REQUESTER> rl, final Waiter<REQUESTER> waiter, final boolean hasDeadline,
      final long deadlineNanos) {
    long spinEnd = System.nanoTime() + spinNanos;
    if (hasDeadline && deadlineNanos - spinEnd < 0)
      spinEnd = deadlineNanos;

    waiter.spinning = true;
    while (!waiter.granted) {
      final boolean expired = System.nanoTime() - spinEnd >= 0;
      if (expired || rl.removed || waiter.thread.isInterrupted()) {
        waiter.spinning = false;
        // A release that still saw the flag set did not unpark this thread: look at the grant once more before parking
        if (waiter.granted)
          return true;
        if (expired)
          spinsExpired.increment();
        return false;
      }
      Thread.onSpinWait();
    }
    waiter.spinning = false;
    return true;
  }

  /**
   * A waiter handed the resource calls this before it returns YES: the hold time is measured from here, when the owner
   * runs, so the time it took to wake up never counts as holding; that time is the hand-off's.
   */
  private LOCK_STATUS granted(final ResourceLock<REQUESTER> rl, final Waiter<REQUESTER> waiter) {
    final long now = System.nanoTime();
    rl.runningSinceNanos = now;
    handOffNanos.add(now - waiter.grantedAtNanos);
    return LOCK_STATUS.YES;
  }

  public void unlock(final RESOURCE resource, final REQUESTER requester) {
    if (resource == null)
      throw new IllegalArgumentException("Resource to unlock is null");

    final ResourceLock<REQUESTER> rl = lockManager.get(resource);
    if (rl == null)
      // Not held.
      return;

    Waiter<REQUESTER> next = null;
    Waiter<REQUESTER> nextHead = null;
    synchronized (rl) {
      if (rl.removed || rl.owner == null)
        // Already released by a concurrent unlock/close.
        return;

      if (!rl.owner.equals(requester))
        throw new LockException(
            "Cannot unlock resource '" + resource + "' because owner '" + rl.owner + "' <> requester '" + requester + "'");

      // Moving average of the hold time, which decides whether the head waiters spin. The first sample seeds it: a 1/8
      // weight from 0 would let the heads of a resource held across an fsync spin through its first several hand-offs
      final long now = System.nanoTime();
      final long held = now - rl.runningSinceNanos;
      if (rl.holdSampled)
        rl.averageHoldNanos += (held - rl.averageHoldNanos) >> HOLD_AVERAGE_SHIFT;
      else {
        rl.averageHoldNanos = held;
        rl.holdSampled = true;
      }

      // Hand off to the oldest waiter. The queue holds only live waiters (a waiter that gives up removes
      // itself under this same monitor), so the head is always a valid grantee.
      final Waiter<REQUESTER> head = rl.queue.pollFirst();
      if (head != null) {
        rl.owner = head.requester;
        rl.when = System.currentTimeMillis();
        head.grantedAtNanos = now;
        head.granted = true;
        if (!head.spinning)
          // A spinning head sees the grant by itself (see Waiter)
          next = head;
        if (spinNanos > 0 && rl.averageHoldNanos <= spinNanos)
          // Early wake: the waiter behind the new owner is now the head. Woken here, it is running (spinning) by the time
          // the new owner releases, so the next hand-off does not wait for a wake-up either
          nextHead = rl.queue.peekFirst();
      } else {
        // No waiters: free the resource and detach the node from the map atomically with marking it removed.
        // A tryLock arriving right after this detach computeIfAbsents a brand-new node for the same key and
        // may acquire it immediately - that is correct: it is a fresh, independent resource instance, while
        // any in-flight waiters are parked on this now-removed node and are handed off (or woken NO) here.
        rl.owner = null;
        rl.removed = true;
        lockManager.remove(resource, rl);
      }
    }

    if (next != null)
      LockSupport.unpark(next.thread);
    if (nextHead != null)
      LockSupport.unpark(nextHead.thread);
  }

  /**
   * Shutdown: frees every resource and wakes all queued waiters with {@link LOCK_STATUS#NO}. A waiter
   * that won an in-flight hand-off (its {@code granted} was set by a concurrent {@link #unlock}) just
   * before its node is detached here is <i>intentionally abandoned</i>: {@code tryLock} checks
   * {@code removed} before {@code granted}, so that waiter returns NO rather than YES. This is correct
   * for shutdown - the node is gone from the map and could never be unlocked anyway.
   */
  public void close() {
    // close() is a shutdown operation. There is a narrow window between it.remove() (detaching the node
    // from the map) and synchronized(rl) below in which a concurrent tryLock could computeIfAbsent a
    // fresh node for the same key, re-opening that resource after the sweep passes it. Acceptable for
    // shutdown: old waiters on the detached node still wake with NO; a resurrected node is not swept.
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
        for (final Waiter<REQUESTER> w : rl.queue)
          toWake.add(w.thread);
        rl.queue.clear();
      }

      // Wake waiters outside the monitor; each observes removed==true and returns NO.
      for (final Thread t : toWake)
        LockSupport.unpark(t);
    }
  }

  /**
   * Immutable diagnostic view of one held resource. {@code owner} is rendered rather than exposed so a
   * snapshot never retains a reference to a live requester (a Thread, a session).
   *
   * @param resource    the locked resource
   * @param owner       the current owner, rendered; never null in a snapshot entry
   * @param sinceMillis wall-clock time the lock was acquired
   * @param heldForMs   how long it has been held at snapshot time
   * @param waiters     number of requesters queued behind it
   * @param averageHoldMicros moving average of how long its owners held it while it stayed contended (0 when it never was)
   */
  public record LockStats(String resource, String owner, long sinceMillis, long heldForMs, int waiters, long averageHoldMicros) {
  }

  /**
   * Point-in-time view of every currently held resource, newest information first computed per entry.
   * Intended for operator diagnostics: a lock held far longer than any transaction should be is the
   * signature of a leaked lock, and this is the only way to see it without a debugger. Entries in a
   * transitional state (owner cleared concurrently) are skipped rather than rendered as free.
   */
  public List<LockStats> statsSnapshot() {
    final long now = System.currentTimeMillis();
    final List<LockStats> result = new ArrayList<>();
    for (final Map.Entry<RESOURCE, ResourceLock<REQUESTER>> entry : lockManager.entrySet()) {
      final ResourceLock<REQUESTER> rl = entry.getValue();
      synchronized (rl) {
        if (rl.removed || rl.owner == null)
          continue;
        result.add(new LockStats(String.valueOf(entry.getKey()), String.valueOf(rl.owner), rl.when, now - rl.when,
            rl.queue.size(), TimeUnit.NANOSECONDS.toMicros(rl.averageHoldNanos)));
      }
    }
    return result;
  }

  /**
   * One-line description of who currently holds {@code resource}, or {@code null} when it is free.
   * <p>
   * Exists for the timeout path: a {@code LockTimeoutException} that says only WHICH file could not be
   * locked leaves an operator with no way to identify the culprit, because the holder is knowable only
   * while the lock is still held — by the time anyone reads the log it is gone, and a leaked lock
   * survives until the process is restarted. Called only when an acquisition has already failed, so the
   * cost does not matter.
   */
  public String describeOwner(final RESOURCE resource) {
    final ResourceLock<REQUESTER> rl = lockManager.get(resource);
    if (rl == null)
      return null;

    final REQUESTER owner;
    final long when;
    final int waiters;
    synchronized (rl) {
      if (rl.removed || rl.owner == null)
        return null;
      owner = rl.owner;
      when = rl.when;
      waiters = rl.queue.size();
    }

    return "heldBy='" + owner + "' for " + (System.currentTimeMillis() - when) + "ms (since " //
        + DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(DateUtils.millisToLocalDateTime(when, null)) //
        + ", " + waiters + " waiters)";
  }

  public String toString() {
    final StringBuilder sb = new StringBuilder();
    for (final Map.Entry<RESOURCE, ResourceLock<REQUESTER>> entry : lockManager.entrySet()) {
      final ResourceLock<REQUESTER> rl = entry.getValue();
      final REQUESTER owner;
      final long when;
      final int waiters;
      final boolean removed;
      synchronized (rl) {
        owner = rl.owner;
        when = rl.when;
        waiters = rl.queue.size();
        removed = rl.removed;
      }
      // owner can be null for a resource caught in a transitional state; render it explicitly (and the
      // timestamp as N/A rather than the epoch 00:00:00.000) so operators reading diagnostics are not
      // confused: <detached> for a node already removed from the map (during shutdown), <free> otherwise.
      final String ownerLabel = owner != null ? String.valueOf(owner) : (removed ? "<detached>" : "<free>");
      sb.append("\n- '").append(entry.getKey()).append("', owner='").append(ownerLabel)
          .append("' on ")
          .append(owner != null ? DateTimeFormatter.ofPattern("HH:mm:ss.SSS").format(DateUtils.millisToLocalDateTime(when, null)) : "N/A")
          .append(" (").append(waiters).append(" waiters)");
    }
    return sb.toString();
  }
}
