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
package com.arcadedb.bench.workload.ramp;

import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.runner.Progress;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntConsumer;

/**
 * An open-loop driver: arrivals on a fixed schedule at a given rate, each served by one of a fixed pool of clients.
 * <ul>
 *   <li>Arrival {@code i} of a step is due at {@code start + i / rate}, computed from {@code i} rather than accumulated, so a
 *   scheduler that falls behind issues the overdue arrivals at once and never drifts: the number of arrivals in a step comes
 *   from the schedule, not from the pace the scheduler managed. An arrival started more than 1 ms after it was due counts as a
 *   late start (a client-capacity signal for the caller, not a server verdict).</li>
 *   <li>No coordinated omission: an arrival's latency is its completion time minus its <em>scheduled</em> time, so the time it
 *   spent behind a late scheduler or a queued executor is part of it.</li>
 *   <li>An arrival that finds no free client is a drop, never queued silently.</li>
 *   <li>Warm traffic runs at the step's rate before the counted window and is not counted.</li>
 *   <li>A request (warm or counted) still running 30 s after the step's last arrival is reported as {@code unfinished} and
 *   breaks the step. It keeps its client, so the next step first waits (up to another 30 s) for every earlier request to give
 *   its client back: a step never starts with fewer clients and never records drops that belong to an earlier step.</li>
 * </ul>
 * The scheduler takes no lock: clients are claimed and released by compare-and-set, latencies land in a per-step
 * {@link AtomicLongArray} indexed by arrival, and errors in a {@link LongAdder}. Every completion counts as {@link Progress}
 * for the stall watchdog. The operation receives the index of the client
 * it runs on, in {@code [0, clients)}; the executor decides where it runs.
 */
public final class OpenLoop {
  public static final  double P99_LIMIT_MS      = 50.0;
  /**
   * The error ratio a step may reach without breaking: errors above 0.1% of the step's arrivals break it.
   */
  public static final  double ERROR_LIMIT       = 0.001;
  private static final long   LATE_START_NANOS  = 1_000_000L;
  private static final long   DRAIN_POLL_NANOS  = 1_000_000L;
  private static final long   DRAIN_TIMEOUT_SECONDS = 30;
  private static final long   DRAIN_TIMEOUT     = Duration.ofSeconds(DRAIN_TIMEOUT_SECONDS).toNanos();
  private static final long   NOT_COMPLETED     = -1L;
  private static final double NANOS_PER_SECOND  = 1_000_000_000.0;
  private static final double NANOS_PER_MS      = 1_000_000.0;

  /**
   * The time source of the driver. {@link #SYSTEM} in production; tests use a fake clock that moves only when parked.
   */
  public interface Clock {
    long nanos();

    /**
     * Returns once {@link #nanos()} has reached {@code nanos}, at once when it already has.
     */
    void parkUntil(long nanos);
  }

  /**
   * The longest single park of {@link #SYSTEM}. A park of a few milliseconds is coalesced by the OS timer (measured on macOS:
   * 70-80% of 5-10 ms parks woke more than 1 ms late), which would read as late starts at a low rate; parks of at most 500 us
   * woke late on 0.2-0.4% of the arrivals, for 3-7% of a core.
   */
  private static final long MAX_PARK_NANOS = 500_000L;

  public static final Clock SYSTEM = new Clock() {
    @Override
    public long nanos() {
      return System.nanoTime();
    }

    @Override
    public void parkUntil(final long nanos) {
      long remaining;
      while ((remaining = nanos - System.nanoTime()) > 0 && !Thread.currentThread().isInterrupted())
        LockSupport.parkNanos(Math.min(remaining, MAX_PARK_NANOS));
    }
  };

  /**
   * One step's outcome. {@code arrivals} counts every arrival of the counted window, dropped ones included; the percentiles
   * are over the arrivals that completed without an error.
   */
  public record StepResult(double ratePerS, long arrivals, long errors, long dropped, long lateStarts, double p99Ms,
                           double p50Ms, long unfinished) {
    /**
     * A step that drained completely.
     */
    public StepResult(final double ratePerS, final long arrivals, final long errors, final long dropped, final long lateStarts,
        final double p99Ms, final double p50Ms) {
      this(ratePerS, arrivals, errors, dropped, lateStarts, p99Ms, p50Ms, 0);
    }

    /**
     * The break rule: p99 over 50 ms, errors over 0.1% of the arrivals, any dropped arrival, or any request (warm included)
     * still unfinished at the drain deadline. When nothing completed, p99 and p50 are 0: the errors or drops break the step.
     */
    public boolean broke() {
      return p99OverLimit() || errorsOverLimit() || dropped > 0 || unfinished > 0;
    }

    /**
     * Why the step broke, the reasons separated by {@code "; "}; empty when it did not.
     */
    public String why() {
      final StringBuilder why = new StringBuilder();
      if (p99OverLimit())
        why.append(String.format(Locale.ROOT, "p99 %.1f ms > %.0f ms", p99Ms, P99_LIMIT_MS));
      if (errorsOverLimit())
        append(why, String.format(Locale.ROOT, "errors %d of %d arrivals > %.1f%%", errors, arrivals, ERROR_LIMIT * 100));
      if (dropped > 0)
        append(why, String.format(Locale.ROOT, "dropped %d arrivals (no free client)", dropped));
      if (unfinished > 0)
        append(why, String.format(Locale.ROOT, "%d arrivals unfinished after %d s", unfinished, DRAIN_TIMEOUT_SECONDS));
      return why.toString();
    }

    /** Whether the step's p99 is over the 50 ms limit. */
    public boolean p99OverLimit() {
      return p99Ms > P99_LIMIT_MS;
    }

    /** Whether the step's errors are over 0.1% of its arrivals. */
    public boolean errorsOverLimit() {
      return errors > arrivals * ERROR_LIMIT;
    }

    private static void append(final StringBuilder why, final String reason) {
      if (!why.isEmpty())
        why.append("; ");
      why.append(reason);
    }
  }

  private final Clock              clock;
  private final int                clients;
  private final IntConsumer        op;
  private final Executor           executor;
  private final AtomicIntegerArray busy;
  /**
   * Requests in flight across steps: a request left running by an earlier step still holds its client.
   */
  private final AtomicInteger      outstanding = new AtomicInteger();
  private       int                nextClient;

  public OpenLoop(final Clock clock, final int clients, final IntConsumer op, final Executor executor) {
    if (clients < 1)
      throw new IllegalArgumentException("clients must be at least 1, got " + clients);
    this.clock = clock;
    this.clients = clients;
    this.op = op;
    this.executor = executor;
    this.busy = new AtomicIntegerArray(clients);
  }

  /**
   * Waits for any request an earlier step left running (up to 30 s, then {@link IllegalStateException}), runs {@code warm} of
   * uncounted traffic and then {@code length} of counted traffic at {@code ratePerS}, waits for the requests in flight (up to
   * 30 s, the ones still running are reported as {@code unfinished}) and returns the counted window's result. Throws
   * {@link CancellationException} when the calling thread is interrupted.
   */
  public StepResult runStep(final double ratePerS, final Duration warm, final Duration length) {
    if (!(ratePerS > 0))
      throw new IllegalArgumentException("rate must be positive, got " + ratePerS);
    final long warmArrivals = Math.round(warm.toNanos() * ratePerS / NANOS_PER_SECOND);
    final long countedArrivals = Math.round(length.toNanos() * ratePerS / NANOS_PER_SECOND);
    if (countedArrivals > Integer.MAX_VALUE)
      throw new IllegalArgumentException("too many arrivals in one step: " + countedArrivals);

    awaitIdle(clock.nanos() + DRAIN_TIMEOUT);
    if (outstanding.get() > 0)
      throw new IllegalStateException(
          outstanding.get() + " requests of an earlier step still hold their clients after " + DRAIN_TIMEOUT_SECONDS + " s");

    final Step step = new Step((int) countedArrivals);
    final double nanosPerArrival = NANOS_PER_SECOND / ratePerS;
    final long total = warmArrivals + countedArrivals;
    final long start = clock.nanos();
    long dropped = 0;
    long lateStarts = 0;

    for (long i = 0; i < total; i++) {
      if (Thread.currentThread().isInterrupted())
        throw new CancellationException("open-loop step interrupted");
      final long due = start + (long) (i * nanosPerArrival);
      clock.parkUntil(due);
      final int slot = i < warmArrivals ? -1 : (int) (i - warmArrivals);
      if (slot >= 0 && clock.nanos() - due > LATE_START_NANOS)
        lateStarts++;

      final int client = takeClient();
      if (client < 0) {
        if (slot >= 0)
          dropped++;
        continue;
      }
      step.inFlight.incrementAndGet();
      outstanding.incrementAndGet();
      try {
        executor.execute(() -> serve(step, client, due, slot));
      } catch (final RejectedExecutionException e) {
        unclaim(step, client);
        if (slot >= 0)
          dropped++;
      } catch (final RuntimeException e) {
        unclaim(step, client);
        throw e;
      }
    }

    final long drainDeadline = clock.nanos() + DRAIN_TIMEOUT;
    while (step.inFlight.get() > 0 && clock.nanos() < drainDeadline) {
      if (Thread.currentThread().isInterrupted())
        throw new CancellationException("open-loop step interrupted while draining");
      clock.parkUntil(clock.nanos() + DRAIN_POLL_NANOS);
    }
    final long unfinished = step.inFlight.get();

    final long[] latencies = new long[step.latencies.length()];
    int completed = 0;
    for (int k = 0; k < latencies.length; k++) {
      final long latency = step.latencies.get(k);
      if (latency != NOT_COMPLETED)
        latencies[completed++] = latency;
    }
    Arrays.sort(latencies, 0, completed);
    return new StepResult(ratePerS, countedArrivals, step.errors.sum(), dropped, lateStarts,
        LatencyRecorder.percentileOfSorted(latencies, completed, 99) / NANOS_PER_MS,
        LatencyRecorder.percentileOfSorted(latencies, completed, 50) / NANOS_PER_MS, unfinished);
  }

  /**
   * The clients currently held by a request, of this step or an earlier one.
   */
  public int busyClients() {
    int held = 0;
    for (int i = 0; i < clients; i++)
      held += busy.get(i);
    return held;
  }

  private void awaitIdle(final long deadline) {
    while (outstanding.get() > 0 && clock.nanos() < deadline) {
      if (Thread.currentThread().isInterrupted())
        throw new CancellationException("open-loop step interrupted while waiting for an earlier step's requests");
      clock.parkUntil(clock.nanos() + DRAIN_POLL_NANOS);
    }
  }

  private void unclaim(final Step step, final int client) {
    busy.set(client, 0);
    step.inFlight.decrementAndGet();
    outstanding.decrementAndGet();
  }

  private void serve(final Step step, final int client, final long due, final int slot) {
    try {
      op.accept(client);
      if (slot >= 0)
        step.latencies.set(slot, clock.nanos() - due);
    } catch (final Throwable t) {
      // an Error too (AssertionError, NoClassDefFoundError, OutOfMemoryError): one swallowed silently would count neither a
      // latency nor an error, so a step whose every request threw one would never break
      if (slot >= 0)
        step.errors.increment();
    } finally {
      busy.set(client, 0);
      Progress.advance();
      step.inFlight.decrementAndGet();
      outstanding.decrementAndGet();
    }
  }

  /**
   * Claims a free client, scanning from the one after the last claimed; -1 when every client is busy. Called only by the
   * scheduler thread, so {@code nextClient} needs no synchronization.
   */
  private int takeClient() {
    for (int j = 0; j < clients; j++) {
      final int candidate = (nextClient + j) % clients;
      if (busy.compareAndSet(candidate, 0, 1)) {
        nextClient = candidate + 1 == clients ? 0 : candidate + 1;
        return candidate;
      }
    }
    return -1;
  }

  private static final class Step {
    final AtomicLongArray latencies;
    final LongAdder       errors   = new LongAdder();
    final AtomicInteger   inFlight = new AtomicInteger();

    Step(final int countedArrivals) {
      latencies = new AtomicLongArray(countedArrivals);
      for (int k = 0; k < countedArrivals; k++)
        latencies.setPlain(k, NOT_COMPLETED);
    }
  }
}
