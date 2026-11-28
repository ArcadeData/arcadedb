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
package com.arcadedb.bench.workload.mixed;

import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.remote.RemoteException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs one operation repeatedly until {@code stop} is set or the operation fails. A conflict (a
 * {@link ConcurrentModificationException}, a {@link NeedRetryException} or a {@link RemoteException} naming one of them, which
 * is what the remote client throws) is retried, up to {@value #MAX_TRIES} tries in all. The latency of a success covers every
 * try. Any other error, or a conflict on the last try, ends the loop and is kept as the failure.
 */
final class OperationLoop implements Runnable {
  static final int MAX_TRIES = 4;

  private static final long PAUSE_SLICE_MS = 50L;

  /**
   * One unit of work.
   */
  interface Operation {
    void run() throws Exception;
  }

  private final String          name;
  private final Operation       op;
  private final long            pauseMillis;
  private final AtomicBoolean   stop;
  private final CommitGaps      gaps;
  private final boolean         commits;
  private final LatencyRecorder latencies;

  private volatile long      completed;
  private volatile long      retries;
  private volatile Throwable failure;

  OperationLoop(final String name, final Operation op, final long pauseMillis, final AtomicBoolean stop, final CommitGaps gaps,
      final boolean commits, final int capacity) {
    this.name = name;
    this.op = op;
    this.pauseMillis = pauseMillis;
    this.stop = stop;
    this.gaps = gaps;
    this.commits = commits;
    this.latencies = new LatencyRecorder(capacity);
  }

  @Override
  public void run() {
    while (!stop.get() && failure == null) {
      final long begin = System.nanoTime();
      int tries = 0;
      while (true) {
        tries++;
        try {
          op.run();
          break;
        } catch (final Throwable e) {
          if (stop.get() && isInterrupt(e)) {
            Thread.currentThread().interrupt();
            return;
          }
          if (tries >= MAX_TRIES || !isConflict(e)) {
            failure = e;
            return;
          }
          // noopengrep: volatile-read-modify-write - written only by the loop's own thread, other threads only read it
          retries++;
        }
      }
      final long end = System.nanoTime();
      latencies.record(end - begin);
      // noopengrep: volatile-read-modify-write - written only by the loop's own thread, other threads only read it
      completed++;
      if (commits)
        gaps.committed(end);
      if (pauseMillis > 0 && !pause())
        return;
    }
  }

  /**
   * Sleeps the pause in slices of at most {@value #PAUSE_SLICE_MS} ms, so a loop between two operations ends soon after
   * {@code stop} is set instead of sleeping out the rest of its pause. False when interrupted.
   */
  private boolean pause() {
    final long until = System.nanoTime() + pauseMillis * 1_000_000L;
    try {
      while (!stop.get()) {
        final long left = until - System.nanoTime();
        if (left <= 0)
          break;
        Thread.sleep(Math.min(PAUSE_SLICE_MS, left / 1_000_000L + 1));
      }
      return true;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private static boolean isInterrupt(final Throwable e) {
    if (Thread.currentThread().isInterrupted())
      return true;
    for (Throwable t = e; t != null; t = t.getCause())
      if (t instanceof InterruptedException)
        return true;
    return false;
  }

  private static boolean isConflict(final Throwable e) {
    if (e instanceof NeedRetryException)
      return true;
    if (e instanceof RemoteException) {
      final String message = e.getMessage();
      return message != null && (message.contains("ConcurrentModificationException") || message.contains("NeedRetryException"));
    }
    return false;
  }

  String name() {
    return name;
  }

  /**
   * The recorded latencies; read them only after the loop thread has been joined.
   */
  LatencyRecorder latencies() {
    return latencies;
  }

  long completed() {
    return completed;
  }

  long retries() {
    return retries;
  }

  Throwable failure() {
    return failure;
  }
}
