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
package com.arcadedb.server.http.handler;

import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;

import java.util.concurrent.TimeUnit;

/**
 * Schedules a delayed task on an XNIO I/O thread from any thread, without the lost-wakeup window of a plain
 * {@link XnioIoThread#executeAfter} called from another thread (issues #9216 and #9439).
 * <p>
 * XNIO 3.8 ({@code WorkerThread}) wakes the selector for a new delayed task only when the I/O thread is already
 * {@code polling}, and after it sets that flag its run loop re-checks only the IMMEDIATE work queue, not the delay
 * queue. A delayed task added from another thread while the I/O thread sits between computing its next wait and
 * setting the flag is therefore not seen: the selector sleeps on the stale wait - indefinitely when nothing else is
 * queued - and the task fires late or never. An immediate task ({@link XnioIoThread#execute}) has no such window, and
 * a delayed task added ON the I/O thread is seen by the next wait it computes. So a call from another thread hands the
 * {@code executeAfter} to the I/O thread through {@code execute}; a call already on the I/O thread schedules directly.
 * <p>
 * Every user of the I/O thread's timers in the HTTP layer goes through here: the relay's silence bound
 * ({@code PostBatchHandler}), the write watchdog of every streamed response ({@link WriteBoundedOutputStream}) and the
 * NDJSON keep-alive ({@link NdJsonKeepAlive}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class IoThreadTimer {
  private IoThreadTimer() {
  }

  /**
   * Runs {@code task} on {@code ioThread} after {@code delayMs}. From another thread the delay starts when the I/O
   * thread runs the hand-over, not at this call: a skew of one I/O-thread turn, nothing next to a watchdog's budget.
   *
   * @return the cancel of the task: idempotent, callable from any thread, and harmless after the task fired. A cancel
   *     that completes before the task fires guarantees it never runs, even when it races the hand-over to the I/O
   *     thread.
   *
   * @throws java.util.concurrent.RejectedExecutionException when the I/O thread is shutting down, as
   *                                                         {@code executeAfter} itself does
   */
  static Runnable schedule(final XnioIoThread ioThread, final Runnable task, final long delayMs) {
    if (Thread.currentThread() == ioThread) {
      final XnioExecutor.Key key = ioThread.executeAfter(task, delayMs, TimeUnit.MILLISECONDS);
      return key::remove;
    }
    final HandedOver handedOver = new HandedOver(ioThread, task, delayMs);
    ioThread.execute(handedOver);
    return handedOver::cancel;
  }

  /**
   * The hand-over task, run on the I/O thread, and the state its cancel shares with it. The two sides publish and then
   * check each other's flag through volatiles - the hand-over writes {@link #key} then reads {@link #cancelled}, the
   * cancel writes {@link #cancelled} then reads {@link #key} - so at least one of them sees the other: a cancel racing
   * the hand-over always removes the task. {@code Key.remove()} on a task already removed or already fired does
   * nothing, so both removing it is harmless.
   */
  private static final class HandedOver implements Runnable {
    private final    XnioIoThread     ioThread;
    private final    Runnable         task;
    private final    long             delayMs;
    private volatile XnioExecutor.Key key;
    private volatile boolean          cancelled;

    private HandedOver(final XnioIoThread ioThread, final Runnable task, final long delayMs) {
      this.ioThread = ioThread;
      this.task = task;
      this.delayMs = delayMs;
    }

    @Override
    public void run() {
      if (cancelled)
        return;
      final XnioExecutor.Key scheduled = ioThread.executeAfter(task, delayMs, TimeUnit.MILLISECONDS);
      key = scheduled;
      if (cancelled)
        scheduled.remove();
    }

    // Not private: it is reached only through the method reference schedule() returns, which PMD does not see as a use
    void cancel() {
      cancelled = true;
      final XnioExecutor.Key scheduled = key;
      if (scheduled != null)
        scheduled.remove();
    }
  }
}
