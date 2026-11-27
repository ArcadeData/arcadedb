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

import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import java.util.function.LongUnaryOperator;

/**
 * A discrete-event clock for the open-loop tests: time moves only when the scheduler parks or an inline operation calls
 * {@link #advance(long)}, and a task handed to {@link #delayingExecutor(LongUnaryOperator)} runs when the clock passes its
 * completion time, so an operation "takes" its duration without any thread sleeping.
 */
final class FakeClock implements OpenLoop.Clock {
  private record Event(long at, long seq, Runnable task) {
  }

  private final PriorityQueue<Event> events = new PriorityQueue<>(
      (a, b) -> a.at != b.at ? Long.compare(a.at, b.at) : Long.compare(a.seq, b.seq));
  private       long                 now;
  private       long                 seq;

  @Override
  public long nanos() {
    return now;
  }

  @Override
  public void parkUntil(final long nanos) {
    while (!events.isEmpty() && events.peek().at <= nanos) {
      final Event event = events.poll();
      now = Math.max(now, event.at);
      event.task.run();
    }
    now = Math.max(now, nanos);
  }

  /**
   * Moves the clock forward without running any pending event: an inline operation that blocks its caller for that long.
   */
  void advance(final long nanos) {
    now += nanos;
  }

  /**
   * An executor whose n-th submitted task (0-based) runs when the clock reaches its submission time plus
   * {@code durationOfSubmission.applyAsLong(n)}.
   */
  Executor delayingExecutor(final LongUnaryOperator durationOfSubmission) {
    final long[] submitted = new long[1];
    return task -> events.add(new Event(now + durationOfSubmission.applyAsLong(submitted[0]++), seq++, task));
  }
}
