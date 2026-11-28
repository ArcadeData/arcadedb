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
package com.arcadedb.bench.workload.ha;

import com.arcadedb.bench.runner.LatencyRecorder;

/**
 * The replication lag seen on one follower. Ids are the positions of an array of the leader's acknowledgement times
 * ({@link System#nanoTime()}, {@link #NOT_ACKED} for a write that threw); a poll of the follower reports the highest id it shows,
 * and every id up to it not seen before is seen at the time the caller stamps the poll with (the ha workload stamps the
 * time the poll was sent, so the response leg is no lag). The lag of an id is when it was first seen minus when the leader
 * acknowledged it, clamped at 0: a follower may show an id before the leader's answer reaches the client.
 * <p>
 * {@link #seen} only stores the times, so it may run while the writer is still filling the acknowledgement array; {@link #lags}
 * reads the array and belongs after the writer is done.
 */
public final class LagMatcher {
  /** The acknowledgement time of an id whose write threw: it has no lag. */
  public static final long NOT_ACKED = Long.MIN_VALUE;

  private final long[] ackNanosById;
  private final long[] seenNanosById;
  // the ids below this one have been seen
  private int          seenUpTo;

  public LagMatcher(final long[] ackNanosById) {
    this.ackNanosById = ackNanosById;
    this.seenNanosById = new long[ackNanosById.length];
  }

  /** The follower shows every id up to {@code upToId} at {@code nowNanos}; ids past the array's end are ignored. */
  public void seen(final long upToId, final long nowNanos) {
    final int last = (int) Math.min(upToId, seenNanosById.length - 1L);
    for (int i = seenUpTo; i <= last; i++)
      seenNanosById[i] = nowNanos;
    if (last + 1 > seenUpTo)
      seenUpTo = last + 1;
  }

  /** The lag of every acknowledged id seen so far, in nanoseconds. */
  public LatencyRecorder lags() {
    final LatencyRecorder lags = new LatencyRecorder(seenUpTo);
    for (int i = 0; i < seenUpTo; i++)
      if (ackNanosById[i] != NOT_ACKED)
        lags.record(Math.max(0L, seenNanosById[i] - ackNanosById[i]));
    return lags;
  }
}
