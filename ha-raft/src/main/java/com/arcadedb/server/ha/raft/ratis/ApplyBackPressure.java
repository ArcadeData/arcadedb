/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft.ratis;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import org.apache.ratis.util.SizeInBytes;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ToLongFunction;

/**
 * Byte-based back-pressure on the leader towards a follower with a large Raft apply backlog (issue #11458).
 * <p>
 * Ratis's {@code SegmentedRaftLog} evicts cached entries only below {@code min(flushed, lastApplied, follower next
 * indexes)}, so a follower that has received N entries it has not applied keeps all N decoded on its heap, whatever
 * {@code arcadedb.ha.logCacheSize} says. The leader cannot see that from the replies (they carry the commit index,
 * which a follower advances on receipt), so the follower's applied index travels on the capability poll and
 * {@link FixedGrpcLogAppender} asks {@link #shouldPause} before sending entries. The backlog is estimated as
 * {@code (nextIndex - 1 - reportedApplied) * averageEntryBytes}: the reported index only ever lags the real one, so the
 * estimate errs on the side of pausing for at most one poll period, and an unknown or stale report never pauses.
 * <p>
 * One instance per leader node, found by the appender through the leader's peer id ({@link #of}) because Ratis builds
 * the appender itself.
 */
public final class ApplyBackPressure {

  /** Floor of the default budget, so a tiny heap does not stall replication on every large transaction. */
  static final long MIN_BUDGET_BYTES = 32L << 20;

  private static final ConcurrentHashMap<String, ApplyBackPressure> BY_LEADER = new ConcurrentHashMap<>();

  private final long                budgetBytes;
  private final ToLongFunction<String> reportedAppliedIndexOf;

  /**
   * @param budgetBytes            the unapplied bytes a follower may hold; {@code 0} or less disables the back-pressure
   * @param reportedAppliedIndexOf the applied index a follower reported lately, or {@code -1} when unknown or stale
   */
  public ApplyBackPressure(final long budgetBytes, final ToLongFunction<String> reportedAppliedIndexOf) {
    this.budgetBytes = budgetBytes;
    this.reportedAppliedIndexOf = reportedAppliedIndexOf;
  }

  public static void register(final String leaderPeerId, final ApplyBackPressure backPressure) {
    BY_LEADER.put(leaderPeerId, backPressure);
  }

  /** Removes {@code backPressure} only if it is still the registered one, so a stopping server cannot drop its successor's. */
  public static void unregister(final String leaderPeerId, final ApplyBackPressure backPressure) {
    BY_LEADER.remove(leaderPeerId, backPressure);
  }

  /** The back-pressure of the leader {@code leaderPeerId}, or {@code null} when none is registered. */
  public static ApplyBackPressure of(final String leaderPeerId) {
    return BY_LEADER.get(leaderPeerId);
  }

  public long getBudgetBytes() {
    return budgetBytes;
  }

  /**
   * Whether the leader should hold entries back from {@code followerId}, to which entries up to
   * {@code sentThroughIndex} were sent, given the average entry size {@code averageEntryBytes}.
   */
  public boolean shouldPause(final String followerId, final long sentThroughIndex, final long averageEntryBytes) {
    if (budgetBytes <= 0)
      return false;
    return exceedsBudget(sentThroughIndex, reportedAppliedIndexOf.applyAsLong(followerId), averageEntryBytes, budgetBytes);
  }

  /** Pure form of {@link #shouldPause}. Division, not multiplication, so a huge backlog cannot overflow into a "no". */
  static boolean exceedsBudget(final long sentThroughIndex, final long appliedIndex, final long averageEntryBytes,
      final long budgetBytes) {
    if (appliedIndex < 0 || averageEntryBytes <= 0 || budgetBytes <= 0)
      return false;
    final long backlogEntries = sentThroughIndex - appliedIndex;
    return backlogEntries > 0 && backlogEntries > budgetBytes / averageEntryBytes;
  }

  /** Exponential moving average (weight 1/8 for the newest batch) of the entry size; the batch's own average seeds it. */
  static long nextAverage(final long previousAverage, final long batchBytes, final int batchEntries) {
    if (batchEntries <= 0 || batchBytes <= 0)
      return previousAverage;
    final long batchAverage = batchBytes / batchEntries;
    return previousAverage <= 0 ? batchAverage : previousAverage + (batchAverage - previousAverage) / 8;
  }

  /** {@code arcadedb.ha.applyBacklogMax}: empty means a quarter of the heap (at least {@link #MIN_BUDGET_BYTES}), 0 disables. */
  public static long resolveBudgetBytes(final ContextConfiguration configuration, final long maxHeapBytes) {
    final String value = configuration.getValueAsString(GlobalConfiguration.HA_APPLY_BACKLOG_MAX);
    if (value == null || value.isBlank())
      return Math.max(MIN_BUDGET_BYTES, maxHeapBytes / 4);
    final long bytes;
    try {
      bytes = SizeInBytes.valueOf(value.trim()).getSize();
    } catch (final IllegalArgumentException e) {
      throw new ConfigurationException("arcadedb.ha.applyBacklogMax (" + value + ") is not a size such as '256MB'", e);
    }
    return Math.max(0L, bytes);
  }
}
