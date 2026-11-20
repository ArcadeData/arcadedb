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
 * indexes)}, so a follower that has received N entries it has not applied keeps all N on its heap, whatever
 * {@code arcadedb.ha.logCacheSize} says. The leader cannot see that from the replies (they carry the commit index,
 * which a follower advances on receipt), so the follower's applied index travels on the capability poll and
 * {@link FixedGrpcLogAppender} asks {@link #shouldPause} before sending entries. The backlog is estimated as
 * {@code (nextIndex - 1 - reportedApplied) * averageEntryBytes}: the reported index only ever lags the real one, so the
 * estimate errs on the side of pausing for at most one poll period, and an unknown or stale report never pauses.
 * <p>
 * The entry size is the serialized size of the {@code LogEntryProto}. That is what the follower's log cache retains (the
 * proto itself, whose payload is a {@code ByteString} of the wire bytes), so it is a fair measure of the retained cost and
 * not a decoded form that could be several times larger.
 * <p>
 * <b>The heap being protected is the follower's, so the budget is the follower's too</b> (issue #11508). Each node
 * advertises the budget it resolved for itself ({@link #resolveBudgetBytes} over its own heap and setting) on the same
 * capability poll, and a follower is held back at the smaller of this leader's budget and the one it advertised
 * ({@link #effectiveBudgetBytes}). A follower that advertises none (a build that predates the field, or a node that set
 * the budget to 0) is bound by the leader's alone, as every follower was before.
 * <p>
 * One instance per leader node, found by the appender through the leader's peer id ({@link #of}) because Ratis builds
 * the appender itself.
 */
public final class ApplyBackPressure {

  /** Floor of the default budget, so a tiny heap does not stall replication on every large transaction. */
  static final long MIN_BUDGET_BYTES = 32L << 20;

  private static final ConcurrentHashMap<String, ApplyBackPressure> BY_LEADER = new ConcurrentHashMap<>();

  private final long                   budgetBytes;
  private final ToLongFunction<String> reportedAppliedIndexOf;
  private final ToLongFunction<String> advertisedBudgetBytesOf;

  /**
   * As {@link #ApplyBackPressure(long, ToLongFunction, ToLongFunction)} with no follower advertising a budget: every
   * follower is bound by {@code budgetBytes}.
   */
  public ApplyBackPressure(final long budgetBytes, final ToLongFunction<String> reportedAppliedIndexOf) {
    this(budgetBytes, reportedAppliedIndexOf, follower -> -1L);
  }

  /**
   * @param budgetBytes             the unapplied bytes this node lets a follower hold, and the budget it advertises for
   *                                itself; {@code 0} or less disables the back-pressure this node applies as a leader
   * @param reportedAppliedIndexOf  the applied index a follower reported lately, or {@code -1} when unknown or stale
   * @param advertisedBudgetBytesOf the budget a follower advertised lately for its own heap, or {@code 0} or less when
   *                                it advertised none, or the answer is unknown or stale (issue #11508)
   */
  public ApplyBackPressure(final long budgetBytes, final ToLongFunction<String> reportedAppliedIndexOf,
      final ToLongFunction<String> advertisedBudgetBytesOf) {
    this.budgetBytes = budgetBytes;
    this.reportedAppliedIndexOf = reportedAppliedIndexOf;
    this.advertisedBudgetBytesOf = advertisedBudgetBytesOf;
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

  /** The budget this node resolved for itself: what it applies as a leader, and what it advertises as a follower. */
  public long getBudgetBytes() {
    return budgetBytes;
  }

  /**
   * The budget {@code followerId} is held to: the smaller of this leader's and the one the follower advertised for its own
   * heap, or this leader's alone when the follower advertised none (issue #11508). {@code 0} when this leader disabled
   * the back-pressure, whatever the follower says: the setting is the operator's way to switch it off on this node.
   */
  public long effectiveBudgetBytes(final String followerId) {
    if (budgetBytes <= 0)
      return 0L;
    return smallerBudget(budgetBytes, advertisedBudgetBytesOf.applyAsLong(followerId));
  }

  /** Pure form of {@link #effectiveBudgetBytes} for an enabled leader: a follower figure of 0 or less is no figure. */
  static long smallerBudget(final long leaderBudgetBytes, final long followerBudgetBytes) {
    return followerBudgetBytes > 0 && followerBudgetBytes < leaderBudgetBytes ? followerBudgetBytes : leaderBudgetBytes;
  }

  /**
   * Whether the leader should hold entries back from {@code followerId}, to which entries up to
   * {@code sentThroughIndex} were sent, given the average entry size {@code averageEntryBytes}.
   */
  public boolean shouldPause(final String followerId, final long sentThroughIndex, final long averageEntryBytes) {
    if (budgetBytes <= 0)
      return false;
    final long appliedIndex = reportedAppliedIndexOf.applyAsLong(followerId);
    // Nothing to compare when the follower reported no applied index, so its budget is not looked up either
    if (appliedIndex < 0)
      return false;
    return exceedsBudget(sentThroughIndex, appliedIndex, averageEntryBytes,
        smallerBudget(budgetBytes, advertisedBudgetBytesOf.applyAsLong(followerId)));
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

  /**
   * {@code arcadedb.ha.applyBacklogMax}: empty means a quarter of the heap (at least {@link #MIN_BUDGET_BYTES}), 0 disables.
   * Resolved by every node over its OWN heap: it is both what the node applies as a leader and what it advertises as a
   * follower (issue #11508).
   */
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
