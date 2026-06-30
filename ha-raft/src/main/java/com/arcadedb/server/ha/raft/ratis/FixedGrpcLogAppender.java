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

import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIndex;

import java.lang.reflect.Field;

/**
 * Drop-in replacement for Apache Ratis 3.2.2's {@link GrpcLogAppender} that fixes
 * <a href="https://issues.apache.org/jira/browse/RATIS-2523">RATIS-2523</a>: on an idle cluster,
 * when a follower restarts with empty Raft storage, the leader's appender keeps emitting
 * {@code received INCONSISTENCY reply with nextIndex 0 ... entriesCount=0} every ~5 s and never
 * advances. The cluster is functionally broken until any user transaction generates a real
 * AppendEntries.
 * <p>
 * Root cause is in {@code LogAppenderBase.getNextIndexForInconsistency}: for a heartbeat
 * ({@code requestFirstIndex == RaftLog.INVALID_LOG_INDEX}) the existing logic prefers the
 * leader's stale {@code matchIndex + 1} over the follower's truthful "I have nothing,
 * nextIndex=0" hint, leaving the appender stuck.
 * <p>
 * This subclass overrides {@code getNextIndexForInconsistency} narrowly: when the request is a
 * heartbeat and the follower's reply hint is at or below the leader's tracked
 * {@code matchIndex}, trust the follower's hint instead. That signals "I lost state since you
 * last heard from me" and the leader's {@code matchIndex} for that follower is therefore stale.
 * For real AppendEntries requests we delegate to the upstream method unchanged.
 * <p>
 * Once RATIS-2523 ships in an Apache Ratis release, drop this class, drop
 * {@link FixedGrpcRpcType} / {@link FixedGrpcFactory}, and revert
 * {@code RaftPropertiesBuilder} to the stock {@code SupportedRpcType.GRPC}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FixedGrpcLogAppender extends GrpcLogAppender {

  /**
   * Cached, validated reflective handle on {@code FollowerInfoImpl.matchIndex}. Resolved once
   * (fail-loud) at the first appender construction and reused for every rewind. {@code volatile}
   * for safe publication across the appender threads.
   */
  private static volatile Field matchIndexField;

  public FixedGrpcLogAppender(final RaftServer.Division server, final LeaderState leaderState, final FollowerInfo f) {
    super(server, leaderState, f);
    // Fail-loud startup self-check: verify the Ratis-internal matchIndex field is still present
    // and of the expected type. If a Ratis upgrade renamed/retyped the field, fail now (loudly)
    // instead of silently no-op'ing the RATIS-2523 workaround later and re-exposing the bug.
    resolveMatchIndexField(f.getClass());
  }

  @Override
  protected long getNextIndexForInconsistency(final long requestFirstIndex, final long replyNextIndex) {
    final long currentMatchIndex = getFollower().getMatchIndex();
    if (requestFirstIndex == RaftLog.INVALID_LOG_INDEX
        && replyNextIndex <= currentMatchIndex) {
      // RATIS-2523: heartbeat path with a follower whose hint is at or below our recorded
      // matchIndex. Our matchIndex is stale (the follower restarted with empty storage and we
      // never observed a SUCCESS that would have rolled it back). Setting only nextIndex back
      // to replyNextIndex is not enough: the SUCCESS handler uses updateMatchIndex which
      // monotonically takes the max, so the stale matchIndex never moves down and SUCCESS
      // skips the corresponding updateNextIndex. The appender then loops at the SUCCESS layer
      // instead of the INCONSISTENCY layer.
      //
      // Rewind matchIndex back to (replyNextIndex - 1) via reflection on FollowerInfoImpl. The
      // public FollowerInfo API only offers setSnapshotIndex which also rewrites snapshotIndex
      // and risks confusing Ratis's snapshot-install detection; we want the surgical reset.
      // The rewind is an atomic compare-and-set against the value we just observed, so a
      // concurrent SUCCESS reply that legitimately raised matchIndex wins and we never push it
      // backwards. Once RATIS-2523 ships, drop this whole subclass.
      forceMatchIndex(getFollower(), currentMatchIndex, replyNextIndex - 1);
      return replyNextIndex;
    }
    return super.getNextIndexForInconsistency(requestFirstIndex, replyNextIndex);
  }

  /**
   * Surgically rewinds {@code FollowerInfoImpl.matchIndex} backwards. The public
   * {@link FollowerInfo#updateMatchIndex} method only takes the max, so to undo a stale value we
   * read the private {@code matchIndex} {@link RaftLogIndex} field and rewrite it atomically. A
   * Ratis layout change throws (the field was already validated at construction time, so this
   * only fires on a genuinely unexpected runtime state) rather than silently degrading.
   *
   * @param observedMatchIndex the matchIndex value observed by the caller; the rewind only
   *                           applies if matchIndex is still that value (compare-and-set)
   */
  private static void forceMatchIndex(final FollowerInfo follower, final long observedMatchIndex, final long newMatchIndex) {
    try {
      final RaftLogIndex idx = (RaftLogIndex) resolveMatchIndexField(follower.getClass()).get(follower);
      rewindMatchIndex(idx, observedMatchIndex, newMatchIndex);
    } catch (final ReflectiveOperationException | ClassCastException e) {
      throw new IllegalStateException("RATIS-2523 workaround: cannot rewind matchIndex for follower " + follower.getName()
          + "; the Ratis internal layout changed. Update or remove FixedGrpcLogAppender.", e);
    }
  }

  /**
   * Atomically rewinds {@code matchIndex} to {@code newMatchIndex}, but only if it is still the
   * {@code observedMatchIndex} the caller saw. Folding the guard into the atomic read-modify-write
   * keeps the operation race-free against Ratis's concurrent {@code updateMatchIndex} (which takes
   * the max): a SUCCESS reply that raised matchIndex above the observed value wins and this call
   * no-ops, preserving Ratis's monotonic-matchIndex invariant.
   * <p>
   * Package-private for direct unit testing of the concurrency-sensitive logic.
   */
  static void rewindMatchIndex(final RaftLogIndex matchIndex, final long observedMatchIndex, final long newMatchIndex) {
    if (newMatchIndex < RaftLog.INVALID_LOG_INDEX)
      throw new IllegalArgumentException(
          "RATIS-2523 workaround: refusing to set matchIndex below " + RaftLog.INVALID_LOG_INDEX + " (got " + newMatchIndex + ")");

    matchIndex.updateUnconditionally(old -> old == observedMatchIndex ? newMatchIndex : old, msg -> {
    });
  }

  /**
   * Resolves and validates the reflective handle on {@code FollowerInfoImpl.matchIndex}, caching
   * the result. Throws {@link IllegalStateException} (fail-loud) if the field is missing or is not
   * a {@link RaftLogIndex}, which can only happen if the Ratis internal layout changes on an
   * upgrade. Package-private so the self-check can be exercised by unit tests.
   */
  static Field resolveMatchIndexField(final Class<?> followerClass) {
    final Field cached = matchIndexField;
    if (cached != null && cached.getDeclaringClass() == followerClass)
      return cached;

    final Field found;
    try {
      found = followerClass.getDeclaredField("matchIndex");
    } catch (final NoSuchFieldException e) {
      throw new IllegalStateException("RATIS-2523 workaround self-check failed: " + followerClass.getName()
          + " has no 'matchIndex' field. The Ratis internal layout changed; update or remove FixedGrpcLogAppender.", e);
    }
    if (!RaftLogIndex.class.isAssignableFrom(found.getType()))
      throw new IllegalStateException("RATIS-2523 workaround self-check failed: " + followerClass.getName()
          + ".matchIndex has unexpected type " + found.getType().getName() + " (expected " + RaftLogIndex.class.getName()
          + "). The Ratis internal layout changed; update or remove FixedGrpcLogAppender.");

    found.setAccessible(true);
    matchIndexField = found;
    return found;
  }
}
