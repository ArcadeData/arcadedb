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

import com.arcadedb.log.LogManager;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIndex;

import java.lang.reflect.Field;
import java.util.logging.Level;

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

  public FixedGrpcLogAppender(final RaftServer.Division server, final LeaderState leaderState, final FollowerInfo f) {
    super(server, leaderState, f);
  }

  @Override
  protected long getNextIndexForInconsistency(final long requestFirstIndex, final long replyNextIndex) {
    if (requestFirstIndex == RaftLog.INVALID_LOG_INDEX
        && replyNextIndex <= getFollower().getMatchIndex()) {
      // RATIS-2523: heartbeat path with a follower whose hint is at or below our recorded
      // matchIndex. Our matchIndex is stale (the follower restarted with empty storage and we
      // never observed a SUCCESS that would have rolled it back). Setting only nextIndex back
      // to replyNextIndex is not enough: the SUCCESS handler uses updateMatchIndex which
      // monotonically takes the max, so the stale matchIndex never moves down and SUCCESS
      // skips the corresponding updateNextIndex. The appender then loops at the SUCCESS layer
      // instead of the INCONSISTENCY layer.
      //
      // Force matchIndex back to (replyNextIndex - 1) via reflection on FollowerInfoImpl. The
      // public FollowerInfo API only offers setSnapshotIndex which also rewrites snapshotIndex
      // and risks confusing Ratis's snapshot-install detection; we want the surgical reset.
      // Once RATIS-2523 ships, drop this whole subclass.
      forceMatchIndex(getFollower(), replyNextIndex - 1);
      return replyNextIndex;
    }
    return super.getNextIndexForInconsistency(requestFirstIndex, replyNextIndex);
  }

  /**
   * Surgically rewinds {@code FollowerInfoImpl.matchIndex} backwards. The public
   * {@link FollowerInfo#updateMatchIndex} method only takes the max, so to undo a stale value
   * we read the private {@code matchIndex} {@link RaftLogIndex} field and call its public
   * {@code setUnconditionally}. Failures (e.g. Ratis layout change) are logged and the
   * recovery falls back to the no-op upstream behaviour.
   */
  private static void forceMatchIndex(final FollowerInfo follower, final long newMatchIndex) {
    try {
      final Field f = follower.getClass().getDeclaredField("matchIndex");
      f.setAccessible(true);
      final RaftLogIndex idx = (RaftLogIndex) f.get(follower);
      idx.setUnconditionally(newMatchIndex, msg -> {
      });
    } catch (final ReflectiveOperationException | ClassCastException e) {
      LogManager.instance().log(FixedGrpcLogAppender.class, Level.WARNING,
          "RATIS-2523 workaround: cannot reset matchIndex for follower %s: %s",
          null, follower.getName(), e.getMessage());
    }
  }
}
