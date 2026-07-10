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

import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #4833: the RATIS-2523 workaround in {@link FixedGrpcLogAppender}
 * must rewind {@code FollowerInfoImpl.matchIndex} atomically (never clobbering a concurrent
 * SUCCESS that legitimately raised it), refuse nonsensical values, and fail loud (not silently
 * no-op) if the Ratis internal field layout changes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FixedGrpcLogAppenderTest {

  @Test
  void rewindLowersStaleMatchIndex() {
    final RaftLogIndex matchIndex = new RaftLogIndex("matchIndex", 5L);
    FixedGrpcLogAppender.rewindMatchIndex(matchIndex, 5L, 4L);
    assertThat(matchIndex.get()).isEqualTo(4L);
  }

  @Test
  void rewindAcceptsInvalidLogIndexAsFloor() {
    // RATIS-2523 primary case: a follower that restarted with empty storage replies nextIndex=0,
    // so newMatchIndex == replyNextIndex - 1 == INVALID_LOG_INDEX (-1). This must be allowed.
    final RaftLogIndex matchIndex = new RaftLogIndex("matchIndex", 5L);
    FixedGrpcLogAppender.rewindMatchIndex(matchIndex, 5L, RaftLog.INVALID_LOG_INDEX);
    assertThat(matchIndex.get()).isEqualTo(RaftLog.INVALID_LOG_INDEX);
  }

  @Test
  void rewindRejectsValuesBelowInvalidLogIndex() {
    final RaftLogIndex matchIndex = new RaftLogIndex("matchIndex", 5L);
    assertThatThrownBy(() -> FixedGrpcLogAppender.rewindMatchIndex(matchIndex, 5L, RaftLog.INVALID_LOG_INDEX - 1))
        .isInstanceOf(IllegalArgumentException.class);
    // The atomic must be left untouched on a rejected request.
    assertThat(matchIndex.get()).isEqualTo(5L);
  }

  @Test
  void rewindIsNoOpWhenMatchIndexChangedSinceObservation() {
    // A concurrent SUCCESS reply raised matchIndex from the observed stale 5 to 100 before our
    // rewind runs. The compare-and-rewind must detect the change and leave 100 in place, keeping
    // Ratis's monotonic-matchIndex invariant.
    final RaftLogIndex matchIndex = new RaftLogIndex("matchIndex", 5L);
    matchIndex.updateToMax(100L, msg -> { });
    FixedGrpcLogAppender.rewindMatchIndex(matchIndex, 5L /* stale value we observed */, 4L);
    assertThat(matchIndex.get()).isEqualTo(100L);
  }

  @Test
  void concurrentSuccessAndRewindNeverLosesMonotonicity() throws Exception {
    // Regardless of interleaving between a SUCCESS (updateToMax(100)) and the stale-hint rewind
    // (observed=5 -> -1), the result must always settle on 100: either the rewind runs first
    // (5 -> -1, then max(-1,100)=100) or the SUCCESS runs first (5 -> 100, then 100 != 5 so the
    // rewind no-ops). With the buggy unconditional setUnconditionally(-1) the second interleaving
    // would leave -1, violating monotonicity.
    for (int i = 0; i < 5_000; i++) {
      final RaftLogIndex matchIndex = new RaftLogIndex("matchIndex", 5L);
      final CyclicBarrier start = new CyclicBarrier(2);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      final Runnable success = () -> {
        try {
          start.await();
          matchIndex.updateToMax(100L, msg -> { });
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      };
      final Runnable rewind = () -> {
        try {
          start.await();
          FixedGrpcLogAppender.rewindMatchIndex(matchIndex, 5L, 4L);
        } catch (final Throwable t) {
          failure.compareAndSet(null, t);
        }
      };

      final Thread t1 = new Thread(success);
      final Thread t2 = new Thread(rewind);
      t1.start();
      t2.start();
      t1.join();
      t2.join();

      assertThat(failure.get()).isNull();
      assertThat(matchIndex.get()).isEqualTo(100L);
    }
  }

  @Test
  void selfCheckResolvesRatisMatchIndexField() throws Exception {
    // Fail-loud guard: if a future Ratis release renames/removes the matchIndex field or changes
    // its type, this resolution throws and the test (and the leader at startup) fail loudly.
    final Class<?> followerImpl = Class.forName("org.apache.ratis.server.impl.FollowerInfoImpl");
    final Field resolved = FixedGrpcLogAppender.resolveMatchIndexField(followerImpl);
    assertThat(resolved.getName()).isEqualTo("matchIndex");
    assertThat(RaftLogIndex.class.isAssignableFrom(resolved.getType())).isTrue();
  }

  @Test
  void selfCheckFailsLoudWhenFieldMissing() {
    assertThatThrownBy(() -> FixedGrpcLogAppender.resolveMatchIndexField(NoMatchIndex.class))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("matchIndex");
  }

  @Test
  void selfCheckFailsLoudWhenFieldHasWrongType() {
    assertThatThrownBy(() -> FixedGrpcLogAppender.resolveMatchIndexField(WrongTypeMatchIndex.class))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("RaftLogIndex");
  }

  /** A class without a {@code matchIndex} field, used to exercise the fail-loud self-check. */
  private static final class NoMatchIndex {
  }

  /** A class whose {@code matchIndex} field is not a {@link RaftLogIndex}. */
  private static final class WrongTypeMatchIndex {
    @SuppressWarnings("unused")
    private long matchIndex;
  }
}
