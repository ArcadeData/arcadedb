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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link RaftReplicatedDatabase#awaitLeaderAddress} (issue #4728 follow-up).
 * <p>
 * During cluster startup or a leader change there is a window with no elected leader. Before this fix a write
 * forwarded by a follower failed immediately with "leader HTTP address is not available" and the caller's
 * transaction was lost. The follower now waits a bounded time for a leader to appear and forwards as soon as
 * one does.
 */
class RaftReplicatedDatabaseLeaderWaitTest {

  @Test
  void returnsImmediatelyWhenLeaderAlreadyKnown() {
    final AtomicInteger probes = new AtomicInteger();
    final String addr = RaftReplicatedDatabase.awaitLeaderAddress(() -> {
      probes.incrementAndGet();
      return "leader:2480";
    }, 10_000L, 100L);

    assertThat(addr).isEqualTo("leader:2480");
    assertThat(probes.get()).isEqualTo(1); // no waiting/polling when a leader is already available
  }

  @Test
  void waitsThenReturnsLeaderElectedDuringTheWindow() {
    // Leader is unknown for the first 3 probes (election in progress), then becomes available.
    final AtomicInteger probes = new AtomicInteger();
    final String addr = RaftReplicatedDatabase.awaitLeaderAddress(
        () -> probes.incrementAndGet() > 3 ? "leader:2480" : null, 10_000L, 20L);

    assertThat(addr).isEqualTo("leader:2480");
    assertThat(probes.get()).isGreaterThanOrEqualTo(4);
  }

  @Test
  void returnsNullAfterTimeoutWhenNoLeaderAppears() {
    final long timeoutMs = 300L;
    final long start = System.currentTimeMillis();
    final String addr = RaftReplicatedDatabase.awaitLeaderAddress(() -> null, timeoutMs, 20L);
    final long elapsed = System.currentTimeMillis() - start;

    assertThat(addr).isNull();
    // It actually waited (roughly) the configured timeout before giving up.
    assertThat(elapsed).isGreaterThanOrEqualTo(timeoutMs - 50L);
  }

  @Test
  void failsFastWhenTimeoutDisabled() {
    final AtomicInteger probes = new AtomicInteger();
    final long start = System.currentTimeMillis();
    final String addr = RaftReplicatedDatabase.awaitLeaderAddress(() -> {
      probes.incrementAndGet();
      return null;
    }, 0L, 100L);
    final long elapsed = System.currentTimeMillis() - start;

    assertThat(addr).isNull();
    assertThat(probes.get()).isEqualTo(1); // probed once, no waiting
    assertThat(elapsed).isLessThan(100L);
  }
}
