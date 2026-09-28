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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.http.HttpTimeoutException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8325 on the ha-raft forwards to the leader: each one bounded the leader with the JDK
 * request timeout alone and read its answer with {@code BodyHandlers.ofString()}. On JDK 21-25 that timeout stops at
 * the response headers, so a leader that sent {@code 200} with a {@code Content-Length} and then stalled inside its
 * body parked the calling thread with no bound at all. Each entry point is driven through its own code here, against a
 * leader that does exactly that; without the fix every one of them waits on it until the hang detector gives up.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8325LeaderBodyDeadlineTest {

  /** The tripwire between "the deadline fired" and "the call is unbounded" (forever, without the fix). */
  private static final long GAVE_UP_BOUND_MS = 15_000L;

  /** {@code RaftReplicatedDatabase.forwardCommandToLeaderViaRaft}: the write forward. */
  @Test
  void aWriteForwardWhoseLeaderStallsInsideItsBodyIsGivenUpOnAsUnknownOutcome() throws Exception {
    try (final StallingBodyPeer leader = new StallingBodyPeer()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 1_000L);

      final ArcadeDBServer server = mock(ArcadeDBServer.class);
      when(server.getConfiguration()).thenReturn(cfg);
      when(server.getHA()).thenReturn(null);
      final RaftHAServer raft = mock(RaftHAServer.class);
      when(raft.getLeaderHttpAddress()).thenReturn(leader.address());
      when(raft.getClusterToken()).thenReturn("test-token");
      final LocalDatabase local = mock(LocalDatabase.class);
      when(local.getConfiguration()).thenReturn(cfg);
      final RaftReplicatedDatabase db = new RaftReplicatedDatabase(server, local, raft);

      final Method forward = RaftReplicatedDatabase.class.getDeclaredMethod("forwardCommandToLeaderViaRaft",
          String.class, String.class, Map.class, Object[].class);
      forward.setAccessible(true);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final Throwable thrown = leader.callWithin(() -> {
        try {
          forward.invoke(db, "sql", "insert into V set a = 1", null, new Object[0]);
          return null;
        } catch (final InvocationTargetException e) {
          return e.getCause();
        }
      });
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s deadline from the unbounded ofString() body read");

      // The leader answered the headers, so the write may have run there: an unknown outcome, never a retry.
      assertThat(thrown).isInstanceOf(TransactionException.class).isNotInstanceOf(NeedRetryException.class);
      assertThat(thrown.getMessage()).contains(leader.address()).contains("did not answer within");
      assertThat(leader.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
    }
  }

  /** {@code LeaderDatabaseQuery.fetch}: the bootstrap-state query. */
  @Test
  void aBootstrapStateQueryWhosePeerStallsInsideItsBodyIsGivenUpOn() throws Exception {
    try (final StallingBodyPeer leader = new StallingBodyPeer()) {
      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> leader.callWithin(
          () -> LeaderDatabaseQuery.fetch(leader.address(), null, "test-token", 1_000L, null)))
          .isInstanceOf(HttpTimeoutException.class);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s deadline from the unbounded ofString() body read");

      assertThat(leader.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
    }
  }

  /**
   * {@code ClusterSecuritySeedQuery}: the security seed requested from the leader. Slow, because its deadline is the
   * seed's retry budget plus a fixed 30 s margin that no setting lowers.
   */
  @Test
  @Tag("slow")
  void aSecuritySeedRequestWhoseLeaderStallsInsideItsBodyIsGivenUpOn() throws Exception {
    try (final StallingBodyPeer leader = new StallingBodyPeer()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_SECURITY_SEED_RETRY_TIMEOUT, 0L);
      final long deadlineMs = ClusterSecuritySeedQuery.reportTimeoutMs(cfg);

      final ArcadeDBServer server = mock(ArcadeDBServer.class);
      when(server.getConfiguration()).thenReturn(cfg);
      final RaftHAServer raft = mock(RaftHAServer.class);
      when(raft.getClusterToken()).thenReturn("test-token");
      final RaftHAPlugin plugin = mock(RaftHAPlugin.class);
      when(plugin.getRaftHAServer()).thenReturn(raft);
      when(plugin.isLeader()).thenReturn(false);
      when(plugin.getLeaderAddress()).thenReturn(leader.address());

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      // The first attempt meets the stall; the leader then stops listening, so the retries fail at once and the
      // seed reports the failure instead of an outcome it never received.
      assertThatThrownBy(() -> leader.callWithin(
          () -> ClusterSecuritySeedQuery.seedForAdmission(server, plugin, "peer-1"),
          deadlineMs + StallingBodyPeer.HANG_DETECT_MS))
          .isInstanceOf(IOException.class);
      watch.assertGaveUpWithin(deadlineMs + GAVE_UP_BOUND_MS,
          "the seed's own deadline from the unbounded ofString() body read");

      assertThat(leader.connectionClosedByCaller.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS)).isTrue();
    }
  }
}
