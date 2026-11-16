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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Issue #11951: a write forwarded while leadership moves off this node used to be refused with the 400 of the #6191
 * misconfiguration ("the HTTP address resolved for the leader is this node's own ... declare every node's HTTP port"). Raft
 * still named this node the leader while it no longer was one, during a leadership transfer, so the leader's address was
 * this node's own. That is a transition, not a configuration fault: it is refused naming no leader, the retryable answer a
 * write gets mid-election (HTTP 503), which this node's own transaction retry gets past once the move settles.
 * <p>
 * Drives the private {@code forwardCommandToLeaderViaRaft} directly, as {@code Issue7527RaftReplicatedDatabaseForwardTimeoutTest}
 * does: the decision is local to the forward, and the window it covers cannot be held open on a live cluster.
 */
class Issue11951ForwardWhileLeadershipMovesTest {
  private static final RaftPeerId THIS_NODE = RaftPeerId.valueOf("this-node_2434");
  private static final RaftPeerId OTHER     = RaftPeerId.valueOf("peer-b_2434");
  private static final String     OWN_HTTP  = "localhost:2480";

  @Test
  void aForwardThatResolvesThisNodeAsTheLeaderItNoLongerIsIsARetryableRefusal() throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false).localPeerId(THIS_NODE).leaderId(THIS_NODE).localHttpAddress(OWN_HTTP).leaderHttpAddress(OWN_HTTP);

    final ServerIsNotTheLeaderException refusal = forwardRefusal(raft);

    assertThat(refusal.namesLeader()).as("names no leader, so HTTP answers 503 rather than 400").isFalse();
    assertThat(refusal.isRetryableOnThisNode()).as("the transaction retry on this node re-resolves the leader").isTrue();
    assertThat(refusal.getMessage()).contains("leadership is moving").doesNotContain("Declare every node's HTTP port");
  }

  /** Control: another peer's address landing on this node is the #6191 misconfiguration, and keeps its refusal. */
  @Test
  void anotherPeersAddressThatLandsOnThisNodeIsStillTheConfigurationRefusal() throws Exception {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leader(false).localPeerId(THIS_NODE).leaderId(OTHER).localHttpAddress(OWN_HTTP).leaderHttpAddress(OWN_HTTP);

    final ServerIsNotTheLeaderException refusal = forwardRefusal(raft);

    assertThat(refusal.namesLeader()).isTrue();
    assertThat(refusal.isRetryableOnThisNode()).isFalse();
    assertThat(refusal.getMessage()).contains("is this node's own").contains("Declare every node's HTTP port");
  }

  private static ServerIsNotTheLeaderException forwardRefusal(final RaftHAServer raft) throws Exception {
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, new ContextConfiguration());
    final LocalDatabase local = mock(LocalDatabase.class);
    when(local.getConfiguration()).thenReturn(server.getConfiguration());
    final RaftReplicatedDatabase db = new RaftReplicatedDatabase(server, local, raft);

    final Method forward = RaftReplicatedDatabase.class.getDeclaredMethod("forwardCommandToLeaderViaRaft", String.class,
        String.class, Map.class, Object[].class);
    forward.setAccessible(true);
    try {
      forward.invoke(db, "sql", "insert into V set a = 1", null, new Object[0]);
      throw new AssertionError("expected the forward to be refused");
    } catch (final InvocationTargetException e) {
      assertThat(e.getCause()).isInstanceOf(ServerIsNotTheLeaderException.class);
      return (ServerIsNotTheLeaderException) e.getCause();
    }
  }
}
