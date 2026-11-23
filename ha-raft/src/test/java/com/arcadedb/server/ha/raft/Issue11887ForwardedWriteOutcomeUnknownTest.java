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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.http.FakeLeader;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Issue #11887: a write a follower forwards to the leader, whose connection is lost after the request went out, or which
 * the leader answers with {@link ReplicationDispatchedTimeoutException}, has an unknown outcome: the leader may have
 * committed it. Measured on a 3-node cluster whose leader was killed mid-run, 3 of 12 such writes were stored. Both used to
 * reach the client as a 500 "Error on transaction commit" - the first naming {@code java.io.EOFException} - which a client
 * cannot tell from a server fault. They are a {@link TransactionOutcomeUnknownException} now, which the HTTP layer answers
 * 504, and never a {@link NeedRetryException}, which a client retries blindly.
 */
class Issue11887ForwardedWriteOutcomeUnknownTest {

  @Test
  void aLeaderConnectionLostAfterTheWriteWasSentIsAnOutcomeUnknown() throws Exception {
    // Reads the whole request head, then closes the connection without a byte of answer: a leader killed mid-request
    try (final FakeLeader leader = FakeLeader.scripted(out -> out.close())) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 30_000L);

      final Throwable thrown = forward(databaseWith(cfg, leader.address()));

      assertThat(leader.awaitRequestReceived(5, TimeUnit.SECONDS)).as("the leader read the forwarded write").isTrue();
      assertThat(thrown).isInstanceOf(TransactionOutcomeUnknownException.class).isNotInstanceOf(NeedRetryException.class);
      assertThat(thrown.getMessage()).contains(leader.address()).contains("may have been applied");
    }
  }

  @Test
  void aLeaderThatDoesNotAnswerInTimeIsAnOutcomeUnknown() throws Exception {
    try (final FakeLeader leader = FakeLeader.draining()) {
      final ContextConfiguration cfg = new ContextConfiguration();
      cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 5_000L);
      cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 1_000L);

      final Throwable thrown = forward(databaseWith(cfg, leader.address()));

      assertThat(thrown).isInstanceOf(TransactionOutcomeUnknownException.class).isNotInstanceOf(NeedRetryException.class);
    }
  }

  @Test
  void aLeaderAnswerOfADispatchedTimeoutIsRebuiltAsAnOutcomeUnknown() {
    final String body = new JSONObject().put("error", "Error on transaction commit")
        .put("detail", "Group commit timed out (entry was dispatched to Raft)")
        .put("exception", ReplicationDispatchedTimeoutException.class.getName()).toString();

    final RuntimeException rebuilt = RaftReplicatedDatabase.reconstructLeaderException(500, body, null);

    // The engine type, not the HA one: on this node the HA type would mean one of its own entries is indeterminate
    assertThat(rebuilt).isExactlyInstanceOf(TransactionOutcomeUnknownException.class);
    assertThat(rebuilt.getMessage()).isEqualTo("Group commit timed out (entry was dispatched to Raft)");
  }

  @Test
  void aRelayedOutcomeUnknownStaysOne() {
    final String body = new JSONObject().put("error", "Transaction outcome unknown")
        .put("detail", "The connection to the leader was lost before it answered")
        .put("exception", TransactionOutcomeUnknownException.class.getName()).toString();

    assertThat(RaftReplicatedDatabase.reconstructLeaderException(504, body, null))
        .isExactlyInstanceOf(TransactionOutcomeUnknownException.class);
  }

  private static RaftReplicatedDatabase databaseWith(final ContextConfiguration cfg, final String leaderHttpAddress) {
    final ArcadeDBServer server = TestServerHelper.unstartedServer((String) null, cfg);
    final LocalDatabase local = mock(LocalDatabase.class);
    when(local.getConfiguration()).thenReturn(server.getConfiguration());
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    raft.leaderHttpAddress(leaderHttpAddress);
    raft.clusterToken("test-token");
    return new RaftReplicatedDatabase(server, local, raft);
  }

  /** Drives the private forward directly, as {@code Issue7527RaftReplicatedDatabaseForwardTimeoutTest} does. */
  private static Throwable forward(final RaftReplicatedDatabase db) throws Exception {
    final Method m = RaftReplicatedDatabase.class.getDeclaredMethod("forwardCommandToLeaderViaRaft", String.class, String.class,
        Map.class, Object[].class, boolean.class);
    m.setAccessible(true);
    try {
      m.invoke(db, "sql", "insert into V set a = 1", null, new Object[0], true);
    } catch (final InvocationTargetException e) {
      return e.getCause();
    }
    throw new AssertionError("expected the forward to throw");
  }
}
