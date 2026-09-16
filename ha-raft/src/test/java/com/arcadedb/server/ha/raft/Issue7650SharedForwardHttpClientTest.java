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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.http.HttpClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for a review finding on PR #7650: {@link RaftReplicatedDatabase} used to build its own
 * connect-timeout-bounded {@code HttpClient} in every constructor call, and {@code RaftHAPlugin}'s
 * {@code server.setDatabaseWrapper} constructs one {@code RaftReplicatedDatabase} per database a node
 * replicates - so a node hosting N databases built N separate clients, each owning its own default
 * executor/selector thread, where {@code PostBatchHandler} and {@code LeaderCommandForwarder.Transport} (the
 * other two {@link com.arcadedb.server.http.handler.LeaderDial#newConnectTimeoutBoundedClient} call sites)
 * both build exactly one per server.
 * <p>
 * {@link RaftHAServer} now builds ONE client and hands it to every {@code RaftReplicatedDatabase} it wraps a
 * database with.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7650SharedForwardHttpClientTest {

  private static HttpClient httpClientOf(final RaftReplicatedDatabase db) throws Exception {
    final Field field = RaftReplicatedDatabase.class.getDeclaredField("httpClient");
    field.setAccessible(true);
    return (HttpClient) field.get(db);
  }

  @Test
  void raftHAServerBuildsOneForwardClientSharedByEveryDatabase() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    config.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 4_000L);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("localhost");
    when(mockServer.getConfiguration()).thenReturn(config);

    final RaftHAServer raft = new RaftHAServer(mockServer, config);

    // Same instance every time it is asked, not rebuilt per call.
    assertThat(raft.getForwardHttpClient()).isSameAs(raft.getForwardHttpClient());

    // What RaftHAPlugin's server.setDatabaseWrapper now does for every database it wraps: pass the ONE
    // shared client into each RaftReplicatedDatabase, instead of letting each build its own.
    final RaftReplicatedDatabase db1 = new RaftReplicatedDatabase(mockServer, mock(LocalDatabase.class), raft,
        raft.getForwardHttpClient());
    final RaftReplicatedDatabase db2 = new RaftReplicatedDatabase(mockServer, mock(LocalDatabase.class), raft,
        raft.getForwardHttpClient());

    assertThat(httpClientOf(db1))
        .as("two databases on the same node must dial the leader on the SAME client, not one each")
        .isSameAs(httpClientOf(db2))
        .isSameAs(raft.getForwardHttpClient());
  }

  /**
   * The three-arg constructor (no shared client given) is what the direct-construction unit tests elsewhere in
   * this package still use; it must keep building its own client rather than requiring every existing test to
   * pass one.
   */
  @Test
  void theThreeArgConstructorStillBuildsItsOwnClientWhenNoneIsShared() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getConfiguration()).thenReturn(config);

    final RaftReplicatedDatabase db = new RaftReplicatedDatabase(mockServer, mock(LocalDatabase.class), mock(RaftHAServer.class));

    assertThat(httpClientOf(db)).isNotNull();
  }
}
