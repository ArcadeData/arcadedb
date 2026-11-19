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
import com.arcadedb.server.TestServerHelper;
import java.lang.reflect.Field;
import java.net.http.HttpClient;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static com.arcadedb.utility.SubclassMocks.mock;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shared HTTP client used to forward to the leader (one per server, closed on stop) and the self-forward guard on the resolved leader address.
 */
class ForwardHttpClientTest {
  // ---- #7650 ----
  // Regression test for a review finding on PR #7650: {@link RaftReplicatedDatabase} used to build its own
  // connect-timeout-bounded {@code HttpClient} in every constructor call, and {@code RaftHAPlugin}'s
  // {@code server.setDatabaseWrapper} constructs one {@code RaftReplicatedDatabase} per database a node
  // replicates - so a node hosting N databases built N separate clients, each owning its own default
  // executor/selector thread, where {@code PostBatchHandler} and {@code LeaderCommandForwarder.Transport} (the
  // other two {@link com.arcadedb.server.http.handler.LeaderDial#newConnectTimeoutBoundedClient} call sites)
  // both build exactly one per server.
  //
  // {@link RaftHAServer} now builds ONE client and hands it to every {@code RaftReplicatedDatabase} it wraps a
  // database with.

  private static HttpClient httpClientOf(final RaftReplicatedDatabase db) throws Exception {
    final Field field = RaftReplicatedDatabase.class.getDeclaredField("httpClient");
    field.setAccessible(true);
    return (HttpClient) field.get(db);
  }

  // Regression for #7650: raft HA server builds one forward client shared by every database
  @Test
  void raftHAServerBuildsOneForwardClientSharedByEveryDatabase() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");
    config.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 4_000L);

    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("localhost", config);

    final RaftHAServer raft = new RaftHAServer(arcadeServer, config);

    // Same instance every time it is asked, not rebuilt per call.
    assertThat(raft.getForwardHttpClient()).isSameAs(raft.getForwardHttpClient());

    // What RaftHAPlugin's server.setDatabaseWrapper now does for every database it wraps: pass the ONE
    // shared client into each RaftReplicatedDatabase, instead of letting each build its own.
    final RaftReplicatedDatabase db1 = new RaftReplicatedDatabase(arcadeServer, mock(LocalDatabase.class), raft,
        raft.getForwardHttpClient());
    final RaftReplicatedDatabase db2 = new RaftReplicatedDatabase(arcadeServer, mock(LocalDatabase.class), raft,
        raft.getForwardHttpClient());

    assertThat(httpClientOf(db1))
        .as("two databases on the same node must dial the leader on the SAME client, not one each")
        .isSameAs(httpClientOf(db2))
        .isSameAs(raft.getForwardHttpClient());
  }

  // Regression for #7650: the three arg constructor still builds its own client when none is shared
  /**
   * The three-arg constructor (no shared client given) is what the direct-construction unit tests elsewhere in
   * this package still use; it must keep building its own client rather than requiring every existing test to
   * pass one.
   */
  @Test
  void theThreeArgConstructorStillBuildsItsOwnClientWhenNoneIsShared() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer((String) null, config);

    final RaftReplicatedDatabase db = new RaftReplicatedDatabase(arcadeServer, mock(LocalDatabase.class), FakeRaftHAServer.detached());

    assertThat(httpClientOf(db)).isNotNull();
  }
  // ---- #7650 ----
  // Regression test for a review finding on PR #7650: {@code RaftHAServer.forwardHttpClient} - the client every
  // {@code RaftReplicatedDatabase} this server wraps a database with now shares (issue: code review finding on
  // the same PR, "one client per node, not one per database") - was built in the constructor but never closed in
  // {@link RaftHAServer#stop()}, unlike its two siblings {@code capabilityHttpsClients}/{@code forwardHttpsClients},
  // which {@code stop()} already closes with an explicit "an unclosed HttpClient holds a connection pool and a
  // selector thread for the life of the JVM" comment (issue #7314). A fresh {@code RaftHAServer} - and a fresh
  // {@code forwardHttpClient} - is built on every {@code RaftHAPlugin.startService()}, so leaving it open leaks a
  // selector thread per server start/stop cycle, which the HA test suites go through many of.

  // Regression for #7650: stop closes the shared forward http client
  @Test
  void stopClosesTheSharedForwardHttpClient() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");

    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("localhost");

    final RaftHAServer raft = new RaftHAServer(arcadeServer, config);
    final HttpClient client = raft.getForwardHttpClient();
    assertThat(client.isTerminated()).as("not yet closed before stop()").isFalse();

    raft.stop();

    assertThat(client.awaitTermination(Duration.ofSeconds(10)))
        .as("stop() must close the shared forward client, not just the ones it already closed").isTrue();
  }
  // ---- #6191 ----
  // The question every automatic redirect has to answer before it dials a resolved leader address: is this
  // address my own? One that is identifies nobody, and following it lands the request back on the node that
  // resolved it - which resolves the same address and redirects again (issue #6191).
  //
  // Exercised directly because the address comparison is shared by the write-forwarding path and the snapshot
  // resync refusal ({@code ArcadeStateMachine.triggerSnapshotDownload}, issue #6111), and neither of those can
  // show what it does with a hostname that differs only in case.

  // Regression for #6191: the same endpoint is recognised
  @Test
  void theSameEndpointIsRecognised() {
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "localhost:2480")).isTrue();
  }

  // Regression for #6191: host case does not make it A different endpoint
  /** Host names are case-insensitive, and "a different case" must not read as "a different node". */
  @Test
  void hostCaseDoesNotMakeItADifferentEndpoint() {
    assertThat(RaftHAServer.isSameHttpEndpoint("DB0.example.com:2480", "db0.example.com:2480")).isTrue();
  }

  // Regression for #6191: a different port is A different endpoint
  /** The whole point on a same-host cluster: the port is what tells the peers apart. */
  @Test
  void aDifferentPortIsADifferentEndpoint() {
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", "localhost:2481")).isFalse();
  }

  // Regression for #6191: a different host is A different endpoint
  @Test
  void aDifferentHostIsADifferentEndpoint() {
    assertThat(RaftHAServer.isSameHttpEndpoint("db0:2480", "db1:2480")).isFalse();
  }

  // Regression for #6191: a host name is not unified with an ip
  /**
   * A declared host name is a statement about which node owns which port, and no name resolution happens
   * here to overrule it. The spellings of the loopback address are the one exception, in
   * {@link PeerEndpointIdentityTest.LoopbackEndpoint}.
   */
  @Test
  void aHostNameIsNotUnifiedWithAnIp() {
    assertThat(RaftHAServer.isSameHttpEndpoint("db0.example.com:2480", "10.0.0.7:2480")).isFalse();
  }

  // Regression for #6191: an unresolved address is never A match
  /** An address that could not be resolved is not evidence of anything, in either position. */
  @Test
  void anUnresolvedAddressIsNeverAMatch() {
    assertThat(RaftHAServer.isSameHttpEndpoint(null, "localhost:2480")).isFalse();
    assertThat(RaftHAServer.isSameHttpEndpoint("localhost:2480", null)).isFalse();
    assertThat(RaftHAServer.isSameHttpEndpoint(null, null)).isFalse();
  }
}
