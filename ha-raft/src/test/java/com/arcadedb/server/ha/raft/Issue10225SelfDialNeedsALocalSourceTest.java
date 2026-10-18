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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.RaftPeerId;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Issue #10225: the misdirected-request listener took a refused Raft RPC for this node's own - and reset this node's
 * channel to the addressed member - whenever the request's requestor id was this node's. That id is a header field any
 * sender writes, so a process outside the cluster, or a node of another cluster that shares this node's id, could make
 * this node tear down a working channel to a member every five seconds. The reset now also needs the connection the
 * request arrived on to come from this host, which a remote sender cannot fake.
 */
class Issue10225SelfDialNeedsALocalSourceTest {

  private static final String     SERVER_LIST = "localhost:2434:2480,localhost:2435:2481,localhost:2436:2482";
  private static final RaftPeerId MEMBER      = RaftPeerId.valueOf("localhost_2435");
  private static final String     SELF_DIAL   = "This node sent a Raft appendEntries meant for peer 'localhost_2435'";
  private static final String     IMPOSTOR    = "names this node as its sender";

  private CapturingTestLogger log;

  @BeforeEach
  void installLogger() {
    log = CapturingTestLogger.install();
  }

  @AfterEach
  void restore() {
    log.uninstall();
  }

  /**
   * The reported shape: a request addressed to a member that writes this node's id as requestor, from wherever the sender
   * is, is reported and repairs nothing. The source cannot be confirmed when it is foreign, missing or a bare name.
   */
  @ParameterizedTest(name = "source = {0}")
  @ValueSource(strings = { "192.0.2.10", "<none>", "<unresolved>" })
  void aRequestClaimingThisNodesIdFromElsewhereResetsNothing(final String source) throws Exception {
    final SocketAddress remote = switch (source) {
      case "<none>" -> null;
      case "<unresolved>" -> InetSocketAddress.createUnresolved("arcadedb-0", 2434);
      default -> new InetSocketAddress(InetAddress.getByName(source), 51_000);
    };
    final RaftHAServer raft = detachedServer();

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), remote);

    Awaitility.await("the refusal is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(IMPOSTOR) == 1);
    assertThat(log.countFormattedContaining(SELF_DIAL)).as("a claim the connection does not confirm is no self-dial").isZero();
    assertThat(raft.getMisdirectedChannelResets()).isZero();
    assertThat(raft.getMisdirectedSelfClaimsFromElsewhere()).as("counted, so an operator can spot it without the log").isEqualTo(1L);
  }

  /**
   * No address at all is not an impostor: it means the RPC was served outside the call's gRPC context, so the line says
   * that a self-dial can no longer be repaired on it rather than accusing a sender.
   */
  @Test
  void aCallWithNoRemoteAddressIsReportedAsUnconfirmableNotAsAnImpostor() {
    final RaftHAServer raft = detachedServer();

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), null);

    Awaitility.await("the refusal is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining("no remote address was reported for the call") == 1);
    assertThat(log.countFormattedContaining("arrived from")).isZero();
    assertThat(raft.getMisdirectedSelfClaimsWithoutAddress()).isEqualTo(1L);
  }

  /**
   * A flood of claims of this node's id from elsewhere cannot hold the slot, nor the WARNING, of a real report about the
   * member they address: a peer whose address for that member leads here is still reported at WARNING.
   */
  @Test
  void claimsFromElsewhereDoNotSilenceARealReportAboutTheSameMember() throws Exception {
    final RaftHAServer raft = detachedServer();
    final InetSocketAddress foreign = new InetSocketAddress(InetAddress.getByName("192.0.2.10"), 51_000);

    for (int i = 0; i < 5; i++)
      raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), foreign);
    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, RaftPeerId.valueOf("localhost_2436"),
        new InetSocketAddress(InetAddress.getByName("192.0.2.11"), 51_000));

    final String report = "from peer 'localhost_2436' addressed to peer 'localhost_2435'";
    Awaitility.await("the peer's report is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(report) == 1);
    assertThat(log.formattedAt(Level.WARNING)).anyMatch(m -> m.contains(report));
    assertThat(raft.getMisdirectedSelfClaimsFromElsewhere()).isEqualTo(5L);
    assertThat(raft.getMisdirectedSelfClaimsWithoutAddress()).as("each of them carried an address").isZero();
  }

  /** The repair #10016 needs still runs: this node's own request comes back over the loopback. */
  @Test
  void aRequestClaimingThisNodesIdOverTheLoopbackIsASelfDial() {
    final RaftHAServer raft = detachedServer();

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(),
        new InetSocketAddress(InetAddress.getLoopbackAddress(), 51_000));

    Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
    assertThat(log.countFormattedContaining(IMPOSTOR)).isZero();
    assertThat(raft.getMisdirectedSelfClaimsFromElsewhere()).isZero();
  }

  /**
   * Nothing from another host can hold the slot the repair needs: neither a stranger claiming this node's id nor a sender
   * naming a member, both inside the interval, keeps this node's own request from being repaired - and reported at WARNING.
   */
  @ParameterizedTest(name = "earlier foreign requestor = {0}")
  @ValueSource(strings = { "<this node>", "localhost_2436" })
  void aForeignRequestCannotHoldTheSlotTheRepairNeeds(final String foreignRequestor) throws Exception {
    final RaftHAServer raft = detachedServer();
    final RaftPeerId requestor = "<this node>".equals(foreignRequestor) ? raft.getLocalPeerId() : RaftPeerId.valueOf(foreignRequestor);
    final InetSocketAddress foreign = new InetSocketAddress(InetAddress.getByName("192.0.2.10"), 51_000);

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, requestor, foreign);
    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(),
        new InetSocketAddress(InetAddress.getLoopbackAddress(), 51_000));

    Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
    assertThat(log.formattedAt(Level.WARNING)).as("the repair is the line the operator needs").anyMatch(m -> m.contains(SELF_DIAL));
  }

  /** ...and so it does when the peer's name resolves to this node's own interface address rather than the loopback. */
  @Test
  void aRequestClaimingThisNodesIdFromOneOfItsInterfacesIsASelfDial() throws Exception {
    final InetAddress own = firstNonLoopbackInterfaceAddress();
    assumeTrue(own != null, "this host has no non-loopback IPv4 interface address");
    final RaftHAServer raft = detachedServer();

    raft.onMisdirectedRaftRequest("appendEntries", MEMBER, raft.getLocalPeerId(), new InetSocketAddress(own, 51_000));

    Awaitility.await("the self-dialled channel is looked at").atMost(10, TimeUnit.SECONDS)
        .until(() -> log.countFormattedContaining(SELF_DIAL) == 1);
  }

  @Test
  void onlyAnAddressOfThisHostCountsAsLocal() throws Exception {
    assertThat(RaftHAServer.isAddressOfThisHost(new InetSocketAddress(InetAddress.getLoopbackAddress(), 1))).isTrue();
    assertThat(RaftHAServer.isAddressOfThisHost(new InetSocketAddress(InetAddress.getByName("127.0.0.2"), 1)))
        .as("the whole loopback range").isTrue();
    assertThat(RaftHAServer.isAddressOfThisHost(new InetSocketAddress(InetAddress.getByName("::1"), 1))).isTrue();
    final InetAddress own = firstNonLoopbackInterfaceAddress();
    if (own != null)
      assertThat(RaftHAServer.isAddressOfThisHost(new InetSocketAddress(own, 1))).isTrue();

    // TEST-NET-1 (RFC 5737): reserved for documentation, never assigned to an interface
    assertThat(RaftHAServer.isAddressOfThisHost(new InetSocketAddress(InetAddress.getByName("192.0.2.10"), 1))).isFalse();
    assertThat(RaftHAServer.isAddressOfThisHost(InetSocketAddress.createUnresolved("localhost", 1))).isFalse();
    assertThat(RaftHAServer.isAddressOfThisHost(null)).isFalse();
  }

  private static InetAddress firstNonLoopbackInterfaceAddress() throws Exception {
    for (final NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces())) {
      if (!nic.isUp() || nic.isLoopback())
        continue;
      for (final InetAddress address : Collections.list(nic.getInetAddresses()))
        if (address instanceof Inet4Address)
          return address;
    }
    return null;
  }

  private static RaftHAServer detachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, SERVER_LIST);
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
    return new RaftHAServer(arcadeServer, config);
  }
}
