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
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerException;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Coverage sweep for {@link RaftPeerAddressResolver}. Focuses on the pure parsing and resolution
 * paths that the existing integration tests do not exercise (different entry formats, the four
 * local-peer resolution strategies, HTTP address derivation, IPv6 handling, port-spec parsing).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftPeerAddressResolverCoverageTest {

  private static final int    DEFAULT_RAFT_PORT = 2424;
  private static final String DEFAULT_HOST      = "localhost";

  // -- parseFirstPort --

  @Test
  void parseFirstPortAcceptsPlainInteger() {
    assertThat(RaftPeerAddressResolver.parseFirstPort("2424")).isEqualTo(2424);
  }

  @Test
  void parseFirstPortReturnsFirstOfRange() {
    assertThat(RaftPeerAddressResolver.parseFirstPort("2424-2430")).isEqualTo(2424);
  }

  @Test
  void parseFirstPortReturnsFirstOfCsvList() {
    assertThat(RaftPeerAddressResolver.parseFirstPort("2424,2425,2426")).isEqualTo(2424);
  }

  // -- parseHostPort (static) --

  @Test
  void parseHostPortSplitsSimpleEntry() {
    final String[] parts = RaftPeerAddressResolver.parseHostPort("host-a:2424");
    assertThat(parts[0]).isEqualTo("host-a");
    assertThat(parts[1]).isEqualTo("2424");
  }

  @Test
  void parseHostPortSplitsThreePartEntry() {
    final String[] parts = RaftPeerAddressResolver.parseHostPort("host-a:2424:2480");
    assertThat(parts).containsExactly("host-a", "2424", "2480");
  }

  @Test
  void parseHostPortSplitsFourPartEntry() {
    final String[] parts = RaftPeerAddressResolver.parseHostPort("host-a:2424:2480:10");
    assertThat(parts).containsExactly("host-a", "2424", "2480", "10");
  }

  @Test
  void parseHostPortHandlesBracketedIPv6() {
    final String[] parts = RaftPeerAddressResolver.parseHostPort("[::1]:2424");
    assertThat(parts[0]).isEqualTo("[::1]");
    assertThat(parts[1]).isEqualTo("2424");
  }

  @Test
  void parseHostPortHandlesBracketedIPv6WithThreePorts() {
    final String[] parts = RaftPeerAddressResolver.parseHostPort("[fe80::1]:2424:2480:5");
    assertThat(parts).containsExactly("[fe80::1]", "2424", "2480", "5");
  }

  @Test
  void parseHostPortRejectsBareIPv6() {
    // Unbracketed IPv6 with :: is ambiguous with host:port syntax and must be rejected.
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort("fe80::1:2424"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("bracketed");
  }

  @Test
  void parseHostPortRejectsMissingClosingBracket() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort("[::1:2424"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("closing bracket");
  }

  @Test
  void parseHostPortRejectsEmptyAddress() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort(""))
        .isInstanceOf(ConfigurationException.class);
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort(null))
        .isInstanceOf(ConfigurationException.class);
  }

  @Test
  void parseHostPortRejectsMissingPort() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.parseHostPort("host-a"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("missing port");
  }

  // -- validatePeerAddress (negative paths not already in RaftHAServerValidatePeerAddressTest) --

  @Test
  void validatePeerAddressRejectsEmptyHost() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.validatePeerAddress(":2424"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("empty host");
  }

  @Test
  void validatePeerAddressRejectsNonNumericPort() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.validatePeerAddress("host-a:abcd"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("non-numeric port");
  }

  @Test
  void validatePeerAddressRejectsOutOfRangePort() {
    assertThatThrownBy(() -> RaftPeerAddressResolver.validatePeerAddress("host-a:0"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("out of range");
    assertThatThrownBy(() -> RaftPeerAddressResolver.validatePeerAddress("host-a:65536"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("out of range");
  }

  // -- parsePeerList (static) --

  @Test
  void parsePeerListAppendsDefaultPortWhenMissing() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("host-a,host-b", DEFAULT_RAFT_PORT);
    assertThat(parsed.peers()).hasSize(2);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("host-a:" + DEFAULT_RAFT_PORT);
    assertThat(parsed.peers().get(1).getAddress()).isEqualTo("host-b:" + DEFAULT_RAFT_PORT);
  }

  @Test
  void parsePeerListPopulatesHttpAddressesForThreePartEntries() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("host-a:2424:2480,host-b:2425:2481", DEFAULT_RAFT_PORT);
    final Map<RaftPeerId, String> http = parsed.httpAddresses();
    assertThat(http.get(RaftPeerId.valueOf("host-a_2424"))).isEqualTo("host-a:2480");
    assertThat(http.get(RaftPeerId.valueOf("host-b_2425"))).isEqualTo("host-b:2481");
  }

  @Test
  void parsePeerListAssignsPriority() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("host-a:2424:2480:10,host-b:2425:2481:5", DEFAULT_RAFT_PORT);
    assertThat(parsed.peers().get(0).getPriority()).isEqualTo(10);
    assertThat(parsed.peers().get(1).getPriority()).isEqualTo(5);
  }

  @Test
  void parsePeerListRejectsInvalidPriority() {
    assertThatThrownBy(() ->
        RaftPeerAddressResolver.parsePeerList("host-a:2424:2480:abc", DEFAULT_RAFT_PORT))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Invalid priority");
  }

  @Test
  void parsePeerListRejectsMixedLocalhostAndRemote() {
    assertThatThrownBy(() ->
        RaftPeerAddressResolver.parsePeerList("localhost:2424,10.0.0.1:2425", DEFAULT_RAFT_PORT))
        .isInstanceOf(ServerException.class)
        .hasMessageContaining("localhost");
  }

  @Test
  void parsePeerListHandlesBracketedIPv6Entries() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("[::1]:2424,[::1]:2425", DEFAULT_RAFT_PORT);
    assertThat(parsed.peers()).hasSize(2);
    assertThat(parsed.peers().get(0).getAddress()).isEqualTo("[::1]:2424");
  }

  @Test
  void parsePeerListIgnoresBlankEntries() {
    final var parsed = RaftPeerAddressResolver.parsePeerList("host-a:2424,,host-b:2425,  ", DEFAULT_RAFT_PORT);
    assertThat(parsed.peers()).hasSize(2);
  }

  // -- parsePeers (instance) --

  @Test
  void parsePeersProducesRaftPeersWithDerivedHttpAddress() {
    final TestArcadeDBServer server = new TestArcadeDBServer("server-0", 2424, 2480);
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.getConfiguration());
    final List<RaftPeer> peers = resolver.parsePeers("host-a:2424,host-b:2425");
    assertThat(peers).hasSize(2);
    assertThat(peers.get(0).getId().toString()).isEqualTo("host-a_2424");
    // Derived from offset: httpPort = raftPort + (2480 - 2424)
    assertThat(resolver.getPeerHTTPAddress(peers.get(1).getId())).isEqualTo("host-b:2481");
  }

  @Test
  void parsePeersHonorsExplicitHttpAddress() {
    final TestArcadeDBServer server = new TestArcadeDBServer("server-0", 2424, 2480);
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.getConfiguration());
    final List<RaftPeer> peers = resolver.parsePeers("host-a:2424:9000");
    assertThat(resolver.getPeerHTTPAddress(peers.get(0).getId())).isEqualTo("host-a:9000");
  }

  @Test
  void parsePeersAssignsPriority() {
    final TestArcadeDBServer server = new TestArcadeDBServer("server-0", 2424, 2480);
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.getConfiguration());
    final List<RaftPeer> peers = resolver.parsePeers("host-a:2424:2480:7");
    assertThat(peers.get(0).getPriority()).isEqualTo(7);
  }

  @Test
  void parsePeersRejectsInvalidPriority() {
    final TestArcadeDBServer server = new TestArcadeDBServer("server-0", 2424, 2480);
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.getConfiguration());
    assertThatThrownBy(() -> resolver.parsePeers("host-a:2424:2480:nope"))
        .isInstanceOf(ConfigurationException.class);
  }

  // -- resolveLocalPeerId --

  @Test
  void resolveLocalPeerIdByExactMatch() {
    final TestArcadeDBServer server = new TestArcadeDBServer("not-used", 2424, 2480);
    server.configuration.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, DEFAULT_HOST);
    server.configuration.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS, "2424");
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.configuration);
    final List<RaftPeer> peers = resolver.parsePeers("localhost:2424,localhost:2425");
    assertThat(resolver.resolveLocalPeerId(peers).toString()).isEqualTo("localhost_2424");
  }

  @Test
  void resolveLocalPeerIdByServerName() {
    final TestArcadeDBServer server = new TestArcadeDBServer("arcadedb-1", 2425, 2481);
    // Use a made-up incoming host so exact match doesn't fire; server-name match should.
    server.configuration.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "unused");
    server.configuration.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS, "2425");
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.configuration);
    final List<RaftPeer> peers = resolver.parsePeers("arcadedb-0:2424,arcadedb-1:2425,arcadedb-2:2426");
    assertThat(resolver.resolveLocalPeerId(peers).toString()).isEqualTo("arcadedb-1_2425");
  }

  @Test
  void resolveLocalPeerIdByPortFallbackWhenUnambiguous() {
    final TestArcadeDBServer server = new TestArcadeDBServer("no-match", 9999, 9950);
    server.configuration.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "no-match");
    server.configuration.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS, "9999");
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.configuration);
    final List<RaftPeer> peers = resolver.parsePeers("something:9999,other:2425,more:2426");
    // Only "something:9999" uses port 9999, so port-only fallback must pick it.
    assertThat(resolver.resolveLocalPeerId(peers).toString()).isEqualTo("something_9999");
  }

  @Test
  void resolveLocalPeerIdThrowsWhenAmbiguousPort() {
    final TestArcadeDBServer server = new TestArcadeDBServer("no-match", 2424, 2480);
    server.configuration.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "no-match");
    server.configuration.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS, "2424");
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.configuration);
    // Two peers use port 2424 → ambiguous → falls through to the throw.
    final List<RaftPeer> peers = resolver.parsePeers("host-a:2424,host-b:2424,host-c:2425");
    assertThatThrownBy(() -> resolver.resolveLocalPeerId(peers))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Cannot find local server");
  }

  // -- getPeerHTTPAddress derivation --

  @Test
  void getPeerHTTPAddressDerivesWhenMappingAbsent() {
    final TestArcadeDBServer server = new TestArcadeDBServer("server-0", 2424, 2480);
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.getConfiguration());
    // No parsePeers call → peerHttpAddresses map is empty; derivation path exercised.
    final String http = resolver.getPeerHTTPAddress(RaftPeerId.valueOf("peer-x_2600"));
    assertThat(http).isEqualTo("peer-x:2656"); // 2600 + (2480 - 2424)
  }

  @Test
  void getPeerHTTPAddressReturnsRawPeerIdWhenUnparsable() {
    final TestArcadeDBServer server = new TestArcadeDBServer("server-0", 2424, 2480);
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.getConfiguration());
    // Peer ID without the underscore+port suffix → derivation fails → raw id returned.
    final String http = resolver.getPeerHTTPAddress(RaftPeerId.valueOf("no-underscore-suffix"));
    assertThat(http).isEqualTo("no-underscore-suffix");
  }

  @Test
  void getPeerHTTPAddressReturnsExplicitWhenRegistered() {
    final TestArcadeDBServer server = new TestArcadeDBServer("server-0", 2424, 2480);
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.getConfiguration());
    resolver.registerPeerHttpAddress("custom_2424", "other:2424", "explicit:9000");
    assertThat(resolver.getPeerHTTPAddress(RaftPeerId.valueOf("custom_2424"))).isEqualTo("explicit:9000");
  }

  @Test
  void registerPeerHttpAddressDerivesWhenNotProvided() {
    final TestArcadeDBServer server = new TestArcadeDBServer("server-0", 2424, 2480);
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.getConfiguration());
    resolver.registerPeerHttpAddress("custom_3000", "other:3000", null);
    assertThat(resolver.getPeerHTTPAddress(RaftPeerId.valueOf("custom_3000"))).isEqualTo("other:3056");
  }

  @Test
  void getLeaderHTTPAddressRoundTrips() {
    final TestArcadeDBServer server = new TestArcadeDBServer("server-0", 2424, 2480);
    final RaftPeerAddressResolver resolver = new RaftPeerAddressResolver(server, server.getConfiguration());
    final List<RaftPeer> peers = resolver.parsePeers("host-a:2424:2480,host-b:2425:2481");
    assertThat(resolver.getLeaderHTTPAddress(peers.get(0).getId().toString())).isEqualTo("host-a:2480");
    assertThat(resolver.getLeaderHTTPAddress(null)).isNull();
  }

  // -- Test harness: a minimal ArcadeDBServer stand-in --

  /**
   * A tiny {@link ArcadeDBServer} subclass that only provides the fields this resolver consults.
   * Avoids spinning up the full server lifecycle (plugins, event log, shutdown hook) for a
   * parser-focused test.
   */
  private static final class TestArcadeDBServer extends ArcadeDBServer {
    final ContextConfiguration configuration;
    final String               serverName;

    TestArcadeDBServer(final String serverName, final int raftPort, final int httpPort) {
      super(buildConfig(serverName, raftPort, httpPort));
      this.serverName = serverName;
      this.configuration = buildConfig(serverName, raftPort, httpPort);
    }

    private static ContextConfiguration buildConfig(final String serverName, final int raftPort, final int httpPort) {
      final ContextConfiguration c = new ContextConfiguration();
      c.setValue(GlobalConfiguration.SERVER_NAME, serverName);
      c.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_PORTS, Integer.toString(raftPort));
      c.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, Integer.toString(httpPort));
      c.setValue(GlobalConfiguration.HA_REPLICATION_INCOMING_HOST, "localhost");
      return c;
    }

    @Override
    public ContextConfiguration getConfiguration() {
      return configuration;
    }

    @Override
    public String getServerName() {
      return serverName;
    }
  }
}
