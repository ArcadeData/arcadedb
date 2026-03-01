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
package com.arcadedb.server.ha.discovery;

import com.arcadedb.server.ha.HAServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.TimeUnit;

/**
 * Unit tests for StaticListDiscovery implementation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StaticListDiscoveryTest {

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testDiscoveryWithSetConstructor() throws DiscoveryException {
    // Given: A static list of servers
    Set<HAServer.ServerInfo> servers = new HashSet<>();
    servers.add(new HAServer.ServerInfo("server1.example.com", 2424, "server1"));
    servers.add(new HAServer.ServerInfo("server2.example.com", 2424, "server2"));
    servers.add(new HAServer.ServerInfo("server3.example.com", 2424, "server3"));

    StaticListDiscovery discovery = new StaticListDiscovery(servers);

    // When: Discovering nodes
    Set<HAServer.ServerInfo> discovered = discovery.discoverNodes("test-cluster");

    // Then: All servers are discovered
    assertThat(discovered).hasSize(3);
    assertThat(discovered).containsExactlyInAnyOrderElementsOf(servers);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testDiscoveryWithStringConstructor() throws DiscoveryException {
    // Given: A comma-separated list of servers
    String serverList = "{server1}server1.example.com:2424,{server2}server2.example.com:2424,{server3}server3.example.com:2424";

    StaticListDiscovery discovery = new StaticListDiscovery(serverList);

    // When: Discovering nodes
    Set<HAServer.ServerInfo> discovered = discovery.discoverNodes("test-cluster");

    // Then: All servers are discovered
    assertThat(discovered).hasSize(3);
    assertThat(discovered)
        .extracting(HAServer.ServerInfo::alias)
        .containsExactlyInAnyOrder("server1", "server2", "server3");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testDiscoveryWithSingleServer() throws DiscoveryException {
    // Given: A single server
    String serverList = "{arcade1}localhost:2424";

    StaticListDiscovery discovery = new StaticListDiscovery(serverList);

    // When: Discovering nodes
    Set<HAServer.ServerInfo> discovered = discovery.discoverNodes("test-cluster");

    // Then: Single server is discovered
    assertThat(discovered).hasSize(1);
    HAServer.ServerInfo server = discovered.iterator().next();
    assertThat(server.host()).isEqualTo("localhost");
    assertThat(server.port()).isEqualTo(2424);
    assertThat(server.alias()).isEqualTo("arcade1");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testDiscoveryIsIdempotent() throws DiscoveryException {
    // Given: A static list of servers
    String serverList = "{server1}server1:2424,{server2}server2:2424";
    StaticListDiscovery discovery = new StaticListDiscovery(serverList);

    // When: Calling discovery multiple times
    Set<HAServer.ServerInfo> discovered1 = discovery.discoverNodes("test-cluster");
    Set<HAServer.ServerInfo> discovered2 = discovery.discoverNodes("test-cluster");

    // Then: Results are consistent
    assertThat(discovered1).isEqualTo(discovered2);
    assertThat(discovered1).hasSize(2);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testNullServerListThrowsException() {
    // When/Then: Creating discovery with null server set
    assertThatThrownBy(() -> new StaticListDiscovery((Set<HAServer.ServerInfo>) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Server list cannot be null");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testNullServerListStringThrowsException() {
    // When/Then: Creating discovery with null server list string
    assertThatThrownBy(() -> new StaticListDiscovery((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Server list string cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testEmptyServerListStringThrowsException() {
    // When/Then: Creating discovery with empty server list string
    assertThatThrownBy(() -> new StaticListDiscovery(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Server list string cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testWhitespaceOnlyServerListStringThrowsException() {
    // When/Then: Creating discovery with whitespace-only server list string
    assertThatThrownBy(() -> new StaticListDiscovery("   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Server list string cannot be null or empty");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testServerListWithWhitespace() throws DiscoveryException {
    // Given: A server list with extra whitespace
    String serverList = " {server1}server1:2424 , {server2}server2:2424 , {server3}server3:2424 ";

    StaticListDiscovery discovery = new StaticListDiscovery(serverList);

    // When: Discovering nodes
    Set<HAServer.ServerInfo> discovered = discovery.discoverNodes("test-cluster");

    // Then: All servers are discovered with whitespace trimmed
    assertThat(discovered).hasSize(3);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testRegisterNodeIsNoOp() throws DiscoveryException {
    // Given: A static discovery service
    String serverList = "{server1}server1:2424";
    StaticListDiscovery discovery = new StaticListDiscovery(serverList);
    HAServer.ServerInfo newServer = new HAServer.ServerInfo("newserver", 2424, "new");

    // When: Registering a node
    discovery.registerNode(newServer);

    // Then: Discovery list remains unchanged
    Set<HAServer.ServerInfo> discovered = discovery.discoverNodes("test-cluster");
    assertThat(discovered).hasSize(1);
    assertThat(discovered).doesNotContain(newServer);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testDeregisterNodeIsNoOp() throws DiscoveryException {
    // Given: A static discovery service
    Set<HAServer.ServerInfo> servers = new HashSet<>();
    HAServer.ServerInfo server1 = new HAServer.ServerInfo("server1", 2424, "s1");
    servers.add(server1);
    StaticListDiscovery discovery = new StaticListDiscovery(servers);

    // When: Deregistering a node
    discovery.deregisterNode(server1);

    // Then: Discovery list remains unchanged
    Set<HAServer.ServerInfo> discovered = discovery.discoverNodes("test-cluster");
    assertThat(discovered).hasSize(1);
    assertThat(discovered).contains(server1);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testGetName() {
    // Given: A static discovery service
    String serverList = "{server1}server1:2424";
    StaticListDiscovery discovery = new StaticListDiscovery(serverList);

    // When/Then: Name is "static"
    assertThat(discovery.getName()).isEqualTo("static");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testGetConfiguredServers() {
    // Given: A static discovery service
    Set<HAServer.ServerInfo> servers = new HashSet<>();
    servers.add(new HAServer.ServerInfo("server1", 2424, "s1"));
    servers.add(new HAServer.ServerInfo("server2", 2424, "s2"));
    StaticListDiscovery discovery = new StaticListDiscovery(servers);

    // When: Getting configured servers
    Set<HAServer.ServerInfo> configured = discovery.getConfiguredServers();

    // Then: Returns all configured servers
    assertThat(configured).isEqualTo(servers);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testConfiguredServersIsUnmodifiable() {
    // Given: A static discovery service
    Set<HAServer.ServerInfo> servers = new HashSet<>();
    servers.add(new HAServer.ServerInfo("server1", 2424, "s1"));
    StaticListDiscovery discovery = new StaticListDiscovery(servers);

    // When: Getting configured servers
    Set<HAServer.ServerInfo> configured = discovery.getConfiguredServers();

    // Then: Returned set is unmodifiable
    assertThatThrownBy(() -> configured.add(new HAServer.ServerInfo("server2", 2424, "s2")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testToString() {
    // Given: A static discovery service with 3 servers
    String serverList = "{s1}server1:2424,{s2}server2:2424,{s3}server3:2424";
    StaticListDiscovery discovery = new StaticListDiscovery(serverList);

    // When/Then: toString contains server count
    assertThat(discovery.toString()).contains("StaticListDiscovery");
    assertThat(discovery.toString()).contains("3");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  void testDiscoveryWithDefaultPort() throws DiscoveryException {
    // Given: A server without explicit port (should use default 2424)
    String serverList = "{server1}server1.example.com";

    StaticListDiscovery discovery = new StaticListDiscovery(serverList);

    // When: Discovering nodes
    Set<HAServer.ServerInfo> discovered = discovery.discoverNodes("test-cluster");

    // Then: Server uses default port
    assertThat(discovered).hasSize(1);
    HAServer.ServerInfo server = discovered.iterator().next();
    assertThat(server.port()).isEqualTo(2424);
  }
}
