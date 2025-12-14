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
package com.arcadedb.server.ha;

import com.arcadedb.server.ha.HAServer.HACluster;
import com.arcadedb.server.ha.HAServer.ServerInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for alias resolution mechanism in HAServer.
 * This tests the fix for issue #2945 where alias placeholders in server addresses
 * were not properly resolved during cluster formation in Docker/K8s environments.
 *
 * @author Claude Sonnet 4.5
 */
class HAServerAliasResolutionTest {

  @Test
  @DisplayName("Test alias resolution with proxy addresses as in SimpleHaScenarioIT")
  void testAliasResolutionWithProxyAddresses() {
    // Create cluster with servers using proxy addresses and aliases
    // This simulates the setup from SimpleHaScenarioIT.java:29-30
    // arcade1: {arcade2}proxy:8667
    // arcade2: {arcade1}proxy:8666

    Set<ServerInfo> servers = new HashSet<>();
    ServerInfo arcade1 = new ServerInfo("arcade1", 2424, "arcade1");
    ServerInfo arcade2 = new ServerInfo("arcade2", 2424, "arcade2");
    servers.add(arcade1);
    servers.add(arcade2);

    HACluster cluster = new HACluster(servers);

    // Test finding server by alias
    Optional<ServerInfo> found1 = cluster.findByAlias("arcade1");
    assertThat(found1).isPresent();
    assertThat(found1.get()).isEqualTo(arcade1);
    assertThat(found1.get().host()).isEqualTo("arcade1");
    assertThat(found1.get().port()).isEqualTo(2424);

    Optional<ServerInfo> found2 = cluster.findByAlias("arcade2");
    assertThat(found2).isPresent();
    assertThat(found2.get()).isEqualTo(arcade2);
    assertThat(found2.get().host()).isEqualTo("arcade2");
    assertThat(found2.get().port()).isEqualTo(2424);
  }

  @Test
  @DisplayName("Test alias resolution with unresolved alias placeholder")
  void testAliasResolutionWithPlaceholder() {
    // This tests the scenario where a ServerInfo is created with an alias placeholder
    // in the host field, as happens when parsing leader address from exception

    Set<ServerInfo> servers = new HashSet<>();
    ServerInfo server1 = new ServerInfo("192.168.1.10", 8666, "server1");
    ServerInfo server2 = new ServerInfo("192.168.1.20", 8667, "server2");
    servers.add(server1);
    servers.add(server2);

    HACluster cluster = new HACluster(servers);

    // Simulate receiving a leader address like "{server1}proxy:8666"
    // After parsing with HostUtil, we get alias="server1", host="proxy", port="8666"
    // We need to resolve "server1" alias to get the real host

    Optional<ServerInfo> resolved = cluster.findByAlias("server1");
    assertThat(resolved).isPresent();
    assertThat(resolved.get().host()).isEqualTo("192.168.1.10");
    assertThat(resolved.get().port()).isEqualTo(8666);
  }

  @Test
  @DisplayName("Test alias resolution with missing alias returns empty")
  void testAliasResolutionMissingAlias() {
    Set<ServerInfo> servers = new HashSet<>();
    servers.add(new ServerInfo("host1", 2424, "alias1"));
    servers.add(new ServerInfo("host2", 2424, "alias2"));

    HACluster cluster = new HACluster(servers);

    Optional<ServerInfo> result = cluster.findByAlias("nonexistent");
    assertThat(result).isEmpty();
  }

  @Test
  @DisplayName("Test ServerInfo toString includes alias format")
  void testServerInfoToStringFormat() {
    ServerInfo server = new ServerInfo("localhost", 2424, "myalias");

    String result = server.toString();

    assertThat(result).isEqualTo("{myalias}localhost:2424");
  }

  @Test
  @DisplayName("Test ServerInfo fromString creates correct instance with alias")
  void testServerInfoFromStringWithAlias() {
    String address = "{arcade1}proxy:8666";

    ServerInfo server = ServerInfo.fromString(address);

    assertThat(server.alias()).isEqualTo("arcade1");
    assertThat(server.host()).isEqualTo("proxy");
    assertThat(server.port()).isEqualTo(8666);
  }

  @Test
  @DisplayName("Test ServerInfo fromString creates correct instance without alias")
  void testServerInfoFromStringWithoutAlias() {
    String address = "localhost:2424";

    ServerInfo server = ServerInfo.fromString(address);

    assertThat(server.host()).isEqualTo("localhost");
    assertThat(server.port()).isEqualTo(2424);
    assertThat(server.alias()).isEqualTo("localhost");
  }

  @Test
  @DisplayName("Test multiple servers with different aliases can be resolved")
  void testMultipleAliasResolution() {
    Set<ServerInfo> servers = new HashSet<>();
    servers.add(new ServerInfo("db1.internal", 2424, "db1"));
    servers.add(new ServerInfo("db2.internal", 2424, "db2"));
    servers.add(new ServerInfo("db3.internal", 2424, "db3"));

    HACluster cluster = new HACluster(servers);

    assertThat(cluster.findByAlias("db1")).isPresent();
    assertThat(cluster.findByAlias("db2")).isPresent();
    assertThat(cluster.findByAlias("db3")).isPresent();

    assertThat(cluster.findByAlias("db1").get().host()).isEqualTo("db1.internal");
    assertThat(cluster.findByAlias("db2").get().host()).isEqualTo("db2.internal");
    assertThat(cluster.findByAlias("db3").get().host()).isEqualTo("db3.internal");
  }

  @Test
  @DisplayName("Test HACluster can find server by exact ServerInfo match")
  void testFindByServerInfo() {
    ServerInfo server1 = new ServerInfo("host1", 2424, "alias1");
    ServerInfo server2 = new ServerInfo("host2", 2424, "alias2");
    ServerInfo server3 = new ServerInfo("host3", 2424, "alias3");

    Set<ServerInfo> servers = new HashSet<>();
    servers.add(server1);
    servers.add(server2);
    servers.add(server3);

    HACluster cluster = new HACluster(servers);

    // Find by exact match
    assertThat(cluster.getServers()).contains(server1);
    assertThat(cluster.getServers()).contains(server2);
    assertThat(cluster.getServers()).contains(server3);
  }

  @Test
  @DisplayName("Test HACluster can find server by host and port")
  void testFindByHostAndPort() {
    ServerInfo server1 = new ServerInfo("host1", 2424, "alias1");
    ServerInfo server2 = new ServerInfo("host2", 2425, "alias2");

    Set<ServerInfo> servers = new HashSet<>();
    servers.add(server1);
    servers.add(server2);

    HACluster cluster = new HACluster(servers);

    // Find servers matching host and port
    Optional<ServerInfo> found1 = cluster.getServers().stream()
        .filter(s -> s.host().equals("host1") && s.port() == 2424)
        .findFirst();
    assertThat(found1).isPresent();
    assertThat(found1.get()).isEqualTo(server1);

    Optional<ServerInfo> found2 = cluster.getServers().stream()
        .filter(s -> s.host().equals("host2") && s.port() == 2425)
        .findFirst();
    assertThat(found2).isPresent();
    assertThat(found2.get()).isEqualTo(server2);
  }
}
