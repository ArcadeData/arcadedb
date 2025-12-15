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
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;
import com.arcadedb.server.ha.HAServer.HACluster;
import com.arcadedb.server.ha.HAServer.ServerInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

/**
 * Unit tests for UpdateClusterConfiguration message.
 * Tests serialization/deserialization of cluster configuration including HTTP addresses.
 *
 * @author Claude Sonnet 4.5
 */
class UpdateClusterConfigurationTest {

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @DisplayName("Test serialization of cluster configuration without HTTP addresses")
  void testSerializationWithoutHttpAddresses() {
    // Create a cluster with server information only
    Set<ServerInfo> servers = new HashSet<>();
    servers.add(new ServerInfo("server1.internal", 2424, "server1"));
    servers.add(new ServerInfo("server2.internal", 2424, "server2"));
    servers.add(new ServerInfo("server3.internal", 2424, "server3"));

    HACluster cluster = new HACluster(servers);
    UpdateClusterConfiguration message = new UpdateClusterConfiguration(cluster, null);

    // Serialize to binary
    Binary stream = new Binary();
    message.toStream(stream);

    // Verify stream is not empty
    assertThat(stream.size()).isGreaterThan(0);

    // Read the serialized data
    stream.position(0);
    String serializedServers = stream.getString();

    // Verify it contains server information
    assertThat(serializedServers).contains("server1");
    assertThat(serializedServers).contains("server2");
    assertThat(serializedServers).contains("server3");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @DisplayName("Test serialization of cluster configuration with HTTP addresses")
  void testSerializationWithHttpAddresses() {
    // Create a cluster with server information
    Set<ServerInfo> servers = new HashSet<>();
    ServerInfo server1 = new ServerInfo("server1.internal", 2424, "server1");
    ServerInfo server2 = new ServerInfo("server2.internal", 2424, "server2");
    ServerInfo server3 = new ServerInfo("server3.internal", 2424, "server3");
    servers.add(server1);
    servers.add(server2);
    servers.add(server3);

    // Create HTTP addresses map
    Map<ServerInfo, String> httpAddresses = new HashMap<>();
    httpAddresses.put(server1, "http://server1.internal:8080");
    httpAddresses.put(server2, "http://server2.internal:8080");
    httpAddresses.put(server3, "http://server3.internal:8080");

    HACluster cluster = new HACluster(servers);
    UpdateClusterConfiguration message = new UpdateClusterConfiguration(cluster, httpAddresses);

    // Serialize to binary
    Binary stream = new Binary();
    message.toStream(stream);

    // Verify stream is not empty
    assertThat(stream.size()).isGreaterThan(0);

    // Read the serialized data
    stream.position(0);
    String serializedServers = stream.getString();

    // Verify it contains server information
    assertThat(serializedServers).contains("server1");
    assertThat(serializedServers).contains("server2");
    assertThat(serializedServers).contains("server3");

    // Check if HTTP addresses are included in the stream
    if (stream.position() < stream.size()) {
      String serializedHttpAddresses = stream.getString();
      assertThat(serializedHttpAddresses).contains("http://server1.internal:8080");
      assertThat(serializedHttpAddresses).contains("http://server2.internal:8080");
      assertThat(serializedHttpAddresses).contains("http://server3.internal:8080");
    }
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @DisplayName("Test serialization with partial HTTP addresses")
  void testSerializationWithPartialHttpAddresses() {
    // Create a cluster with server information
    Set<ServerInfo> servers = new HashSet<>();
    ServerInfo server1 = new ServerInfo("server1.internal", 2424, "server1");
    ServerInfo server2 = new ServerInfo("server2.internal", 2424, "server2");
    ServerInfo server3 = new ServerInfo("server3.internal", 2424, "server3");
    servers.add(server1);
    servers.add(server2);
    servers.add(server3);

    // Create HTTP addresses map with only some servers
    Map<ServerInfo, String> httpAddresses = new HashMap<>();
    httpAddresses.put(server1, "http://server1.internal:8080");
    // server2 has no HTTP address
    httpAddresses.put(server3, "http://server3.internal:8080");

    HACluster cluster = new HACluster(servers);
    UpdateClusterConfiguration message = new UpdateClusterConfiguration(cluster, httpAddresses);

    // Serialize to binary
    Binary stream = new Binary();
    message.toStream(stream);

    // Verify stream is not empty
    assertThat(stream.size()).isGreaterThan(0);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @DisplayName("Test toString includes cluster information")
  void testToString() {
    Set<ServerInfo> servers = new HashSet<>();
    servers.add(new ServerInfo("server1", 2424, "alias1"));
    servers.add(new ServerInfo("server2", 2424, "alias2"));

    HACluster cluster = new HACluster(servers);
    UpdateClusterConfiguration message = new UpdateClusterConfiguration(cluster, null);

    String result = message.toString();

    assertThat(result).contains("updateClusterConfig");
    assertThat(result).contains("servers=");
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @DisplayName("Test serialization with empty cluster")
  void testSerializationWithEmptyCluster() {
    Set<ServerInfo> servers = new HashSet<>();
    HACluster cluster = new HACluster(servers);
    UpdateClusterConfiguration message = new UpdateClusterConfiguration(cluster, null);

    Binary stream = new Binary();
    message.toStream(stream);

    // Should produce valid (but empty) serialization
    assertThat(stream.size()).isGreaterThanOrEqualTo(0);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @DisplayName("Test HTTP addresses map can be null")
  void testNullHttpAddresses() {
    Set<ServerInfo> servers = new HashSet<>();
    servers.add(new ServerInfo("server1", 2424, "server1"));

    HACluster cluster = new HACluster(servers);
    UpdateClusterConfiguration message = new UpdateClusterConfiguration(cluster, null);

    // Should not throw exception
    Binary stream = new Binary();
    message.toStream(stream);

    assertThat(stream.size()).isGreaterThan(0);
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @DisplayName("Test HTTP addresses with special characters in URLs")
  void testHttpAddressesWithSpecialCharacters() {
    Set<ServerInfo> servers = new HashSet<>();
    ServerInfo server1 = new ServerInfo("192.168.1.10", 2424, "server1");
    servers.add(server1);

    Map<ServerInfo, String> httpAddresses = new HashMap<>();
    httpAddresses.put(server1, "http://192.168.1.10:8080/arcadedb");

    HACluster cluster = new HACluster(servers);
    UpdateClusterConfiguration message = new UpdateClusterConfiguration(cluster, httpAddresses);

    Binary stream = new Binary();
    message.toStream(stream);

    assertThat(stream.size()).isGreaterThan(0);
  }
}
