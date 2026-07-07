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
package com.arcadedb.bolt;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.packstream.PackStreamWriter;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Certifies that the Bolt ROUTE response reflects real HA topology: the true leader is advertised as the
 * writer and the followers as readers, and a neo4j:// driver can route reads and writes accordingly.
 * Covers conformance scenario CONN-004.
 */
@Tag("slow")
class Bolt5002RoutingTableIT extends BaseRaftHATest {

  private static final int    BASE_BOLT_PORT = 57697;
  private static final String VERTEX_TYPE    = "Bolt5002Route";

  private Driver driver;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    // Object form so each node declares its own Bolt port (nodes share localhost, differ only by port).
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < getServerCount(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append("localhost:{raft:").append(2434 + i)
          .append(",http:").append(2480 + i)
          .append(",bolt:").append(BASE_BOLT_PORT + i).append("}");
    }
    return sb.toString();
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final String serverName = config.getValueAsString(GlobalConfiguration.SERVER_NAME);
    final int index = Integer.parseInt(serverName.substring(serverName.lastIndexOf('_') + 1));
    config.setValue(GlobalConfiguration.SERVER_PLUGINS.getKey(), "Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    config.setValue(GlobalConfiguration.BOLT_PORT.getKey(), String.valueOf(BASE_BOLT_PORT + index));
  }

  @AfterEach
  void closeDriver() {
    if (driver != null) {
      driver.close();
      driver = null;
    }
  }

  @Test
  void routeTableReflectsLeaderAndFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    waitForAllServers();

    final String leaderBolt = "localhost:" + (BASE_BOLT_PORT + leaderIndex);
    final List<String> followerBolt = new ArrayList<>();
    for (int i = 0; i < getServerCount(); i++)
      if (i != leaderIndex)
        followerBolt.add("localhost:" + (BASE_BOLT_PORT + i));

    // Ask a follower (not the leader) for its routing table to prove routing is not leader-local. Poll
    // until the follower has learned the leader (a freshly-connected follower may briefly not know it).
    final int askIndex = (leaderIndex + 1) % getServerCount();
    final Map<String, Object> rt = awaitRoutingTable(BASE_BOLT_PORT + askIndex, leaderBolt);

    assertThat(BoltRouteTestSupport.addressesForRole(rt, "WRITE")).containsExactly(leaderBolt);
    assertThat(BoltRouteTestSupport.addressesForRole(rt, "READ")).containsExactlyInAnyOrderElementsOf(followerBolt);
    assertThat(BoltRouteTestSupport.addressesForRole(rt, "ROUTE")).contains(leaderBolt).containsAll(followerBolt);
  }

  @Test
  void neo4jSchemeRoutesReadsAndWrites() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);

    driver = GraphDatabase.driver(
        "neo4j://localhost:" + (BASE_BOLT_PORT + leaderIndex),
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
        Config.builder().withoutEncryption().build());
    driver.verifyConnectivity();

    try (Session session = driver.session(SessionConfig.builder().withDatabase(getDatabaseName()).build())) {
      session.executeWrite(tx -> {
        tx.run("CREATE (n:" + VERTEX_TYPE + " {name: 'routed'})").consume();
        return null;
      });
    }
    waitForAllServers();

    try (Session session = driver.session(SessionConfig.builder().withDatabase(getDatabaseName()).build())) {
      final long count = session.executeRead(tx ->
          tx.run("MATCH (n:" + VERTEX_TYPE + ") RETURN count(n) AS c").single().get("c").asLong());
      assertThat(count).isEqualTo(1L);
    }
  }

  @Test
  void writerClassificationTracksLeaderChange() throws Exception {
    final int firstLeader = findLeaderIndex();
    assertThat(firstLeader).isGreaterThanOrEqualTo(0);

    // Stop the current leader and wait for the surviving nodes to elect a new one.
    getServer(firstLeader).stop();

    int newLeader = -1;
    for (int attempt = 0; attempt < 60 && newLeader < 0; attempt++) {
      final int candidate = findLeaderIndex();
      if (candidate >= 0 && candidate != firstLeader)
        newLeader = candidate;
      else
        Thread.sleep(500);
    }
    assertThat(newLeader).as("A new leader must be elected after the old one stops").isGreaterThanOrEqualTo(0);

    final String newLeaderBolt = "localhost:" + (BASE_BOLT_PORT + newLeader);
    final Map<String, Object> rt = awaitRoutingTable(BASE_BOLT_PORT + newLeader, newLeaderBolt);
    assertThat(BoltRouteTestSupport.addressesForRole(rt, "WRITE")).containsExactly(newLeaderBolt);
  }

  // --- raw Bolt ROUTE helper -------------------------------------------------

  /**
   * Polls the routing table on {@code boltPort} until its WRITE role advertises {@code expectedWriter},
   * tolerating the brief window in which a freshly-queried node has not yet learned the current leader.
   */
  private Map<String, Object> awaitRoutingTable(final int boltPort, final String expectedWriter) throws Exception {
    Map<String, Object> rt = null;
    for (int attempt = 0; attempt < 40; attempt++) {
      try {
        rt = fetchRoutingTable(boltPort);
        if (List.of(expectedWriter).equals(BoltRouteTestSupport.addressesForRole(rt, "WRITE")))
          return rt;
      } catch (final Exception e) {
        // A node contacted mid-failover may reset the connection; retry until it settles.
      }
      Thread.sleep(250);
    }
    assertThat(rt)
        .as("no routing table advertising writer %s obtained from bolt port %d after retries", expectedWriter, boltPort)
        .isNotNull();
    return rt;
  }

  private Map<String, Object> fetchRoutingTable(final int boltPort) throws Exception {
    try (Socket socket = new Socket("localhost", boltPort)) {
      final OutputStream rawOut = socket.getOutputStream();
      final ByteBuffer handshake = ByteBuffer.allocate(20);
      handshake.put((byte) 0x60).put((byte) 0x60).put((byte) 0xB0).put((byte) 0x17);
      handshake.putInt(0x00020404);
      handshake.putInt(0x00000004);
      handshake.putInt(0x00000003);
      handshake.putInt(0x00000000);
      handshake.flip();
      rawOut.write(handshake.array());
      rawOut.flush();

      final DataInputStream rawIn = new DataInputStream(socket.getInputStream());
      final byte[] negotiated = new byte[4];
      rawIn.readFully(negotiated);

      final BoltChunkedOutput out = new BoltChunkedOutput(rawOut);
      final BoltChunkedInput in = new BoltChunkedInput(socket.getInputStream());

      final PackStreamWriter hello = new PackStreamWriter();
      hello.writeStructureHeader(BoltMessage.HELLO, 1);
      hello.writeMap(Map.of("user_agent", "bolt5002/1.0", "scheme", "basic",
          "principal", "root", "credentials", DEFAULT_PASSWORD_FOR_TESTS));
      out.writeMessage(hello.toByteArray());
      in.readMessage();

      final PackStreamWriter route = new PackStreamWriter();
      route.writeStructureHeader(BoltMessage.ROUTE, 3);
      route.writeMap(Map.of());
      route.writeList(List.of());
      route.writeMap(Map.of("db", getDatabaseName()));
      out.writeMessage(route.toByteArray());

      final byte[] response = in.readMessage();
      // Throw (rather than assert) on a non-SUCCESS ROUTE so awaitRoutingTable's retry loop, which
      // catches Exception, keeps polling through a failover window instead of failing on an AssertionError.
      if (response[1] != BoltMessage.SUCCESS)
        throw new IOException("ROUTE did not return SUCCESS (signature 0x" + Integer.toHexString(response[1] & 0xFF) + ")");
      return BoltRouteTestSupport.readRoutingTable(response);
    }
  }
}
