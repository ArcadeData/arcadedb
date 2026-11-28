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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.PeerRequest;
import com.arcadedb.server.security.ServerSecurity;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A request that reaches a replica over HTTP and has to run on the leader is relayed under the name of the user who sent it. The
 * user name is checked where the relayed request is built, so a user whose name a header cannot carry as written gets a client
 * error that says so, and nothing is sent to the leader. A user whose name is fine is relayed under that name, as before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReplicaForwardedPrincipalHttpTest extends BaseGraphServerTest {

  private static final String PASSWORD = "TestPass123!";

  private final AtomicInteger         leaderRequests  = new AtomicInteger();
  private final AtomicReference<String> leaderPrincipal = new AtomicReference<>();
  private       HttpServer            leader;
  private       JSONArray             originalUsers;

  @BeforeEach
  void startTheLeaderStubAndTheUsers() throws IOException {
    leader = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 4);
    leader.createContext("/", exchange -> {
      leaderRequests.incrementAndGet();
      leaderPrincipal.set(exchange.getRequestHeaders().getFirst(PeerRequest.FORWARDED_USER_HEADER));
      exchange.getRequestBody().readAllBytes();
      final byte[] answer = "{\"result\":\"relayed\"}".getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, answer.length);
      exchange.getResponseBody().write(answer);
      exchange.close();
    });
    leader.start();

    // The users are installed as a whole list, the way a node receives them from the cluster: that route does not apply the rule a
    // new user is held to, so it stands for a user that is stored already
    final ServerSecurity security = getServer(0).getSecurity();
    originalUsers = new JSONArray();
    for (final JSONObject user : security.usersToJSON())
      originalUsers.put(user);

    final JSONArray users = new JSONArray();
    for (final JSONObject user : security.usersToJSON())
      users.put(user);
    for (final String name : new String[] { "reader ", "jane.doe" })
      users.put(new JSONObject().put("name", name).put("password", security.encodePassword(PASSWORD))
          .put("databases", new JSONObject().put("*", new JSONArray().put("admin"))));
    security.applyReplicatedUsers(users.toString());

    getServer(0).setHA(new ReplicaStub("127.0.0.1:" + leader.getAddress().getPort()));
  }

  @AfterEach
  void restoreTheServer() {
    getServer(0).setHA(null);
    leader.stop(0);
    getServer(0).getSecurity().applyReplicatedUsers(originalUsers.toString());
  }

  @Test
  void aUserWhoseNameCannotBeCarriedGetsAClearClientErrorAndNothingIsRelayed() throws Exception {
    final HttpURLConnection connection = post("reader ");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(400);
      assertThat(readError(connection)).contains("cannot be named in a header").contains("starts or ends with whitespace");
    } finally {
      connection.disconnect();
    }
    assertThat(leaderRequests.get()).isZero();
  }

  @Test
  void aUserWithAnOrdinaryNameIsRelayedUnderThatName() throws Exception {
    final HttpURLConnection connection = post("jane.doe");
    try {
      assertThat(connection.getResponseCode()).isEqualTo(200);
      assertThat(readResponse(connection)).contains("relayed");
    } finally {
      connection.disconnect();
    }
    assertThat(leaderRequests.get()).isEqualTo(1);
    assertThat(leaderPrincipal.get()).isEqualTo("jane.doe");
  }

  private HttpURLConnection post(final String user) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        getServerHttpUrl(0, "/api/v1/batch/" + getDatabaseName())).toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);
    connection.getOutputStream().write("{\"@type\":\"vertex\",\"@class\":\"V1\",\"@id\":\"a\"}\n".getBytes(StandardCharsets.UTF_8));
    connection.connect();
    return connection;
  }

  /** The follower the request lands on: it is not the leader, and it knows where the leader is and the token it is relayed with. */
  private static final class ReplicaStub implements HAServerPlugin {
    private final String leaderAddress;

    private ReplicaStub(final String leaderAddress) {
      this.leaderAddress = leaderAddress;
    }

    @Override
    public String getLeaderAddress() {
      return leaderAddress;
    }

    @Override
    public String getClusterToken() {
      return "cluster-token";
    }

    @Override
    public boolean isLeader() {
      return false;
    }

    @Override
    public ELECTION_STATUS getElectionStatus() {
      return ELECTION_STATUS.DONE;
    }

    @Override
    public String getLeaderName() {
      return "leader";
    }

    @Override
    public String getClusterName() {
      return "test";
    }

    @Override
    public Map<String, Object> getStats() {
      return Collections.emptyMap();
    }

    @Override
    public int getConfiguredServers() {
      return 3;
    }

    @Override
    public String getReplicaAddresses() {
      return "";
    }

    @Override
    public void shutdownRemoteServer(final String serverName) {
    }

    @Override
    public void disconnectCluster() {
    }

    @Override
    public void startService() {
    }
  }
}
