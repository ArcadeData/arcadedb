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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.LeaderForwardContext;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8313: a follower sized the deadline of a forwarded write from the {@code arcadedb.command.timeout} in the
 * configuration its caller passed - which the convenience {@code command(...)} overloads and the HTTP handler fill with
 * the SERVER configuration, and which no command context reads - and the leader enforced its own database setting. The
 * forward now resolves the budget from this database's configuration, the rule every command context applies, and
 * sends it to the leader so both sides enforce the same number. The leader's half is
 * {@code Issue8313ForwardedCommandTimeoutTest} in the server module.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8313ForwardCarriesCommandBudgetTest {
  private HeaderRecordingLeader leader;

  @BeforeEach
  void startLeader() throws IOException {
    leader = new HeaderRecordingLeader();
  }

  @AfterEach
  void stopLeader() {
    leader.close();
  }

  @Test
  void theDatabasesBudgetTravelsWithTheForward() throws Exception {
    final ContextConfiguration databaseConfig = new ContextConfiguration();
    databaseConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 5_000L);

    forward(database(serverConfig(), databaseConfig, "test-token"));

    assertThat(leader.budgets()).containsExactly(Optional.of("5000"));
  }

  /**
   * The reported second case: the server configuration the convenience overloads pass is not the database's, and a
   * budget set on the database was not seen by the follower either. The server's value plays no part now.
   */
  @Test
  void theServerConfigurationIsNotTheBudget() throws Exception {
    final ContextConfiguration serverConfig = serverConfig();
    serverConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 99_000L);
    final ContextConfiguration databaseConfig = new ContextConfiguration();
    databaseConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 7_000L);

    forward(database(serverConfig, databaseConfig, "test-token"));

    assertThat(leader.budgets()).containsExactly(Optional.of("7000"));
  }

  @Test
  void anUnboundedCommandSendsNoBudget() throws Exception {
    final ContextConfiguration databaseConfig = new ContextConfiguration();
    databaseConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 0L);

    forward(database(serverConfig(), databaseConfig, "test-token"));

    assertThat(leader.budgets()).as("the leader then applies its own setting").containsExactly(Optional.empty());
  }

  @Test
  void withoutAClusterTokenNoBudgetIsSent() throws Exception {
    final ContextConfiguration databaseConfig = new ContextConfiguration();
    databaseConfig.setValue(GlobalConfiguration.COMMAND_TIMEOUT, 5_000L);

    forward(database(serverConfig(), databaseConfig, null));

    assertThat(leader.budgets()).as("the leader honours it under the token only, so it is not put on the wire without one")
        .containsExactly(Optional.empty());
  }

  private static void forward(final RaftReplicatedDatabase db) throws Exception {
    final Method m = RaftReplicatedDatabase.class.getDeclaredMethod("forwardCommandToLeaderViaRaft",
        String.class, String.class, Map.class, Object[].class);
    m.setAccessible(true);
    m.invoke(db, "sql", "INSERT INTO V SET id = 1", null, new Object[0]);
  }

  private static ContextConfiguration serverConfig() {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_COMMAND_TIMEOUT, 30_000L);
    return cfg;
  }

  private RaftReplicatedDatabase database(final ContextConfiguration serverConfig, final ContextConfiguration databaseConfig,
      final String clusterToken) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(serverConfig);
    when(server.getHA()).thenReturn(null); // plain HTTP forward, no HTTPS dial to resolve
    final RaftHAServer raft = mock(RaftHAServer.class);
    when(raft.getLeaderHttpAddress()).thenReturn(leader.address());
    when(raft.getClusterToken()).thenReturn(clusterToken);
    final LocalDatabase local = mock(LocalDatabase.class);
    when(local.getConfiguration()).thenReturn(databaseConfig);
    return new RaftReplicatedDatabase(server, local, raft);
  }

  /** A leader that answers every command with an empty result and records the budget header it was sent. */
  private static final class HeaderRecordingLeader implements AutoCloseable {
    private final HttpServer                   server;
    private final List<Optional<String>>       budgets = new CopyOnWriteArrayList<>();

    HeaderRecordingLeader() throws IOException {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 16);
      server.createContext("/", exchange -> {
        budgets.add(Optional.ofNullable(
            exchange.getRequestHeaders().getFirst(LeaderForwardContext.FORWARDED_COMMAND_TIMEOUT_HEADER)));
        final byte[] bytes = "{\"result\":[]}".getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (final OutputStream out = exchange.getResponseBody()) {
          out.write(bytes);
        }
      });
      server.start();
    }

    String address() {
      return server.getAddress().getAddress().getHostAddress() + ":" + server.getAddress().getPort();
    }

    List<Optional<String>> budgets() {
      return budgets;
    }

    @Override
    public void close() {
      server.stop(0);
    }
  }
}
