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
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.utility.StallAwareStopwatch;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static com.arcadedb.server.StaticBaseServerTest.allocateFreePorts;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12142: {@code refreshRaftClient()}, which {@code notifyLeaderChanged} calls inside Ratis's
 * {@code setLeader} with the division's monitor held, closed the client it replaced inline. Closing a Ratis gRPC client
 * waits up to 3 s for the calls still in flight on its channels, so a leader change under load held the new leader (no
 * heartbeat, no commit) and every follower (no append, no vote) for 3 s.
 * <p>
 * The call in flight here is real: the client's peers are sockets that accept the connection and never answer, so the
 * call stays pending in the channel exactly as one waiting on a busy or paused peer does, and the channel's graceful
 * shutdown has something to wait for.
 */
class Issue12142RefreshDoesNotWaitForTheRetiredClientTest {

  private final List<ServerSocket> silentPeers = new CopyOnWriteArrayList<>();
  private final List<Socket>       accepted    = new CopyOnWriteArrayList<>();
  private       String             serverList;

  @BeforeEach
  void startSilentPeers() throws IOException {
    final int[] ports = allocateFreePorts(4);
    final StringBuilder list = new StringBuilder();
    for (int i = 0; i < 2; i++) {
      final ServerSocket peer = new ServerSocket(ports[i], 50, InetAddress.getLoopbackAddress());
      silentPeers.add(peer);
      Thread.ofVirtual().name("silent-raft-peer-" + i).start(() -> {
        try {
          while (!peer.isClosed())
            accepted.add(peer.accept());
        } catch (final IOException e) {
          // closed by the test
        }
      });
      if (i > 0)
        list.append(',');
      list.append("127.0.0.1:").append(ports[i]).append(':').append(ports[i + 2]);
    }
    serverList = list.toString();
  }

  @AfterEach
  void stopSilentPeers() throws IOException {
    for (final ServerSocket peer : silentPeers)
      peer.close();
    for (final Socket socket : accepted)
      socket.close();
  }

  @Test
  void aRefreshDoesNotWaitForTheCallsInFlightOnTheClientItReplaces() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);
    final RaftHAServer server = new RaftHAServer(TestServerHelper.unstartedServer("ArcadeDB_0"), config);
    server.buildParameters(config);
    server.setRaftPropertiesForTest(RaftPropertiesBuilder.build(config));
    server.refreshRaftClient();

    final RaftClient retired = server.getClient();
    final CompletableFuture<RaftClientReply> inFlight = retired.async().send(Message.valueOf("entry"));
    Awaitility.await("the call reached a peer").atMost(30, TimeUnit.SECONDS).pollInterval(10, TimeUnit.MILLISECONDS)
        .until(() -> !accepted.isEmpty());
    assertThat(inFlight).as("the peer never answers, so the call is still in flight").isNotDone();

    final StallAwareStopwatch clock = StallAwareStopwatch.start();
    server.refreshRaftClient();
    clock.assertGaveUpWithin(1_500L,
        "a refresh that leaves the retired client's close to another thread from one that waits out its 3 s graceful channel shutdown");

    assertThat(server.getClient()).isNotSameAs(retired);
    // The retired client is still closed, only not on this thread: its call ends instead of waiting out the retry policy.
    Awaitility.await("the retired client is closed").atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
        .until(inFlight::isDone);
    assertThat(inFlight).isCompletedExceptionally();
    server.getClient().close();
  }
}
