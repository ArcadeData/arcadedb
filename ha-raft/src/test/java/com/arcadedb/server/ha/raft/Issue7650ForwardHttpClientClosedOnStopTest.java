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
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for a review finding on PR #7650: {@code RaftHAServer.forwardHttpClient} - the client every
 * {@code RaftReplicatedDatabase} this server wraps a database with now shares (issue: claude-review finding on
 * the same PR, "one client per node, not one per database") - was built in the constructor but never closed in
 * {@link RaftHAServer#stop()}, unlike its two siblings {@code capabilityHttpsClients}/{@code forwardHttpsClients},
 * which {@code stop()} already closes with an explicit "an unclosed HttpClient holds a connection pool and a
 * selector thread for the life of the JVM" comment (issue #7314). A fresh {@code RaftHAServer} - and a fresh
 * {@code forwardHttpClient} - is built on every {@code RaftHAPlugin.startService()}, so leaving it open leaks a
 * selector thread per server start/stop cycle, which the HA test suites go through many of.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7650ForwardHttpClientClosedOnStopTest {

  @Test
  void stopClosesTheSharedForwardHttpClient() throws Exception {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480");

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("localhost");

    final RaftHAServer raft = new RaftHAServer(mockServer, config);
    final HttpClient client = raft.getForwardHttpClient();
    assertThat(client.isTerminated()).as("not yet closed before stop()").isFalse();

    raft.stop();

    assertThat(client.awaitTermination(Duration.ofSeconds(10)))
        .as("stop() must close the shared forward client, not just the ones it already closed").isTrue();
  }
}
