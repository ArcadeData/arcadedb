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
import com.arcadedb.server.http.handler.LeaderDial;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12442: every client a follower forwards a command to the leader on asks for HTTP/1.1
 * ({@link LeaderDial#FORWARD_HTTP_VERSION}), because the leader lifts its read timeout for a long statement on an HTTP/1.1
 * exchange only. At the JDK's default version the plain-HTTP forward is an h2c upgrade and the TLS one an ALPN {@code h2},
 * and the leader closed the connection one {@code arcadedb.network.socketTimeout} into the statement.
 * <p>
 * The end-to-end proof, a statement three socket timeouts long forwarded across a real cluster, is
 * {@code HaForwardedLongCommandIT} and {@code HaForwardedLongCommandTlsIT}, which run in the integration lane. This is the
 * cheap guard that runs everywhere: it fails the moment one of the two forward clients of this module is built without the
 * pin. The plain-HTTP clients of {@code LeaderCommandForwarder} and {@code PostBatchHandler} are pinned by
 * {@code Issue12442ForwardClientsSpeakHttp11Test} in the {@code server} module, where they live.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12442FollowerForwardClientsHttp11Test {

  /** The client every {@code RaftReplicatedDatabase} of a node forwards on over plain HTTP: one per node, built by {@link RaftHAServer}. */
  @Test
  void thePlainHttpForwardClientOfANodeAsksForHttp11() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");
    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0", config);
    final RaftHAServer raft = new RaftHAServer(arcadeServer, config);
    try {
      assertThat(raft.getForwardHttpClient().version())
          .as("the plain-HTTP forward of a node: an h2c upgrade here ends a statement longer than the socket timeout")
          .isEqualTo(HttpClient.Version.HTTP_1_1);
    } finally {
      LeaderDial.releaseBounded(raft.getForwardHttpClient());
    }
  }

  /**
   * The client the HTTPS forward is sent on, built from the node's truststore: {@code RaftHAServer.getForwardHttpsClient}
   * is one instance of this cache. ALPN would pick {@code h2} for it, and the leader's read timeout would cut the exchange
   * one socket timeout into the statement.
   * <p>
   * The pin reaches every other user of the class as well - the second instance a node holds for its peer RPCs
   * ({@code PeerAuthSessionQuery}, {@code VoterQuarantineCheck}, {@code UnverifiedClosedCopyCheck},
   * {@code UnverifiedCopyReverifier}) and for the capability probe. Those are short requests, and HTTP/1.1 over TLS is as
   * secure as {@code h2}: stated here so that changing it later is a decision and not a surprise.
   */
  @Test
  void theHttpsClientCacheAsksForHttp11() throws Exception {
    final ArcadeDBServer server = TestServerHelper.unstartedServer("ArcadeDB_0", new ContextConfiguration());
    final TrustedHttpClientCache cache = new TrustedHttpClientCache();
    try {
      assertThat(cache.clientFor(server).version())
          .as("the HTTPS forward of a node: ALPN would pick h2 for a client left at the default version")
          .isEqualTo(HttpClient.Version.HTTP_1_1);
    } finally {
      cache.close();
    }
  }
}
