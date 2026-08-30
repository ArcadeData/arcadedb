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
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSocket;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The other half of {@code arcadedb.ha.tls.mutualAuth}, and the reason the setting logs a WARNING when it is
 * turned off: with {@code mutualAuth=false} the Raft port is encrypted but NOT authenticated, so the very
 * client {@link Issue3890RaftMtlsIT} shows being rejected - one holding no certificate at all - completes the
 * handshake here.
 * <p>
 * Asserting that positively matters more than it looks. Without it, the only evidence that {@code mutualAuth}
 * does anything is that turning it off makes the rejection tests go red, which is a claim someone has to
 * re-establish by hand. This pins it in the suite, so a regression that quietly stopped requesting a client
 * certificate would turn {@link Issue3890RaftMtlsIT} red while this class stayed green, and the pair of
 * results would say which of the two settings had broken.
 */
class Issue3890RaftServerOnlyTlsIT extends BaseRaftHATest {

  private static final String VERTEX_TYPE  = "ServerOnlyTlsReplicated";
  private static final int    RECORD_COUNT = 50;

  private static RaftTestPki clusterPki;

  @BeforeAll
  static void generateCertificates() throws Exception {
    clusterPki = RaftTestPki.create(Path.of("target", "issue3890-server-only-tls-pki"), "cluster");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_TLS_ENABLED, true);
    config.setValue(GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE, clusterPki.nodeCertificate().toString());
    config.setValue(GlobalConfiguration.HA_TLS_PRIVATE_KEY_FILE, clusterPki.nodePrivateKey().toString());
    config.setValue(GlobalConfiguration.HA_TLS_TRUST_CERT_COLLECTION_FILE, clusterPki.caCertificate().toString());
    config.setValue(GlobalConfiguration.HA_TLS_MUTUAL_AUTH, false);
  }

  @Test
  void serverOnlyTlsAcceptsAPeerWithoutAClientCertificate() throws Exception {
    try (final SSLSocket socket = RaftTestPki.connect(RaftTestPki.anonymousClientContext(clusterPki),
        "localhost", raftPortOf(0))) {
      socket.startHandshake();
      assertThat(socket.getSession().isValid())
          .as("with mutualAuth=false the Raft port must not demand a client certificate")
          .isTrue();
    }
  }

  /** Server-only TLS still has to be a working transport, not merely a reachable one. */
  @Test
  void clusterReplicatesOverServerOnlyTls() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected over the TLS transport").isGreaterThanOrEqualTo(0);

    final var db = getServerDatabase(leaderIndex, getDatabaseName());
    db.transaction(() -> {
      if (!db.getSchema().existsType(VERTEX_TYPE))
        db.getSchema().createVertexType(VERTEX_TYPE);
    });

    db.transaction(() -> {
      for (int i = 0; i < RECORD_COUNT; i++) {
        final MutableVertex vertex = db.newVertex(VERTEX_TYPE);
        vertex.set("idx", i);
        vertex.save();
      }
    });

    assertClusterConsistency();

    for (int i = 0; i < getServerCount(); i++)
      assertThat(getServerDatabase(i, getDatabaseName()).countType(VERTEX_TYPE, true))
          .as("server %d must have caught up over server-only TLS", i)
          .isEqualTo(RECORD_COUNT);
  }

  private int raftPortOf(final int serverIndex) {
    final String peerId = peerIdForIndex(serverIndex);
    return Integer.parseInt(peerId.substring(peerId.lastIndexOf('_') + 1));
  }
}
