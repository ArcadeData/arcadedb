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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.X509Certificate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #3890: the Raft gRPC port used for log replication, leader election and snapshot chunk transfer had
 * no peer authentication and no in-transit encryption, so any host able to reach it could open a stream to
 * the Ratis server and inject log entries. The peer-address allowlist added earlier is IP-based, and an IP
 * is not an identity.
 * <p>
 * This test boots a three-node cluster with {@code arcadedb.ha.tls.enabled=true} against a throwaway cluster
 * CA and asserts both halves of the fix:
 * <ul>
 *   <li>the cluster still elects a leader, commits transactions and catches followers up - over TLS;</li>
 *   <li>a peer holding a certificate signed by a <em>different</em> CA, and an anonymous peer holding none,
 *       are both rejected during the TLS handshake, before any Raft message is read.</li>
 * </ul>
 * The negative handshakes pin TLS 1.2 deliberately: under TLS 1.3 the client finishes the handshake before
 * the server has verified its certificate, so the rejection would surface asynchronously on a later read
 * rather than out of {@code startHandshake()}, and the assertion would be racy.
 */
class Issue3890RaftMtlsIT extends BaseRaftHATest {

  private static final String  VERTEX_TYPE   = "MtlsReplicated";
  private static final int     RECORD_COUNT  = 200;
  private static final String  TLS_1_2       = "TLSv1.2";
  private static final int     HANDSHAKE_TIMEOUT_MS = 15_000;

  private static RaftTestPki clusterPki;
  private static RaftTestPki foreignPki;

  @BeforeAll
  static void generateCertificates() throws Exception {
    final Path pkiDirectory = Path.of("target", "issue3890-mtls-pki");
    clusterPki = RaftTestPki.create(pkiDirectory, "cluster");
    foreignPki = RaftTestPki.create(pkiDirectory, "foreign");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_TLS_ENABLED, true);
    config.setValue(GlobalConfiguration.HA_TLS_CERT_CHAIN_FILE, clusterPki.nodeCertificate().toString());
    config.setValue(GlobalConfiguration.HA_TLS_PRIVATE_KEY_FILE, clusterPki.nodePrivateKey().toString());
    config.setValue(GlobalConfiguration.HA_TLS_TRUST_CERT_COLLECTION_FILE, clusterPki.caCertificate().toString());
    config.setValue(GlobalConfiguration.HA_TLS_MUTUAL_AUTH, true);
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  /**
   * The whole replication round-trip over the TLS transport: an elected leader, a committed transaction, and
   * every follower holding the same records. Without the {@code Parameters} wiring on both the Ratis server
   * and the leader's self-client, this never gets past the first AppendEntries.
   */
  @Test
  void clusterReplicatesOverMutualTls() {
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
          .as("server %d must have caught up over TLS", i)
          .isEqualTo(RECORD_COUNT);
  }

  /**
   * Positive control for the two rejection tests below: the port really is speaking TLS, and a peer holding
   * a cluster-CA certificate completes the handshake and is served the node's certificate.
   */
  @Test
  void raftPortCompletesHandshakeWithClusterCertificate() throws Exception {
    try (final SSLSocket socket = connect(RaftTestPki.clientContext(clusterPki, clusterPki))) {
      socket.startHandshake();

      final var peerCertificates = socket.getSession().getPeerCertificates();
      assertThat(peerCertificates).isNotEmpty();
      assertThat(((X509Certificate) peerCertificates[0]).getSubjectX500Principal().getName())
          .contains("CN=localhost");
    }
  }

  /**
   * The attack the issue describes: a host that can reach the Raft port, presenting an identity the cluster
   * CA never signed. It must not get as far as a gRPC stream.
   */
  @Test
  void raftPortRejectsPeerSignedByAForeignCa() throws Exception {
    assertThatThrownBy(() -> handshake(RaftTestPki.clientContext(foreignPki, clusterPki)))
        .isInstanceOf(IOException.class);
  }

  /**
   * The weaker version of the same attack, and the one the peer-address allowlist alone could not stop: a
   * host that merely knows the port and offers no certificate at all.
   */
  @Test
  void raftPortRejectsPeerWithoutCertificate() throws Exception {
    assertThatThrownBy(() -> handshake(RaftTestPki.anonymousClientContext(clusterPki)))
        .isInstanceOf(IOException.class);
  }

  private void handshake(final SSLContext context) throws IOException {
    try (final SSLSocket socket = connect(context)) {
      socket.startHandshake();
    }
  }

  private SSLSocket connect(final SSLContext context) throws IOException {
    final SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket("localhost", raftPortOf(0));
    socket.setEnabledProtocols(new String[] { TLS_1_2 });
    socket.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
    return socket;
  }

  /**
   * Derives the Raft port from the peer id ({@code localhost_<raftPort>}) rather than duplicating the base
   * port constant, so this test cannot drift away from what the cluster actually bound.
   */
  private int raftPortOf(final int serverIndex) {
    final String peerId = peerIdForIndex(serverIndex);
    return Integer.parseInt(peerId.substring(peerId.lastIndexOf('_') + 1));
  }
}
