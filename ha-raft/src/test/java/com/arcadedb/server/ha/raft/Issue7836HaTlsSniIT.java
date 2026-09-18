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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A peer that serves several names from one listener, dialled with SNI (issue #7836).
 * <p>
 * The reason this is worth pinning is the same one the rest of issue #7563 had: the name a peer is dialled by
 * comes from {@code arcadedb.ha.serverList} or is derived from the peer's Raft host, and the names its
 * certificate certifies come from the key material the deployment was handed. A Kubernetes node is reachable as
 * a pod DNS name, as a service name and as an address, and a dial may legitimately ask for any of them.
 * <p>
 * <b>What ArcadeDB's HTTPS listener actually does, which is what these assertions say.</b> It is configured with
 * ONE keystore ({@code arcadedb.ssl.keyStore}) and therefore serves ONE identity. The {@code server_name}
 * extension a client sends selects nothing: the same certificate comes back whichever name is advertised. So the
 * operational invariant is not "configure a certificate per name" but "every name any dial can ask for must be
 * in that one certificate's SAN" - and that is exactly what the two tests below pin, from the two sides:
 * <ul>
 * <li>a name IN the SAN is accepted with that name advertised in SNI, and the certificate that comes back
 * carries it;</li>
 * <li>a name NOT in the SAN changes nothing about what the listener presents - so it would be refused by a
 * client that verifies, which {@link Issue7836HaTlsWrongCertificateNameIT} shows happening.</li>
 * </ul>
 * Each test asserts the {@code server_name} extension was actually sent before asserting what came back, so
 * neither can pass against a dial that stopped setting SNI (CodeRabbit on PR #7854).
 * Tagged {@code slow}: it generates a certificate authority with {@code keytool} and starts a cluster with two
 * listeners per node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue7836HaTlsSniIT extends BaseRaftHASslTest {

  /** A second name this cluster's one certificate also certifies - a service name, as Kubernetes issues them. */
  private static final String ALIAS_NAME = "arcadedb.cluster.local";
  /** A name the certificate does NOT certify. */
  private static final String FOREIGN_NAME = "arcadedb.somewhere-else.local";

  @Override
  protected String pkiDirectoryName() {
    return "issue7836-sni-pki";
  }

  @Override
  protected String subjectAltNames() {
    return "san=dns:localhost,dns:" + ALIAS_NAME + ",ip:127.0.0.1";
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  /**
   * The cluster still works when its certificate certifies more than one name: the dial asks for
   * {@code localhost}, which is one of them, and the ordinary peer-to-peer trust context accepts it. Without
   * this, the two SNI assertions below would be equally consistent with a listener nobody can talk to.
   */
  @Test
  void aCertificateCarryingSeveralNamesStillServesTheOneThePeersDial() throws Exception {
    final String url = "https://localhost:" + getServer(0).getHttpServer().getHttpsPort() + "/api/v1/ready";

    try (final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(SnapshotInstaller.buildSSLContext(getServer(1)))
        .build()) {
      assertThat(client.send(
          HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
          HttpResponse.BodyHandlers.discarding()).statusCode())
          .as("the peer must complete the handshake for the name the dial asks for")
          .isBetween(200, 599);
    }
  }

  /**
   * A dial that advertises the ALIAS in SNI is served a certificate that covers it - so a deployment whose
   * server list names the peer by its service name gets a handshake that a verifying client will accept.
   */
  @Test
  void aNameInTheCertificateIsServedWhenItIsAdvertisedInSni() throws Exception {
    try (final SSLSocket socket = RaftTestPki.connect(
        RaftTestPki.anonymousClientContext(clusterPki()), "localhost",
        getServer(0).getHttpServer().getHttpsPort(), ALIAS_NAME, false)) {
      socket.startHandshake();

      assertThat(RaftTestPki.requestedServerNames(socket))
          .as("the dial must actually carry the server_name extension, or the rest asserts nothing about SNI")
          .containsExactly(ALIAS_NAME);
      assertThat(RaftTestPki.peerSubjectAlternativeNames(socket))
          .as("the identity served for SNI '%s' must certify it", ALIAS_NAME)
          .contains(ALIAS_NAME, "localhost");
    }
  }

  /**
   * And the shape of the guarantee: the listener holds one keystore, so SNI selects nothing. A name it does not
   * certify gets the same certificate as every other name - which a client that verifies then refuses. This is
   * the assertion an operator's configuration has to satisfy: put every dialled name in the one SAN.
   */
  @Test
  void aNameTheCertificateDoesNotCarryIsNotServedByAdvertisingItInSni() throws Exception {
    try (final SSLSocket socket = RaftTestPki.connect(
        RaftTestPki.anonymousClientContext(clusterPki()), "localhost",
        getServer(0).getHttpServer().getHttpsPort(), FOREIGN_NAME, false)) {
      socket.startHandshake();

      assertThat(RaftTestPki.requestedServerNames(socket))
          .as("the foreign name really was asked for")
          .containsExactly(FOREIGN_NAME);
      assertThat(RaftTestPki.peerSubjectAlternativeNames(socket))
          .as("a single-keystore listener answers every SNI with the same identity")
          .contains("localhost", ALIAS_NAME)
          .doesNotContain(FOREIGN_NAME);
    }
  }
}
