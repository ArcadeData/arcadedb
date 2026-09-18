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
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A peer whose certificate is issued for a name the dial does NOT ask for (issue #7836).
 * <p>
 * {@code Issue7563HaTlsPeerDialIT} proves that a peer-to-peer dial completes a real handshake and that a client
 * trusting a foreign CA is rejected. Neither covers the third thing a handshake decides: whether the name on the
 * certificate is the name that was dialled. Nothing in the suite could notice if that check stopped happening,
 * because {@code RaftTestPki} issued one certificate - {@code CN=localhost},
 * {@code SAN=dns:localhost,ip:127.0.0.1} - and every in-process node is dialled as {@code localhost}, so the
 * name always matched.
 * <p>
 * This suite issues the cluster's certificate for a name nothing dials instead. It matters because the endpoint
 * comes from {@code arcadedb.ha.serverList} (or is derived from the peer's Raft host) while the name is
 * certified by whatever key material the deployment was handed: the two are configured independently, and a
 * mismatch between them is a real misconfiguration rather than a contrived one.
 * <p>
 * <b>The two assertions are a pair.</b> A failed handshake on its own does not say WHY it failed - an untrusted
 * chain and a trusted certificate for the wrong name are the same {@code IOException} to a caller. The second
 * test turns endpoint identification off and shows the very same connection then succeeds, which leaves the
 * name as the only thing that was wrong.
 * <p>
 * Tagged {@code slow}: it generates a certificate authority with {@code keytool} and starts a cluster with two
 * listeners per node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue7836HaTlsWrongCertificateNameIT extends BaseRaftHASslTest {

  /** A name that is not {@code localhost} and is not an address: no dial in this suite can ask for it. */
  private static final String UNDIALLED_NAME = "arcadedb-not-the-name-that-is-dialled";

  @Override
  protected String pkiDirectoryName() {
    return "issue7836-wrong-certificate-name-pki";
  }

  @Override
  protected String subjectAltNames() {
    return "san=dns:" + UNDIALLED_NAME;
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  /**
   * The regression. The peer-to-peer trust context - the one every dial in this module is built from - refuses
   * a peer whose certificate does not cover the name the dial asked for.
   */
  @Test
  void aDialToANameTheCertificateDoesNotCoverFailsAtTheHandshake() throws Exception {
    final String url = "https://localhost:" + getServer(0).getHttpServer().getHttpsPort() + "/api/v1/ready";

    try (final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .sslContext(SnapshotInstaller.buildSSLContext(getServer(1)))
        .build()) {
      assertThatThrownBy(() -> client.send(
          HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
          HttpResponse.BodyHandlers.discarding()))
          .as("a certificate issued for '%s' must not be accepted for a dial to 'localhost'", UNDIALLED_NAME)
          .isInstanceOf(IOException.class);
    }
  }

  /**
   * What makes the refusal above a NAME check rather than a trust failure: the same listener, the same cluster
   * truststore, the same TLS version - and the handshake completes as soon as endpoint identification is off.
   * The certificate it then presents is the one issued for the undialled name, read back from the session.
   */
  @Test
  void theSameCertificateIsTrustedAndIsSimplyForAnotherName() throws Exception {
    try (final SSLSocket socket = RaftTestPki.connect(
        RaftTestPki.anonymousClientContext(clusterPki()), "localhost",
        getServer(0).getHttpServer().getHttpsPort(), null, false)) {
      socket.startHandshake();

      assertThat(RaftTestPki.peerSubjectAlternativeNames(socket))
          .as("the chain is trusted; what the dial above refused is the name on it")
          .containsExactly(UNDIALLED_NAME);
    }
  }
}
