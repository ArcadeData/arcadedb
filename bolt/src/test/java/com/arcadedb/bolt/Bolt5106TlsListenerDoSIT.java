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
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Regression tests for issue #5106: a single failed/aborted TLS handshake must not wedge the shared
 * BOLT listener (busy-spin at ~100% CPU / accept thread blocked) and deny service to all other clients.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Bolt5106TlsListenerDoSIT extends BaseGraphServerTest {

  private static final String KEYSTORE_PASSWORD   = "testPassword123";
  private static final String TRUSTSTORE_PASSWORD = "testPassword123";
  private static final int    BOLT_PORT           = 7687;
  private static Path         keystorePath;
  private static Path         truststorePath;
  private static Path         tempDir;

  @BeforeAll
  static void generateCertificates() throws Exception {
    tempDir = Files.createTempDirectory("bolt-tls-dos-test");

    keystorePath = tempDir.resolve("keystore.pkcs12");
    truststorePath = tempDir.resolve("truststore.jks");

    final Process keytoolGen = new ProcessBuilder(
        "keytool", "-genkeypair",
        "-alias", "bolt-test",
        "-keyalg", "RSA",
        "-keysize", "2048",
        "-validity", "365",
        "-dname", "CN=localhost, O=ArcadeDB Test, L=Test, ST=Test, C=US",
        "-keystore", keystorePath.toString(),
        "-storepass", KEYSTORE_PASSWORD,
        "-storetype", "PKCS12"
    ).redirectErrorStream(true).start();
    keytoolGen.waitFor();

    final Path certPath = tempDir.resolve("bolt-test.cer");
    final Process keytoolExport = new ProcessBuilder(
        "keytool", "-exportcert",
        "-alias", "bolt-test",
        "-keystore", keystorePath.toString(),
        "-storepass", KEYSTORE_PASSWORD,
        "-file", certPath.toString()
    ).redirectErrorStream(true).start();
    keytoolExport.waitFor();

    final Process keytoolImport = new ProcessBuilder(
        "keytool", "-importcert",
        "-alias", "bolt-test",
        "-keystore", truststorePath.toString(),
        "-storepass", TRUSTSTORE_PASSWORD,
        "-storetype", "JKS",
        "-file", certPath.toString(),
        "-noprompt"
    ).redirectErrorStream(true).start();
    keytoolImport.waitFor();

    Files.deleteIfExists(certPath);
  }

  @AfterAll
  static void cleanupCertificates() throws Exception {
    if (keystorePath != null)
      Files.deleteIfExists(keystorePath);
    if (truststorePath != null)
      Files.deleteIfExists(truststorePath);
    if (tempDir != null)
      Files.deleteIfExists(tempDir);
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    GlobalConfiguration.BOLT_SSL.setValue("OPTIONAL");
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration configuration) {
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE, keystorePath.toString());
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE, truststorePath.toString());
    configuration.setValue(GlobalConfiguration.NETWORK_SSL_TRUSTSTORE_PASSWORD, TRUSTSTORE_PASSWORD);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    GlobalConfiguration.BOLT_SSL.setValue("DISABLED");
    super.endTest();
  }

  /**
   * A client that opens a TCP connection and closes it before sending the 4 TLS-detection bytes must not
   * push the listener's accept thread into a busy-spin (the peek loop used to re-read a closed socket forever).
   */
  @Test
  @DisplayName("[#5106] Early-close before TLS detection does not wedge the listener")
  void earlyCloseDoesNotWedgeListener() throws Exception {
    for (int i = 0; i < 5; i++) {
      try (final Socket s = new Socket()) {
        s.connect(new InetSocketAddress("localhost", BOLT_PORT), 2000);
        // Close immediately without sending any byte.
      }
    }

    assertListenerStillServesClients();
  }

  /**
   * A client that starts a TLS handshake (sends a TLS record header) then stalls must not block the shared
   * accept thread: the handshake must run on the per-connection thread, never on the listener's accept path.
   */
  @Test
  @DisplayName("[#5106] Stalled TLS handshake does not block the accept thread")
  void stalledTlsHandshakeDoesNotWedgeListener() throws Exception {
    try (final Socket attacker = new Socket()) {
      attacker.connect(new InetSocketAddress("localhost", BOLT_PORT), 2000);
      final OutputStream out = attacker.getOutputStream();
      // TLS record header (handshake, TLS 1.0 record version) then stall - never finish the ClientHello.
      out.write(new byte[] { 0x16, 0x03, 0x01, 0x00 });
      out.flush();

      // Attacker keeps the socket open (stalled). The listener must still serve everyone else.
      assertListenerStillServesClients();
    }
  }

  /**
   * The exact issue scenario: a client whose TLS stack rejects the (untrusted) server certificate aborts the
   * handshake with a fatal alert. That single failed handshake must not wedge Bolt-over-TLS for other clients.
   */
  @Test
  @DisplayName("[#5106] Untrusted-cert TLS handshake failure does not wedge the listener")
  void failedTlsHandshakeDoesNotWedgeListener() throws Exception {
    // Default SSL context uses the JVM truststore, which does NOT trust our self-signed server cert,
    // so the handshake fails fatally - mirroring a verify_peer client rejecting an untrusted server cert.
    final SSLContext defaultCtx = SSLContext.getDefault();
    for (int i = 0; i < 3; i++) {
      try (final Socket raw = new Socket()) {
        raw.connect(new InetSocketAddress("localhost", BOLT_PORT), 2000);
        final SSLSocket ssl = (SSLSocket) defaultCtx.getSocketFactory()
            .createSocket(raw, "localhost", BOLT_PORT, true);
        ssl.setUseClientMode(true);
        try {
          ssl.startHandshake();
        } catch (final Exception expected) {
          // Handshake is expected to fail (untrusted certificate).
        }
      } catch (final Exception ignore) {
        // Connection-level errors are fine for this negative path.
      }
    }

    assertListenerStillServesClients();
  }

  private void assertListenerStillServesClients() {
    assertTimeoutPreemptively(Duration.ofSeconds(25), () -> {
      try (final Driver driver = GraphDatabase.driver(
          "bolt+ssc://localhost:" + BOLT_PORT,
          AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS),
          Config.builder().withConnectionTimeout(5, java.util.concurrent.TimeUnit.SECONDS).build())) {
        try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
          final Result result = session.run("RETURN 1 AS value");
          assertThat(result.hasNext()).isTrue();
          assertThat(result.next().get("value").asLong()).isEqualTo(1L);
        }
      }
    });
  }
}
