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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5978: unlike {@link RedisNetworkExecutor} (issue #5912),
 * {@link BoltNetworkExecutor} left the AUTH/HELLO-LOGON phase - and, on a plaintext connection, the whole
 * pre-auth handshake - completely unbounded, so a client that connects and never authenticates could hold
 * the connection thread (and its file descriptor) open forever.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Bolt5978AuthTimeoutIT extends BaseGraphServerTest {

  private static final int BOLT_PORT = GlobalConfiguration.BOLT_PORT.getValueAsInteger();

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  @Tag("slow")
  void idlePostHandshakeUnauthenticatedConnectionIsClosedInsteadOfHeldOpenIndefinitely() throws IOException {
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(500);
    try {
      try (final Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("localhost", BOLT_PORT), 5000);
        // Safety bound for the test itself only, well above the lowered server-side timeout: if this fires
        // instead of a clean EOF, the server is still holding the idle connection open.
        socket.setSoTimeout(10_000);

        completeHandshake(socket);
        // Handshake is done, connection is in AUTHENTICATION state; never send HELLO/LOGON.
        assertThat(socket.getInputStream().read()).isEqualTo(-1);
      }
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.reset();
    }

    // The listener/thread pool must still be healthy: a fresh connection behaves normally afterward.
    assertBoltStillServesClients();
  }

  @Test
  @Tag("slow")
  void stalledBeforeHandshakeConnectionIsClosedInsteadOfHeldOpenIndefinitely() throws IOException {
    // Even before the BOLT magic/version negotiation completes, the connection must be bounded - not just
    // the subsequent AUTH/HELLO-LOGON phase.
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(500);
    try {
      try (final Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("localhost", BOLT_PORT), 5000);
        socket.setSoTimeout(10_000);
        // Never send anything at all, not even the magic bytes.
        assertThat(socket.getInputStream().read()).isEqualTo(-1);
      }
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.reset();
    }

    assertBoltStillServesClients();
  }

  @Test
  @Tag("slow")
  void authenticatedConnectionIsNotClosedWhileIdle() throws Exception {
    // The pre-auth timeout must not keep applying once HELLO/LOGON succeeded: a BOLT client is expected to
    // keep a long-lived, often idle connection open between queries (driver session pooling).
    GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.setValue(500);
    try (final Driver driver = GraphDatabase.driver("bolt://localhost:" + BOLT_PORT,
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS))) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        assertThat(session.run("RETURN 1 AS value").hasNext()).isTrue();

        Thread.sleep(1_500); // well over the lowered pre-auth timeout

        assertThat(session.run("RETURN 1 AS value").hasNext()).isTrue();
      }
    } finally {
      GlobalConfiguration.NETWORK_SOCKET_TIMEOUT.reset();
    }
  }

  private void assertBoltStillServesClients() {
    try (final Driver driver = GraphDatabase.driver("bolt://localhost:" + BOLT_PORT,
        AuthTokens.basic("root", DEFAULT_PASSWORD_FOR_TESTS))) {
      try (final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
        final Result result = session.run("RETURN 1 AS value");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().get("value").asLong()).isEqualTo(1L);
      }
    }
  }

  /**
   * Completes the BOLT magic-bytes + version-negotiation handshake so the connection reaches the
   * AUTHENTICATION state, without sending HELLO.
   */
  private static void completeHandshake(final Socket socket) throws IOException {
    final OutputStream out = socket.getOutputStream();
    out.write(new byte[] { 0x60, 0x60, (byte) 0xB0, 0x17 }); // BOLT magic

    // The real handshake always proposes 4 versions; only the first slot needs to be one the server
    // supports (BoltNetworkExecutor.SUPPORTED_VERSIONS is package-private, visible here).
    final int version = BoltNetworkExecutor.SUPPORTED_VERSIONS[0];
    writeRawInt(out, version);
    writeRawInt(out, 0);
    writeRawInt(out, 0);
    writeRawInt(out, 0);
    out.flush();

    // Read the 4-byte negotiated version response.
    final byte[] response = new byte[4];
    int read = 0;
    while (read < 4) {
      final int n = socket.getInputStream().read(response, read, 4 - read);
      if (n == -1)
        throw new IOException("Server closed the connection during handshake");
      read += n;
    }
  }

  private static void writeRawInt(final OutputStream out, final int value) throws IOException {
    out.write((value >>> 24) & 0xFF);
    out.write((value >>> 16) & 0xFF);
    out.write((value >>> 8) & 0xFF);
    out.write(value & 0xFF);
  }
}
