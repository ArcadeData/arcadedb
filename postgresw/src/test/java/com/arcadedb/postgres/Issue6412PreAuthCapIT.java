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
package com.arcadedb.postgres;

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the first half of issue #6412: bounding how <i>long</i> a connection may stay
 * un-authenticated (#6377) does not bound how <i>many</i> may be, and the accept loop committed one thread
 * and one file descriptor per accepted socket with no ceiling at all. A client opening connections faster
 * than the handshake timeout reaps them still drove both arbitrarily high.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue6412PreAuthCapIT extends PostgresWireProtocolTestBase {

  private static final int POSTGRES_PORT = 5432;
  private static final int MAX_PREAUTH   = 2;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    // Read by the listener when the plugin starts it, so it has to be in place before the server comes up.
    GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS.setValue(MAX_PREAUTH);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS.reset();
    super.endTest();
  }

  @Test
  void connectionsPastTheCapAreClosedWithoutBeingGivenAThread() throws Exception {
    final List<Socket> sockets = new ArrayList<>();
    try {
      // Fill the pre-auth budget with connections that say nothing at all, which is exactly what the flood
      // this cap exists for looks like.
      for (int i = 0; i < MAX_PREAUTH; i++)
        sockets.add(openSocket());

      try (final Socket refused = openSocket()) {
        // Well under the 30s handshake timeout that would eventually close an *accepted* connection: the
        // refusal has to happen on the accept path, not by letting the connection in and timing it out. If
        // this read times out instead of returning EOF, the cap did not apply.
        refused.setSoTimeout(10_000);
        assertThat(refused.getInputStream().read())
            .as("a connection over the pre-authentication cap must be closed straight away")
            .isEqualTo(-1);
      }

      // The cap must not be a one-way door: authenticating one of the held connections hands its permit
      // back, and the next client gets in.
      authenticate(sockets.get(0));

      try (final Socket admitted = openSocket()) {
        final DataOutputStream out = new DataOutputStream(admitted.getOutputStream());
        final DataInputStream in = new DataInputStream(admitted.getInputStream());
        sendStartupMessage(out, "root", getDatabaseName());
        assertThat(readMessageType(in))
            .as("the permit released by an authenticated connection must let the next one in")
            .isEqualTo('R');
      }
    } finally {
      for (final Socket socket : sockets)
        socket.close();
    }
  }

  @Test
  void authenticatedConnectionsDoNotHoldPreAuthPermits() throws Exception {
    final List<Socket> sockets = new ArrayList<>();
    try {
      // Three times the cap, all of them authenticated: a permit is held only until the handshake completes,
      // so a healthy pool of long-lived connections is never refused however small the cap is.
      for (int i = 0; i < MAX_PREAUTH * 3; i++) {
        final Socket socket = openSocket();
        sockets.add(socket);
        authenticate(socket);
      }
    } finally {
      for (final Socket socket : sockets)
        socket.close();
    }
  }

  private void authenticate(final Socket socket) throws Exception {
    final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
    final DataInputStream in = new DataInputStream(socket.getInputStream());

    sendStartupMessage(out, "root", getDatabaseName());
    readMessageOfType(in, 'R');
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // ReadyForQuery: the handshake is over
  }

  private Socket openSocket() throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", POSTGRES_PORT), 5_000);
    socket.setSoTimeout(30_000);
    return socket;
  }
}
