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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.network.ServerSocketFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A connection whose serving thread cannot be started is closed, it no longer counts as an open connection, its
 * pre-authentication permit goes back, and the listener goes on accepting: the accept thread must outlive a failure that
 * belongs to one connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltListenerStartFailureTest {
  /** Set by a test to make the next start fail: true with an Error, false with a RuntimeException. */
  private static final AtomicBoolean FAIL_NEXT_WITH_ERROR = new AtomicBoolean();
  private static final AtomicBoolean FAIL_NEXT            = new AtomicBoolean();
  private static volatile CountDownLatch failedStart;
  private static volatile CountDownLatch servedStart;

  private BoltNetworkListener listener;
  private Socket              refused;
  private Socket              served;

  /** A listener whose first start, once armed, fails. */
  private static final class FailingStartListener extends BoltNetworkListener {
    FailingStartListener(final ArcadeDBServer server, final ServerSocketFactory factory, final BoltSslHelper ssl) {
      super(server, factory, ssl, "127.0.0.1", "0");
    }

    @Override
    void startConnection(final BoltNetworkExecutor connection) {
      if (FAIL_NEXT.compareAndSet(true, false)) {
        failedStart.countDown();
        if (FAIL_NEXT_WITH_ERROR.get())
          throw new OutOfMemoryError("start failed");
        throw new IllegalStateException("unable to start the connection");
      }
      super.startConnection(connection);
      servedStart.countDown();
    }
  }

  @AfterEach
  void closeEverything() throws IOException {
    FAIL_NEXT.set(false);
    if (listener != null)
      listener.close();
    if (refused != null)
      refused.close();
    if (served != null)
      served.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  void aConnectionThatCannotBeStartedIsClosedAndTheListenerKeepsAccepting(final boolean error) throws Exception {
    failedStart = new CountDownLatch(1);
    servedStart = new CountDownLatch(1);
    FAIL_NEXT_WITH_ERROR.set(error);

    final ContextConfiguration configuration = new ContextConfiguration();
    // A single pre-authentication permit and a single open connection: a second connection reaches the start only when
    // the first returned the permit and left the set of open connections
    configuration.setValue(GlobalConfiguration.NETWORK_MAX_PREAUTH_CONNECTIONS, 1);
    configuration.setValue(GlobalConfiguration.BOLT_MAX_CONNECTIONS, 1);
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
    final ServerSocketFactory factory = new ServerSocketFactory() {
      @Override
      public ServerSocket createServerSocket(final int port, final int backlog, final InetAddress ifAddress) throws IOException {
        return new ServerSocket(port, backlog, ifAddress);
      }
    };
    listener = new FailingStartListener(server, factory, new BoltSslHelper(configuration));

    FAIL_NEXT.set(true);
    refused = new Socket(InetAddress.getByName("127.0.0.1"), listener.getPort());
    assertThat(failedStart.await(30, TimeUnit.SECONDS)).as("the connection reached the start").isTrue();

    assertThat(closedByPeer(refused)).as("the connection that could not be started is closed").isTrue();
    assertThat(listener.isAlive()).as("the accept thread is still running").isTrue();

    served = new Socket(InetAddress.getByName("127.0.0.1"), listener.getPort());
    assertThat(servedStart.await(30, TimeUnit.SECONDS)).as("a later connection is accepted and started, so nothing stayed held").isTrue();
  }

  /** Whether the server hung up on {@code socket}: end of stream or a reset, as opposed to silence. */
  private static boolean closedByPeer(final Socket socket) throws IOException {
    socket.setSoTimeout(10_000);
    try {
      return socket.getInputStream().read() == -1;
    } catch (final SocketTimeoutException e) {
      return false;
    } catch (final SocketException e) {
      return true;
    }
  }
}
