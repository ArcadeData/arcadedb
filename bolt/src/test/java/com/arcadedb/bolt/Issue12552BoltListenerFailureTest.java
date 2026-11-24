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
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.network.ServerSocketFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12552: an {@code OutOfMemoryError} ended the thread "n2 BOLT listening at 0.0.0.0:7687",
 * and the node went on with its Bolt port closed and nothing but the log to show it. The accept loop now outlives an
 * error as it does an exception, and the plugin still reports a listener thread that has ended anyway, which the
 * readiness probe reads.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12552BoltListenerFailureTest {

  /** A listener whose thread ends as soon as it starts, with the port still bound and the listener not closed. */
  private static final class EndedListener extends BoltNetworkListener {
    EndedListener(final ArcadeDBServer server, final ServerSocketFactory factory, final BoltSslHelper ssl) {
      super(server, factory, ssl, "127.0.0.1", "0");
    }

    @Override
    public void run() {
      // ends here, which nothing short of close() does to the real accept loop
    }
  }

  private static ServerSocketFactory plainFactory() {
    return new ServerSocketFactory() {
      @Override
      public ServerSocket createServerSocket(final int port, final int backlog, final InetAddress ifAddress) throws IOException {
        return new ServerSocket(port, backlog, ifAddress);
      }
    };
  }

  private static BoltProtocolPlugin pluginOf(final BoltNetworkListener listener) throws ReflectiveOperationException {
    final BoltProtocolPlugin plugin = new BoltProtocolPlugin();
    final Field field = BoltProtocolPlugin.class.getDeclaredField("listener");
    field.setAccessible(true);
    field.set(plugin, listener);
    return plugin;
  }

  // Regression for #12552
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  void aListenerThreadThatEndedWhileListeningIsReported() throws Exception {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(new ContextConfiguration());
    final BoltSslHelper ssl = new BoltSslHelper(server.getConfiguration());
    final EndedListener listener = new EndedListener(server, plainFactory(), ssl);
    final BoltProtocolPlugin plugin = pluginOf(listener);
    try {
      listener.join(TimeUnit.SECONDS.toMillis(30));

      assertThat(listener.isAlive()).as("the listener thread ended").isFalse();
      assertThat(plugin.getListenerFailure()).isNotNull().contains(listener.getName()).contains("terminated");
    } finally {
      listener.close();
    }
    assertThat(plugin.getListenerFailure()).as("a listener closed on purpose is not a failure").isNull();
  }

  // Regression for #12552
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  void aListenerThreadThatMetAnErrorKeepsAcceptingAndIsNotReportedAsFailed() throws Exception {
    final CountDownLatch acceptReached = new CountDownLatch(1);
    final CountDownLatch acceptedAgain = new CountDownLatch(1);
    final AtomicInteger accepts = new AtomicInteger();
    final ServerSocketFactory failingOnce = new ServerSocketFactory() {
      @Override
      public ServerSocket createServerSocket(final int port, final int backlog, final InetAddress ifAddress) throws IOException {
        return new ServerSocket(port, backlog, ifAddress) {
          @Override
          public Socket accept() throws IOException {
            final int call = accepts.incrementAndGet();
            if (call == 1) {
              acceptReached.countDown();
              throw new OutOfMemoryError("Java heap space");
            }
            acceptedAgain.countDown();
            return super.accept();
          }
        };
      }
    };

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(new ContextConfiguration());
    final BoltSslHelper ssl = new BoltSslHelper(server.getConfiguration());
    // Port 0: the operating system picks a free one
    final BoltNetworkListener listener = new BoltNetworkListener(server, failingOnce, ssl, "127.0.0.1", "0");
    final BoltProtocolPlugin plugin = pluginOf(listener);
    try {
      assertThat(acceptReached.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(acceptedAgain.await(30, TimeUnit.SECONDS)).as("the loop went back to accepting after the error").isTrue();

      assertThat(listener.isAlive()).as("the error did not end the listener thread").isTrue();
      assertThat(plugin.getListenerFailure()).isNull();
    } finally {
      listener.close();
    }
  }

  // Regression for #12552
  @Test
  void aPluginThatWasNeverStartedReportsNothing() {
    assertThat(new BoltProtocolPlugin().getListenerFailure()).isNull();
  }
}
