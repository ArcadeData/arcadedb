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
package com.arcadedb.bolt;

import com.arcadedb.ContextConfiguration;
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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12552: an {@code OutOfMemoryError} ended the thread "n2 BOLT listening at 0.0.0.0:7687",
 * whose accept loop survives an exception but not an error, and the node went on with its Bolt port closed and nothing
 * but the log to show it. The plugin now reports the dead listener, which the readiness probe reads.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12552BoltListenerFailureTest {

  // Regression for #12552
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  void aListenerThreadKilledByAnErrorIsReported() throws Exception {
    final CountDownLatch acceptReached = new CountDownLatch(1);
    final CountDownLatch die = new CountDownLatch(1);
    final ServerSocketFactory dyingOnAccept = new ServerSocketFactory() {
      @Override
      public ServerSocket createServerSocket(final int port, final int backlog, final InetAddress ifAddress) throws IOException {
        return new ServerSocket(port, backlog, ifAddress) {
          @Override
          public Socket accept() throws IOException {
            acceptReached.countDown();
            try {
              die.await();
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            throw new OutOfMemoryError("Java heap space");
          }
        };
      }
    };

    final FakeArcadeDBServer server = FakeArcadeDBServer.create(new ContextConfiguration());
    final BoltSslHelper ssl = new BoltSslHelper(server.getConfiguration());
    // Port 0: the operating system picks a free one
    final BoltNetworkListener listener = new BoltNetworkListener(server, dyingOnAccept, ssl, "127.0.0.1", "0");
    final BoltProtocolPlugin plugin = new BoltProtocolPlugin();
    final Field field = BoltProtocolPlugin.class.getDeclaredField("listener");
    field.setAccessible(true);
    field.set(plugin, listener);
    try {
      assertThat(acceptReached.await(30, TimeUnit.SECONDS)).isTrue();
      assertThat(plugin.getListenerFailure()).as("the listener is accepting").isNull();

      die.countDown();
      listener.join(TimeUnit.SECONDS.toMillis(30));

      assertThat(listener.isAlive()).as("the error ended the listener thread").isFalse();
      assertThat(plugin.getListenerFailure()).isNotNull().contains(listener.getName()).contains("terminated");
    } finally {
      die.countDown();
      listener.close();
    }
    assertThat(plugin.getListenerFailure()).as("a listener closed on purpose is not a failure").isNull();
  }

  // Regression for #12552
  @Test
  void aPluginThatWasNeverStartedReportsNothing() {
    assertThat(new BoltProtocolPlugin().getListenerFailure()).isNull();
  }
}
