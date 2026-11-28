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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.network.ServerSocketFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A listening socket that keeps failing when asked for the next connection fails at once on every call, for as long as its
 * cause lasts: the accept loop must retry at a bounded rate, and must serve again by itself once the socket works again.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresListenerAcceptFailureTest {
  private PostgresNetworkListener listener;

  @AfterEach
  void closeListener() {
    if (listener != null)
      listener.close();
  }

  /** A factory whose sockets fail every {@code accept()} up to the {@code failures}-th call, then accept for real. */
  private static ServerSocketFactory failingFactory(final long failures, final AtomicLong calls, final CountDownLatch firstCall,
      final CountDownLatch recovered) {
    return new ServerSocketFactory() {
      @Override
      public ServerSocket createServerSocket(final int port, final int backlog, final InetAddress ifAddress) throws IOException {
        return new ServerSocket(port, backlog, ifAddress) {
          @Override
          public Socket accept() throws IOException {
            final long call = calls.incrementAndGet();
            firstCall.countDown();
            if (call <= failures)
              throw new IOException("accept failed");
            recovered.countDown();
            return super.accept();
          }
        };
      }
    };
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  void aSocketThatKeepsFailingToAcceptIsRetriedAtABoundedRate() throws Exception {
    final AtomicLong calls = new AtomicLong();
    final CountDownLatch firstCall = new CountDownLatch(1);
    final ContextConfiguration configuration = new ContextConfiguration();
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
    listener = new PostgresNetworkListener(server, failingFactory(Long.MAX_VALUE, calls, firstCall, new CountDownLatch(1)),
        new PostgresSslHelper(configuration), "127.0.0.1", "0");

    assertThat(firstCall.await(30, TimeUnit.SECONDS)).as("the loop reached the socket").isTrue();
    TimeUnit.SECONDS.sleep(1);

    // An upper bound: a stall can only make the loop retry less often. Spinning is millions of calls a second
    assertThat(calls.get()).as("calls to a socket that keeps failing, in one second").isLessThan(50);
    assertThat(listener.isAlive()).as("the accept thread is still running").isTrue();
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  void theListenerAcceptsAgainOnceTheSocketRecovers() throws Exception {
    final AtomicLong calls = new AtomicLong();
    final CountDownLatch recovered = new CountDownLatch(1);
    final ContextConfiguration configuration = new ContextConfiguration();
    final FakeArcadeDBServer server = FakeArcadeDBServer.create(configuration);
    listener = new PostgresNetworkListener(server, failingFactory(3, calls, new CountDownLatch(1), recovered),
        new PostgresSslHelper(configuration), "127.0.0.1", "0");

    assertThat(recovered.await(30, TimeUnit.SECONDS)).as("the loop went back to accepting after the failures").isTrue();
    assertThat(listener.isAlive()).as("the failures did not end the accept thread").isTrue();
  }
}
