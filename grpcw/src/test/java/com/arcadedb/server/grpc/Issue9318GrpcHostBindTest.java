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
package com.arcadedb.server.grpc;

import io.grpc.Server;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9318: {@code arcadedb.grpc.host} was read and logged but never bound, so the listener was
 * always on the wildcard address.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9318GrpcHostBindTest {

  @Test
  void loopbackHostBindsOnlyLoopback() throws Exception {
    final Server server = GrpcServerPlugin.newListeningBuilder("127.0.0.1", 0).build().start();
    try {
      assertThat(server.getListenSockets()).isNotEmpty();
      for (final SocketAddress address : server.getListenSockets()) {
        final InetSocketAddress inet = (InetSocketAddress) address;
        assertThat(inet.getAddress().isLoopbackAddress()).as("bound to %s", inet).isTrue();
        assertThat(inet.getAddress().isAnyLocalAddress()).isFalse();
      }
    } finally {
      server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test
  void wildcardHostStillBindsWildcard() throws Exception {
    final Server server = GrpcServerPlugin.newListeningBuilder("0.0.0.0", 0).build().start();
    try {
      assertThat(((InetSocketAddress) server.getListenSockets().get(0)).getAddress().isAnyLocalAddress()).isTrue();
    } finally {
      server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}
