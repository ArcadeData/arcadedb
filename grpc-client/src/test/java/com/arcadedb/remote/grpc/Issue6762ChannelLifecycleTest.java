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
package com.arcadedb.remote.grpc;

import io.grpc.ClientInterceptor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6762 item 6 and its PR #6783 review follow-up: the channel field is read without the monitor that mutates
 * it, so it has to be {@code volatile}, read once per decision, and it must not be resurrected after close.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6762ChannelLifecycleTest {

  /**
   * The field is mutated only under this object's monitor by start()/close() but read WITHOUT it by channel() and
   * the stub factories, so a reader racing either has no happens-before edge on the write.
   */
  @Test
  void theChannelAndItsEventLoopGroupAreVolatile() throws Exception {
    assertThat(java.lang.reflect.Modifier.isVolatile(
        RemoteGrpcServer.class.getDeclaredField("channel").getModifiers()))
        .as("channel is read outside the monitor that writes it")
        .isTrue();
    assertThat(java.lang.reflect.Modifier.isVolatile(
        RemoteGrpcServer.class.getDeclaredField("eventLoopGroup").getModifiers()))
        .as("eventLoopGroup is torn down on the same path")
        .isTrue();
  }

  /**
   * close() used to leave channel() free to observe the null it had just written and silently BUILD A NEW CHANNEL
   * for a server the caller had explicitly closed - a reconnect nobody asked for, to a server being shut down.
   */
  @Test
  void channelRefusesToResurrectAClosedServer() {
    final RemoteGrpcServer server = new RemoteGrpcServer("localhost", 50051, "root", "pwd", true, List.of(), 30_000, true);
    server.start();
    assertThatNoException().isThrownBy(server::channel);

    server.close();

    assertThatThrownBy(server::channel)
        .as("a closed server hands out no channel, and certainly does not open a fresh one")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");
  }

  /** An EXPLICIT restart is still allowed: it is the lazy resurrection from a stale reference that is not. */
  @Test
  void anExplicitRestartAfterCloseStillWorks() {
    final RemoteGrpcServer server = new RemoteGrpcServer("localhost", 50051, "root", "pwd", true, List.of(), 30_000, true);
    server.start();
    server.close();

    server.start();
    try {
      assertThatNoException().isThrownBy(server::channel);
    } finally {
      server.close();
    }
  }
}
