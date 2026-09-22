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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7496: the gRPC HA integration tests bound hand-picked ports ({@code 51141 + serverIndex}), so a server from
 * an earlier class that had not released its port yet - or anything else on the runner holding it - failed
 * whichever test started next, with "Address already in use". {@link StaticBaseServerTest#allocateFreePorts} is
 * what they take their ports from now; these are the two properties that makes them rely on.
 */
class Issue7496AllocateFreePortsTest {

  /**
   * The defect itself, turned around: a port somebody still holds is never handed out. A fixed port cannot make
   * this promise, which is the whole reason for moving off one.
   */
  @Test
  void neverHandsOutAPortSomebodyStillHolds() throws IOException {
    try (final ServerSocket lingering = new ServerSocket(0)) {
      final int held = lingering.getLocalPort();

      final int[] ports = StaticBaseServerTest.allocateFreePorts(32);

      assertThat(ports).doesNotContain(held);
    }
  }

  /**
   * One fixture's servers each need their own port, and the caller must be able to bind every one of them once the
   * call returns: the probing sockets are closed, not leaked.
   */
  @Test
  void returnsDistinctPortsTheCallerCanBind() throws IOException {
    final int[] ports = StaticBaseServerTest.allocateFreePorts(6);

    assertThat(ports).hasSize(6);
    assertThat(Arrays.stream(ports).distinct().count()).isEqualTo(6);
    for (final int port : ports)
      assertThat(port).isBetween(1, 65535);

    final ServerSocket[] bound = new ServerSocket[ports.length];
    try {
      for (int i = 0; i < ports.length; i++) {
        bound[i] = new ServerSocket();
        bound[i].setReuseAddress(true);
        bound[i].bind(new InetSocketAddress(ports[i]));
        assertThat(bound[i].getLocalPort()).isEqualTo(ports[i]);
      }
    } finally {
      for (final ServerSocket socket : bound)
        if (socket != null)
          socket.close();
    }
  }
}
