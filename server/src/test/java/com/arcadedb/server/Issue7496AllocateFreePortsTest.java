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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7496: the gRPC HA integration tests bound hand-picked ports ({@code 51141 + serverIndex}), so a server from
 * an earlier class that had not released its port yet - or anything else on the runner holding it - failed
 * whichever test started next, with "Address already in use". {@link StaticBaseServerTest#allocateFreePorts} is
 * what they take their ports from now; these are the properties that makes them rely on.
 */
class Issue7496AllocateFreePortsTest {

  /**
   * The ports must come from BELOW every operating system's ephemeral range. That range is where each outgoing TCP
   * connection on the host takes its local port from, so a port probed there and released can be taken by the very
   * next connect() anywhere on the machine - including the fixture's own first Raft node dialling the second one
   * before the second has bound. Ratis answers that bind failure with System.exit(1), which kills the whole test fork
   * (seen while verifying #7496 with the previous, ephemeral-range allocator).
   */
  @Test
  void allocatesBelowEveryEphemeralRange() {
    final int[] ports = StaticBaseServerTest.allocateFreePorts(16);

    for (final int port : ports)
      assertThat(port).isBetween(StaticBaseServerTest.FREE_PORT_RANGE_FIRST, StaticBaseServerTest.FREE_PORT_RANGE_LAST);
    // Linux starts its ephemeral range at 32768; macOS and Windows at 49152.
    assertThat(StaticBaseServerTest.FREE_PORT_RANGE_LAST).isLessThan(32768);
  }

  /**
   * The defect itself, turned around: a port somebody still holds is never handed out. A two-port range with one of
   * them held leaves exactly one right answer, so this cannot pass by the luck of a random draw.
   */
  @Test
  void neverHandsOutAPortSomebodyStillHolds() throws IOException {
    final int held = freeAdjacentPair();
    try (final ServerSocket lingering = new ServerSocket()) {
      lingering.bind(new InetSocketAddress(held));

      final int[] ports = StaticBaseServerTest.allocateFreePorts(1, held, held + 1);

      assertThat(ports).containsExactly(held + 1);
    }
  }

  /** A range with nothing free left must fail loudly, never return a port that is taken. */
  @Test
  void failsWhenTheRangeHasNoFreePortLeft() throws IOException {
    final int held = freeAdjacentPair();
    try (final ServerSocket lingering = new ServerSocket()) {
      lingering.bind(new InetSocketAddress(held));

      assertThatThrownBy(() -> StaticBaseServerTest.allocateFreePorts(1, held, held))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("free");
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

    final ServerSocket[] bound = new ServerSocket[ports.length];
    try {
      for (int i = 0; i < ports.length; i++) {
        bound[i] = new ServerSocket();
        bound[i].bind(new InetSocketAddress(ports[i]));
        assertThat(bound[i].getLocalPort()).isEqualTo(ports[i]);
      }
    } finally {
      for (final ServerSocket socket : bound)
        if (socket != null)
          socket.close();
    }
  }

  /** The first port {@code p} of the allocation range such that {@code p} and {@code p + 1} are both free now. */
  private static int freeAdjacentPair() {
    for (int port = StaticBaseServerTest.FREE_PORT_RANGE_FIRST; port < StaticBaseServerTest.FREE_PORT_RANGE_LAST; port++)
      if (isFree(port) && isFree(port + 1))
        return port;
    throw new IllegalStateException("No two adjacent free ports in the allocation range");
  }

  private static boolean isFree(final int port) {
    try (final ServerSocket probe = new ServerSocket()) {
      probe.bind(new InetSocketAddress(port));
      return true;
    } catch (final IOException busy) {
      return false;
    }
  }
}
