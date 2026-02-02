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
package com.arcadedb.network.binary;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExceptionTest {

  @Test
  void connectionExceptionWithThrowable() {
    final Exception cause = new RuntimeException("Connection refused");
    final ConnectionException exception = new ConnectionException("localhost:2480", cause);

    assertThat(exception.getUrl()).isEqualTo("localhost:2480");
    assertThat(exception.getReason()).isEqualTo(cause.toString());
    assertThat(exception.getMessage()).contains("localhost:2480");
    assertThat(exception.getMessage()).contains(cause.toString());
    assertThat(exception.getCause()).isEqualTo(cause);
  }

  @Test
  void connectionExceptionWithReason() {
    final ConnectionException exception = new ConnectionException("localhost:2480", "Server unavailable");

    assertThat(exception.getUrl()).isEqualTo("localhost:2480");
    assertThat(exception.getReason()).isEqualTo("Server unavailable");
    assertThat(exception.getMessage()).contains("localhost:2480");
    assertThat(exception.getMessage()).contains("Server unavailable");
    assertThat(exception.getCause()).isNull();
  }

  @Test
  void networkProtocolExceptionWithMessage() {
    final NetworkProtocolException exception = new NetworkProtocolException("Protocol error");

    assertThat(exception.getMessage()).isEqualTo("Protocol error");
    assertThat(exception.getCause()).isNull();
  }

  @Test
  void networkProtocolExceptionWithCause() {
    final Exception cause = new RuntimeException("IO error");
    final NetworkProtocolException exception = new NetworkProtocolException("Protocol error", cause);

    assertThat(exception.getMessage()).isEqualTo("Protocol error");
    assertThat(exception.getCause()).isEqualTo(cause);
  }

  @Test
  void quorumNotReachedException() {
    final QuorumNotReachedException exception = new QuorumNotReachedException("Quorum not reached: 1/3");

    assertThat(exception.getMessage()).isEqualTo("Quorum not reached: 1/3");
  }

  @Test
  void serverIsNotTheLeaderException() {
    final ServerIsNotTheLeaderException exception = new ServerIsNotTheLeaderException(
        "Server is not the leader", "192.168.1.100:2480");

    assertThat(exception.getMessage()).isEqualTo("Server is not the leader");
    assertThat(exception.getLeaderAddress()).isEqualTo("192.168.1.100:2480");
    assertThat(exception.toString()).contains("Server is not the leader");
    assertThat(exception.toString()).contains("192.168.1.100:2480");
  }
}
