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
package com.arcadedb.server.ha;

import com.arcadedb.network.binary.ConnectionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class ExceptionClassificationTest {

  @Test
  void testTransientNetworkFailureClassification() {
    assertThat(isTransientNetworkFailure(new SocketTimeoutException())).isTrue();
    assertThat(isTransientNetworkFailure(new SocketException())).isTrue();
    assertThat(isTransientNetworkFailure(
        new IOException("Connection reset"))).isTrue();
    assertThat(isTransientNetworkFailure(
        new IOException("Something else"))).isFalse();
  }

  @Test
  void testLeadershipChangeClassification() {
    assertThat(isLeadershipChange(new ServerIsNotTheLeaderException("test", "http://leader:2480"))).isTrue();
    assertThat(isLeadershipChange(
        new ConnectionException("http://leader:2480", "Server is not the Leader"))).isTrue();
    assertThat(isLeadershipChange(
        new ReplicationException("An election in progress"))).isTrue();
    assertThat(isLeadershipChange(new IOException())).isFalse();
  }

  @Test
  void testProtocolErrorClassification() {
    assertThat(isProtocolError(new NetworkProtocolException("test"))).isTrue();
    assertThat(isProtocolError(
        new IOException("Protocol version mismatch"))).isTrue();
    assertThat(isProtocolError(new SocketException())).isFalse();
  }

  @Test
  void testCategorizeException() {
    assertThat(categorizeException(new SocketTimeoutException()))
        .isEqualTo(ExceptionCategory.TRANSIENT_NETWORK);

    assertThat(categorizeException(new ServerIsNotTheLeaderException("test", "http://leader:2480")))
        .isEqualTo(ExceptionCategory.LEADERSHIP_CHANGE);

    assertThat(categorizeException(new NetworkProtocolException("test")))
        .isEqualTo(ExceptionCategory.PROTOCOL_ERROR);

    assertThat(categorizeException(new RuntimeException()))
        .isEqualTo(ExceptionCategory.UNKNOWN);
  }

  // Helper methods (will be implemented in Leader2ReplicaNetworkExecutor)
  private static boolean isTransientNetworkFailure(Exception e) {
    return e instanceof SocketTimeoutException ||
           e instanceof SocketException ||
           (e instanceof IOException &&
            e.getMessage() != null &&
            e.getMessage().contains("Connection reset"));
  }

  private static boolean isLeadershipChange(Exception e) {
    return e instanceof ServerIsNotTheLeaderException ||
           (e instanceof ConnectionException &&
            e.getMessage() != null &&
            e.getMessage().contains("not the Leader")) ||
           (e instanceof ReplicationException &&
            e.getMessage() != null &&
            e.getMessage().contains("election in progress"));
  }

  private static boolean isProtocolError(Exception e) {
    return e instanceof NetworkProtocolException ||
           (e instanceof IOException &&
            e.getMessage() != null &&
            e.getMessage().contains("Protocol"));
  }

  private static ExceptionCategory categorizeException(Exception e) {
    if (isTransientNetworkFailure(e)) {
      return ExceptionCategory.TRANSIENT_NETWORK;
    } else if (isLeadershipChange(e)) {
      return ExceptionCategory.LEADERSHIP_CHANGE;
    } else if (isProtocolError(e)) {
      return ExceptionCategory.PROTOCOL_ERROR;
    } else {
      return ExceptionCategory.UNKNOWN;
    }
  }
}
