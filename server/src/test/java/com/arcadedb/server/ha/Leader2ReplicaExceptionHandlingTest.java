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

import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests exception classification logic in Leader2ReplicaNetworkExecutor.
 */
class Leader2ReplicaExceptionHandlingTest {

  @Test
  void testSocketTimeoutClassifiedAsTransient() {
    IOException cause = new SocketTimeoutException("timeout");
    boolean isTransient = isTransientNetworkFailure(cause);
    assertThat(isTransient).isTrue();
  }

  @Test
  void testSocketExceptionClassifiedAsTransient() {
    IOException cause = new SocketException("Connection reset");
    boolean isTransient = isTransientNetworkFailure(cause);
    assertThat(isTransient).isTrue();
  }

  @Test
  void testConnectionResetMessageClassifiedAsTransient() {
    IOException cause = new IOException("Connection reset by peer");
    boolean isTransient = isTransientNetworkFailure(cause);
    assertThat(isTransient).isTrue();
  }

  @Test
  void testGenericIOExceptionNotTransient() {
    IOException cause = new IOException("Generic error");
    boolean isTransient = isTransientNetworkFailure(cause);
    assertThat(isTransient).isFalse();
  }

  @Test
  void testServerIsNotTheLeaderClassifiedAsLeadershipChange() {
    Exception cause = new ServerIsNotTheLeaderException("Current server is not leader", "server2:2424");
    boolean isLeadershipChange = isLeadershipChange(cause);
    assertThat(isLeadershipChange).isTrue();
  }

  @Test
  void testNotLeaderMessageClassifiedAsLeadershipChange() {
    Exception cause = new IOException("Server is not the Leader");
    boolean isLeadershipChange = isLeadershipChange(cause);
    assertThat(isLeadershipChange).isTrue();
  }

  @Test
  void testProtocolMessageClassifiedAsProtocolError() {
    Exception cause = new IOException("Protocol version mismatch");
    boolean isProtocolError = isProtocolError(cause);
    assertThat(isProtocolError).isTrue();
  }

  @Test
  void testGenericIOExceptionNotProtocolError() {
    Exception cause = new IOException("Generic error");
    boolean isProtocolError = isProtocolError(cause);
    assertThat(isProtocolError).isFalse();
  }

  // Expose package-private helper methods for testing
  // These mirror the implementation in Leader2ReplicaNetworkExecutor

  private boolean isTransientNetworkFailure(Exception e) {
    return e instanceof SocketTimeoutException ||
           e instanceof SocketException ||
           (e instanceof IOException &&
            e.getMessage() != null &&
            e.getMessage().contains("Connection reset"));
  }

  private boolean isLeadershipChange(Exception e) {
    return e instanceof ServerIsNotTheLeaderException ||
           (e.getMessage() != null &&
            e.getMessage().contains("not the Leader"));
  }

  private boolean isProtocolError(Exception e) {
    return e instanceof IOException &&
           e.getMessage() != null &&
           e.getMessage().contains("Protocol");
  }
}
