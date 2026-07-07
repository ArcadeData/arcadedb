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

import io.grpc.CallCredentials;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5048 (SEC-5): the gRPC client must refuse to attach credentials
 * (username/password metadata) to calls made over a plaintext channel, because that ships the password in cleartext
 * on every RPC. The refusal can be explicitly overridden with {@code allowInsecureCredentials=true}.
 */
class RemoteGrpcCredentialSecurityTest {

  /**
   * Exposes the otherwise-protected credential factory methods so the guard can be exercised without a live server.
   */
  private static final class TestableRemoteGrpcServer extends RemoteGrpcServer {
    TestableRemoteGrpcServer(final boolean plaintext, final boolean allowInsecureCredentials) {
      super("localhost", 50051, "root", "secret", plaintext, List.of(), 30_000, allowInsecureCredentials);
    }

    CallCredentials callCredentials() {
      return createCredentials();
    }

    CallCredentials adminCallCredentials() {
      return createCallCredentials("root", "secret");
    }
  }

  @Test
  void plaintextChannelRefusesCredentialsByDefaultOptIn() {
    final TestableRemoteGrpcServer server = new TestableRemoteGrpcServer(true, false);
    try {
      assertThatThrownBy(server::callCredentials)
          .isInstanceOf(SecurityException.class)
          .hasMessageContaining("plaintext");
      assertThatThrownBy(server::adminCallCredentials)
          .isInstanceOf(SecurityException.class)
          .hasMessageContaining("plaintext");
    } finally {
      server.close();
    }
  }

  @Test
  void plaintextChannelAllowsCredentialsWhenExplicitlyOptedIn() {
    final TestableRemoteGrpcServer server = new TestableRemoteGrpcServer(true, true);
    try {
      assertThatCode(server::callCredentials).doesNotThrowAnyException();
      assertThat(server.callCredentials()).isNotNull();
      assertThatCode(server::adminCallCredentials).doesNotThrowAnyException();
    } finally {
      server.close();
    }
  }

  @Test
  void secureChannelAlwaysAllowsCredentials() {
    // plaintext=false -> transport security is present, credentials are safe to attach regardless of the opt-in flag.
    final TestableRemoteGrpcServer server = new TestableRemoteGrpcServer(false, false);
    try {
      assertThatCode(server::callCredentials).doesNotThrowAnyException();
      assertThat(server.callCredentials()).isNotNull();
      assertThatCode(server::adminCallCredentials).doesNotThrowAnyException();
    } finally {
      server.close();
    }
  }
}
