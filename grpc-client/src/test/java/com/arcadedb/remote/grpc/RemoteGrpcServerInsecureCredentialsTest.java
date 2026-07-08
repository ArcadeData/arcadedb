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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SEC-5: the client must not attach username/password credentials that would travel in cleartext over a plaintext
 * channel to a non-loopback host. Loopback plaintext (no wire exposure) and an explicit opt-in remain allowed.
 * <p>
 * A non-loopback TEST-NET address (RFC 5737) is used so no DNS resolution and no live connection is required.
 */
class RemoteGrpcServerInsecureCredentialsTest {

  private static final String REMOTE_HOST = "192.0.2.1"; // TEST-NET-1, guaranteed non-routable and non-loopback

  private RemoteGrpcServer server(final String host, final boolean plaintext, final boolean allowInsecure) {
    return new RemoteGrpcServer(host, 50051, "root", "secret", plaintext, List.of(), 30_000, allowInsecure);
  }

  @Test
  void loopbackHostnamePlaintextCredentialsAllowed() {
    final RemoteGrpcServer server = server("localhost", true, false);
    final CallCredentials credentials = server.createCredentials();
    assertThat(credentials).isNotNull();
  }

  @Test
  void loopbackIpPlaintextCredentialsAllowed() {
    final RemoteGrpcServer server = server("127.0.0.1", true, false);
    assertThat(server.createCredentials()).isNotNull();
  }

  @Test
  void remotePlaintextCredentialsRefusedByDefault() {
    final RemoteGrpcServer server = server(REMOTE_HOST, true, false);
    assertThatThrownBy(server::createCredentials)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("plaintext")
        .hasMessageContaining(REMOTE_HOST);
  }

  @Test
  void remotePlaintextCallCredentialsRefusedByDefault() {
    final RemoteGrpcServer server = server(REMOTE_HOST, true, false);
    assertThatThrownBy(() -> server.createCallCredentials("root", "secret"))
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("Refusing to send credentials");
  }

  @Test
  void remotePlaintextCredentialsAllowedWhenOptedIn() {
    final RemoteGrpcServer server = server(REMOTE_HOST, true, true);
    assertThat(server.createCredentials()).isNotNull();
    assertThat(server.createCallCredentials("root", "secret")).isNotNull();
  }

  @Test
  void remoteTlsCredentialsAllowed() {
    // plaintext=false -> transport is secure, so credentials are safe to attach regardless of host.
    final RemoteGrpcServer server = server(REMOTE_HOST, false, false);
    assertThat(server.createCredentials()).isNotNull();
  }
}
