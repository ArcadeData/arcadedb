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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

/**
 * Regression test for issue #5048 (SEC-3, SEC-4): when gRPC TLS is enabled but misconfigured (missing cert/key path
 * or missing file), the server must fail closed - it must refuse to start rather than silently downgrading to
 * cleartext. This applies to both the standard server and the XDS server.
 */
class GrpcServerTlsFailClosedTest {

  @TempDir
  Path tempDir;

  private GrpcServerPlugin pluginFor(final ContextConfiguration config) {
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    lenient().when(mockServer.getRootPath()).thenReturn(tempDir.toString());
    lenient().when(mockServer.getConfiguration()).thenReturn(config);
    plugin.configure(mockServer, config);
    return plugin;
  }

  @Test
  void standardServerRefusesToStartWhenCertPathMissing() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.mode", "standard");
    config.setValue("arcadedb.grpc.tls.enabled", "true");
    // No arcadedb.grpc.tls.cert / .key set.

    final GrpcServerPlugin plugin = pluginFor(config);
    assertThatThrownBy(plugin::startService)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("cleartext");
  }

  @Test
  void standardServerRefusesToStartWhenCertFileNotFound() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.mode", "standard");
    config.setValue("arcadedb.grpc.tls.enabled", "true");
    config.setValue("arcadedb.grpc.tls.cert", tempDir.resolve("does-not-exist.crt").toString());
    config.setValue("arcadedb.grpc.tls.key", tempDir.resolve("does-not-exist.key").toString());

    final GrpcServerPlugin plugin = pluginFor(config);
    assertThatThrownBy(plugin::startService)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("cleartext");
  }

  @Test
  void xdsServerRefusesToStartWhenCertPathMissing() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.mode", "xds");
    config.setValue("arcadedb.grpc.tls.enabled", "true");
    // No arcadedb.grpc.tls.cert / .key set.

    final GrpcServerPlugin plugin = pluginFor(config);
    assertThatThrownBy(plugin::startService)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("cleartext");
  }

  @Test
  void bothModeRefusesToStartWhenCertPathMissing() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.mode", "both");
    config.setValue("arcadedb.grpc.tls.enabled", "true");

    final GrpcServerPlugin plugin = pluginFor(config);
    assertThatThrownBy(plugin::startService)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("cleartext");
  }
}
