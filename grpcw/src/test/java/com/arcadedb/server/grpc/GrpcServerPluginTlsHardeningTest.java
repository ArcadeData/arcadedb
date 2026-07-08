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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

/**
 * Transport-security hardening for {@link GrpcServerPlugin}: TLS must fail closed instead of silently downgrading to
 * plaintext (SEC-3 standard mode, SEC-4 xds mode), and the inbound metadata cap must be resolved from configuration
 * with a sane default (SEC-8).
 */
class GrpcServerPluginTlsHardeningTest {

  @TempDir
  Path tempDir;

  private GrpcServerPlugin pluginWithConfig(final ContextConfiguration config) {
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    lenient().when(mockServer.getRootPath()).thenReturn(tempDir.toString());
    lenient().when(mockServer.getConfiguration()).thenReturn(config);
    plugin.configure(mockServer, config);
    return plugin;
  }

  @Test
  void standardModeFailsClosedWhenCertPathMissing() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.tls.enabled", "true");
    // No cert/key path configured at all.
    final GrpcServerPlugin plugin = pluginWithConfig(config);

    assertThatThrownBy(plugin::startService)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("Refusing to start with cleartext");

    assertThat(plugin.getStatus().standardServerRunning).isFalse();
  }

  @Test
  void standardModeFailsClosedWhenCertFileAbsent() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.tls.enabled", "true");
    config.setValue("arcadedb.grpc.tls.cert", tempDir.resolve("missing-cert.pem").toString());
    config.setValue("arcadedb.grpc.tls.key", tempDir.resolve("missing-key.pem").toString());
    final GrpcServerPlugin plugin = pluginWithConfig(config);

    assertThatThrownBy(plugin::startService)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("Refusing to start with cleartext");

    assertThat(plugin.getStatus().standardServerRunning).isFalse();
  }

  @Test
  void xdsModeFailsClosedWhenCertMissing() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.mode", "xds");
    config.setValue("arcadedb.grpc.tls.enabled", "true");
    final GrpcServerPlugin plugin = pluginWithConfig(config);

    assertThatThrownBy(plugin::startService)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("Refusing to start with cleartext");

    assertThat(plugin.getStatus().xdsServerRunning).isFalse();
  }

  @Test
  void bothModeFailsClosedWhenCertMissing() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.mode", "both");
    config.setValue("arcadedb.grpc.tls.enabled", "true");
    final GrpcServerPlugin plugin = pluginWithConfig(config);

    assertThatThrownBy(plugin::startService)
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("Refusing to start with cleartext");

    assertThat(plugin.getStatus().standardServerRunning).isFalse();
    assertThat(plugin.getStatus().xdsServerRunning).isFalse();
  }

  @Test
  void metadataCapDefaultsToSixteenKilobytes() {
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    assertThat(plugin.getMaxMetadataSizeBytes(new ContextConfiguration())).isEqualTo(16 * 1024);
  }

  @Test
  void metadataCapIsConfigurableInKilobytes() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.maxMetadataSize", "8");
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    assertThat(plugin.getMaxMetadataSizeBytes(config)).isEqualTo(8 * 1024);
  }

  @Test
  void metadataCapIsClampedToAtLeastOneKilobyte() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.maxMetadataSize", "0");
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    assertThat(plugin.getMaxMetadataSizeBytes(config)).isEqualTo(1024);
  }
}
