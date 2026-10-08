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
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcServerPluginTest {

  @TempDir
  Path tempDir;

  @Test
  void pluginConfiguresWithDefaults() {
    GrpcServerPlugin plugin = new GrpcServerPlugin();
    ContextConfiguration config = new ContextConfiguration();

    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);

    plugin.configure(server, config);

    // Plugin should be configured without errors
    GrpcServerPlugin.ServerStatus status = plugin.getStatus();
    assertThat(status.standardServerRunning).isFalse(); // Not started yet
  }

  @Test
  void pluginDisabledWhenConfigured() {
    GrpcServerPlugin plugin = new GrpcServerPlugin();
    ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.enabled", "false");

    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);

    plugin.configure(server, config);
    plugin.startService();

    GrpcServerPlugin.ServerStatus status = plugin.getStatus();
    assertThat(status.standardServerRunning).isFalse();
    assertThat(status.xdsServerRunning).isFalse();
  }

  @Test
  void serverStatusReportsCorrectPorts() {
    GrpcServerPlugin.ServerStatus status = new GrpcServerPlugin.ServerStatus(
        true, false, 50051, -1);

    assertThat(status.standardServerRunning).isTrue();
    assertThat(status.xdsServerRunning).isFalse();
    assertThat(status.standardPort).isEqualTo(50051);
    assertThat(status.xdsPort).isEqualTo(-1);
  }

  /** Issue #8935: "yes" must not silently start a plaintext endpoint, only true/false are booleans. */
  @Test
  void tlsEnabledWithANonBooleanValueRefusesToStart() {
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.tls.enabled", "yes");

    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);

    plugin.configure(server, config);
    assertThatThrownBy(plugin::startService)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("arcadedb.grpc.tls.enabled")
        .hasMessageContaining("yes");
    assertThat(plugin.getStatus().standardServerRunning).isFalse();
  }

  @Test
  void enabledAcceptsTrimmedCaseInsensitiveBooleans() {
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue("arcadedb.grpc.enabled", " FALSE ");

    final ArcadeDBServer server = TestServerHelper.unstartedServer(tempDir, config);

    plugin.configure(server, config);
    plugin.startService();
    assertThat(plugin.getStatus().standardServerRunning).isFalse();
  }
}
