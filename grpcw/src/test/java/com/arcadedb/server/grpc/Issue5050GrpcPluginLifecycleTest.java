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
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #5050 (gRPC plugin lifecycle & cleanup):
 * <ul>
 *   <li>CON-2: in "both" mode {@code configureServer} is invoked once per server builder; it must build exactly one
 *       {@link ArcadeDbGrpcService} (and one health manager) and reuse it, otherwise the first service's idle-reaper
 *       thread and transaction registry leak forever.</li>
 *   <li>CON-6: {@code stopService} must be idempotent so the JVM shutdown hook and the plugin-lifecycle stop cannot
 *       double-run the cleanup.</li>
 * </ul>
 * These tests exercise the builder-configuration path directly (no ports are bound) so they stay fast and deterministic.
 */
class Issue5050GrpcPluginLifecycleTest {

  @TempDir
  Path tempDir;

  private ContextConfiguration config;

  private GrpcServerPlugin newConfiguredPlugin() {
    final GrpcServerPlugin plugin = new GrpcServerPlugin();
    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    config = new ContextConfiguration();
    when(mockServer.getRootPath()).thenReturn(tempDir.toString());
    when(mockServer.getConfiguration()).thenReturn(config);
    plugin.configure(mockServer, config);
    return plugin;
  }

  @Test
  void bothModeReusesSingleServiceInstance() {
    final GrpcServerPlugin plugin = newConfiguredPlugin();

    // Simulate "both" mode: configure the standard builder, then the xDS builder, against the same plugin.
    plugin.configureServer(NettyServerBuilder.forPort(0), config);
    final ArcadeDbGrpcService first = plugin.getService();
    assertThat(first).isNotNull();
    assertThat(first.isIdleReaperActive()).isTrue();

    plugin.configureServer(NettyServerBuilder.forPort(0), config);
    final ArcadeDbGrpcService second = plugin.getService();

    // The single service must be reused across both builders - no orphaned second instance whose reaper thread leaks.
    assertThat(second).isSameAs(first);

    plugin.stopService();
  }

  @Test
  void stopServiceIsIdempotent() {
    final GrpcServerPlugin plugin = newConfiguredPlugin();

    plugin.configureServer(NettyServerBuilder.forPort(0), config);
    assertThat(plugin.getService()).isNotNull();

    // First stop performs cleanup; every subsequent stop must be a safe no-op (shutdown hook vs. lifecycle race).
    assertThatCode(() -> {
      plugin.stopService();
      plugin.stopService();
      plugin.stopService();
    }).doesNotThrowAnyException();
  }
}
