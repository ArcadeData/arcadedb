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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for the second residue of issue #7035: {@code GrpcServerPlugin.startService()}'s cleanup added by
 * issue #6756 caught {@code IOException} only, so a runtime failure out of the server build - after
 * {@code configureServer()} had already created the service and started its idle-transaction reaper - still
 * escaped without {@code stopService()}, leaking both. The catch now covers runtime failures too, and rethrows them
 * with their own type.
 * <p>
 * The failure is injected at {@code getMaxMetadataSizeBytes()}, which {@code startStandardServer()} asks right after
 * {@code configureServer()} and right before {@code build().start()}: the same point in the sequence a runtime
 * failure of the builder would land on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7035StartServiceRuntimeFailureCleanupTest {

  @TempDir
  Path tempDir;

  private GrpcServerPlugin plugin;

  @AfterEach
  void cleanup() {
    if (plugin != null)
      plugin.stopService();
  }

  @Test
  void runtimeFailureWhileBuildingTheServerStopsTheLeakedServiceAndReaper() throws IOException {
    final int freePort;
    try (final ServerSocket probe = new ServerSocket(0)) {
      freePort = probe.getLocalPort();
    }

    plugin = new GrpcServerPlugin() {
      @Override
      int getMaxMetadataSizeBytes(final ContextConfiguration config) {
        throw new IllegalStateException("simulated runtime failure while building the gRPC server");
      }
    };
    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(freePort));
    when(mockServer.getRootPath()).thenReturn(tempDir.toString());
    when(mockServer.getConfiguration()).thenReturn(config);
    plugin.configure(mockServer, config);

    assertThatThrownBy(plugin::startService)
        .as("a runtime failure keeps its own type on the way out")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("simulated runtime failure");

    final ArcadeDbGrpcService service = plugin.getService();
    assertThat(service).as("the service must have been constructed before the failure, or this proves nothing").isNotNull();
    assertThat(service.isIdleReaperShutdown())
        .as("the idle-transaction reaper thread must not leak past a failed startService()").isTrue();
  }
}
