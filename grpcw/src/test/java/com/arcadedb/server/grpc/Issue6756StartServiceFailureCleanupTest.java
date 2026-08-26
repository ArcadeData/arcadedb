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
 * Regression test for issue #6756 (1): {@code GrpcServerPlugin.startService()} constructs the
 * {@link ArcadeDbGrpcService} (which starts its idle-transaction reaper thread) inside
 * {@code configureServer()}, before the Netty server actually binds the port. If binding fails (e.g. the
 * port is already in use), {@code ArcadeDBServer.start()} deliberately never calls {@code stopService()}
 * on a plugin whose {@code startService()} threw - so the already-created service and its reaper thread
 * leaked with no teardown path.
 * <p>
 * The fix wraps {@code startService()}'s body so any failure calls the (idempotent) {@code stopService()}
 * before rethrowing.
 */
class Issue6756StartServiceFailureCleanupTest {

  @TempDir
  Path tempDir;

  private ServerSocket portHog;
  private GrpcServerPlugin plugin;

  @AfterEach
  void cleanup() throws IOException {
    if (plugin != null)
      plugin.stopService();
    if (portHog != null)
      portHog.close();
  }

  @Test
  void startServiceFailureStopsTheLeakedServiceAndReaper() throws IOException {
    // Occupy a real port so the plugin's own bind attempt fails with a genuine "address already in use".
    portHog = new ServerSocket(0);
    final int occupiedPort = portHog.getLocalPort();

    plugin = new GrpcServerPlugin();
    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    final ContextConfiguration config = new ContextConfiguration();
    // GrpcServerPlugin reads this back via ContextConfiguration.getValueAsString, which casts the stored
    // value directly to String - it must be stored as a String, not the boxed Integer overload would give.
    config.setValue(GlobalConfiguration.GRPC_PORT.getKey(), String.valueOf(occupiedPort));
    when(mockServer.getRootPath()).thenReturn(tempDir.toString());
    when(mockServer.getConfiguration()).thenReturn(config);
    plugin.configure(mockServer, config);

    assertThatThrownBy(plugin::startService).isInstanceOf(RuntimeException.class);

    // configureServer() built the service (and its reaper) before the failing bind - confirm the leak
    // scenario is real, not a no-op test.
    final ArcadeDbGrpcService service = plugin.getService();
    assertThat(service).as("the service must have been constructed before the bind failure").isNotNull();

    // The fix: startService()'s catch block must have called stopService(), which shuts the reaper down.
    assertThat(service.isIdleReaperShutdown())
        .as("the idle-transaction reaper thread must not leak past a failed startService()").isTrue();
  }
}
