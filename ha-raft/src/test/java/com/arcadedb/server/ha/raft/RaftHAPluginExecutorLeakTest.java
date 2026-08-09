/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import io.undertow.server.handlers.PathHandler;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5890: {@code RaftHAPlugin.stopService()} left the {@link SnapshotHttpHandler}
 * watchdog scheduler and the {@link PostVerifyDatabaseHandler} peer-query pool running after the plugin
 * stopped. {@code registerAPI()} runs once per {@code HttpServer.setupRoutes()} call, and a fresh
 * {@code RaftHAPlugin} (and thus fresh handlers) is created by {@code PluginManager} on every server start,
 * so an in-JVM stop/start cycle (test suites, embedded deployments) leaked one watchdog thread and one
 * cached thread pool per restart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RaftHAPluginExecutorLeakTest {

  @Test
  void stopServiceShutsDownSnapshotHandlerWatchdogExecutor() throws Exception {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.registerAPI(null, new PathHandler());

    final SnapshotHttpHandler snapshotHandler = getField(plugin, "snapshotHttpHandler", SnapshotHttpHandler.class);
    final ScheduledExecutorService watchdogExecutor = getField(snapshotHandler, "watchdogExecutor", ScheduledExecutorService.class);
    assertThat(watchdogExecutor.isShutdown()).isFalse();

    plugin.stopService();

    assertThat(watchdogExecutor.isShutdown())
        .as("SnapshotHttpHandler's watchdog scheduler must be shut down when the plugin stops, or every "
            + "in-JVM server restart leaks a watchdog thread (issue #5890)")
        .isTrue();
  }

  @Test
  void stopServiceShutsDownVerifyHandlerPeerQueryExecutor() throws Exception {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.registerAPI(null, new PathHandler());

    final PostVerifyDatabaseHandler verifyHandler = getField(plugin, "postVerifyDatabaseHandler", PostVerifyDatabaseHandler.class);
    final ExecutorService peerQueryExecutor = getField(verifyHandler, "peerQueryExecutor", ExecutorService.class);
    assertThat(peerQueryExecutor.isShutdown()).isFalse();

    plugin.stopService();

    assertThat(peerQueryExecutor.isShutdown())
        .as("PostVerifyDatabaseHandler's peer-query pool must be shut down when the plugin stops, or every "
            + "in-JVM server restart leaks a cached thread pool (issue #5890)")
        .isTrue();
  }

  @Test
  void stopServiceIsNullSafeWhenHandlersWereNeverRegistered() {
    // registerAPI() is only reached once the plugin is actually wired into the HTTP server; stopService()
    // must not NPE if it is invoked without that having happened.
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.stopService();
  }

  @Test
  void stopServiceIsIdempotentWhenCalledTwiceBackToBack() throws Exception {
    // The real-world trigger for the null-guards: ArcadeDBServer.stopInternal() invokes stopService() on
    // this SAME instance twice on every HA shutdown - once via PluginManager.stopPlugins() (RaftHAPlugin
    // is itself a discovered ServerPlugin) and once directly via haServer.stopService(), since
    // startService() aliases ArcadeDBServer.haServer to this instance via server.setHA(this) (issue #5890).
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.registerAPI(null, new PathHandler());
    final SnapshotHttpHandler snapshotHandler = getField(plugin, "snapshotHttpHandler", SnapshotHttpHandler.class);
    final ScheduledExecutorService watchdogExecutor = getField(snapshotHandler, "watchdogExecutor", ScheduledExecutorService.class);
    final PostVerifyDatabaseHandler verifyHandler = getField(plugin, "postVerifyDatabaseHandler", PostVerifyDatabaseHandler.class);
    final ExecutorService peerQueryExecutor = getField(verifyHandler, "peerQueryExecutor", ExecutorService.class);

    plugin.stopService();
    plugin.stopService(); // must be a no-op, not throw

    assertThat(watchdogExecutor.isShutdown()).isTrue();
    assertThat(peerQueryExecutor.isShutdown()).isTrue();
  }

  @Test
  void registerAPIClosesAPreviouslyRegisteredSnapshotHandlerBeforeReplacingIt() throws Exception {
    // Defensive close in registerAPI() itself: even without an intervening stopService(), a second
    // registerAPI() call must not leak the first handler's executor (issue #5890).
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.registerAPI(null, new PathHandler());
    final SnapshotHttpHandler firstHandler = getField(plugin, "snapshotHttpHandler", SnapshotHttpHandler.class);
    final ScheduledExecutorService firstWatchdogExecutor = getField(firstHandler, "watchdogExecutor", ScheduledExecutorService.class);

    plugin.registerAPI(null, new PathHandler());

    assertThat(firstWatchdogExecutor.isShutdown())
        .as("A second registerAPI() call must close the previous SnapshotHttpHandler's watchdog executor")
        .isTrue();
    assertThat(getField(plugin, "snapshotHttpHandler", SnapshotHttpHandler.class)).isNotSameAs(firstHandler);
  }

  @Test
  void registerAPIClosesAPreviouslyRegisteredVerifyHandlerBeforeReplacingIt() throws Exception {
    final RaftHAPlugin plugin = new RaftHAPlugin();
    plugin.registerAPI(null, new PathHandler());
    final PostVerifyDatabaseHandler firstHandler = getField(plugin, "postVerifyDatabaseHandler", PostVerifyDatabaseHandler.class);
    final ExecutorService firstPeerQueryExecutor = getField(firstHandler, "peerQueryExecutor", ExecutorService.class);

    plugin.registerAPI(null, new PathHandler());

    assertThat(firstPeerQueryExecutor.isShutdown())
        .as("A second registerAPI() call must close the previous PostVerifyDatabaseHandler's peer-query pool")
        .isTrue();
    assertThat(getField(plugin, "postVerifyDatabaseHandler", PostVerifyDatabaseHandler.class)).isNotSameAs(firstHandler);
  }

  @SuppressWarnings("unchecked")
  private static <T> T getField(final Object target, final String fieldName, final Class<T> type) throws Exception {
    final Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return (T) field.get(target);
  }
}
