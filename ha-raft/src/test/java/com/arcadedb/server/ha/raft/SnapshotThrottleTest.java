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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SnapshotThrottleTest {

  /**
   * Issue #7233 moved the permits off a {@code static final} sized at class-initialisation - i.e. before any
   * server configuration exists - onto the handler instance, sized from that server's own configuration.
   */
  private static Semaphore semaphoreOf(final SnapshotHttpHandler handler) throws Exception {
    final Field f = SnapshotHttpHandler.class.getDeclaredField("concurrencySemaphore");
    f.setAccessible(true);
    return (Semaphore) f.get(handler);
  }

  @Test
  void semaphoreHasConfiguredPermits() throws Exception {
    GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.reset();
    // Closed at the end: the handler owns a stall-watchdog scheduler, and a test that builds one and walks away
    // leaks its thread for the rest of the JVM.
    final SnapshotHttpHandler handler = new SnapshotHttpHandler(null);
    try {
      assertThat(semaphoreOf(handler).availablePermits())
          .isEqualTo(GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getValueAsInteger());
    } finally {
      handler.close();
    }
  }

  /**
   * Issue #7233: the permits used to be sized from the process-wide {@link GlobalConfiguration} enum, which only a
   * system property or an environment variable ever writes. A cluster that set the limit in its server
   * configuration file - or through {@code SET SERVER SETTING} - ran on the compiled-in default instead.
   */
  @Test
  void permitsComeFromTheServerConfiguration() throws Exception {
    final SnapshotHttpHandler handler = handlerFor(configurationWith(5));
    try {
      assertThat(semaphoreOf(handler).availablePermits()).isEqualTo(5);
    } finally {
      handler.close();
    }
  }

  /**
   * {@code new Semaphore(n)} takes a negative {@code n} without complaint - every acquire simply fails - so an
   * unusable limit would answer every snapshot with 503 and leave a lagging follower unable to resync, silently.
   * Floored to the default instead, the way the Redis and BOLT protocol limits already are.
   */
  @Test
  void anUnusableLimitFallsBackToTheDefaultInsteadOfBlockingEverySnapshot() throws Exception {
    for (final int unusable : new int[] { 0, -1 }) {
      final SnapshotHttpHandler handler = handlerFor(configurationWith(unusable));
      try {
        assertThat(semaphoreOf(handler).availablePermits()).as("limit %d", unusable)
            .isEqualTo(((Number) GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getDefValue()).intValue());
        assertThat(semaphoreOf(handler).tryAcquire()).as("a snapshot is still servable at limit %d", unusable)
            .isTrue();
      } finally {
        handler.close();
      }
    }
  }

  private static ContextConfiguration configurationWith(final int maxConcurrent) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT, maxConcurrent);
    return configuration;
  }

  private static SnapshotHttpHandler handlerFor(final ContextConfiguration configuration) {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(configuration);
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    return new SnapshotHttpHandler(httpServer);
  }

  @Test
  void tryAcquireExhaustsAtMaxConcurrent() throws Exception {
    final SnapshotHttpHandler handler = new SnapshotHttpHandler(null);
    try {
      final Semaphore sem = semaphoreOf(handler);

      final int configured = GlobalConfiguration.HA_SNAPSHOT_MAX_CONCURRENT.getValueAsInteger();
      for (int i = 0; i < configured; i++)
        assertThat(sem.tryAcquire()).as("acquire %d", i).isTrue();
      assertThat(sem.tryAcquire()).as("over-limit acquire").isFalse();
      sem.release(configured);
    } finally {
      handler.close();
    }
  }
}
