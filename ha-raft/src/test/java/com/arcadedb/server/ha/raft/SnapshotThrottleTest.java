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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;

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
