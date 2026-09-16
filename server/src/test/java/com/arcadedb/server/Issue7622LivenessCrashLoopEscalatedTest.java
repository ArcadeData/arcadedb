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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #7622: before this fix, once a node's HA layer escalated a crash loop and gave
 * up restarting, {@code ServerControlPlane.isLive()} - the Kubernetes liveness probe - stayed unconditionally
 * {@code true}. Readiness already failed (the division's own lifecycle folds into
 * {@code RaftHAServer.isReadyForTraffic()}), so the node sat NotReady forever with liveness green: removed
 * from the Service, but never restarted, because the only probe that triggers a restart was the one still
 * passing. The SEVERE alert {@code HealthMonitor} raises at give-up said "a pod/process restart is the way
 * out", but nothing performed it.
 * <p>
 * {@code isLive()} now consults {@link HAServerPlugin#isCrashLoopEscalated()} so that restart happens
 * automatically.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7622LivenessCrashLoopEscalatedTest {

  @Test
  void isLiveWhenHaIsNotEnabled() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getHA()).thenReturn(null);

    assertThat(new ServerControlPlane(server).isLive()).isTrue();
  }

  @Test
  void isLiveWhenHaHasNotEscalatedACrashLoop() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isCrashLoopEscalated()).thenReturn(false);
    when(server.getHA()).thenReturn(ha);

    assertThat(new ServerControlPlane(server).isLive()).isTrue();
  }

  @Test
  void isNotLiveOnceHaHasEscalatedACrashLoop() {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isCrashLoopEscalated()).thenReturn(true);
    when(server.getHA()).thenReturn(ha);

    assertThat(new ServerControlPlane(server).isLive()).isFalse();
  }
}
