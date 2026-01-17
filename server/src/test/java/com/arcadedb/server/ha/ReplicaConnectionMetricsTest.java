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
package com.arcadedb.server.ha;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class ReplicaConnectionMetricsTest {

  @Test
  void testStateTransitionRecording() {
    var metrics = new ReplicaConnectionMetrics();

    metrics.recordStateChange(
        Leader2ReplicaNetworkExecutor.STATUS.JOINING,
        Leader2ReplicaNetworkExecutor.STATUS.ONLINE
    );

    assertThat(metrics.getCurrentStatus())
        .isEqualTo(Leader2ReplicaNetworkExecutor.STATUS.ONLINE);
    assertThat(metrics.getRecentTransitions()).hasSize(1);
  }

  @Test
  void testFailureCategoryIncrement() {
    var metrics = new ReplicaConnectionMetrics();

    metrics.getTransientNetworkFailures().incrementAndGet();
    metrics.getLeadershipChanges().incrementAndGet();

    assertThat(metrics.getTransientNetworkFailures().get()).isEqualTo(1);
    assertThat(metrics.getLeadershipChanges().get()).isEqualTo(1);
    assertThat(metrics.getProtocolErrors().get()).isEqualTo(0);
  }

  @Test
  void testRecoveryMetrics() {
    var metrics = new ReplicaConnectionMetrics();

    metrics.recordSuccessfulRecovery(3, 2500);

    assertThat(metrics.getSuccessfulRecoveries().get()).isEqualTo(1);
    assertThat(metrics.getFastestRecoveryMs().get()).isEqualTo(2500);
    assertThat(metrics.getSlowestRecoveryMs().get()).isEqualTo(2500);
  }

  @Test
  void testRecentTransitionsLimit() {
    var metrics = new ReplicaConnectionMetrics();

    // Record 15 transitions
    for (int i = 0; i < 15; i++) {
      metrics.recordStateChange(
          Leader2ReplicaNetworkExecutor.STATUS.ONLINE,
          Leader2ReplicaNetworkExecutor.STATUS.RECONNECTING
      );
    }

    // Should keep only last 10
    assertThat(metrics.getRecentTransitions()).hasSize(10);
  }
}
