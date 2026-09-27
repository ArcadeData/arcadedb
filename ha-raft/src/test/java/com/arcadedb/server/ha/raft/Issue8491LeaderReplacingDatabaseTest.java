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
package com.arcadedb.server.ha.raft;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8491: a node replacing one of its databases with the leader's copy can be elected leader, and then rejects
 * every write to that database with a healthy majority. The health tick must drive the leadership hand-off, and the
 * cluster status document must report the condition, since every other signal on it reads healthy.
 */
class Issue8491LeaderReplacingDatabaseTest {

  private static final class CountingTarget implements HealthMonitor.HealthTarget {
    final AtomicReference<LifeCycle.State> state    = new AtomicReference<>(LifeCycle.State.RUNNING);
    final AtomicInteger                    handOffs = new AtomicInteger();
    volatile String                        logFailure;

    @Override
    public LifeCycle.State getRaftLifeCycleState() {
      return state.get();
    }

    @Override
    public boolean isShutdownRequested() {
      return false;
    }

    @Override
    public void restartRatisIfNeeded() {
    }

    @Override
    public String getRaftLogFailure() {
      return logFailure;
    }

    @Override
    public void handOffLeadershipWhileReplacingDatabase() {
      handOffs.incrementAndGet();
    }
  }

  @Test
  void everyHealthyTickDrivesTheHandOff() {
    final CountingTarget target = new CountingTarget();
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();
    monitor.tick();

    assertThat(target.handOffs.get()).isEqualTo(2);
  }

  @Test
  void aClosedDivisionDoesNotTryToHandOff() {
    // A division that is not RUNNING has no leadership to hand over: the tick restarts it instead.
    final CountingTarget target = new CountingTarget();
    target.state.set(LifeCycle.State.CLOSED);
    final HealthMonitor monitor = new HealthMonitor(target, 0);

    monitor.tick();

    assertThat(target.handOffs.get()).isZero();
  }

  @Test
  void aLeaderReplacingADatabaseRaisesACriticalAlert() {
    final JSONArray alerts = new JSONArray();

    ClusterAlerts.addLeaderReplacingDatabaseAlert(true, List.of("chaos", "other"), null, alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("leader-replacing-database");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    final JSONObject details = alert.getJSONObject("details");
    assertThat(details.getInt("count")).isEqualTo(2);
    assertThat(details.getJSONArray("databases").toList()).containsExactly("chaos", "other");
  }

  @Test
  void theAlertNamesOnlyTheDatabasesTheCallerMaySee() {
    final JSONArray alerts = new JSONArray();

    ClusterAlerts.addLeaderReplacingDatabaseAlert(true, List.of("chaos", "secret"), Set.of("chaos"), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject details = alerts.getJSONObject(0).getJSONObject("details");
    // The count is the node-level fact and reaches every caller; the names are reduced.
    assertThat(details.getInt("count")).isEqualTo(2);
    assertThat(details.getJSONArray("databases").toList()).containsExactly("chaos");
  }

  @Test
  void aFollowerReplacingADatabaseRaisesNoLeaderAlert() {
    // An ordinary resync on a follower: the leader serves the writes, and the #8363 gate refuses this node's clients.
    final JSONArray alerts = new JSONArray();

    ClusterAlerts.addLeaderReplacingDatabaseAlert(false, List.of("chaos"), null, alerts);

    assertThat(alerts.length()).isZero();
  }

  @Test
  void aLeaderReplacingNothingRaisesNoAlert() {
    final JSONArray alerts = new JSONArray();

    ClusterAlerts.addLeaderReplacingDatabaseAlert(true, List.of(), null, alerts);

    assertThat(alerts.length()).isZero();
  }
}
