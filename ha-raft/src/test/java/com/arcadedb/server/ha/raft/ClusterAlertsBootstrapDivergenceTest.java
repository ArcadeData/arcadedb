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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the bootstrap-divergence cluster alert (issue #6124).
 * <p>
 * A node that kept its own copy of a database through the bootstrap "local is fresher, refuse to
 * overwrite" guard is diverged from the rest of the cluster for that database, and the only prior
 * evidence was one SEVERE log line emitted at bootstrap - possibly several restarts ago. The alert
 * makes the condition readable from {@code GET /api/v1/cluster} for as long as it lasts.
 */
class ClusterAlertsBootstrapDivergenceTest {

  @Test
  void noAlertWhenNothingIsDiverged() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addBootstrapDivergedAlert(List.of(), alerts);
    assertThat(alerts.isEmpty()).isTrue();
  }

  @Test
  void nullListRaisesNoAlert() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addBootstrapDivergedAlert(null, alerts);
    assertThat(alerts.isEmpty()).isTrue();
  }

  @Test
  void divergedDatabasesRaiseACriticalAlertNamingThemAndTheResyncEndpoint() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addBootstrapDivergedAlert(List.of("beta", "alpha"), alerts);

    assertThat(alerts.length()).isEqualTo(1);
    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("id")).isEqualTo("bootstrap-diverged-databases");
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    // The operator has to choose which copy the cluster keeps, so the alert must name both options and
    // the endpoint that performs the destructive one.
    assertThat(alert.getString("recommendation")).contains("POST /api/v1/cluster/resync/{database}");
    assertThat(alert.getString("message")).contains("2 database(s)");

    final JSONArray names = alert.getJSONObject("details").getJSONArray("databases");
    assertThat(names.length()).isEqualTo(2);
    // The list is reported in the order given: getBootstrapUnreconciledDatabases() already sorts it.
    assertThat(names.getString(0)).isEqualTo("beta");
    assertThat(names.getString(1)).isEqualTo("alpha");
  }
}
